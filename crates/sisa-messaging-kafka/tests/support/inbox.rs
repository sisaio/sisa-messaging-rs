//! Test-local transactional inbox used to run the generic partitioned consumer over real Kafka.
//!
//! Handler effects are staged in the transaction and applied only by a successful commit, so
//! tests can observe exactly-once effects, commit-before-ack ordering, and aborted work.

#![allow(
    dead_code,
    reason = "the Kafka tests and the consume benchmark each use a subset"
)]

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use sisa_messaging::{ErrorClassifier, FailureKind, MessageId};
use sisa_messaging_inbox::{
    DeadReason, InboxClaimOutcome, InboxFailure, InboxFailureOutcome, InboxId, InboxReceipt,
    InboxRecord, InboxScope, InboxStore, InboxUnitOfWork,
};

type Key = (InboxScope, MessageId);

/// Durable receipt state held by the fake store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    /// Claimed at least once and not terminal; `attempts` counts recorded failures.
    Pending { attempts: u32 },

    /// A commit completed the receipt.
    Completed { attempts: u32 },

    /// A recorded failure made the receipt terminal.
    Dead { attempts: u32, reason: DeadReason },
}

#[derive(Default)]
struct State {
    receipts: HashMap<Key, Status>,

    /// Receipts claimed by a live transaction.
    locked: HashMap<Key, u64>,

    /// Committed completions per receipt; more than one would be a duplicate effect.
    completions: HashMap<Key, u32>,

    /// Committed handler effects in commit order.
    effects: Vec<String>,

    /// Recorded failure summaries in recording order.
    summaries: Vec<String>,

    failing_commits: u32,

    next_transaction: u64,

    begun: u64,

    committed: u64,

    rolled_back: u64,

    live: u64,
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Shared handle to the fake inbox; clones observe the same state.
#[derive(Clone)]
pub struct FakeInbox {
    state: Arc<Mutex<State>>,

    max_attempts: NonZeroU32,
}

impl FakeInbox {
    pub fn new(max_attempts: u32) -> Self {
        Self {
            state: Arc::default(),
            max_attempts: NonZeroU32::new(max_attempts).unwrap_or(NonZeroU32::MIN),
        }
    }

    /// Makes the next `count` commits fail transiently without applying their effects.
    pub fn fail_next_commits(&self, count: u32) {
        lock(&self.state).failing_commits = count;
    }

    /// Pre-seeds a committed completion, as if an earlier delivery had succeeded.
    pub fn mark_completed(&self, scope: &InboxScope, message_id: MessageId) {
        lock(&self.state).receipts.insert(
            (scope.clone(), message_id),
            Status::Completed { attempts: 0 },
        );
    }

    pub fn status(&self, scope: &InboxScope, message_id: MessageId) -> Option<Status> {
        lock(&self.state)
            .receipts
            .get(&(scope.clone(), message_id))
            .copied()
    }

    pub fn completions(&self, scope: &InboxScope, message_id: MessageId) -> u32 {
        lock(&self.state)
            .completions
            .get(&(scope.clone(), message_id))
            .copied()
            .unwrap_or(0)
    }

    pub fn completed_total(&self) -> u32 {
        lock(&self.state).completions.values().sum()
    }

    pub fn effects(&self) -> Vec<String> {
        lock(&self.state).effects.clone()
    }

    pub fn summaries(&self) -> Vec<String> {
        lock(&self.state).summaries.clone()
    }

    pub fn begun(&self) -> u64 {
        lock(&self.state).begun
    }

    pub fn committed(&self) -> u64 {
        lock(&self.state).committed
    }

    pub fn rolled_back(&self) -> u64 {
        lock(&self.state).rolled_back
    }

    /// Transactions neither committed, rolled back, nor dropped.
    pub fn live(&self) -> u64 {
        lock(&self.state).live
    }
}

/// Transaction whose staged effects and completion apply only on commit.
pub struct FakeTransaction {
    id: u64,

    state: Arc<Mutex<State>>,

    claimed: Vec<Key>,

    completed: Vec<Key>,

    effects: Vec<String>,

    finished: bool,
}

impl FakeTransaction {
    /// Stages a business effect applied by a successful commit.
    pub fn record_effect(&mut self, effect: impl Into<String>) {
        self.effects.push(effect.into());
    }

    fn release(&mut self, state: &mut State) {
        for key in self.claimed.drain(..) {
            if state.locked.get(&key) == Some(&self.id) {
                state.locked.remove(&key);
            }
        }

        if !self.finished {
            self.finished = true;
            state.live = state.live.saturating_sub(1);
        }
    }
}

impl Drop for FakeTransaction {
    fn drop(&mut self) {
        let state = Arc::clone(&self.state);
        let mut state = lock(&state);
        self.release(&mut state);
    }
}

/// Completion evidence for one claimed receipt.
#[derive(Debug)]
pub struct FakeReceipt {
    key: Key,

    attempts: u32,
}

impl InboxReceipt for FakeReceipt {
    fn id(&self) -> InboxId {
        InboxId::from_uuid(self.key.1.into_uuid())
    }

    fn recorded_failures(&self) -> u32 {
        self.attempts
    }
}

/// Safely rendered fake inbox failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FakeInboxError {
    kind: FailureKind,
}

impl fmt::Display for FakeInboxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("fake inbox operation failed")
    }
}

impl Error for FakeInboxError {}

impl ErrorClassifier for FakeInboxError {
    fn classify(&self) -> FailureKind {
        self.kind
    }
}

impl InboxUnitOfWork for FakeInbox {
    type Transaction = FakeTransaction;
    type Error = FakeInboxError;

    async fn begin(&self) -> Result<Self::Transaction, Self::Error> {
        let mut state = lock(&self.state);
        state.next_transaction = state.next_transaction.saturating_add(1);
        state.begun = state.begun.saturating_add(1);
        state.live = state.live.saturating_add(1);

        Ok(FakeTransaction {
            id: state.next_transaction,
            state: Arc::clone(&self.state),
            claimed: Vec::new(),
            completed: Vec::new(),
            effects: Vec::new(),
            finished: false,
        })
    }

    async fn commit(&self, mut transaction: Self::Transaction) -> Result<(), Self::Error> {
        let mut state = lock(&self.state);

        if state.failing_commits > 0 {
            state.failing_commits -= 1;
            transaction.release(&mut state);

            return Err(FakeInboxError {
                kind: FailureKind::Transient,
            });
        }

        for key in transaction.completed.drain(..) {
            let attempts = match state.receipts.get(&key) {
                Some(Status::Pending { attempts }) => *attempts,
                _ => 0,
            };

            state
                .receipts
                .insert(key.clone(), Status::Completed { attempts });

            *state.completions.entry(key).or_default() += 1;
        }

        state.effects.append(&mut transaction.effects);
        state.committed = state.committed.saturating_add(1);
        transaction.release(&mut state);

        Ok(())
    }

    async fn rollback(&self, mut transaction: Self::Transaction) -> Result<(), Self::Error> {
        let mut state = lock(&self.state);
        state.rolled_back = state.rolled_back.saturating_add(1);
        transaction.release(&mut state);

        Ok(())
    }
}

impl InboxStore<FakeTransaction> for FakeInbox {
    type Error = FakeInboxError;
    type Receipt = FakeReceipt;

    fn max_attempts(&self) -> NonZeroU32 {
        self.max_attempts
    }

    async fn claim(
        &self,
        transaction: &mut FakeTransaction,
        record: &InboxRecord,
    ) -> Result<InboxClaimOutcome<Self::Receipt>, Self::Error> {
        let key = (record.scope.clone(), record.message_id);
        let mut state = lock(&self.state);

        if state
            .locked
            .get(&key)
            .is_some_and(|owner| *owner != transaction.id)
        {
            return Ok(InboxClaimOutcome::InProgressDuplicate);
        }

        let status = *state
            .receipts
            .entry(key.clone())
            .or_insert(Status::Pending { attempts: 0 });

        match status {
            Status::Completed { .. } => Ok(InboxClaimOutcome::CompletedDuplicate),
            Status::Dead { reason, .. } => Ok(InboxClaimOutcome::DeadDuplicate { reason }),
            Status::Pending { attempts } => {
                state.locked.insert(key.clone(), transaction.id);
                transaction.claimed.push(key.clone());

                Ok(InboxClaimOutcome::Claimed(FakeReceipt { key, attempts }))
            }
        }
    }

    async fn complete(
        &self,
        transaction: &mut FakeTransaction,
        receipt: Self::Receipt,
    ) -> Result<(), Self::Error> {
        transaction.completed.push(receipt.key);

        Ok(())
    }

    async fn fail(
        &self,
        record: &InboxRecord,
        failure: InboxFailure,
    ) -> Result<InboxFailureOutcome, Self::Error> {
        let key = (record.scope.clone(), record.message_id);
        let mut state = lock(&self.state);
        state.summaries.push(failure.error.as_str().to_owned());

        let status = *state
            .receipts
            .entry(key.clone())
            .or_insert(Status::Pending { attempts: 0 });

        let (next, outcome) = match status {
            Status::Completed { .. } => (status, InboxFailureOutcome::CompletedDuplicate),
            Status::Dead { attempts, reason } => {
                (status, InboxFailureOutcome::Dead { attempts, reason })
            }
            Status::Pending { attempts } => {
                let attempts = attempts.saturating_add(1);

                let reason = if !failure.kind.is_retryable() {
                    Some(DeadReason::Permanent)
                } else if attempts >= self.max_attempts.get() {
                    Some(DeadReason::Exhausted)
                } else {
                    None
                };

                match reason {
                    Some(reason) => (
                        Status::Dead { attempts, reason },
                        InboxFailureOutcome::Dead { attempts, reason },
                    ),
                    None => (
                        Status::Pending { attempts },
                        InboxFailureOutcome::Retry { attempts },
                    ),
                }
            }
        };

        state.receipts.insert(key, next);

        Ok(outcome)
    }
}
