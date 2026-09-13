//! In-memory ownership, capacity, and outcome state.

mod leases;
mod outcomes;

use std::collections::{HashMap, VecDeque};

use sisa_messaging::SerializedEnvelope;
use tokio::task::{AbortHandle, Id};
use tokio::time::Instant;

use crate::{Claim, ClaimedRecord};

pub(crate) use outcomes::ResolvedOutcome;

#[derive(Debug)]
enum Phase {
    Pending,
    Publishing { task_id: Id, abort: AbortHandle },
    Resolved(ResolvedOutcome),
    Release,
}

#[derive(Debug)]
struct OwnedClaim {
    attempts: u32,

    renewal_at: Instant,

    phase: Phase,
}

pub(crate) struct PublishWork {
    pub(crate) claim: Claim,

    pub(crate) envelope: SerializedEnvelope,

    pub(crate) attempts: u32,
}

pub(crate) struct State {
    capacity: usize,

    claims: HashMap<Claim, OwnedClaim>,

    envelopes: HashMap<Claim, SerializedEnvelope>,

    pending_order: VecDeque<Claim>,

    tasks: HashMap<Id, Claim>,
}

impl State {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            claims: HashMap::with_capacity(capacity),
            envelopes: HashMap::with_capacity(capacity),
            pending_order: VecDeque::with_capacity(capacity),
            tasks: HashMap::with_capacity(capacity),
        }
    }

    pub(crate) fn available(&self) -> usize {
        self.capacity.saturating_sub(self.claims.len())
    }

    pub(crate) fn insert_claimed(
        &mut self,
        records: Vec<ClaimedRecord>,
        renewal_at: Instant,
    ) -> Vec<Claim> {
        let mut overflow = Vec::new();

        for record in records {
            if self.available() == 0 || self.claims.contains_key(&record.claim) {
                overflow.push(record.claim);
                continue;
            }

            let claim = record.claim;
            self.envelopes.insert(claim, record.envelope);
            self.claims.insert(
                claim,
                OwnedClaim {
                    attempts: record.attempts,
                    renewal_at,
                    phase: Phase::Pending,
                },
            );
            self.pending_order.push_back(claim);
        }

        overflow
    }

    pub(crate) fn next_publish(&mut self) -> Option<PublishWork> {
        while let Some(claim) = self.pending_order.pop_front() {
            let Some(owned) = self.claims.get_mut(&claim) else {
                continue;
            };
            if !matches!(owned.phase, Phase::Pending) {
                continue;
            }
            owned.phase = Phase::Release;
            let Some(envelope) = self.envelopes.remove(&claim) else {
                continue;
            };

            return Some(PublishWork {
                claim,
                envelope,
                attempts: owned.attempts,
            });
        }

        None
    }

    pub(crate) fn publishing(&mut self, claim: Claim, task_id: Id, abort: AbortHandle) {
        if let Some(owned) = self.claims.get_mut(&claim) {
            owned.phase = Phase::Publishing { task_id, abort };
            self.tasks.insert(task_id, claim);
        }
    }

    pub(crate) fn task_claim(&self, task_id: Id) -> Option<Claim> {
        self.tasks.get(&task_id).copied()
    }

    pub(crate) fn resolve(&mut self, task_id: Id, outcome: ResolvedOutcome) -> Option<Claim> {
        let claim = self.tasks.remove(&task_id)?;
        let owned = self.claims.get_mut(&claim)?;
        owned.phase = Phase::Resolved(outcome);

        Some(claim)
    }

    pub(crate) fn publisher_task_failed(&mut self, task_id: Id) -> Option<Claim> {
        let claim = self.tasks.remove(&task_id)?;
        let owned = self.claims.get_mut(&claim)?;
        owned.phase = Phase::Release;

        Some(claim)
    }

    pub(crate) fn mark_release(&mut self, claims: &[Claim]) {
        for claim in claims {
            if let Some(owned) = self.claims.get_mut(claim) {
                if let Phase::Publishing { task_id, abort } = &owned.phase {
                    abort.abort();
                    self.tasks.remove(task_id);
                }
                owned.phase = Phase::Release;
            }
        }
    }

    pub(crate) fn remove(&mut self, claims: &[Claim]) {
        for claim in claims {
            self.envelopes.remove(claim);
            if let Some(owned) = self.claims.remove(claim)
                && let Phase::Publishing { task_id, abort } = owned.phase
            {
                abort.abort();
                self.tasks.remove(&task_id);
            }
        }
    }

    pub(crate) fn all_claims(&self) -> Vec<Claim> {
        self.claims.keys().copied().collect()
    }

    pub(crate) fn unstarted_claims(&self) -> Vec<Claim> {
        self.claims
            .iter()
            .filter_map(|(claim, owned)| matches!(owned.phase, Phase::Pending).then_some(*claim))
            .collect()
    }

    pub(crate) fn unresolved_claims(&self) -> Vec<Claim> {
        self.claims
            .iter()
            .filter_map(|(claim, owned)| {
                matches!(owned.phase, Phase::Publishing { .. }).then_some(*claim)
            })
            .collect()
    }

    pub(crate) fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }
}
