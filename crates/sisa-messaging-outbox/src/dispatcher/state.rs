//! In-memory ownership, capacity, and outcome state.

mod leases;
mod outcomes;

use std::collections::{HashMap, HashSet, VecDeque};

use sisa_messaging::SerializedEnvelope;
use tokio::task::{AbortHandle, Id};
use tokio::time::Instant;

use crate::{Claim, ClaimedRecord};

pub(crate) use outcomes::ResolvedOutcome;

#[derive(Debug)]
enum Phase {
    Pending,
    Publishing { task_id: Id, abort: AbortHandle },
    Retiring { task_id: Id },
    Resolved(ResolvedOutcome),
    Release,
}

#[derive(Debug)]
struct OwnedClaim {
    attempts: u32,

    renewal_at: Instant,

    lease_safe_until: Instant,

    phase: Phase,
}

pub(crate) struct PublishWork {
    pub(crate) claim: Claim,

    pub(crate) envelope: SerializedEnvelope,

    pub(crate) attempts: u32,
}

pub(crate) struct ClaimInsert {
    pub(crate) inserted: usize,

    pub(crate) retained_rejected: usize,

    pub(crate) dropped_to_expiry: usize,

    pub(crate) duplicates: usize,
}

pub(crate) struct State {
    capacity: usize,

    claims: HashMap<Claim, OwnedClaim>,

    envelopes: HashMap<Claim, SerializedEnvelope>,

    pending_order: VecDeque<Claim>,

    tasks: HashMap<Id, Claim>,

    rejected: VecDeque<Claim>,

    rejected_set: HashSet<Claim>,
}

impl State {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            claims: HashMap::with_capacity(capacity),
            envelopes: HashMap::with_capacity(capacity),
            pending_order: VecDeque::with_capacity(capacity),
            tasks: HashMap::with_capacity(capacity),
            rejected: VecDeque::with_capacity(capacity),
            rejected_set: HashSet::with_capacity(capacity),
        }
    }

    pub(crate) fn available(&self) -> usize {
        self.capacity.saturating_sub(self.claims.len())
    }

    pub(crate) fn insert_claimed(
        &mut self,
        records: Vec<ClaimedRecord>,
        renewal_at: Instant,
        lease_safe_until: Instant,
    ) -> ClaimInsert {
        let mut inserted = 0_usize;
        let mut retained_rejected = 0_usize;
        let mut dropped_to_expiry = 0_usize;
        let mut duplicates = 0_usize;

        for record in records {
            if self.claims.contains_key(&record.claim) || self.rejected_set.contains(&record.claim)
            {
                duplicates = duplicates.saturating_add(1);
                continue;
            }
            if self.available() == 0 {
                if self.rejected.len() < self.capacity {
                    self.rejected_set.insert(record.claim);
                    self.rejected.push_back(record.claim);
                    retained_rejected = retained_rejected.saturating_add(1);
                } else {
                    dropped_to_expiry = dropped_to_expiry.saturating_add(1);
                }
                continue;
            }

            let claim = record.claim;
            self.envelopes.insert(claim, record.envelope);
            self.claims.insert(
                claim,
                OwnedClaim {
                    attempts: record.attempts,
                    renewal_at,
                    lease_safe_until,
                    phase: Phase::Pending,
                },
            );
            self.pending_order.push_back(claim);
            inserted = inserted.saturating_add(1);
        }

        ClaimInsert {
            inserted,
            retained_rejected,
            dropped_to_expiry,
            duplicates,
        }
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

    pub(crate) fn finish_retiring_cancelled(&mut self, task_id: Id) -> Option<Claim> {
        let claim = self.tasks.get(&task_id).copied()?;
        let owned = self.claims.get_mut(&claim)?;
        if !matches!(owned.phase, Phase::Retiring { task_id: retiring } if retiring == task_id) {
            return None;
        }
        self.tasks.remove(&task_id);
        owned.phase = Phase::Release;

        Some(claim)
    }

    pub(crate) fn publisher_task_failed(&mut self, task_id: Id) -> Option<Claim> {
        let claim = self.tasks.remove(&task_id)?;
        let owned = self.claims.get_mut(&claim)?;
        owned.phase = Phase::Release;

        Some(claim)
    }

    pub(crate) fn mark_release(&mut self, claims: &[Claim]) -> usize {
        let mut retired_publishers = 0;
        for claim in claims {
            if let Some(owned) = self.claims.get_mut(claim) {
                match &owned.phase {
                    Phase::Publishing { task_id, abort } => {
                        abort.abort();
                        self.tasks.remove(task_id);
                        retired_publishers += 1;
                    }
                    Phase::Retiring { task_id } => {
                        self.tasks.remove(task_id);
                        retired_publishers += 1;
                    }
                    Phase::Pending | Phase::Resolved(_) | Phase::Release => {}
                }
                owned.phase = Phase::Release;
            }
        }
        retired_publishers
    }

    pub(crate) fn retire_publishers(&mut self, claims: &[Claim]) -> usize {
        let mut retiring = 0;
        for claim in claims {
            let Some(owned) = self.claims.get_mut(claim) else {
                continue;
            };
            let task_id = match &owned.phase {
                Phase::Publishing { task_id, abort } => {
                    abort.abort();
                    *task_id
                }
                _ => continue,
            };
            owned.phase = Phase::Retiring { task_id };
            retiring += 1;
        }
        retiring
    }

    pub(crate) fn remove(&mut self, claims: &[Claim]) -> usize {
        let mut retired_publishers = 0;
        for claim in claims {
            self.envelopes.remove(claim);
            if let Some(owned) = self.claims.remove(claim) {
                match owned.phase {
                    Phase::Publishing { task_id, abort } => {
                        abort.abort();
                        self.tasks.remove(&task_id);
                        retired_publishers += 1;
                    }
                    Phase::Retiring { task_id } => {
                        self.tasks.remove(&task_id);
                        retired_publishers += 1;
                    }
                    Phase::Pending | Phase::Resolved(_) | Phase::Release => {}
                }
            }
        }
        retired_publishers
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
                matches!(
                    owned.phase,
                    Phase::Publishing { .. } | Phase::Retiring { .. }
                )
                .then_some(*claim)
            })
            .collect()
    }

    pub(crate) fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }

    pub(crate) fn has_persistence(&self) -> bool {
        self.claims
            .values()
            .any(|owned| matches!(owned.phase, Phase::Resolved(_) | Phase::Release))
    }

    pub(crate) fn has_rejected(&self) -> bool {
        !self.rejected.is_empty()
    }

    pub(crate) fn rejected_batch(&self) -> Vec<Claim> {
        self.rejected.iter().take(self.capacity).copied().collect()
    }

    pub(crate) fn retire_rejected(&mut self, count: usize) {
        for _ in 0..count {
            let Some(claim) = self.rejected.pop_front() else {
                break;
            };
            self.rejected_set.remove(&claim);
        }
    }
}
