//! Lease readiness and fenced renewal application.

use std::collections::HashSet;

use tokio::time::Instant;

use crate::Claim;

use super::{Phase, State};

pub(crate) struct RenewalLoss {
    pub(crate) total: usize,
    pub(crate) retired_publishers: usize,
}

impl State {
    pub(crate) fn due_renewals(&self, now: Instant) -> Vec<Claim> {
        self.claims
            .iter()
            .filter_map(|(claim, owned)| {
                (matches!(owned.phase, Phase::Publishing { .. }) && owned.renewal_at <= now)
                    .then_some(*claim)
            })
            .collect()
    }

    pub(crate) fn next_renewal(&self) -> Option<Instant> {
        self.claims
            .values()
            .filter(|owned| matches!(owned.phase, Phase::Publishing { .. }))
            .map(|owned| owned.renewal_at)
            .min()
    }

    pub(crate) fn apply_renewal(
        &mut self,
        requested: &[Claim],
        confirmed: &[Claim],
        renewal_at: Instant,
    ) -> RenewalLoss {
        let matches = confirmed.iter().copied().collect::<HashSet<_>>();
        let mut lost = Vec::new();

        for claim in requested {
            if matches.contains(claim) {
                if let Some(owned) = self.claims.get_mut(claim) {
                    owned.renewal_at = renewal_at;
                }
            } else {
                lost.push(*claim);
            }
        }

        let total = lost.len();
        let retired_publishers = self.remove(&lost);
        RenewalLoss {
            total,
            retired_publishers,
        }
    }
}
