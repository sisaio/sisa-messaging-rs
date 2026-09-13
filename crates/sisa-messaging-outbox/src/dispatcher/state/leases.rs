//! Lease readiness and fenced renewal application.

use std::collections::HashSet;
use std::time::Duration;

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
        lease_safe_until: Instant,
    ) -> RenewalLoss {
        let matches = confirmed.iter().copied().collect::<HashSet<_>>();
        let mut lost = Vec::new();

        for claim in requested {
            if matches.contains(claim) {
                if let Some(owned) = self.claims.get_mut(claim) {
                    owned.renewal_at = renewal_at;
                    owned.lease_safe_until = lease_safe_until;
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

    pub(crate) fn store_call_blockers(&self, now: Instant, timeout: Duration) -> Vec<Claim> {
        self.claims
            .iter()
            .filter_map(|(claim, owned)| {
                if !matches!(owned.phase, Phase::Publishing { .. }) {
                    return None;
                }
                let safe = has_store_headroom(now, timeout, owned.lease_safe_until);
                (!safe).then_some(*claim)
            })
            .collect()
    }
}

fn has_store_headroom(now: Instant, timeout: Duration, lease_safe_until: Instant) -> bool {
    now.checked_add(timeout)
        .and_then(|deadline| deadline.checked_add(timeout))
        .is_some_and(|deadline| deadline < lease_safe_until)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_headroom_is_strict_at_the_two_timeout_boundary() {
        let now = Instant::now();
        let timeout = Duration::from_millis(19);
        let boundary = now.checked_add(timeout.saturating_mul(2)).unwrap_or(now);
        let just_inside = boundary
            .checked_add(Duration::from_nanos(1))
            .unwrap_or(boundary);

        assert!(!has_store_headroom(now, timeout, boundary));
        assert!(has_store_headroom(now, timeout, just_inside));
    }
}
