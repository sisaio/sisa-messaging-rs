//! How often and how soon an endpoint tries a failed delivery again.

use std::time::Duration;

/// How an endpoint retries a delivery whose handler failed: up to
/// [`attempts`](RetryPolicy::attempts) deliveries in all, waiting
/// [`first_delay`](RetryPolicy::first_delay) after the first failure and twice as long after
/// each next one, but never longer than [`max_delay`](RetryPolicy::max_delay). After the last
/// attempt the envelope goes to the endpoint's error destination.
///
/// ```
/// use std::time::Duration;
///
/// use sisa_eventing_runtime::RetryPolicy;
///
/// let mut policy = RetryPolicy::default();
/// policy.attempts = 3;
/// assert_eq!(policy.delay(1), Duration::from_secs(1));
/// assert_eq!(policy.delay(2), Duration::from_secs(2));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// How many deliveries a message gets, the first one included. 5 by default; 1, or 0,
    /// sends a failed message to the error destination at once.
    pub attempts: u32,
    /// How long to wait after the first failure. 1 second by default.
    pub first_delay: Duration,
    /// The longest wait between two attempts. 1 minute by default.
    pub max_delay: Duration,
}

impl RetryPolicy {
    /// How long to wait after attempt number `attempt` failed: `first_delay` doubled for each
    /// attempt after the first, up to `max_delay`.
    #[must_use]
    pub fn delay(&self, attempt: u32) -> Duration {
        let mut delay = self.first_delay;
        for _ in 1..attempt {
            if delay.is_zero() || delay >= self.max_delay {
                break;
            }
            delay = delay.saturating_mul(2);
        }
        delay.min(self.max_delay)
    }

    // Whether attempt number `attempt` was the last one.
    pub(crate) fn is_last(&self, attempt: u32) -> bool {
        attempt >= self.attempts
    }
}

// Five attempts, one second apart at first, at most a minute apart.
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 5,
            first_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_the_delay_after_each_attempt_up_to_the_maximum() {
        let policy = RetryPolicy::default();

        let delays: Vec<u64> = (1..=8)
            .map(|attempt| policy.delay(attempt).as_secs())
            .collect();

        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 60, 60]);
        assert_eq!(policy.delay(u32::MAX), Duration::from_secs(60));
    }

    #[test]
    fn reaches_the_maximum_from_a_tiny_first_delay() {
        let policy = RetryPolicy {
            first_delay: Duration::from_nanos(1),
            ..RetryPolicy::default()
        };

        assert_eq!(policy.delay(100), Duration::from_secs(60));
    }

    #[test]
    fn gives_up_after_the_last_attempt() {
        let policy = RetryPolicy::default();

        assert!(!policy.is_last(4));
        assert!(policy.is_last(5));
        assert!(policy.is_last(6));
    }
}
