//! Bounded retry policy and overflow-safe exponential backoff.

use std::num::NonZeroU32;
use std::time::Duration;

/// Validates and calculates retry delays without performing I/O.
///
/// Implementations must deterministically return non-zero, representable delays and eventually
/// return `None` so retry is finitely exhausted. [`validate`](Self::validate) must check those
/// invariants without I/O; dispatcher construction calls it once before any runtime work begins.
/// Custom policy constructors remain responsible for establishing the same invariants when they
/// expose validated policy values independently of a dispatcher.
pub trait RetryPolicy: Send + Sync + 'static {
    /// Returns the delay after a recorded attempt, or `None` when retry is exhausted.
    fn retry_delay(&self, attempt: NonZeroU32) -> Option<Duration>;

    /// Validates finite exhaustion and every delay invariant during dispatcher construction.
    fn validate(&self) -> Result<(), RetryPolicyError>;
}

/// Exponential retry with a finite attempt budget and maximum delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExponentialBackoff {
    /// Delay after the first failed attempt.
    base_delay: Duration,

    /// Upper bound applied after overflow-safe exponential growth.
    max_delay: Duration,

    /// Maximum recorded attempts, including the final non-retried attempt.
    max_attempts: NonZeroU32,
}

impl ExponentialBackoff {
    /// Validates and constructs one finite exponential retry policy.
    pub fn new(
        base_delay: Duration,
        max_delay: Duration,
        max_attempts: NonZeroU32,
    ) -> Result<Self, RetryPolicyError> {
        let policy = Self {
            base_delay,
            max_delay,
            max_attempts,
        };
        policy.validate()?;

        Ok(policy)
    }
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(5 * 60),
            max_attempts: NonZeroU32::new(10).unwrap_or(NonZeroU32::MIN),
        }
    }
}

impl RetryPolicy for ExponentialBackoff {
    fn retry_delay(&self, attempt: NonZeroU32) -> Option<Duration> {
        if attempt >= self.max_attempts {
            return None;
        }

        let exponent = attempt.get().saturating_sub(1);
        let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);

        Some(
            self.base_delay
                .checked_mul(multiplier)
                .unwrap_or(Duration::MAX)
                .min(self.max_delay),
        )
    }

    fn validate(&self) -> Result<(), RetryPolicyError> {
        if self.base_delay.is_zero() {
            return Err(RetryPolicyError::ZeroBaseDelay);
        }
        if self.max_delay.is_zero() {
            return Err(RetryPolicyError::ZeroMaximumDelay);
        }
        if self.base_delay > self.max_delay {
            return Err(RetryPolicyError::BaseExceedsMaximum);
        }
        if std::time::Instant::now()
            .checked_add(self.max_delay)
            .is_none()
        {
            return Err(RetryPolicyError::DeadlineNotRepresentable);
        }

        Ok(())
    }
}

/// Invalid retry-policy configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum RetryPolicyError {
    /// The first retry would have no delay and could create a busy loop.
    #[error("retry base delay must be non-zero")]
    ZeroBaseDelay,

    /// The configured cap is zero.
    #[error("retry maximum delay must be non-zero")]
    ZeroMaximumDelay,

    /// Exponential delays cannot be capped below their starting value.
    #[error("retry base delay must not exceed maximum delay")]
    BaseExceedsMaximum,

    /// The maximum delay cannot be scheduled by the monotonic clock.
    #[error("retry maximum delay cannot be represented as a monotonic deadline")]
    DeadlineNotRepresentable,
}
