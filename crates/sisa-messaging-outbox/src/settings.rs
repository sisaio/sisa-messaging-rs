//! Dispatcher settings and one-time validation.

use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use crate::{ExponentialBackoff, RetryPolicy, SettingsError};

const MAX_WORKER_ID_BYTES: usize = 255;

/// Complete worker policy supplied by the application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatcherSettings<R = ExponentialBackoff> {
    /// Stable diagnostic worker identity stored with claims.
    pub worker_id: String,

    /// Maximum retained claims, including outcomes awaiting persistence.
    pub max_in_flight: NonZeroUsize,

    /// Lease applied by claim and renewal operations using database time.
    pub lease: Duration,

    /// Delay between productive claim passes and after transient claim failures.
    pub poll_interval: Duration,

    /// Delay after an empty claim pass.
    pub idle_poll_interval: Duration,

    /// Bound for one publisher acknowledgement attempt.
    pub publish_timeout: Duration,

    /// Bound for every individual store operation.
    pub store_timeout: Duration,

    /// Bound for graceful publication drain after cancellation.
    pub drain_timeout: Duration,

    /// Retry decision applied after classified publication failures.
    pub retry_policy: R,
}

impl Default for DispatcherSettings<ExponentialBackoff> {
    fn default() -> Self {
        Self {
            worker_id: "outbox-dispatcher".to_owned(),
            max_in_flight: NonZeroUsize::new(32).unwrap_or(NonZeroUsize::MIN),
            lease: Duration::from_secs(30),
            poll_interval: Duration::from_millis(25),
            idle_poll_interval: Duration::from_millis(250),
            publish_timeout: Duration::from_secs(10),
            store_timeout: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(20),
            retry_policy: ExponentialBackoff::default(),
        }
    }
}

impl<R: RetryPolicy> DispatcherSettings<R> {
    pub(crate) fn validate(&self) -> Result<(), SettingsError> {
        validate_worker_id(&self.worker_id)?;
        validate_duration("lease", self.lease)?;
        validate_duration("poll_interval", self.poll_interval)?;
        validate_duration("idle_poll_interval", self.idle_poll_interval)?;
        validate_duration("publish_timeout", self.publish_timeout)?;
        validate_duration("store_timeout", self.store_timeout)?;
        validate_duration("drain_timeout", self.drain_timeout)?;

        let half_lease = self.lease / 2;
        if self.store_timeout >= half_lease {
            return Err(SettingsError::StoreTimeoutNotBelowHalfLease);
        }
        if self.max_in_flight.get() > u32::MAX as usize {
            return Err(SettingsError::CapacityNotRepresentable);
        }

        for duration in [
            self.lease,
            self.poll_interval,
            self.idle_poll_interval,
            self.publish_timeout,
            self.store_timeout,
            self.drain_timeout,
        ] {
            if Instant::now().checked_add(duration).is_none() {
                return Err(SettingsError::DeadlineNotRepresentable);
            }
        }

        self.retry_policy
            .validate()
            .map_err(SettingsError::RetryPolicy)
    }

    pub(crate) fn renewal_offset(&self) -> Duration {
        self.lease / 2 - self.store_timeout
    }
}

fn validate_worker_id(worker_id: &str) -> Result<(), SettingsError> {
    if worker_id.is_empty() {
        return Err(SettingsError::InvalidWorkerId);
    }
    if worker_id.len() > MAX_WORKER_ID_BYTES
        || worker_id
            .as_bytes()
            .iter()
            .any(|byte| byte.is_ascii_control() || *byte == 0x7f)
    {
        return Err(SettingsError::InvalidWorkerId);
    }

    Ok(())
}

fn validate_duration(field: &'static str, duration: Duration) -> Result<(), SettingsError> {
    if duration.is_zero() {
        return Err(SettingsError::ZeroDuration { field });
    }

    Ok(())
}
