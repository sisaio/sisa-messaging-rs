use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use sisa_messaging_outbox::{
    DispatcherSettings, ExponentialBackoff, OutboxDispatcher, RetryPolicy, RetryPolicyError,
    SettingsError,
};

use super::support::{CompileCapabilities, CompilePublisher};

#[derive(Clone, Copy, Debug)]
struct RejectingPolicy;

impl RetryPolicy for RejectingPolicy {
    fn retry_delay(&self, _attempt: NonZeroU32) -> Option<Duration> {
        None
    }

    fn validate(&self) -> Result<(), RetryPolicyError> {
        Err(RetryPolicyError::ZeroBaseDelay)
    }
}

#[derive(Clone, Debug)]
struct CountingPolicy {
    validations: Arc<AtomicUsize>,
}

impl RetryPolicy for CountingPolicy {
    fn retry_delay(&self, _attempt: NonZeroU32) -> Option<Duration> {
        None
    }

    fn validate(&self) -> Result<(), RetryPolicyError> {
        self.validations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn settings_with<R>(retry_policy: R) -> DispatcherSettings<R> {
    let defaults = DispatcherSettings::default();
    DispatcherSettings {
        worker_id: defaults.worker_id,
        max_in_flight: defaults.max_in_flight,
        lease: defaults.lease,
        poll_interval: defaults.poll_interval,
        idle_poll_interval: defaults.idle_poll_interval,
        publish_timeout: defaults.publish_timeout,
        store_timeout: defaults.store_timeout,
        drain_timeout: defaults.drain_timeout,
        retry_policy,
    }
}

#[test]
fn exponential_retry_is_capped_exhaustible_and_overflow_safe() {
    let policy = ExponentialBackoff::new(
        Duration::from_secs(3),
        Duration::from_secs(20),
        NonZeroU32::new(40).unwrap_or(NonZeroU32::MIN),
    )
    .unwrap_or_else(|error| panic!("valid retry rejected: {error}"));

    assert_eq!(
        policy.retry_delay(NonZeroU32::new(1).unwrap_or(NonZeroU32::MIN)),
        Some(Duration::from_secs(3))
    );
    assert_eq!(
        policy.retry_delay(NonZeroU32::new(4).unwrap_or(NonZeroU32::MIN)),
        Some(Duration::from_secs(20))
    );
    assert_eq!(
        policy.retry_delay(NonZeroU32::new(39).unwrap_or(NonZeroU32::MIN)),
        Some(Duration::from_secs(20))
    );
    assert_eq!(
        policy.retry_delay(NonZeroU32::new(40).unwrap_or(NonZeroU32::MIN)),
        None
    );
}

#[test]
fn retry_and_dispatcher_settings_validate_once_at_construction() {
    let result = ExponentialBackoff::new(
        Duration::from_secs(2),
        Duration::from_secs(1),
        NonZeroU32::MIN,
    );
    assert!(matches!(result, Err(RetryPolicyError::BaseExceedsMaximum)));

    let settings = DispatcherSettings {
        store_timeout: Duration::from_secs(5),
        lease: Duration::from_secs(10),
        ..DispatcherSettings::default()
    };
    assert!(matches!(
        OutboxDispatcher::new(CompileCapabilities, CompilePublisher, settings),
        Err(SettingsError::StoreTimeoutNotBelowHalfLease)
    ));

    let settings = DispatcherSettings {
        worker_id: "bad\nworker".to_owned(),
        ..DispatcherSettings::default()
    };
    assert!(matches!(
        OutboxDispatcher::new(CompileCapabilities, CompilePublisher, settings),
        Err(SettingsError::InvalidWorkerId)
    ));

    assert!(matches!(
        OutboxDispatcher::new(
            CompileCapabilities,
            CompilePublisher,
            settings_with(RejectingPolicy),
        ),
        Err(SettingsError::RetryPolicy(RetryPolicyError::ZeroBaseDelay))
    ));
}

#[test]
fn dispatcher_validates_a_valid_custom_retry_policy_exactly_once() {
    let validations = Arc::new(AtomicUsize::new(0));
    let policy = CountingPolicy {
        validations: Arc::clone(&validations),
    };

    let dispatcher =
        OutboxDispatcher::new(CompileCapabilities, CompilePublisher, settings_with(policy));

    assert!(dispatcher.is_ok());
    assert_eq!(validations.load(Ordering::SeqCst), 1);
}
