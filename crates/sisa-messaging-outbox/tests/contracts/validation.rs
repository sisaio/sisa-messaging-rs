use std::num::NonZeroU32;
use std::time::Duration;

use sisa_messaging_outbox::{
    DispatcherSettings, ExponentialBackoff, OutboxDispatcher, RetryPolicy, RetryPolicyError,
    SettingsError,
};

use super::support::{CompileCapabilities, CompilePublisher};

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
}
