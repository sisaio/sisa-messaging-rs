//! Construction validation, source opening, and startup requirement checks.

use std::time::Duration;

use sisa_messaging::{FailureKind, IndividualSourceRequirement, IndividualSourceRequirements};
use sisa_messaging_consumer::{
    ConsumerConfigError, ConsumerErrorKind, ConsumerExit, ConsumerSettings, SettingsField,
    SettlementMode,
};
use tokio_util::sync::CancellationToken;

use super::support::*;

type Zeroing = fn(&mut ConsumerSettings);

fn rejected(harness: &Harness) -> Option<ConsumerConfigError> {
    harness.consumer().err()
}

#[test]
fn defaults_select_broker_mode_with_non_zero_bounds() {
    let settings = ConsumerSettings::default();

    assert_eq!(settings.mode, SettlementMode::Broker);
    assert_eq!(SettlementMode::default(), SettlementMode::Broker);
    assert!(!settings.nak_delay.is_zero());
    assert!(!settings.drain_timeout.is_zero());
}

#[test]
fn construction_rejects_each_zero_duration() {
    let cases: [(SettingsField, Zeroing); 5] = [
        (SettingsField::SourceTimeout, |settings| {
            settings.source_timeout = Duration::ZERO;
        }),
        (SettingsField::DatabaseTimeout, |settings| {
            settings.database_timeout = Duration::ZERO;
        }),
        (SettingsField::SettlementTimeout, |settings| {
            settings.settlement_timeout = Duration::ZERO;
        }),
        (SettingsField::DrainTimeout, |settings| {
            settings.drain_timeout = Duration::ZERO;
        }),
        (SettingsField::NakDelay, |settings| {
            settings.nak_delay = Duration::ZERO;
        }),
    ];

    for (field, zero) in cases {
        let mut harness = Harness::new(SettlementMode::Broker);
        zero(&mut harness.settings);

        let error = rejected(&harness);

        assert_eq!(error, Some(ConsumerConfigError::ZeroDuration(field)));

        assert_eq!(
            error.map(|error| error.to_string()),
            Some(format!(
                "consumer setting {} must be non-zero",
                field.as_str()
            ))
        );
    }
}

#[test]
fn pending_recovery_accepts_a_zero_nak_delay() {
    let mut harness = Harness::new(SettlementMode::PendingRecovery);
    harness.settings.nak_delay = Duration::ZERO;

    assert!(harness.consumer().is_ok());

    harness.settings.drain_timeout = Duration::ZERO;

    assert_eq!(
        rejected(&harness),
        Some(ConsumerConfigError::ZeroDuration(
            SettingsField::DrainTimeout
        ))
    );
}

#[tokio::test(start_paused = true)]
async fn open_requests_exactly_the_mode_requirements() {
    let expected = [
        (
            SettlementMode::Broker,
            IndividualSourceRequirements::new()
                .requiring_delayed_retry()
                .requiring_terminal_discard(),
        ),
        (
            SettlementMode::PendingRecovery,
            IndividualSourceRequirements::new(),
        ),
    ];

    for (mode, requirements) in expected {
        let harness = Harness::new(mode);
        harness.close();

        assert!(matches!(
            harness.run().await,
            Ok(ConsumerExit::SourceClosed)
        ));

        assert_eq!(harness.probe.events()[0], Event::Open(requirements));
    }
}

#[tokio::test(start_paused = true)]
async fn broker_mode_rejects_missing_capabilities_before_receiving() {
    let cases = [
        (
            descriptor(None, false, true),
            IndividualSourceRequirement::DelayedRetry,
        ),
        (
            descriptor(None, true, false),
            IndividualSourceRequirement::TerminalDiscard,
        ),
    ];

    for (source, missing) in cases {
        for validate in [true, false] {
            let mut harness = Harness::with_descriptor(SettlementMode::Broker, source);
            // A source that skips validation is still rejected by the consumer's own check.
            harness.validate = validate;
            harness.deliver(1, "never received");

            let error = expect_error(harness.run().await);

            assert_eq!(error.kind(), ConsumerErrorKind::Unsupported(missing));
            assert_eq!(error.failure_kind(), FailureKind::Permanent);
            assert_eq!(harness.probe.count(|event| *event == Event::Receive), 0);
            assert_eq!(harness.queued(), 1);
            assert_redacted(&error);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn pending_recovery_opens_a_source_without_retry_or_discard() {
    let harness = Harness::with_descriptor(
        SettlementMode::PendingRecovery,
        descriptor(None, false, false),
    );

    harness.deliver(1, "ok");
    harness.close();

    assert!(matches!(
        harness.run().await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert_eq!(harness.probe.count(|event| *event == Event::Ack(1)), 1);
}

#[tokio::test(start_paused = true)]
async fn attempt_bound_above_max_deliver_is_rejected_before_receiving() {
    for mode in [SettlementMode::Broker, SettlementMode::PendingRecovery] {
        let broker = mode == SettlementMode::Broker;
        let harness = Harness::with_descriptor(mode, descriptor(Some(2), broker, broker));
        harness.deliver(1, "never received");

        let error = expect_error(harness.run().await);

        assert_eq!(
            error.kind(),
            ConsumerErrorKind::AttemptBoundExceedsMaxDeliver
        );

        assert_eq!(harness.probe.count(|event| *event == Event::Receive), 0);
        assert_eq!(harness.queued(), 1);

        let equal = Harness::with_descriptor(mode, descriptor(Some(3), broker, broker));
        equal.close();

        assert!(matches!(equal.run().await, Ok(ConsumerExit::SourceClosed)));
    }
}

#[tokio::test(start_paused = true)]
async fn open_failure_retains_the_typed_source_without_rendering_it() {
    let mut harness = Harness::new(SettlementMode::Broker);
    harness.open = Step::Error(FailureKind::Transient);

    let error = expect_error(harness.run().await);

    assert_eq!(error.kind(), ConsumerErrorKind::SourceOpen);
    assert_eq!(error.failure_kind(), FailureKind::Transient);
    assert_eq!(error.to_string(), "consumer source failed to open");

    assert_eq!(
        error
            .provider_source()
            .and_then(|source| source.downcast_ref::<FakeError>())
            .map(FakeError::kind),
        Some(FailureKind::Transient)
    );

    assert_redacted(&error);
}

#[tokio::test(start_paused = true)]
async fn open_is_bounded_by_the_source_timeout() {
    let mut harness = Harness::new(SettlementMode::Broker);
    harness.open = Step::Hang;

    let error = expect_error(harness.run().await);

    assert_eq!(error.kind(), ConsumerErrorKind::SourceOpenTimeout);
    assert_eq!(error.failure_kind(), FailureKind::Transient);
    assert!(error.provider_source().is_none());
}

#[tokio::test(start_paused = true)]
async fn cancellation_before_open_exits_without_opening() {
    let harness = Harness::new(SettlementMode::Broker);
    let cancel = CancellationToken::new();
    cancel.cancel();

    let (result, _) = join(harness.spawn(cancel)).await;

    assert!(matches!(result, Ok(ConsumerExit::Cancelled)));
    assert!(harness.probe.events().is_empty());
}

#[tokio::test(start_paused = true)]
async fn clean_close_and_source_failure_are_distinct() {
    let closed = Harness::new(SettlementMode::Broker);
    closed.close();

    assert!(matches!(closed.run().await, Ok(ConsumerExit::SourceClosed)));

    let failed = Harness::new(SettlementMode::Broker);
    failed.fail_source(FailureKind::Transient);

    let error = expect_error(failed.run().await);

    assert_eq!(error.kind(), ConsumerErrorKind::Source);
    assert_eq!(error.failure_kind(), FailureKind::Transient);

    assert!(
        error
            .provider_source()
            .is_some_and(|source| source.is::<FakeError>())
    );

    assert_redacted(&error);
}
