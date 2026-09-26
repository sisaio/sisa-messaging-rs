//! Construction validation, source opening, and startup requirement checks.

use std::sync::Arc;
use std::time::Duration;

use sisa_messaging::{
    ErrorClassifier, FailureKind, IndividualSourceRequirement, IndividualSourceRequirements,
};
use sisa_messaging_consumer::{
    ConsumerConfigError, ConsumerErrorKind, ConsumerExit, ConsumerSettings, SettingsField,
    SettlementMode,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::support::*;

type Zeroing = fn(&mut ConsumerSettings);

fn rejected(harness: &Harness) -> Option<ConsumerConfigError> {
    harness.consumer().err()
}

#[test]
fn public_error_and_settings_inventory() {
    fn classifier<T: ErrorClassifier>() {}
    classifier::<ConsumerConfigError>();
    classifier::<sisa_messaging_consumer::ConsumerError>();

    for (field, name) in [
        (SettingsField::SourceTimeout, "source_timeout"),
        (SettingsField::DatabaseTimeout, "database_timeout"),
        (SettingsField::SettlementTimeout, "settlement_timeout"),
        (SettingsField::HeartbeatInterval, "heartbeat_interval"),
        (SettingsField::NakDelay, "nak_delay"),
        (SettingsField::DrainTimeout, "drain_timeout"),
    ] {
        assert_eq!(field.as_str(), name);
    }
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
async fn heartbeat_preflight_requires_capability_and_safe_ack_wait() {
    let mut missing = Harness::new(SettlementMode::Broker);
    missing.settings.heartbeat_interval = Some(Duration::from_secs(2));
    let error = expect_error(missing.run().await);

    assert_eq!(
        error.kind(),
        ConsumerErrorKind::Unsupported(IndividualSourceRequirement::AckWait)
    );

    assert!(!missing.probe.events().contains(&Event::Receive));

    let short = sisa_messaging::IndividualSourceDescriptor::new(
        Some(Duration::from_secs(3)),
        None,
        true,
        true,
        true,
    )
    .unwrap_or_else(|_| panic!("valid descriptor"));

    let mut harness = Harness::with_descriptor(SettlementMode::Broker, short);
    harness.settings.heartbeat_interval = Some(Duration::from_secs(2));
    let error = expect_error(harness.run().await);
    assert_eq!(error.kind(), ConsumerErrorKind::HeartbeatDeadlineTooShort);
    assert!(!harness.probe.events().contains(&Event::Receive));

    let boundary = sisa_messaging::IndividualSourceDescriptor::new(
        Some(Duration::from_secs(4)),
        None,
        true,
        true,
        true,
    )
    .unwrap_or_else(|_| panic!("valid descriptor"));

    let mut equal = Harness::with_descriptor(SettlementMode::Broker, boundary);
    equal.settings.heartbeat_interval = Some(Duration::from_secs(2));
    let error = expect_error(equal.run().await);
    assert_eq!(error.kind(), ConsumerErrorKind::HeartbeatDeadlineTooShort);
    assert!(!equal.probe.events().contains(&Event::Receive));
}

#[tokio::test(start_paused = true)]
async fn heartbeat_extends_a_running_individual_delivery() {
    let opened = sisa_messaging::IndividualSourceDescriptor::new(
        Some(Duration::from_secs(10)),
        None,
        true,
        true,
        true,
    )
    .unwrap_or_else(|_| panic!("valid descriptor"));

    let mut harness = Harness::with_descriptor(SettlementMode::Broker, opened);
    harness.settings.heartbeat_interval = Some(Duration::from_secs(2));
    let gate = Arc::new(Semaphore::new(0));

    harness
        .probe
        .script_handler(1, HandlerStep::Block(Arc::clone(&gate)));

    harness.deliver(1, "slow");
    harness.close();
    let task = harness.spawn(CancellationToken::new());
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::task::yield_now().await;

    assert!(
        harness
            .probe
            .events_for(1)
            .iter()
            .any(|event| matches!(event, Event::Heartbeat(1)))
    );

    gate.add_permits(1);
    let (result, live) = join(task).await;
    assert!(matches!(result, Ok(ConsumerExit::SourceClosed)));
    assert_eq!(live, 0);

    assert!(
        harness
            .probe
            .events_for(1)
            .iter()
            .any(|event| matches!(event, Event::Ack(1)))
    );
}

#[tokio::test(start_paused = true)]
async fn heartbeat_failure_does_not_cancel_a_healthy_handler() {
    for step in [Step::Error(FailureKind::Transient), Step::Hang] {
        let capture = Capture::default();
        let _installed = capture.install();

        let opened = sisa_messaging::IndividualSourceDescriptor::new(
            Some(Duration::from_secs(10)),
            None,
            true,
            true,
            true,
        )
        .unwrap_or_else(|_| panic!("valid descriptor"));

        let mut harness = Harness::with_descriptor(SettlementMode::Broker, opened);
        harness.settings.heartbeat_interval = Some(Duration::from_secs(2));
        let gate = Arc::new(Semaphore::new(0));

        harness
            .probe
            .script_handler(1, HandlerStep::Block(Arc::clone(&gate)));

        harness.probe.script_settle(1, step);
        harness.deliver(1, "healthy handler");
        harness.close();
        let running = harness.spawn(CancellationToken::new());

        harness
            .probe
            .wait_until(|events| {
                events
                    .iter()
                    .any(|event| matches!(event, Event::Heartbeat(1)))
            })
            .await;

        gate.add_permits(1);
        let (result, live) = join(running).await;
        assert!(matches!(result, Ok(ConsumerExit::SourceClosed)));
        assert_eq!(live, 0);

        assert!(
            harness
                .probe
                .events_for(1)
                .iter()
                .any(|event| matches!(event, Event::Ack(1)))
        );

        let lines = capture.lines();

        assert!(
            lines
                .iter()
                .any(|line| line.contains("delivery heartbeat unconfirmed"))
        );

        for line in lines {
            for sentinel in SENTINELS {
                assert!(!line.contains(sentinel), "heartbeat log leaked {sentinel}");
            }
        }
    }
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
        (
            SettlementMode::BrokerImmediateRequeue,
            IndividualSourceRequirements::new()
                .requiring_immediate_requeue()
                .requiring_terminal_discard(),
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

#[test]
fn immediate_requeue_requires_exactly_zero_delay() {
    let mut harness = Harness::new(SettlementMode::BrokerImmediateRequeue);
    assert!(harness.consumer().is_ok());
    harness.settings.nak_delay = Duration::from_nanos(1);

    assert_eq!(
        rejected(&harness),
        Some(ConsumerConfigError::ImmediateRequeueRequiresZeroDelay)
    );
}

#[tokio::test(start_paused = true)]
async fn immediate_requeue_requires_both_capabilities_before_receive() {
    for (opened, missing) in [
        (
            descriptor(None, false, true),
            IndividualSourceRequirement::ImmediateRequeue,
        ),
        (
            descriptor(None, false, false).with_immediate_requeue(),
            IndividualSourceRequirement::TerminalDiscard,
        ),
    ] {
        for validate in [true, false] {
            let mut harness =
                Harness::with_descriptor(SettlementMode::BrokerImmediateRequeue, opened);

            harness.validate = validate;
            harness.deliver(1, "must not receive");
            let error = expect_error(harness.run().await);
            assert_eq!(error.kind(), ConsumerErrorKind::Unsupported(missing));
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
