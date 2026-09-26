//! Pending-recovery settlement: acknowledge completed work and otherwise leave it pending.

use sisa_messaging::FailureKind;
use sisa_messaging_consumer::{ConsumerErrorKind, ConsumerExit, OperatorReason, SettlementMode};
use sisa_messaging_inbox::DeadReason;
use std::num::NonZeroUsize;

use super::support::*;

const MODE: SettlementMode = SettlementMode::PendingRecovery;

fn harness() -> Harness {
    Harness::new(MODE)
}

fn trace(harness: &Harness, tag: u8) -> Vec<Event> {
    harness
        .probe
        .events_for(tag)
        .into_iter()
        .filter(|event| !matches!(event, Event::TxDropped(_)))
        .collect()
}

fn assert_never_naks_or_terminates(harness: &Harness) {
    assert_eq!(
        harness
            .probe
            .count(|event| matches!(event, Event::Nak(..) | Event::Terminate(_))),
        0
    );
}

async fn closes_cleanly(harness: &Harness) {
    harness.close();

    assert!(matches!(
        harness.run().await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert_never_naks_or_terminates(harness);
}

#[tokio::test(start_paused = true)]
async fn success_and_duplicate_completion_ack() {
    let harness = harness();
    harness.probe.mark_completed(2);
    harness.deliver(1, "ok");
    harness.deliver(2, "duplicate");

    closes_cleanly(&harness).await;

    assert_eq!(trace(&harness, 1).last(), Some(&Event::Ack(1)));

    assert_eq!(
        trace(&harness, 2),
        [Event::Claim(2), Event::Rollback(Some(2)), Event::Ack(2)]
    );
}

#[tokio::test(start_paused = true)]
async fn retryable_failure_is_recorded_then_left_pending() {
    let harness = harness();

    harness
        .probe
        .script_handler(1, HandlerStep::Fail(FailureKind::Transient));

    harness.deliver(1, "retry");
    harness.deliver(2, "ok");

    closes_cleanly(&harness).await;

    assert_eq!(
        trace(&harness, 1),
        [
            Event::Claim(1),
            Event::Handle(1),
            Event::Rollback(Some(1)),
            Event::Fail(1, FailureKind::Transient),
            Event::Left(1),
        ]
    );

    assert_eq!(trace(&harness, 2).last(), Some(&Event::Ack(2)));
}

#[tokio::test(start_paused = true)]
async fn in_progress_and_ambiguous_commit_are_left_pending() {
    let harness = harness();
    harness.probe.script_claim(1, ClaimStep::InProgress);
    harness.probe.script_commit(2, Step::Hang);

    harness
        .probe
        .script_commit(3, Step::Error(FailureKind::Transient));

    harness.deliver(1, "busy");
    harness.deliver(2, "commit timeout");
    harness.deliver(3, "commit error");

    closes_cleanly(&harness).await;

    assert_eq!(
        trace(&harness, 1),
        [Event::Claim(1), Event::Rollback(Some(1)), Event::Left(1)]
    );

    for tag in [2, 3] {
        assert_eq!(
            trace(&harness, tag),
            [
                Event::Claim(tag),
                Event::Handle(tag),
                Event::Complete(tag),
                Event::Commit(tag),
                Event::Left(tag),
            ]
        );
    }
}

#[tokio::test(start_paused = true)]
async fn durable_dead_result_stops_for_operator_action() {
    let harness = harness();

    harness
        .probe
        .script_handler(1, HandlerStep::Fail(FailureKind::Permanent));

    harness.deliver(1, "poison");

    let error = expect_error(harness.run().await);

    assert_eq!(
        error.kind(),
        ConsumerErrorKind::OperatorActionRequired(OperatorReason::Dead(DeadReason::Permanent))
    );

    assert_eq!(error.failure_kind(), FailureKind::Permanent);

    assert_eq!(
        trace(&harness, 1)[2..],
        [
            Event::Rollback(Some(1)),
            Event::Fail(1, FailureKind::Permanent),
            Event::Left(1),
        ]
    );

    assert_never_naks_or_terminates(&harness);
    assert_redacted(&error);
}

#[tokio::test(start_paused = true)]
async fn dead_duplicate_is_never_acknowledged() {
    let harness = harness();
    harness.probe.mark_dead(1, DeadReason::Exhausted);
    harness.deliver(1, "dead");

    let error = expect_error(harness.run().await);

    assert_eq!(
        error.kind(),
        ConsumerErrorKind::OperatorActionRequired(OperatorReason::Dead(DeadReason::Exhausted))
    );

    assert_eq!(
        trace(&harness, 1),
        [Event::Claim(1), Event::Rollback(Some(1)), Event::Left(1)]
    );

    assert!(!harness.probe.events().iter().any(is_settlement));
}

#[tokio::test(start_paused = true)]
async fn malformed_wire_stops_for_operator_action() {
    let harness = harness();
    harness.deliver_malformed(1);

    let error = expect_error(harness.run().await);

    assert_eq!(
        error.kind(),
        ConsumerErrorKind::OperatorActionRequired(OperatorReason::Malformed)
    );

    assert!(error.provider_source().is_none());
    assert_eq!(trace(&harness, 1), [Event::Left(1)]);
    assert_redacted(&error);
}

#[tokio::test(start_paused = true)]
async fn unrecorded_failure_leaves_the_delivery_and_stops() {
    let harness = harness();

    harness
        .probe
        .script_handler(1, HandlerStep::Fail(FailureKind::Transient));

    harness.probe.script_fail(1, Step::Hang);
    harness.deliver(1, "not recorded");

    let error = expect_error(harness.run().await);

    assert_eq!(error.kind(), ConsumerErrorKind::FailureNotRecorded);
    assert_eq!(error.failure_kind(), FailureKind::Transient);
    assert_eq!(trace(&harness, 1).last(), Some(&Event::Left(1)));
    assert_never_naks_or_terminates(&harness);
}

#[tokio::test(start_paused = true)]
async fn transient_begin_failure_is_left_and_permanent_stops() {
    let transient = harness();

    transient
        .probe
        .script_begin(Step::Error(FailureKind::Transient));

    transient.deliver(1, "begin");
    closes_cleanly(&transient).await;
    assert_eq!(trace(&transient, 1), [Event::Left(1)]);

    let permanent = harness();

    permanent
        .probe
        .script_commit(1, Step::Error(FailureKind::Permanent));

    permanent.deliver(1, "commit");
    let error = expect_error(permanent.run().await);
    assert_eq!(error.kind(), ConsumerErrorKind::Inbox);
    assert_never_naks_or_terminates(&permanent);
}

#[tokio::test(start_paused = true)]
async fn ack_timeout_is_resolved_by_a_completed_redelivery() {
    let mut harness = harness();
    harness.settings.max_in_flight = NonZeroUsize::MIN;
    harness.probe.script_settle(1, Step::Hang);
    harness.deliver(1, "ambiguous ack");
    harness.deliver(1, "redelivered");

    closes_cleanly(&harness).await;

    assert_eq!(harness.probe.count(|event| *event == Event::Ack(1)), 2);
    assert_eq!(harness.probe.count(|event| *event == Event::Handle(1)), 1);
}

#[tokio::test(start_paused = true)]
async fn permanent_ack_failure_stops() {
    let harness = harness();

    harness
        .probe
        .script_settle(1, Step::Error(FailureKind::Permanent));

    harness.deliver(1, "ack broken");

    let error = expect_error(harness.run().await);

    assert_eq!(error.kind(), ConsumerErrorKind::Settlement);
}

#[tokio::test(start_paused = true)]
async fn undecodable_or_mismatched_body_stops_for_operator_action() {
    for wrong_type in [false, true] {
        let harness = harness();

        if wrong_type {
            harness.deliver_as(1, "test.other", "wrong type");
        } else {
            harness.deliver(1, "undecodable");
        }

        let error = expect_error(harness.run().await);

        assert_eq!(
            error.kind(),
            ConsumerErrorKind::OperatorActionRequired(OperatorReason::Dead(DeadReason::Permanent))
        );

        assert_eq!(
            trace(&harness, 1),
            [Event::Fail(1, FailureKind::Permanent), Event::Left(1)]
        );

        assert_eq!(harness.probe.count(|event| *event == Event::Begin), 0);
        assert_never_naks_or_terminates(&harness);
        assert_redacted(&error);
    }
}

#[tokio::test(start_paused = true)]
async fn source_failure_returns_a_source_error() {
    let harness = harness();
    harness.fail_source(FailureKind::Transient);

    let error = expect_error(harness.run().await);

    assert_eq!(error.kind(), ConsumerErrorKind::Source);
    assert_eq!(error.failure_kind(), FailureKind::Transient);

    assert!(
        error
            .provider_source()
            .is_some_and(|source| source.is::<FakeError>())
    );

    assert_redacted(&error);
}
