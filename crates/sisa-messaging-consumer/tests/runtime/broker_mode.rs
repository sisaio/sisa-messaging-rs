//! Broker settlement: acknowledge, delayed negative acknowledgement, and terminal discard.

use std::num::NonZeroUsize;

use sisa_messaging::FailureKind;
use sisa_messaging_consumer::{ConsumerErrorKind, ConsumerExit, SettlementMode};
use sisa_messaging_inbox::DeadReason;

use super::support::*;

const MODE: SettlementMode = SettlementMode::Broker;

fn trace(harness: &Harness, tag: u8) -> Vec<Event> {
    harness
        .probe
        .events_for(tag)
        .into_iter()
        .filter(|event| !matches!(event, Event::TxDropped(_)))
        .collect()
}

async fn closes_cleanly(harness: &Harness) {
    harness.close();

    assert!(matches!(
        harness.run().await,
        Ok(ConsumerExit::SourceClosed)
    ));
}

#[tokio::test(start_paused = true)]
async fn success_claims_handles_completes_commits_then_acks() {
    let harness = Harness::new(MODE);
    harness.deliver(1, "ok");

    closes_cleanly(&harness).await;

    assert_eq!(
        trace(&harness, 1),
        [
            Event::Claim(1),
            Event::Handle(1),
            Event::Complete(1),
            Event::Commit(1),
            Event::Ack(1),
        ]
    );

    assert_eq!(harness.probe.committed_writes().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn completed_duplicate_rolls_back_and_acks_without_the_handler() {
    let harness = Harness::new(MODE);
    harness.probe.mark_completed(1);
    harness.deliver(1, "duplicate");

    closes_cleanly(&harness).await;

    assert_eq!(
        trace(&harness, 1),
        [Event::Claim(1), Event::Rollback(Some(1)), Event::Ack(1)]
    );
}

#[tokio::test(start_paused = true)]
async fn retryable_failure_rolls_back_records_then_naks() {
    let harness = Harness::new(MODE);

    harness
        .probe
        .script_handler(1, HandlerStep::Fail(FailureKind::Transient));

    harness.deliver(1, "retry");

    closes_cleanly(&harness).await;

    assert_eq!(
        trace(&harness, 1),
        [
            Event::Claim(1),
            Event::Handle(1),
            Event::Rollback(Some(1)),
            Event::Fail(1, FailureKind::Transient),
            Event::Nak(1, NAK_DELAY),
        ]
    );

    assert!(harness.probe.committed_writes().is_empty());
    // The handler's safe error text is the persisted summary.
    assert!(harness.probe.summaries()[0].contains(HANDLER_SENTINEL));
}

#[tokio::test(start_paused = true)]
async fn permanent_failure_becomes_dead_and_terminates() {
    let harness = Harness::new(MODE);

    harness
        .probe
        .script_handler(1, HandlerStep::Fail(FailureKind::Permanent));

    harness.deliver(1, "poison");
    harness.deliver(2, "ok");

    closes_cleanly(&harness).await;

    assert_eq!(
        trace(&harness, 1)[3..],
        [Event::Fail(1, FailureKind::Permanent), Event::Terminate(1)]
    );

    assert_eq!(harness.probe.count(|event| *event == Event::Ack(2)), 1);
}

#[tokio::test(start_paused = true)]
async fn in_progress_duplicate_naks_with_the_configured_delay() {
    let harness = Harness::new(MODE);
    harness.probe.script_claim(1, ClaimStep::InProgress);
    harness.deliver(1, "busy");

    closes_cleanly(&harness).await;

    assert_eq!(
        trace(&harness, 1),
        [
            Event::Claim(1),
            Event::Rollback(Some(1)),
            Event::Nak(1, NAK_DELAY)
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn dead_duplicate_terminates_without_the_handler() {
    let harness = Harness::new(MODE);
    harness.probe.mark_dead(1, DeadReason::Exhausted);
    harness.deliver(1, "dead");

    closes_cleanly(&harness).await;

    assert_eq!(
        trace(&harness, 1),
        [
            Event::Claim(1),
            Event::Rollback(Some(1)),
            Event::Terminate(1)
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn malformed_wire_terminates_without_database_work() {
    let harness = Harness::new(MODE);
    harness.deliver_malformed(1);

    closes_cleanly(&harness).await;

    assert_eq!(trace(&harness, 1), [Event::Terminate(1)]);
    assert_eq!(harness.probe.count(|event| *event == Event::Begin), 0);
}

#[tokio::test(start_paused = true)]
async fn undecodable_or_mismatched_body_records_a_fixed_permanent_failure() {
    let harness = Harness::new(MODE);
    harness.deliver(1, "undecodable");
    harness.deliver_as(2, "test.other", "wrong type");

    closes_cleanly(&harness).await;

    for tag in [1, 2] {
        assert_eq!(
            trace(&harness, tag),
            [
                Event::Fail(tag, FailureKind::Permanent),
                Event::Terminate(tag)
            ]
        );
    }

    assert_eq!(harness.probe.count(|event| *event == Event::Begin), 0);

    assert_eq!(
        harness.probe.summaries(),
        [
            "message body could not be decoded",
            "message body could not be decoded"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn commit_error_or_timeout_is_ambiguous_never_fails_or_acks() {
    for step in [Step::Error(FailureKind::Transient), Step::Hang] {
        let harness = Harness::new(MODE);
        harness.probe.script_commit(1, step);
        harness.deliver(1, "ambiguous");

        closes_cleanly(&harness).await;

        assert_eq!(
            trace(&harness, 1),
            [
                Event::Claim(1),
                Event::Handle(1),
                Event::Complete(1),
                Event::Commit(1),
                Event::Nak(1, NAK_DELAY),
            ]
        );
    }
}

#[tokio::test(start_paused = true)]
async fn permanent_commit_error_leaves_the_delivery_and_stops() {
    let harness = Harness::new(MODE);

    harness
        .probe
        .script_commit(1, Step::Error(FailureKind::Permanent));

    harness.deliver(1, "broken database");

    let error = expect_error(harness.run().await);

    assert_eq!(error.kind(), ConsumerErrorKind::Inbox);
    assert_eq!(error.failure_kind(), FailureKind::Permanent);

    assert!(
        error
            .provider_source()
            .is_some_and(|source| source.is::<FakeError>())
    );

    assert_eq!(trace(&harness, 1).last(), Some(&Event::Left(1)));
    assert_redacted(&error);
}

#[tokio::test(start_paused = true)]
async fn transient_begin_claim_or_complete_failures_nak() {
    let begin = Harness::new(MODE);

    begin
        .probe
        .script_begin(Step::Error(FailureKind::Transient));

    begin.deliver(1, "begin");
    closes_cleanly(&begin).await;
    assert_eq!(trace(&begin, 1), [Event::Nak(1, NAK_DELAY)]);

    let claim = Harness::new(MODE);

    claim
        .probe
        .script_claim(1, ClaimStep::Db(Step::Error(FailureKind::Transient)));

    claim.deliver(1, "claim");
    closes_cleanly(&claim).await;

    assert_eq!(
        trace(&claim, 1),
        [
            Event::Claim(1),
            Event::Rollback(Some(1)),
            Event::Nak(1, NAK_DELAY)
        ]
    );

    let complete = Harness::new(MODE);
    complete.probe.script_complete(1, Step::Hang);
    complete.deliver(1, "complete");
    closes_cleanly(&complete).await;

    assert_eq!(
        trace(&complete, 1),
        [
            Event::Claim(1),
            Event::Handle(1),
            Event::Complete(1),
            Event::Rollback(Some(1)),
            Event::Nak(1, NAK_DELAY),
        ]
    );

    assert!(complete.probe.committed_writes().is_empty());
}

#[tokio::test(start_paused = true)]
async fn permanent_begin_failure_stops() {
    let harness = Harness::new(MODE);

    harness
        .probe
        .script_begin(Step::Error(FailureKind::Permanent));

    harness.deliver(1, "begin");

    let error = expect_error(harness.run().await);

    assert_eq!(error.kind(), ConsumerErrorKind::Inbox);
    assert_eq!(trace(&harness, 1), [Event::Left(1)]);
}

#[tokio::test(start_paused = true)]
async fn unrecorded_failure_naks_when_transient_and_stops_when_permanent() {
    let transient = Harness::new(MODE);

    transient
        .probe
        .script_handler(1, HandlerStep::Fail(FailureKind::Transient));

    transient.probe.script_fail(1, Step::Hang);
    transient.deliver(1, "fail timeout");
    closes_cleanly(&transient).await;

    assert_eq!(
        trace(&transient, 1)[2..],
        [
            Event::Rollback(Some(1)),
            Event::Fail(1, FailureKind::Transient),
            Event::Nak(1, NAK_DELAY),
        ]
    );

    let rollback = Harness::new(MODE);

    rollback
        .probe
        .script_handler(1, HandlerStep::Fail(FailureKind::Permanent));

    rollback
        .probe
        .script_rollback(Step::Error(FailureKind::Transient));

    rollback.deliver(1, "rollback failure");
    closes_cleanly(&rollback).await;

    assert_eq!(
        trace(&rollback, 1)[2..],
        [Event::Rollback(Some(1)), Event::Nak(1, NAK_DELAY)]
    );

    let permanent = Harness::new(MODE);

    permanent
        .probe
        .script_handler(1, HandlerStep::Fail(FailureKind::Transient));

    permanent
        .probe
        .script_fail(1, Step::Error(FailureKind::Permanent));

    permanent.deliver(1, "fail broken");
    let error = expect_error(permanent.run().await);
    assert_eq!(error.kind(), ConsumerErrorKind::FailureNotRecorded);
    assert_eq!(error.failure_kind(), FailureKind::Permanent);
    assert_eq!(trace(&permanent, 1).last(), Some(&Event::Left(1)));
}

#[tokio::test(start_paused = true)]
async fn ack_timeout_is_resolved_by_a_completed_redelivery() {
    let mut harness = Harness::new(MODE);
    harness.settings.max_in_flight = NonZeroUsize::MIN;
    harness.probe.script_settle(1, Step::Hang);
    harness.deliver(1, "ambiguous ack");
    harness.deliver(1, "redelivered");

    closes_cleanly(&harness).await;

    assert_eq!(
        trace(&harness, 1),
        [
            Event::Claim(1),
            Event::Handle(1),
            Event::Complete(1),
            Event::Commit(1),
            Event::Ack(1),
            Event::Claim(1),
            Event::Rollback(Some(1)),
            Event::Ack(1),
        ]
    );

    assert_eq!(harness.probe.count(|event| *event == Event::Handle(1)), 1);
}

#[tokio::test(start_paused = true)]
async fn transient_settlement_failures_continue() {
    let harness = Harness::new(MODE);

    harness
        .probe
        .script_settle(1, Step::Error(FailureKind::Transient));

    harness
        .probe
        .script_handler(2, HandlerStep::Fail(FailureKind::Transient));

    harness.probe.script_settle(2, Step::Hang);
    harness.deliver(1, "ack fails");
    harness.deliver(2, "nak hangs");

    closes_cleanly(&harness).await;

    assert_eq!(harness.probe.count(|event| *event == Event::Ack(1)), 1);

    assert_eq!(
        harness
            .probe
            .count(|event| *event == Event::Nak(2, NAK_DELAY)),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn permanent_or_unsupported_settlement_stops() {
    for (step, typed_source) in [
        (Step::Error(FailureKind::Permanent), true),
        (Step::Unsupported, false),
    ] {
        let harness = Harness::new(MODE);
        harness.probe.script_settle(1, step);
        harness.deliver(1, "settlement broken");

        let error = expect_error(harness.run().await);

        assert_eq!(error.kind(), ConsumerErrorKind::Settlement);
        assert_eq!(error.failure_kind(), FailureKind::Permanent);

        // The provider error itself is retained, not the settlement-contract wrapper; an
        // unsupported operation has no provider error.
        let downcast = error
            .provider_source()
            .and_then(|source| source.downcast_ref::<FakeError>())
            .map(FakeError::kind);

        assert_eq!(downcast, typed_source.then_some(FailureKind::Permanent));
        assert_eq!(error.provider_source().is_some(), typed_source);
        assert_redacted(&error);
    }
}

#[tokio::test(start_paused = true)]
async fn permanent_cleanup_rollback_failure_stops_without_settling() {
    // A completed duplicate would otherwise be acknowledged.
    let completed = Harness::new(MODE);
    completed.probe.mark_completed(1);

    completed
        .probe
        .script_rollback(Step::Error(FailureKind::Permanent));

    completed.deliver(1, "duplicate");

    let error = expect_error(completed.run().await);

    assert_eq!(error.kind(), ConsumerErrorKind::Inbox);
    assert_eq!(error.failure_kind(), FailureKind::Permanent);

    assert!(
        error
            .provider_source()
            .is_some_and(|source| source.is::<FakeError>())
    );

    assert_eq!(
        trace(&completed, 1),
        [Event::Claim(1), Event::Rollback(Some(1)), Event::Left(1)]
    );

    // A transient claim failure would otherwise be negatively acknowledged.
    let claim = Harness::new(MODE);

    claim
        .probe
        .script_claim(1, ClaimStep::Db(Step::Error(FailureKind::Transient)));

    claim
        .probe
        .script_rollback(Step::Error(FailureKind::Permanent));

    claim.deliver(1, "claim");

    let error = expect_error(claim.run().await);

    assert_eq!(error.kind(), ConsumerErrorKind::Inbox);
    assert_eq!(error.failure_kind(), FailureKind::Permanent);

    assert_eq!(
        trace(&claim, 1),
        [Event::Claim(1), Event::Rollback(Some(1)), Event::Left(1)]
    );
}

#[tokio::test(start_paused = true)]
async fn transient_cleanup_rollback_failure_keeps_the_resolution() {
    let harness = Harness::new(MODE);
    harness.probe.mark_completed(1);

    harness
        .probe
        .script_rollback(Step::Error(FailureKind::Transient));

    harness.deliver(1, "duplicate");

    closes_cleanly(&harness).await;

    assert_eq!(
        trace(&harness, 1),
        [Event::Claim(1), Event::Rollback(Some(1)), Event::Ack(1)]
    );
}
