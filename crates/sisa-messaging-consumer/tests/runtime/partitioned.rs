//! Deterministic partition coordinator cases; fake advance uses the fake ack event.

use std::sync::Arc;
use std::time::Duration;

use sisa_messaging::FailureKind;
use sisa_messaging_consumer::{ConsumerErrorKind, ConsumerExit, SettlementMode};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::support::*;

#[tokio::test(start_paused = true)]
async fn committed_record_advances_before_clean_close() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.deliver(1, "first");
    harness.close();

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let result = consumer.run_partitioned(CancellationToken::new()).await;
    assert!(matches!(result, Ok(ConsumerExit::SourceClosed)));
    let events = harness.probe.events_for(1);

    let commit = events
        .iter()
        .position(|event| matches!(event, Event::Commit(1)));

    let advance = events
        .iter()
        .position(|event| matches!(event, Event::Ack(1)));

    assert!(commit.is_some() && advance.is_some() && commit < advance);
}

#[tokio::test(start_paused = true)]
async fn same_partition_overlap_stops_without_advancing_later_record() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.script_handler(1, HandlerStep::Hang);
    harness.deliver(1, "first");
    harness.deliver(3, "later same partition");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let result = consumer.run_partitioned(CancellationToken::new()).await;
    let error = expect_error(result);
    assert_eq!(error.kind(), ConsumerErrorKind::PartitionOrder);

    assert!(
        !harness
            .probe
            .events_for(3)
            .iter()
            .any(|event| matches!(event, Event::Ack(3)))
    );
}

#[tokio::test(start_paused = true)]
async fn ownership_loss_cancels_work_and_leaves_record_unadvanced() {
    let harness = Harness::new(SettlementMode::Broker);

    harness
        .probe
        .script_handler(1, HandlerStep::Block(Arc::new(Semaphore::new(0))));

    harness.deliver(1, "first");
    harness.lose_partition(1);
    harness.close();

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let result = consumer.run_partitioned(CancellationToken::new()).await;
    assert!(matches!(result, Ok(ConsumerExit::SourceClosed)));

    assert!(
        !harness
            .probe
            .events_for(1)
            .iter()
            .any(|event| matches!(event, Event::Ack(1)))
    );

    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn fenced_new_generation_can_process_after_stale_work_is_dropped() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.script_handler(1, HandlerStep::Hang);
    harness.deliver(1, "old generation");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Handle(1))))
        .await;

    harness.lose_partition(1);

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Left(1))))
        .await;

    tokio::task::yield_now().await;
    // The source models generation fencing by waiting for old work to quiesce first.
    harness.deliver(3, "new generation");
    harness.close();

    assert!(matches!(
        join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert!(
        !harness
            .probe
            .events_for(1)
            .iter()
            .any(|event| matches!(event, Event::Ack(1)))
    );

    assert!(
        harness
            .probe
            .events_for(3)
            .iter()
            .any(|event| matches!(event, Event::Ack(3)))
    );
}

#[tokio::test(start_paused = true)]
async fn conclusive_fenced_advance_releases_partition_for_reconciled_generation() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.fence_advance(1);
    harness.deliver(1, "old generation");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(1))))
        .await;

    tokio::task::yield_now().await;
    // The fake source emits the next generation only after conclusive fencing and its own
    // authoritative cursor reconciliation; no separate OwnershipLost event is required.
    harness.deliver(3, "reconciled generation");
    harness.close();

    assert!(matches!(
        join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert_eq!(
        harness
            .probe
            .count(|event| matches!(event, Event::Handle(1))),
        1
    );

    assert!(
        harness
            .probe
            .events_for(3)
            .iter()
            .any(|event| matches!(event, Event::Ack(3)))
    );
}

#[tokio::test(start_paused = true)]
async fn timed_out_advance_pauses_partition_but_allows_another_partition() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.script_settle(1, Step::Hang);
    harness.deliver(1, "first");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(1))))
        .await;

    tokio::time::advance(harness.settings.settlement_timeout + Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    harness.deliver(2, "independent partition");
    harness.close();

    assert!(matches!(
        join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert!(
        harness
            .probe
            .events_for(1)
            .iter()
            .any(|event| matches!(event, Event::Commit(1)))
    );

    assert!(
        harness
            .probe
            .events_for(2)
            .iter()
            .any(|event| matches!(event, Event::Ack(2)))
    );
}

#[tokio::test(start_paused = true)]
async fn returned_advance_errors_follow_their_failure_classification() {
    for failure in [
        sisa_messaging::FailureKind::Transient,
        sisa_messaging::FailureKind::Permanent,
    ] {
        let harness = Harness::new(SettlementMode::Broker);
        harness.probe.script_settle(1, Step::Error(failure));
        harness.deliver(1, "advance error");

        let consumer = harness
            .partitioned_consumer()
            .unwrap_or_else(|error| panic!("settings: {error}"));

        let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

        harness
            .probe
            .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(1))))
            .await;

        if failure == sisa_messaging::FailureKind::Transient {
            harness.deliver(2, "independent partition");

            harness
                .probe
                .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(2))))
                .await;

            harness.close();

            assert!(matches!(
                join(running).await,
                Ok(ConsumerExit::SourceClosed)
            ));
        } else {
            let error = expect_error(join(running).await);
            assert_eq!(error.kind(), ConsumerErrorKind::Settlement);
            assert_eq!(error.failure_kind(), sisa_messaging::FailureKind::Permanent);
            assert!(error.provider_source().is_some());
            assert_redacted(&error);
        }

        assert_eq!(harness.probe.live_tx(), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn replacement_waits_for_old_transaction_drop_after_ownership_loss() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.script_handler(1, HandlerStep::Hang);
    harness.deliver(1, "old generation");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Handle(1))))
        .await;

    harness.lose_partition(1);
    harness.deliver(3, "reconciled replacement");
    harness.deliver(2, "independent partition");

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(3))))
        .await;

    harness.close();

    assert!(matches!(
        join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    let events = harness.probe.events();

    let dropped = events
        .iter()
        .position(|event| matches!(event, Event::TxDropped(Some(1))));

    let replacement_claim = events
        .iter()
        .position(|event| matches!(event, Event::Claim(3)));

    assert!(dropped.is_some() && replacement_claim.is_some() && dropped < replacement_claim);
    assert!(events.iter().any(|event| matches!(event, Event::Ack(2))));
    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_second_replacement_on_the_same_partition_stops_without_skipping() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.script_handler(1, HandlerStep::Hang);
    harness.deliver(1, "old generation");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Handle(1))))
        .await;

    harness.lose_partition(1);
    harness.deliver(3, "first replacement");
    harness.deliver(5, "overlapping replacement");

    let error = expect_error(join(running).await);
    assert_eq!(error.kind(), ConsumerErrorKind::PartitionOrder);

    assert_eq!(
        harness.probe.count(|event| matches!(event, Event::Ack(5))),
        0
    );

    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn ownership_loss_after_uncertain_advance_releases_finished_token_for_replay() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.script_settle(1, Step::Hang);
    harness.deliver(1, "old generation");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(1))))
        .await;

    tokio::time::advance(harness.settings.settlement_timeout + Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // The fake source has fenced the old generation and reconciled an unchanged cursor.
    harness.lose_partition(1);
    harness.deliver(1, "replayed generation");
    harness.close();

    assert!(matches!(
        join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert_eq!(
        harness
            .probe
            .count(|event| matches!(event, Event::Handle(1))),
        1
    );

    assert_eq!(
        harness.probe.count(|event| matches!(event, Event::Ack(1))),
        2
    );
}

#[tokio::test(start_paused = true)]
async fn panicking_advance_wakes_pending_receive_and_returns_typed_error() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.script_settle(1, Step::Panic);
    harness.deliver(1, "advance panic");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let run = tokio::time::timeout(
        Duration::from_secs(60),
        consumer.run_partitioned(CancellationToken::new()),
    )
    .await;

    let Ok(result) = run else {
        panic!("pending receive hid a panicking advance");
    };

    let error = expect_error(result);
    assert_eq!(error.kind(), ConsumerErrorKind::Runtime);
    assert_redacted(&error);
    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn blocked_partition_does_not_block_another_partition() {
    let harness = Harness::new(SettlementMode::Broker);
    let release = Arc::new(Semaphore::new(0));

    harness
        .probe
        .script_handler(1, HandlerStep::Block(Arc::clone(&release)));

    harness.deliver(1, "blocked odd partition");
    harness.deliver(2, "free even partition");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(2))))
        .await;

    assert!(
        !harness
            .probe
            .events_for(1)
            .iter()
            .any(|event| matches!(event, Event::Ack(1)))
    );

    release.add_permits(1);

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(1))))
        .await;

    harness.close();

    assert!(matches!(
        join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_work_leaves_the_record_unadvanced() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.script_handler(1, HandlerStep::Hang);
    harness.deliver(1, "work to cancel");
    let cancel = CancellationToken::new();

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(cancel.clone()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Handle(1))))
        .await;

    cancel.cancel();
    assert!(matches!(join(running).await, Ok(ConsumerExit::Cancelled)));

    assert!(
        !harness
            .probe
            .events_for(1)
            .iter()
            .any(|event| matches!(event, Event::Ack(1)))
    );

    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_advance_waits_for_uncertain_partition_to_pause() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.script_settle(1, Step::Hang);
    harness.deliver(1, "advance to cancel");
    let cancel = CancellationToken::new();

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(cancel.clone()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(1))))
        .await;

    cancel.cancel();
    assert!(matches!(join(running).await, Ok(ConsumerExit::Cancelled)));
    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn ownership_loss_during_advance_defers_reconciled_delivery_until_old_guard_exits() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.script_settle(1, Step::Hang);
    harness.deliver(1, "advance under old ownership");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(1))))
        .await;

    harness.lose_partition(1);
    harness.deliver(3, "reconciled next generation");
    harness.deliver(2, "independent partition");

    tokio::time::advance(harness.settings.settlement_timeout + Duration::from_secs(1)).await;

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(3))))
        .await;

    harness.close();

    assert!(matches!(
        join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert!(
        harness
            .probe
            .events_for(2)
            .iter()
            .any(|event| matches!(event, Event::Ack(2)))
    );

    assert!(
        harness
            .probe
            .events_for(3)
            .iter()
            .any(|event| matches!(event, Event::Ack(3)))
    );
}

#[tokio::test(start_paused = true)]
async fn provider_replay_after_unchanged_cursor_reuses_completed_receipt() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.script_settle(1, Step::Hang);
    harness.deliver(1, "first attempt");
    harness.close();

    let first = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    assert!(matches!(
        first.run_partitioned(CancellationToken::new()).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    // The fake provider models a fenced advance and an unchanged authoritative cursor by
    // replaying the same record on a fresh source after the first run has stopped.
    harness.deliver(1, "provider replay");
    harness.close();

    let replay = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    assert!(matches!(
        replay.run_partitioned(CancellationToken::new()).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert_eq!(
        harness
            .probe
            .count(|event| matches!(event, Event::Handle(1))),
        1
    );

    assert_eq!(
        harness.probe.count(|event| matches!(event, Event::Ack(1))),
        2
    );
}

#[tokio::test(start_paused = true)]
async fn provider_continuation_after_reconciled_advance_accepts_next_offset() {
    let harness = Harness::new(SettlementMode::Broker);
    harness.probe.script_settle(1, Step::Hang);
    harness.deliver(1, "first offset");
    harness.close();

    let first = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    assert!(matches!(
        first.run_partitioned(CancellationToken::new()).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    // The fake provider models an authoritative advanced cursor and a new generation by
    // emitting only the next offset after the indeterminate run is quiescent.
    harness.deliver(3, "next offset");
    harness.close();

    let resumed = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    assert!(matches!(
        resumed.run_partitioned(CancellationToken::new()).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert!(
        harness
            .probe
            .events_for(3)
            .iter()
            .any(|event| matches!(event, Event::Ack(3)))
    );
}

/// Yields until the intake has received every queued source step.
async fn drain_source_queue(harness: &Harness) {
    for _ in 0..64 {
        if harness.queued() == 0 {
            break;
        }

        tokio::task::yield_now().await;
    }

    assert_eq!(harness.queued(), 0);

    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn advanced_release_race_defers_next_offset_until_guard_drops() {
    let harness = Harness::new(SettlementMode::Broker);
    let gate = Arc::new(Semaphore::new(0));
    harness.probe.gate_advance(1, Arc::clone(&gate));
    harness.deliver(1, "confirmed offset");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(1))))
        .await;

    // The source resumed after confirming offset 1 and emits the next offset on the same
    // partition while the coordinator has not yet released its entry.
    harness.deliver(3, "next offset before release");
    drain_source_queue(&harness).await;

    assert!(!running.is_finished());

    assert!(
        !harness
            .probe
            .events_for(3)
            .iter()
            .any(|event| matches!(event, Event::Handle(3)))
    );

    gate.add_permits(1);

    // A regressed coordinator stops the run instead, so bound the wait on the paused clock.
    let advanced = tokio::time::timeout(
        Duration::from_secs(3_600),
        harness
            .probe
            .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(3)))),
    )
    .await;

    assert!(advanced.is_ok(), "deferred next offset did not advance");

    harness.close();

    assert!(matches!(
        join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert_eq!(
        harness
            .probe
            .count(|event| matches!(event, Event::Handle(3))),
        1
    );

    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn deferred_next_offset_after_unresolved_advance_stops() {
    let harness = Harness::new(SettlementMode::Broker);
    let gate = Arc::new(Semaphore::new(0));
    harness.probe.gate_advance(1, Arc::clone(&gate));

    harness
        .probe
        .script_settle(1, Step::Error(FailureKind::Transient));

    harness.deliver(1, "indeterminate offset");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(1))))
        .await;

    harness.deliver(3, "next offset behind an unresolved advance");
    drain_source_queue(&harness).await;

    assert!(!running.is_finished());
    gate.add_permits(1);

    let error = expect_error(join(running).await);
    assert_eq!(error.kind(), ConsumerErrorKind::PartitionOrder);

    assert!(
        !harness
            .probe
            .events_for(3)
            .iter()
            .any(|event| matches!(event, Event::Handle(3) | Event::Ack(3)))
    );

    assert_eq!(harness.probe.live_tx(), 0);
}

/// Runs until the consumer stops, failing instead of hanging if it never does.
async fn bounded_join(
    running: tokio::task::JoinHandle<Result<ConsumerExit, sisa_messaging_consumer::ConsumerError>>,
) -> Result<ConsumerExit, sisa_messaging_consumer::ConsumerError> {
    tokio::time::timeout(Duration::from_secs(3_600), join(running))
        .await
        .unwrap_or_else(|_| panic!("the consumer stalled instead of stopping"))
}

async fn next_offset_after_unresolved_advance(timed_out: bool) {
    let harness = Harness::new(SettlementMode::Broker);

    harness.probe.script_settle(
        1,
        if timed_out {
            Step::Hang
        } else {
            Step::Error(FailureKind::Transient)
        },
    );

    harness.deliver(1, "unresolved offset");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(1))))
        .await;

    if timed_out {
        tokio::time::advance(harness.settings.settlement_timeout + Duration::from_secs(1)).await;
    }

    // Let the coordinator observe the unresolved outcome and exit before the next offset.
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    harness.deliver(3, "next offset behind an unresolved advance");

    let error = expect_error(bounded_join(running).await);
    assert_eq!(error.kind(), ConsumerErrorKind::PartitionOrder);

    assert!(
        !harness
            .probe
            .events_for(3)
            .iter()
            .any(|event| matches!(event, Event::Handle(3) | Event::Ack(3)))
    );
}

#[tokio::test(start_paused = true)]
async fn next_offset_after_timed_out_advance_stops_without_handling() {
    next_offset_after_unresolved_advance(true).await;
}

#[tokio::test(start_paused = true)]
async fn next_offset_after_transient_advance_error_stops_without_handling() {
    next_offset_after_unresolved_advance(false).await;
}

#[tokio::test(start_paused = true)]
async fn ownership_loss_after_deferred_next_offset_drops_it_unhandled() {
    let harness = Harness::new(SettlementMode::Broker);
    let gate = Arc::new(Semaphore::new(0));
    harness.probe.gate_advance(1, Arc::clone(&gate));
    harness.deliver(1, "advancing offset");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    harness
        .probe
        .wait_until(|events| events.iter().any(|event| matches!(event, Event::Ack(1))))
        .await;

    // The next offset is deferred behind the running advance, then ownership is lost.
    harness.deliver(3, "deferred then lost");
    drain_source_queue(&harness).await;
    harness.lose_partition(1);
    drain_source_queue(&harness).await;

    gate.add_permits(1);

    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    harness.close();

    assert!(matches!(
        bounded_join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert!(
        !harness
            .probe
            .events_for(3)
            .iter()
            .any(|event| matches!(event, Event::Handle(3) | Event::Ack(3)))
    );

    assert_eq!(harness.probe.live_tx(), 0);
}
