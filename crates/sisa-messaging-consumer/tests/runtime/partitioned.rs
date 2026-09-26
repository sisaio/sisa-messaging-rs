//! Deterministic partition coordinator cases; fake advance uses the fake ack event.

use std::sync::Arc;
use std::time::Duration;

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

/// Bound for a handoff scenario; exceeding it means the runtime stopped or stalled.
const HANDOFF_TIMEOUT: Duration = Duration::from_secs(10);

fn handoff_harness() -> Harness {
    let mut harness = Harness::new(SettlementMode::Broker);
    // Bounds the drain when a held advance never returns.
    harness.settings.drain_timeout = Duration::from_millis(200);

    harness
}

/// Waits for `condition`, or fails with the run's own result if it ended first.
async fn wait_or_report(
    harness: &Harness,
    running: &mut tokio::task::JoinHandle<
        Result<ConsumerExit, sisa_messaging_consumer::ConsumerError>,
    >,
    what: &str,
    condition: impl Fn(&[Event]) -> bool,
) {
    tokio::select! {
        () = harness.probe.wait_until(condition) => {}
        ended = &mut *running => panic!("{what}: the run ended first with {ended:?}"),
        () = tokio::time::sleep(HANDOFF_TIMEOUT) => panic!("{what}: timed out"),
    }
}

/// A source may report a partition's next record as soon as the previous advance reports
/// success, before that record's coordinator has released the partition.
async fn next_record_after_a_reported_advance_waits_for_the_releasing_coordinator() {
    let harness = handoff_harness();
    harness.gate_advance(1, GateOutcome::Advanced, &[Then::Deliver(3)], None);
    harness.deliver(1, "first");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let mut running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    wait_or_report(&harness, &mut running, "next record", |events| {
        events.iter().any(|event| matches!(event, Event::Ack(3)))
    })
    .await;

    harness.close();

    assert!(matches!(
        join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    let events = harness.probe.events();
    let position = |wanted: Event| events.iter().position(|event| *event == wanted);

    assert!(position(Event::Ack(1)) < position(Event::Handle(3)));
    assert!(position(Event::Handle(3)) < position(Event::Ack(3)));

    assert_eq!(
        harness
            .probe
            .count(|event| matches!(event, Event::Handle(1))),
        1
    );

    assert_eq!(
        harness
            .probe
            .count(|event| matches!(event, Event::Handle(3))),
        1
    );

    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn next_record_after_a_reported_advance_is_processed_in_order_current_thread() {
    next_record_after_a_reported_advance_waits_for_the_releasing_coordinator().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn next_record_after_a_reported_advance_is_processed_in_order_multi_thread() {
    next_record_after_a_reported_advance_waits_for_the_releasing_coordinator().await;
}

/// A replay-only source withdraws an indeterminate advance and replays the record at once.
async fn withdrawn_indeterminate_advance_replays_with_one_effect() {
    let harness = handoff_harness();

    harness.gate_advance(
        1,
        GateOutcome::Error(sisa_messaging::FailureKind::Transient),
        &[Then::Lose(1), Then::Deliver(1)],
        None,
    );

    harness.deliver(1, "first attempt");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let mut running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    wait_or_report(&harness, &mut running, "replayed advance", |events| {
        events
            .iter()
            .filter(|event| matches!(event, Event::Ack(1)))
            .count()
            == 2
    })
    .await;

    harness.close();

    assert!(matches!(
        join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    // The replay found the committed receipt: one handler effect, two advances.
    assert_eq!(
        harness
            .probe
            .count(|event| matches!(event, Event::Handle(1))),
        1
    );

    assert_eq!(harness.probe.committed_writes().len(), 1);
    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn withdrawn_indeterminate_advance_replays_with_one_effect_current_thread() {
    withdrawn_indeterminate_advance_replays_with_one_effect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn withdrawn_indeterminate_advance_replays_with_one_effect_multi_thread() {
    withdrawn_indeterminate_advance_replays_with_one_effect().await;
}

/// An ownership loss that arrives while a record is held behind a reported advance cancels the
/// held record too.
async fn ownership_loss_cancels_a_held_record() {
    let harness = handoff_harness();

    harness.gate_advance(
        1,
        GateOutcome::Advanced,
        &[Then::Deliver(3), Then::Lose(1)],
        None,
    );

    harness.deliver(1, "first");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let mut running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    wait_or_report(&harness, &mut running, "held record release", |events| {
        events.iter().any(|event| matches!(event, Event::Left(3)))
    })
    .await;

    harness.close();

    assert!(matches!(
        join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert!(
        harness
            .probe
            .events_for(3)
            .iter()
            .all(|event| matches!(event, Event::Left(3)))
    );

    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn ownership_loss_cancels_a_held_record_current_thread() {
    ownership_loss_cancels_a_held_record().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ownership_loss_cancels_a_held_record_multi_thread() {
    ownership_loss_cancels_a_held_record().await;
}

/// Shutdown while a record is held behind an unfinished advance neither handles nor advances
/// the held record.
async fn shutdown_while_a_record_is_held_exits_cleanly() {
    let harness = handoff_harness();
    let hold = Arc::new(Semaphore::new(0));

    harness.gate_advance(
        1,
        GateOutcome::Advanced,
        &[Then::Deliver(3)],
        Some(Arc::clone(&hold)),
    );

    harness.deliver(1, "first");
    let cancel = CancellationToken::new();

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let mut running = tokio::spawn(consumer.run_partitioned(cancel.clone()));

    wait_or_report(&harness, &mut running, "first advance", |events| {
        events.iter().any(|event| matches!(event, Event::Ack(1)))
    })
    .await;

    tokio::select! {
        () = harness.wait_dispatched() => {}
        ended = &mut running => panic!("held record: the run ended first with {ended:?}"),
    }

    cancel.cancel();
    assert!(matches!(join(running).await, Ok(ConsumerExit::Cancelled)));

    assert!(
        harness
            .probe
            .events_for(3)
            .iter()
            .all(|event| matches!(event, Event::Left(3)))
    );

    assert_eq!(harness.probe.live_tx(), 0);
    drop(hold);
}

#[tokio::test(start_paused = true)]
async fn shutdown_while_a_record_is_held_exits_cleanly_current_thread() {
    shutdown_while_a_record_is_held_exits_cleanly().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_while_a_record_is_held_exits_cleanly_multi_thread() {
    shutdown_while_a_record_is_held_exits_cleanly().await;
}

/// A record for a partition whose advance finished unconfirmed, with no withdrawal, overlaps the
/// unresolved record and stops the run at once instead of waiting on a release that never comes.
async fn a_record_after_an_unconfirmed_advance_fails_fast() {
    let mut harness = handoff_harness();
    harness.settings.settlement_timeout = Duration::from_millis(100);
    harness.probe.script_settle(1, Step::Hang);
    harness.deliver(1, "first");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let mut running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    wait_or_report(&harness, &mut running, "first advance", |events| {
        events.iter().any(|event| matches!(event, Event::Ack(1)))
    })
    .await;

    // Past the settlement timeout, the coordinator has finished with the advance unconfirmed.
    tokio::time::sleep(Duration::from_millis(400)).await;
    harness.deliver(3, "later record without withdrawal");

    let ended = tokio::time::timeout(HANDOFF_TIMEOUT, running)
        .await
        .unwrap_or_else(|_| panic!("an overlapping record stalled the run"));

    let error = expect_error(ended.unwrap_or_else(|error| panic!("run panicked: {error}")));
    assert_eq!(error.kind(), ConsumerErrorKind::PartitionOrder);

    assert_eq!(
        harness
            .probe
            .count(|event| matches!(event, Event::Handle(3))),
        0
    );

    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_record_after_an_unconfirmed_advance_fails_fast_current_thread() {
    a_record_after_an_unconfirmed_advance_fails_fast().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_after_an_unconfirmed_advance_fails_fast_multi_thread() {
    a_record_after_an_unconfirmed_advance_fails_fast().await;
}

/// A record held behind an advance that then finishes unconfirmed, with no withdrawal, is
/// rejected when released and never reaches the handler.
async fn a_record_held_behind_an_unconfirmed_advance_is_rejected() {
    let harness = handoff_harness();

    harness.gate_advance(
        1,
        GateOutcome::Error(sisa_messaging::FailureKind::Transient),
        &[Then::Deliver(3)],
        None,
    );

    harness.deliver(1, "first");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    let ended = tokio::time::timeout(HANDOFF_TIMEOUT, running)
        .await
        .unwrap_or_else(|_| panic!("a rejected held record stalled the run"));

    let error = expect_error(ended.unwrap_or_else(|error| panic!("run panicked: {error}")));
    assert_eq!(error.kind(), ConsumerErrorKind::PartitionOrder);

    assert_eq!(
        harness
            .probe
            .count(|event| matches!(event, Event::Handle(3))),
        0
    );

    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_record_held_behind_an_unconfirmed_advance_is_rejected_current_thread() {
    a_record_held_behind_an_unconfirmed_advance_is_rejected().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_held_behind_an_unconfirmed_advance_is_rejected_multi_thread() {
    a_record_held_behind_an_unconfirmed_advance_is_rejected().await;
}

/// Ownership churn while a record is held: the second loss cancels the held record, and the
/// replacement that follows supersedes it instead of stopping the run.
async fn a_replacement_supersedes_a_held_record_cancelled_by_a_second_loss() {
    let harness = handoff_harness();

    harness.gate_advance(
        1,
        GateOutcome::Error(sisa_messaging::FailureKind::Transient),
        &[
            Then::Lose(1),
            Then::Deliver(3),
            Then::Lose(1),
            Then::Deliver(5),
        ],
        None,
    );

    harness.deliver(1, "first");

    let consumer = harness
        .partitioned_consumer()
        .unwrap_or_else(|error| panic!("settings: {error}"));

    let mut running = tokio::spawn(consumer.run_partitioned(CancellationToken::new()));

    wait_or_report(&harness, &mut running, "replacement", |events| {
        events.iter().any(|event| matches!(event, Event::Ack(5)))
            && events.iter().any(|event| matches!(event, Event::Left(3)))
    })
    .await;

    harness.close();

    assert!(matches!(
        join(running).await,
        Ok(ConsumerExit::SourceClosed)
    ));

    assert_eq!(
        harness
            .probe
            .count(|event| matches!(event, Event::Handle(5))),
        1
    );

    assert!(
        harness
            .probe
            .events_for(3)
            .iter()
            .all(|event| matches!(event, Event::Left(3)))
    );

    assert_eq!(harness.probe.live_tx(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_replacement_supersedes_a_held_record_cancelled_by_a_second_loss_current_thread() {
    a_replacement_supersedes_a_held_record_cancelled_by_a_second_loss().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_supersedes_a_held_record_cancelled_by_a_second_loss_multi_thread() {
    a_replacement_supersedes_a_held_record_cancelled_by_a_second_loss().await;
}
