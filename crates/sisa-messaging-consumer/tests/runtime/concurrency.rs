//! Cancellation, bounded concurrency, drain, exit precedence, panics, and redaction.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use sisa_messaging::FailureKind;
use sisa_messaging_consumer::{ConsumerErrorKind, ConsumerExit, SettlementMode};
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::support::*;

const DRAIN: Duration = Duration::from_secs(20);

fn gate() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(0))
}

async fn wait_for(harness: &Harness, event: Event) {
    harness
        .probe
        .wait_until(|events| events.contains(&event))
        .await;
}

fn receives(harness: &Harness) -> usize {
    harness.probe.count(|event| *event == Event::Receive)
}

#[tokio::test(start_paused = true)]
async fn cancellation_while_receiving_loses_nothing() {
    for mode in [SettlementMode::Broker, SettlementMode::PendingRecovery] {
        let harness = Harness::new(mode);
        let cancel = CancellationToken::new();
        let handle = harness.spawn(cancel.clone());

        wait_for(&harness, Event::Receive).await;

        // The delivery and the cancellation become ready together; cancellation wins and the
        // dropped receive future leaves the delivery queued.
        cancel.cancel();
        harness.deliver(1, "late");

        let (result, live) = join(handle).await;

        assert!(matches!(result, Ok(ConsumerExit::Cancelled)));
        assert_eq!(live, 0);
        assert_eq!(harness.queued(), 1);
        assert!(harness.probe.events_for(1).is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_the_handler_commits_and_acks_within_the_drain() {
    for mode in [SettlementMode::Broker, SettlementMode::PendingRecovery] {
        let harness = Harness::new(mode);
        let gate = gate();

        harness
            .probe
            .script_handler(1, HandlerStep::Block(Arc::clone(&gate)));

        harness.deliver(1, "slow");

        let cancel = CancellationToken::new();
        let handle = harness.spawn(cancel.clone());

        wait_for(&harness, Event::Handle(1)).await;
        cancel.cancel();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let receives_at_cancel = receives(&harness);
        assert_eq!(harness.probe.count(|event| *event == Event::Commit(1)), 0);

        gate.add_permits(1);

        let (result, live) = join(handle).await;

        assert!(matches!(result, Ok(ConsumerExit::Cancelled)));
        assert_eq!(live, 0);
        assert_eq!(receives(&harness), receives_at_cancel);

        assert_eq!(
            harness.probe.events_for(1)[1..],
            [
                Event::Handle(1),
                Event::Complete(1),
                Event::Commit(1),
                Event::TxDropped(Some(1)),
                Event::Ack(1),
            ]
        );
    }
}

#[tokio::test(start_paused = true)]
async fn drain_deadline_drops_every_transaction_before_returning() {
    enum Stall {
        Handler,
        Commit,
        Ack,
    }

    for stall in [Stall::Handler, Stall::Commit, Stall::Ack] {
        let mut harness = Harness::new(SettlementMode::Broker);
        harness.settings.database_timeout = Duration::from_secs(60);
        harness.settings.settlement_timeout = Duration::from_secs(60);
        harness.settings.drain_timeout = DRAIN;

        let stalled = match stall {
            Stall::Handler => {
                harness.probe.script_handler(1, HandlerStep::Hang);

                Event::Handle(1)
            }
            Stall::Commit => {
                harness.probe.script_commit(1, Step::Hang);

                Event::Commit(1)
            }
            Stall::Ack => {
                harness.probe.script_settle(1, Step::Hang);

                Event::Ack(1)
            }
        };

        harness.deliver(1, "stalled");

        let cancel = CancellationToken::new();
        let handle = harness.spawn(cancel.clone());

        wait_for(&harness, stalled.clone()).await;

        let cancelled_at = Instant::now();
        cancel.cancel();

        let (result, live) = join(handle).await;

        assert!(matches!(result, Ok(ConsumerExit::Cancelled)));
        assert_eq!(cancelled_at.elapsed(), DRAIN);
        assert_eq!(live, 0, "a transaction outlived run");

        assert!(
            harness
                .probe
                .events_for(1)
                .contains(&Event::TxDropped(Some(1)))
        );

        let events = harness.probe.events_for(1);

        match stall {
            Stall::Handler | Stall::Commit => {
                assert!(!events.iter().any(is_settlement));
                assert!(events.contains(&Event::Left(1)));
            }
            Stall::Ack => {
                // The ack was attempted after commit but never confirmed.
                assert_eq!(events.last(), Some(&Event::Ack(1)));
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn at_capacity_the_source_is_not_polled() {
    for mode in [SettlementMode::Broker, SettlementMode::PendingRecovery] {
        let mut harness = Harness::new(mode);
        harness.settings.max_in_flight = NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN);

        let gate = gate();

        for tag in 1..=5 {
            harness
                .probe
                .script_handler(tag, HandlerStep::Block(Arc::clone(&gate)));

            harness.deliver(tag, "bounded");
        }

        let handle = harness.spawn(CancellationToken::new());

        harness
            .probe
            .wait_until(|events| {
                events
                    .iter()
                    .filter(|event| matches!(event, Event::Handle(_)))
                    .count()
                    == 2
            })
            .await;

        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        assert_eq!(receives(&harness), 2);
        assert_eq!(harness.queued(), 3);
        assert_eq!(harness.probe.live_tx(), 2);

        gate.add_permits(5);
        harness.close();

        let (result, live) = join(handle).await;

        assert!(matches!(result, Ok(ConsumerExit::SourceClosed)));
        assert_eq!(live, 0);

        assert_eq!(
            harness.probe.count(|event| matches!(event, Event::Ack(_))),
            5
        );

        assert_eq!(harness.probe.max_live_tx(), 2);
        assert!(harness.probe.max_outstanding_at_receive() < 2);
    }
}

#[tokio::test(start_paused = true)]
async fn a_fatal_delivery_stops_receiving_and_drains_the_others() {
    for mode in [SettlementMode::Broker, SettlementMode::PendingRecovery] {
        let harness = Harness::new(mode);
        let gate = gate();

        harness
            .probe
            .script_handler(1, HandlerStep::Block(Arc::clone(&gate)));

        harness
            .probe
            .script_commit(2, Step::Error(FailureKind::Permanent));

        harness.deliver(1, "in flight");
        harness.deliver(2, "fatal");

        let handle = harness.spawn(CancellationToken::new());

        wait_for(&harness, Event::Left(2)).await;
        harness.deliver(3, "never received");
        tokio::time::sleep(Duration::from_secs(1)).await;
        gate.add_permits(1);

        let (result, live) = join(handle).await;
        let error = expect_error(result);

        assert_eq!(error.kind(), ConsumerErrorKind::Inbox);
        assert_eq!(live, 0);
        assert_eq!(harness.queued(), 1);
        assert_eq!(harness.probe.events_for(1).last(), Some(&Event::Ack(1)));
    }
}

#[tokio::test(start_paused = true)]
async fn a_fatal_error_wins_over_cancellation_in_either_order() {
    // Stop first, then cancel.
    let stop_first = Harness::new(SettlementMode::Broker);

    stop_first
        .probe
        .script_commit(1, Step::Error(FailureKind::Permanent));

    stop_first.deliver(1, "fatal");

    let cancel = CancellationToken::new();
    let handle = stop_first.spawn(cancel.clone());

    wait_for(&stop_first, Event::Left(1)).await;
    cancel.cancel();

    let (result, _) = join(handle).await;
    assert_eq!(expect_error(result).kind(), ConsumerErrorKind::Inbox);

    // Cancel first; the failure happens during the drain.
    let cancel_first = Harness::new(SettlementMode::Broker);
    let gate = gate();

    cancel_first
        .probe
        .script_handler(1, HandlerStep::Block(Arc::clone(&gate)));

    cancel_first
        .probe
        .script_settle(1, Step::Error(FailureKind::Permanent));

    cancel_first.deliver(1, "fails while draining");

    let cancel = CancellationToken::new();
    let handle = cancel_first.spawn(cancel.clone());

    wait_for(&cancel_first, Event::Handle(1)).await;
    cancel.cancel();
    tokio::time::sleep(Duration::from_secs(1)).await;
    gate.add_permits(1);

    let (result, _) = join(handle).await;
    assert_eq!(expect_error(result).kind(), ConsumerErrorKind::Settlement);
}

#[tokio::test(start_paused = true)]
async fn close_and_source_failure_with_in_flight_work_have_distinct_exits() {
    for fail in [false, true] {
        let harness = Harness::new(SettlementMode::Broker);
        let gate = gate();

        harness
            .probe
            .script_handler(1, HandlerStep::Block(Arc::clone(&gate)));

        harness.deliver(1, "in flight");

        let handle = harness.spawn(CancellationToken::new());

        wait_for(&harness, Event::Handle(1)).await;

        if fail {
            harness.fail_source(FailureKind::Permanent);
        } else {
            harness.close();
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        gate.add_permits(1);

        let (result, live) = join(handle).await;

        assert_eq!(live, 0);
        assert_eq!(harness.probe.events_for(1).last(), Some(&Event::Ack(1)));

        if fail {
            let error = expect_error(result);
            assert_eq!(error.kind(), ConsumerErrorKind::Source);
            assert_eq!(error.failure_kind(), FailureKind::Permanent);
        } else {
            assert!(matches!(result, Ok(ConsumerExit::SourceClosed)));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_handler_panic_never_commits_or_acknowledges() {
    for mode in [SettlementMode::Broker, SettlementMode::PendingRecovery] {
        let harness = Harness::new(mode);
        harness.probe.script_handler(1, HandlerStep::Panic);
        harness.deliver(1, "panics");

        let error = expect_error(harness.run().await);

        assert_eq!(error.kind(), ConsumerErrorKind::HandlerPanicked);
        assert_eq!(error.failure_kind(), FailureKind::Permanent);
        assert_eq!(error.to_string(), "consumer handler panicked");
        assert!(error.provider_source().is_none());
        assert_redacted(&error);

        assert_eq!(
            harness.probe.events_for(1),
            [
                Event::Claim(1),
                Event::Handle(1),
                Event::TxDropped(Some(1)),
                Event::Left(1),
            ]
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_provider_panic_is_not_reported_as_a_handler_panic() {
    for mode in [SettlementMode::Broker, SettlementMode::PendingRecovery] {
        for in_commit in [false, true] {
            let harness = Harness::new(mode);

            if in_commit {
                harness.probe.script_commit(1, Step::Panic);
            } else {
                harness.probe.script_claim(1, ClaimStep::Db(Step::Panic));
            }

            harness.deliver(1, "provider panics");

            // `run` asserts that no transaction is live when it returns.
            let error = expect_error(harness.run().await);

            assert_eq!(error.kind(), ConsumerErrorKind::ProviderPanicked);
            assert_eq!(error.failure_kind(), FailureKind::Permanent);
            assert_eq!(error.to_string(), "consumer processing task panicked");
            assert!(error.provider_source().is_none());
            assert_redacted(&error);

            let events = harness.probe.events_for(1);

            assert!(events.contains(&Event::TxDropped(Some(1))));
            assert!(events.contains(&Event::Left(1)));
            assert!(!events.iter().any(is_settlement));
            assert!(harness.probe.committed_writes().is_empty());

            assert_eq!(
                harness.probe.count(|event| *event == Event::Handle(1)),
                usize::from(in_commit)
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn sentinel_values_never_reach_errors_or_captured_logs() {
    let capture = Capture::default();
    let _installed = capture.install();

    let broker = Harness::new(SettlementMode::Broker);
    broker.deliver_malformed(1);
    broker.deliver(2, "undecodable");

    broker
        .probe
        .script_handler(3, HandlerStep::Fail(FailureKind::Transient));

    broker
        .probe
        .script_fail(3, Step::Error(FailureKind::Transient));

    broker.deliver(3, "fail not recorded");

    broker
        .probe
        .script_commit(4, Step::Error(FailureKind::Transient));

    broker.deliver(4, "commit ambiguous");

    broker
        .probe
        .script_rollback(Step::Error(FailureKind::Transient));

    broker.probe.script_claim(5, ClaimStep::InProgress);
    broker.deliver(5, "rollback cleanup fails");

    broker
        .probe
        .script_settle(6, Step::Error(FailureKind::Permanent));

    broker.deliver(6, "settlement fails");

    let broker_error = expect_error(broker.run().await);
    assert_eq!(broker_error.kind(), ConsumerErrorKind::Settlement);
    assert_redacted(&broker_error);

    let pending = Harness::new(SettlementMode::PendingRecovery);
    pending.probe.script_handler(1, HandlerStep::Panic);
    pending.deliver(1, "panics");

    let pending_error = expect_error(pending.run().await);
    assert_redacted(&pending_error);

    let provider_panic = Harness::new(SettlementMode::Broker);

    provider_panic
        .probe
        .script_claim(1, ClaimStep::Db(Step::Panic));

    provider_panic.deliver(1, "provider panic");
    let provider_error = expect_error(provider_panic.run().await);
    assert_eq!(provider_error.kind(), ConsumerErrorKind::ProviderPanicked);
    assert_redacted(&provider_error);

    let lines = capture.lines();

    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("messaging.consumer ") && line.contains("message.type="))
    );

    assert!(lines.iter().any(|line| line.contains("delivery resolved")));
    assert!(lines.iter().any(|line| line.contains("consumer stopping")));
    assert!(lines.iter().any(|line| line.contains("provider_panicked")));

    // Settlement failures name the static transport error type, never its rendering.
    assert!(
        lines
            .iter()
            .any(|line| line.contains("delivery settlement unconfirmed")
                && line.contains("error.type=")
                && line.contains("FakeError"))
    );

    for line in &lines {
        for sentinel in SENTINELS {
            assert!(!line.contains(sentinel), "log leaked {sentinel}: {line}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_panicking_coordinator_stops_an_idle_consumer() {
    for mode in [SettlementMode::Broker, SettlementMode::PendingRecovery] {
        let harness = Harness::new(mode);
        harness.probe.script_settle(1, Step::Panic);
        harness.deliver(1, "ack panics");

        // No further delivery arrives, so only the panic itself can stop the receive loop.
        let run = tokio::time::timeout(Duration::from_secs(60), harness.run()).await;

        let Ok(result) = run else {
            panic!("an idle consumer hid a panicked coordinator");
        };

        let error = expect_error(result);

        assert_eq!(error.kind(), ConsumerErrorKind::Runtime);
        assert_redacted(&error);
        // The first receive yielded the delivery; the second was pending when the stop arrived.
        assert_eq!(receives(&harness), 2);
        assert_eq!(harness.probe.count(|event| *event == Event::Ack(1)), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_deadline_waits_for_a_workflow_blocked_mid_poll() {
    let mut harness = Harness::new(SettlementMode::Broker);
    harness.settings.drain_timeout = Duration::from_millis(20);

    // The workflow is inside one poll when the deadline aborts it, so its transaction can only
    // drop after that poll returns; `run` must wait for it.
    harness.probe.script_handler(
        1,
        HandlerStep::BlockThreadThenHang(Duration::from_millis(300)),
    );

    harness.deliver(1, "blocked mid-poll");

    let cancel = CancellationToken::new();
    let handle = harness.spawn(cancel.clone());

    wait_for(&harness, Event::Handle(1)).await;
    cancel.cancel();

    let (result, live) = join(handle).await;

    assert!(matches!(result, Ok(ConsumerExit::Cancelled)));

    assert_eq!(
        live, 0,
        "run returned before the aborted workflow dropped its transaction"
    );

    let events = harness.probe.events_for(1);
    assert!(events.contains(&Event::TxDropped(Some(1))));
    assert!(!events.iter().any(is_settlement));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_thread_stress_respects_capacity_and_resolves_every_delivery() {
    const COUNT: u8 = 64;

    let mut harness = Harness::new(SettlementMode::PendingRecovery);
    harness.settings.max_in_flight = NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN);

    // A fixed linear congruential sequence selects a reproducible mix of outcomes.
    let mut seed: u32 = 0x5eed_1234;
    let mut failing = Vec::new();

    for tag in 1..=COUNT {
        seed = seed.wrapping_mul(1_103_515_245).wrapping_add(12_345);

        let step = match (seed >> 16) % 3 {
            0 => {
                failing.push(tag);

                HandlerStep::Fail(FailureKind::Transient)
            }
            1 => HandlerStep::Yield,
            _ => HandlerStep::Ok,
        };

        harness.probe.script_handler(tag, step);
        harness.deliver(tag, "stress");
    }

    harness.close();

    let (result, live) = join(harness.spawn(CancellationToken::new())).await;

    assert!(matches!(result, Ok(ConsumerExit::SourceClosed)));
    assert_eq!(live, 0);
    assert!(!failing.is_empty() && failing.len() < usize::from(COUNT));
    assert!(harness.probe.max_live_tx() <= 2);
    assert!(harness.probe.max_outstanding_at_receive() < 2);

    for tag in 1..=COUNT {
        let events = harness.probe.events_for(tag);

        let acked = events
            .iter()
            .filter(|event| **event == Event::Ack(tag))
            .count();

        let left = events
            .iter()
            .filter(|event| **event == Event::Left(tag))
            .count();

        assert_eq!(
            acked + left,
            1,
            "delivery {tag} was not resolved exactly once"
        );

        assert_eq!(left == 1, failing.contains(&tag));
    }
}
