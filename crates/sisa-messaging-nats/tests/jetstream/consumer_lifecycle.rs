//! Heartbeat, in-flight bound, cancellation, and startup validation of the NATS-typed consumer.

use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use sisa_messaging_consumer::{ConsumerErrorKind, ConsumerExit};
use tokio_util::sync::CancellationToken;

use super::consumer::{
    Broker, PROGRESS_TIMEOUT, Running, ScriptedHandler, Step, consumer, eventually, scope, settings,
};
use super::inbox::{FakeInbox, Status};

#[tokio::test]
#[ignore = "requires a real JetStream server at NATS_URL"]
async fn heartbeat_keeps_a_slow_delivery_active_past_ack_wait() {
    let ack_wait = Duration::from_secs(2);
    let (broker, pull_consumer) = Broker::new(ack_wait, -1).await;
    let inbox = FakeInbox::new(5);
    let handler = ScriptedHandler::default();
    handler.script("slow", &[Step::Sleep(Duration::from_secs(5))]);
    let scope = scope();
    let mut settings = settings();
    settings.heartbeat_interval = Some(Duration::from_millis(500));

    let running = Running::spawn(
        consumer(
            pull_consumer,
            inbox.clone(),
            scope.clone(),
            handler.clone(),
            settings,
        )
        .unwrap(),
    );

    let started = Instant::now();
    let id = broker.publish("slow").await;
    let info = broker.wait_acked(1).await;
    assert!(started.elapsed() >= Duration::from_secs(5));
    assert_eq!(info.delivered.consumer_sequence, 1);
    assert_eq!(inbox.completions(&scope, id), 1);

    tokio::time::sleep(ack_wait + Duration::from_millis(500)).await;

    let info = broker.info().await;
    assert_eq!(info.delivered.consumer_sequence, 1);
    assert_eq!(info.num_ack_pending, 0);
    assert_eq!(handler.invocations("slow").len(), 1);

    assert_eq!(running.stop().await.unwrap(), ConsumerExit::Cancelled);
    broker.delete().await;
}

#[tokio::test]
#[ignore = "requires a real JetStream server at NATS_URL"]
async fn in_flight_bound_limits_concurrent_handlers_and_open_transactions() {
    const BOUND: usize = 4;
    const MESSAGES: usize = 12;

    let (broker, pull_consumer) = Broker::new(Duration::from_secs(30), -1).await;
    let inbox = FakeInbox::new(5);
    let handler = ScriptedHandler::default();
    let scope = scope();
    let mut settings = settings();
    settings.max_in_flight = NonZeroUsize::new(BOUND).unwrap();

    let labels: Vec<String> = (0..MESSAGES)
        .map(|index| format!("gated-{index}"))
        .collect();

    for label in &labels {
        handler.script(label, &[Step::Gate]);
    }

    let running = Running::spawn(
        consumer(
            pull_consumer,
            inbox.clone(),
            scope.clone(),
            handler.clone(),
            settings,
        )
        .unwrap(),
    );

    for label in &labels {
        broker.publish(label).await;
    }

    eventually("the in-flight bound to fill", || handler.active() == BOUND).await;

    // With every permit taken the source is not polled, so no further handler starts.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(handler.active(), BOUND);
    assert_eq!(handler.total_invocations(), BOUND);
    assert_eq!(inbox.live(), BOUND as u64);

    let info = broker.info().await;

    assert!(
        info.num_ack_pending >= BOUND && info.num_ack_pending <= BOUND + 1,
        "broker holds the bounded deliveries plus at most one buffered pull"
    );

    handler.release(MESSAGES);
    broker.wait_acked(MESSAGES as u64).await;

    assert_eq!(inbox.completed_total(), MESSAGES as u32);
    assert_eq!(handler.total_invocations(), MESSAGES);
    assert_eq!(handler.peak(), BOUND);

    assert_eq!(running.stop().await.unwrap(), ConsumerExit::Cancelled);
    broker.delete().await;
}

#[tokio::test]
#[ignore = "requires a real JetStream server at NATS_URL"]
async fn cancellation_drain_aborts_unfinished_work_for_redelivery_to_a_fresh_source() {
    let ack_wait = Duration::from_secs(3);
    let drain_timeout = Duration::from_millis(1_500);
    let (broker, pull_consumer) = Broker::new(ack_wait, -1).await;
    let inbox = FakeInbox::new(5);
    let handler = ScriptedHandler::default();
    let scope = scope();
    let mut settings = settings();
    settings.max_in_flight = NonZeroUsize::new(5).unwrap();
    settings.drain_timeout = drain_timeout;

    for stuck in ["stuck-a", "stuck-b"] {
        handler.script(stuck, &[Step::Gate, Step::Succeed]);
    }

    // Still running at cancellation, but finishes well inside the drain window.
    handler.script("draining", &[Step::Sleep(Duration::from_millis(800))]);

    let running = Running::spawn(
        consumer(
            pull_consumer,
            inbox.clone(),
            scope.clone(),
            handler.clone(),
            settings,
        )
        .unwrap(),
    );

    let stuck_a = broker.publish("stuck-a").await;
    let stuck_b = broker.publish("stuck-b").await;
    let draining = broker.publish("draining").await;
    let quick_a = broker.publish("quick-a").await;
    let quick_b = broker.publish("quick-b").await;

    eventually("quick work to complete while stuck work runs", || {
        inbox.completed_total() == 2 && handler.active() == 3
    })
    .await;

    let deadline = Instant::now() + PROGRESS_TIMEOUT;

    while broker.info().await.num_ack_pending != 3 {
        assert!(Instant::now() < deadline, "quick work was not acknowledged");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert_eq!(
        inbox.completions(&scope, draining),
        0,
        "draining work must still be running at cancellation"
    );

    let stopping = Instant::now();
    assert_eq!(running.stop().await.unwrap(), ConsumerExit::Cancelled);
    let stopped = stopping.elapsed();

    assert!(
        stopped >= drain_timeout && stopped < drain_timeout + Duration::from_secs(3),
        "drain lasted {stopped:?}"
    );

    assert_eq!(inbox.live(), 0, "aborted transactions were dropped");
    assert_eq!(handler.active(), 0);

    // Work finished inside the drain was acknowledged; aborted work was neither acknowledged
    // nor negatively acknowledged, so the broker still holds exactly those two deliveries.
    let info = broker.info().await;
    assert_eq!(info.delivered.consumer_sequence, 5);
    assert_eq!(info.num_ack_pending, 2);
    assert_eq!(inbox.completions(&scope, draining), 1);

    for stuck in [stuck_a, stuck_b] {
        assert_eq!(
            inbox.status(&scope, stuck),
            Some(Status::Pending { attempts: 0 })
        );

        assert_eq!(inbox.completions(&scope, stuck), 0);
    }

    let restarted = Running::spawn(
        consumer(
            broker.lookup().await,
            inbox.clone(),
            scope.clone(),
            handler.clone(),
            super::consumer::settings(),
        )
        .unwrap(),
    );

    broker.wait_acked(5).await;

    for id in [stuck_a, stuck_b, draining, quick_a, quick_b] {
        assert_eq!(inbox.completions(&scope, id), 1);
    }

    // Redelivery waited for the acknowledgement deadline from the original delivery, which a
    // negative acknowledgement on abort would have shortened to the nak delay.
    for stuck in ["stuck-a", "stuck-b"] {
        let invocations = handler.invocations(stuck);
        assert_eq!(invocations.len(), 2);
        let gap = invocations[1].duration_since(invocations[0]);

        assert!(
            gap >= ack_wait.mul_f32(0.9),
            "redelivery after {gap:?} preceded ack_wait"
        );
    }

    for finished in ["draining", "quick-a", "quick-b"] {
        assert_eq!(handler.invocations(finished).len(), 1);
    }

    let mut effects = inbox.effects();
    effects.sort();

    assert_eq!(
        effects,
        ["draining", "quick-a", "quick-b", "stuck-a", "stuck-b"]
    );

    assert_eq!(restarted.stop().await.unwrap(), ConsumerExit::Cancelled);
    broker.delete().await;
}

/// `NatsDeliverySource::open` starts its pull stream before the runtime validates the
/// descriptor-dependent heartbeat and delivery bounds, so one pull may already have fetched a
/// message when startup fails. That message is never processed and stays unacknowledged for
/// `ack_wait` redelivery; nothing beyond this single prefetch is consumed.
async fn assert_at_most_one_prefetched_delivery(broker: &Broker) {
    let info = broker.info().await;

    assert!(
        info.delivered.consumer_sequence <= 1,
        "rejected startup consumed more than one pull"
    );

    assert_eq!(
        info.num_ack_pending as u64,
        info.delivered.consumer_sequence
    );
}

#[tokio::test]
#[ignore = "requires a real JetStream server at NATS_URL"]
async fn startup_rejects_unsafe_delivery_bounds_before_receiving() {
    // The durable stops delivering after three attempts; the inbox records dead at five.
    let (broker, pull_consumer) = Broker::new(Duration::from_secs(10), 3).await;
    broker.publish("before-start").await;
    let inbox = FakeInbox::new(5);
    let handler = ScriptedHandler::default();

    let error = consumer(
        pull_consumer,
        inbox.clone(),
        scope(),
        handler.clone(),
        settings(),
    )
    .unwrap()
    .run(CancellationToken::new())
    .await
    .unwrap_err();

    assert_eq!(
        error.kind(),
        ConsumerErrorKind::AttemptBoundExceedsMaxDeliver
    );

    assert_eq!(handler.total_invocations(), 0);
    assert_eq!(inbox.begun(), 0);
    assert_at_most_one_prefetched_delivery(&broker).await;
    broker.delete().await;

    // Heartbeat every second cannot keep a two-second acknowledgement deadline safely alive.
    let (broker, pull_consumer) = Broker::new(Duration::from_secs(2), -1).await;
    broker.publish("before-start").await;
    let mut settings = settings();
    settings.heartbeat_interval = Some(Duration::from_secs(1));

    let error = consumer(
        pull_consumer,
        inbox.clone(),
        scope(),
        handler.clone(),
        settings,
    )
    .unwrap()
    .run(CancellationToken::new())
    .await
    .unwrap_err();

    assert_eq!(error.kind(), ConsumerErrorKind::HeartbeatDeadlineTooShort);
    assert_eq!(handler.total_invocations(), 0);
    assert_eq!(inbox.begun(), 0);
    assert_at_most_one_prefetched_delivery(&broker).await;
    broker.delete().await;
}
