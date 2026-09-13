use std::sync::atomic::Ordering;
use std::time::Duration;

use sisa_messaging_outbox::OutboxDispatcher;
use tokio_util::sync::CancellationToken;

use super::support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_shortfall_aborts_publish_and_retires_ownership() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.renew_mode.store(RENEW_NONE, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), settings(1))
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| !store.lock().renewals.is_empty()).await;
    wait_for(|| publisher.active.load(Ordering::SeqCst) == 0).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert_eq!(report.fenced, 1);
    assert_eq!(report.aborted, 1);
    assert!(store.lock().releases.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_join_after_lease_retirement_is_benign() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.renew_mode.store(RENEW_NONE, Ordering::SeqCst);
    store.renew_delay_ms.store(25, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();
    let mut configured = settings(1);
    configured.store_timeout = Duration::from_millis(35);
    configured.lease = Duration::from_millis(100);
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), configured)
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| store.renew_entered.load(Ordering::SeqCst)).await;
    publisher.release();
    wait_for(|| publisher.active.load(Ordering::SeqCst) == 0).await;
    wait_for(|| !store.lock().renewals.is_empty()).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    assert_eq!(report.fenced, 1);
    assert_eq!(report.aborted, 1);
    assert!(store.lock().completes.is_empty());
}

#[tokio::test(start_paused = true)]
async fn slow_renewal_completion_schedules_a_future_wait_and_cancellation_stays_responsive() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.renew_delay_ms.store(18, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();
    let mut configured = settings(2);
    configured.lease = Duration::from_millis(40);
    configured.store_timeout = Duration::from_millis(19);
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), configured)
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| publisher.active.load(Ordering::SeqCst) == 1).await;
    tokio::time::advance(Duration::from_millis(1)).await;
    wait_for(|| {
        store
            .lock()
            .operations
            .iter()
            .filter(|operation| **operation == "renew")
            .count()
            == 1
    })
    .await;
    tokio::time::advance(Duration::from_millis(18)).await;
    wait_for(|| store.lock().renewals.len() == 1).await;
    tokio::task::yield_now().await;
    assert_eq!(
        store
            .lock()
            .operations
            .iter()
            .filter(|operation| **operation == "renew")
            .count(),
        1,
        "completion-anchored cadence must leave a future wait"
    );

    tokio::time::advance(Duration::from_millis(1)).await;
    wait_for(|| {
        store
            .lock()
            .operations
            .iter()
            .filter(|operation| **operation == "renew")
            .count()
            == 2
    })
    .await;
    cancellation.cancel();
    publisher.release();
    tokio::time::advance(Duration::from_millis(18)).await;
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    assert_eq!(store.lock().renewals.len(), 2);
    assert_eq!(report.completed, 1);
}

#[tokio::test(start_paused = true)]
async fn full_capacity_ignores_elapsed_claim_timer_after_slow_renewal() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.renew_delay_ms.store(18, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();
    let mut configured = settings(1);
    configured.lease = Duration::from_millis(40);
    configured.store_timeout = Duration::from_millis(19);
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), configured)
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| publisher.active.load(Ordering::SeqCst) == 1).await;
    tokio::time::advance(Duration::from_millis(1)).await;
    wait_for(|| store.lock().operations.contains(&"renew")).await;
    tokio::time::advance(Duration::from_millis(18)).await;
    wait_for(|| store.lock().renewals.len() == 1).await;
    tokio::task::yield_now().await;

    assert_eq!(store.claim_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .lock()
            .operations
            .iter()
            .filter(|operation| **operation == "renew")
            .count(),
        1,
        "full capacity must wait for renewal readiness instead of an elapsed claim timer"
    );

    publisher.release();
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert_eq!(report.completed, 1);
    assert_eq!(report.aborted, 0);
}
