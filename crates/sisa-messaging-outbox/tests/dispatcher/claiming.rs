use std::sync::atomic::Ordering;
use std::time::Duration;

use sisa_messaging_outbox::{OutboxDispatcher, PoisonReport};
use tokio_util::sync::CancellationToken;

use super::support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_capacity_includes_unresolved_publications_and_shutdown_drains() {
    let store = FakeStore::new((0..4).map(|index| record(index, 0)).collect());
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), settings(2))
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| publisher.active.load(Ordering::SeqCst) == 2).await;
    assert_eq!(publisher.maximum.load(Ordering::SeqCst), 2);
    assert_eq!(store.claim_calls.load(Ordering::SeqCst), 1);

    cancellation.cancel();
    publisher.release();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert_eq!(report.claimed, 2);
    assert_eq!(report.completed, 2);
    assert_eq!(publisher.maximum.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn claim_timeout_backs_off_and_shutdown_remains_bounded() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.claim_delay_ms.store(40, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_SUCCESS);
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), settings(1))
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| store.claim_calls.load(Ordering::SeqCst) >= 1).await;
    tokio::time::sleep(Duration::from_millis(15)).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert!(report.store_failures >= 1);
    assert_eq!(publisher.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn poison_reporting_does_not_block_healthy_records() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.lock().poison = PoisonReport {
        observed: 2,
        marked_dead: 1,
    };
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(
        store.clone(),
        FakePublisher::new(PUBLISH_SUCCESS),
        settings(1),
    )
    .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| !store.lock().completes.is_empty()).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert_eq!(report.poisoned, 2);
    assert_eq!(report.dead, 1);
    assert_eq!(report.completed, 1);
}

#[tokio::test]
async fn observed_poison_without_confirmed_dead_transition_is_not_counted_dead() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.lock().poison = PoisonReport {
        observed: 2,
        marked_dead: 0,
    };
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(
        store.clone(),
        FakePublisher::new(PUBLISH_SUCCESS),
        settings(1),
    )
    .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| !store.lock().completes.is_empty()).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert_eq!(report.poisoned, 2);
    assert_eq!(report.dead, 0);
    assert_eq!(report.completed, 1);
}
