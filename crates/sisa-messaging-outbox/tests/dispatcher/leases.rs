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
