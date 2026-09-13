use std::sync::atomic::Ordering;

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
