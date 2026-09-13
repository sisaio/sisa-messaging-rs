use std::sync::atomic::Ordering;

use sisa_messaging::FailureKind;
use sisa_messaging_outbox::{DispatcherError, OutboxDispatcher};
use tokio_util::sync::CancellationToken;

use super::support::*;

#[tokio::test]
async fn permanent_store_failure_cleans_up_and_returns_the_original_source() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store
        .complete_mode
        .store(COMPLETE_PERMANENT, Ordering::SeqCst);
    let dispatcher = OutboxDispatcher::new(
        store.clone(),
        FakePublisher::new(PUBLISH_SUCCESS),
        settings(1),
    )
    .unwrap_or_else(|error| panic!("settings rejected: {error}"));

    match dispatcher.run(CancellationToken::new()).await {
        Err(DispatcherError::Store(source)) => {
            assert_eq!(source.kind, FailureKind::Permanent);
        }
        other => panic!("expected sourced permanent store error, got {other:?}"),
    }
    assert_eq!(store.lock().releases.len(), 1);
}

#[tokio::test]
async fn publisher_panic_is_terminal_and_claim_is_released() {
    let store = FakeStore::new(vec![record(1, 0)]);
    let dispatcher = OutboxDispatcher::new(
        store.clone(),
        FakePublisher::new(PUBLISH_PANIC),
        settings(1),
    )
    .unwrap_or_else(|error| panic!("settings rejected: {error}"));

    let result = dispatcher.run(CancellationToken::new()).await;
    assert!(matches!(result, Err(DispatcherError::PublisherTask(_))));
    assert_eq!(store.lock().releases.len(), 1);
}
