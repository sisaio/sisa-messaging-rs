use std::error::Error;
use std::fmt;
use std::sync::atomic::Ordering;

use sisa_messaging::FailureKind;
use sisa_messaging_outbox::{DispatcherError, OutboxDispatcher};
use tokio_util::sync::CancellationToken;

use super::support::*;

const STORE_SOURCE_MARKER: &str = "foreign-store-source-marker-alpha";
const PUBLISHER_PANIC_MARKER: &str = "intentional publisher panic";

#[derive(Debug)]
struct MarkerStoreError;

impl fmt::Display for MarkerStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(STORE_SOURCE_MARKER)
    }
}

impl Error for MarkerStoreError {}

#[test]
fn dispatcher_error_outer_formatting_is_safe_while_store_source_is_inspectable() {
    let error = DispatcherError::Store(MarkerStoreError);
    let display = error.to_string();
    let debug = format!("{error:?}");

    assert_eq!(display, "outbox store operation failed permanently");
    assert_eq!(debug, "DispatcherError::Store(permanent)");
    assert!(!display.contains(STORE_SOURCE_MARKER));
    assert!(!debug.contains(STORE_SOURCE_MARKER));
    let source = error
        .source()
        .and_then(|source| source.downcast_ref::<MarkerStoreError>())
        .unwrap_or_else(|| panic!("store source was not inspectable"));
    assert_eq!(source.to_string(), STORE_SOURCE_MARKER);
}

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

    let error = dispatcher.run(CancellationToken::new()).await.unwrap_err();
    assert!(matches!(error, DispatcherError::PublisherTask(_)));
    let display = error.to_string();
    let debug = format!("{error:?}");
    assert_eq!(display, "outbox publisher task failed");
    assert_eq!(debug, "DispatcherError::PublisherTask(unexpected)");
    assert!(!display.contains(PUBLISHER_PANIC_MARKER));
    assert!(!debug.contains(PUBLISHER_PANIC_MARKER));
    let source = error
        .source()
        .and_then(|source| source.downcast_ref::<tokio::task::JoinError>())
        .unwrap_or_else(|| panic!("publisher JoinError source was not inspectable"));
    assert!(source.is_panic());
    let join = match error {
        DispatcherError::PublisherTask(join) => join,
        _ => panic!("expected publisher task error"),
    };
    let panic = join.into_panic();
    let marker = panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str));
    assert_eq!(marker, Some(PUBLISHER_PANIC_MARKER));
    assert_eq!(store.lock().releases.len(), 1);
}
