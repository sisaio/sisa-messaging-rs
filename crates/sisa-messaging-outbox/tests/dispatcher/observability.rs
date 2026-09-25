use std::error::Error;
use std::fmt;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sisa_messaging::{ErrorClassifier, FailureKind};
use sisa_messaging_outbox::{
    Claim, ClaimBatch, ClaimRequest, FailureRecord, FencedClaims, OutboxDispatcher, OutboxStore,
};
use tokio_util::sync::CancellationToken;

use super::support::*;

const FOREIGN_DISPLAY_MARKERS: [&str; 3] = [
    "foreign-display-marker-alpha",
    "foreign-display-marker-beta",
    "foreign-display-marker-gamma",
];
const STORE_ERROR_DISPLAY_MARKER: &str = "foreign-store-display-marker-delta";

#[derive(Clone)]
struct MarkerStore(FakeStore);

#[derive(Debug)]
struct MarkerStoreError;

impl fmt::Display for MarkerStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(STORE_ERROR_DISPLAY_MARKER)
    }
}

impl Error for MarkerStoreError {}

impl ErrorClassifier for MarkerStoreError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

impl OutboxStore for MarkerStore {
    type Error = MarkerStoreError;

    async fn claim(&self, request: ClaimRequest) -> Result<ClaimBatch, Self::Error> {
        self.0.claim(request).await.map_err(|_| MarkerStoreError)
    }

    async fn complete(&self, claims: &[Claim]) -> Result<FencedClaims, Self::Error> {
        self.0.complete(claims).await.map_err(|_| MarkerStoreError)
    }

    async fn fail(&self, failures: &[FailureRecord]) -> Result<FencedClaims, Self::Error> {
        self.0.fail(failures).await.map_err(|_| MarkerStoreError)
    }

    async fn release(&self, claims: &[Claim]) -> Result<FencedClaims, Self::Error> {
        self.0.release(claims).await.map_err(|_| MarkerStoreError)
    }

    async fn extend_lease(
        &self,
        claims: &[Claim],
        lease: Duration,
    ) -> Result<FencedClaims, Self::Error> {
        self.0
            .extend_lease(claims, lease)
            .await
            .map_err(|_| MarkerStoreError)
    }
}

async fn run_redacted_failure() -> FakeStore {
    let mut secret_record = record(1, 0);
    secret_record.envelope.payload = b"TOP_SECRET_PAYLOAD".to_vec();
    let store = FakeStore::new(vec![secret_record]);
    let cancellation = CancellationToken::new();

    let dispatcher = OutboxDispatcher::new(store.clone(), SecretPublisher, settings(1))
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));

    let run_cancel = cancellation.clone();

    let cancel_after_failure = async {
        wait_for(|| !store.lock().failures.is_empty()).await;
        cancellation.cancel();
    };

    let (result, ()) = tokio::join!(dispatcher.run(run_cancel), cancel_after_failure);
    result.unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    store
}

#[test]
fn instrumentation_and_persisted_failure_never_record_foreign_sensitive_text() {
    let output = Arc::new(Mutex::new(Vec::new()));

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_target(true)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NEW)
        .without_time()
        .with_writer(SharedWriter(Arc::clone(&output)))
        .finish();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| panic!("test runtime failed: {error}"));

    tracing::info!(target: "messaging.outbox", "outside-scope-before");

    tracing::subscriber::with_default(subscriber, || {
        // Register the complete static callsite set under this scoped dispatcher before the
        // asserted pass. Other integration tests may have cached some callsites first.
        runtime.block_on(run_redacted_failure());
        tracing::callsite::rebuild_interest_cache();

        output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();

        runtime.block_on(async {
            let store = run_redacted_failure().await;
            let state = store.lock();
            let persisted = state.failures[0][0].error.as_str();
            assert_eq!(persisted, "publisher failed transiently");

            for marker in FOREIGN_DISPLAY_MARKERS {
                assert!(
                    !persisted.contains(marker),
                    "persisted foreign marker {marker}"
                );
            }
        });
    });

    tracing::callsite::rebuild_interest_cache();
    tracing::info!(target: "messaging.outbox", "outside-scope-after");

    let rendered = String::from_utf8(
        output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
    .unwrap_or_else(|error| panic!("subscriber emitted invalid UTF-8: {error}"));

    for expected in [
        "outbox.dispatch",
        "outbox.claim",
        "outbox.publish",
        "outbox.persist_outcome",
        "failure.kind=\"transient\"",
    ] {
        assert!(
            rendered.contains(expected),
            "missing {expected}: {rendered}"
        );
    }

    for forbidden in FOREIGN_DISPLAY_MARKERS.into_iter().chain([
        "TOP_SECRET_PAYLOAD",
        "outside-scope-before",
        "outside-scope-after",
    ]) {
        assert!(!rendered.contains(forbidden), "leaked {forbidden}");
    }
}

async fn complete_warning(mode: u8, delay_ms: usize) {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.complete_mode.store(mode, Ordering::SeqCst);
    store.complete_delay_ms.store(delay_ms, Ordering::SeqCst);
    let cancellation = CancellationToken::new();

    let dispatcher = OutboxDispatcher::new(
        store.clone(),
        FakePublisher::new(PUBLISH_SUCCESS),
        settings(1),
    )
    .unwrap_or_else(|error| panic!("settings rejected: {error}"));

    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    if mode == COMPLETE_NONE {
        wait_for(|| !store.lock().completes.is_empty()).await;
    } else {
        wait_for(|| store.release_entered.load(Ordering::SeqCst)).await;
    }

    cancellation.cancel();

    task.await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
}

async fn fail_warning(mode: u8, delay_ms: usize) {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.fail_mode.store(mode, Ordering::SeqCst);
    store.fail_delay_ms.store(delay_ms, Ordering::SeqCst);
    let cancellation = CancellationToken::new();

    let dispatcher = OutboxDispatcher::new(
        store.clone(),
        FakePublisher::new(PUBLISH_TRANSIENT),
        settings(1),
    )
    .unwrap_or_else(|error| panic!("settings rejected: {error}"));

    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    if mode == FAIL_NONE {
        wait_for(|| !store.lock().failures.is_empty()).await;
    } else {
        wait_for(|| store.release_entered.load(Ordering::SeqCst)).await;
    }

    cancellation.cancel();

    task.await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
}

async fn release_warning(mode: u8, delay_ms: usize) {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.release_mode.store(mode, Ordering::SeqCst);
    store.release_delay_ms.store(delay_ms, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();
    let mut configured = settings(1);
    configured.drain_timeout = Duration::from_millis(1);

    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), configured)
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));

    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });
    wait_for(|| publisher.active.load(Ordering::SeqCst) == 1).await;
    cancellation.cancel();

    task.await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
}

async fn rejected_release_warning(mode: u8, delay_ms: usize) {
    let store = FakeStore::new(vec![record(1, 0), record(2, 0)]);
    store.claim_limit_extra.store(1, Ordering::SeqCst);
    store.release_mode.store(mode, Ordering::SeqCst);
    store.release_delay_ms.store(delay_ms, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();

    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), settings(1))
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));

    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });
    wait_for(|| store.release_entered.load(Ordering::SeqCst)).await;

    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(12)).await;
    }

    publisher.release();
    cancellation.cancel();

    task.await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
}

async fn cleanup_release_warning(mode: u8, delay_ms: usize) {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.release_mode.store(mode, Ordering::SeqCst);
    store.release_delay_ms.store(delay_ms, Ordering::SeqCst);

    let dispatcher = OutboxDispatcher::new(store, FakePublisher::new(PUBLISH_PANIC), settings(1))
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));

    assert!(dispatcher.run(CancellationToken::new()).await.is_err());
}

async fn permanent_cleanup_release_warning() {
    let store = FakeStore::new(vec![record(1, 0)]);

    store
        .release_mode
        .store(RELEASE_PERMANENT, Ordering::SeqCst);

    let dispatcher = OutboxDispatcher::new(
        MarkerStore(store),
        FakePublisher::new(PUBLISH_PANIC),
        settings(1),
    )
    .unwrap_or_else(|error| panic!("settings rejected: {error}"));

    assert!(dispatcher.run(CancellationToken::new()).await.is_err());
}

#[test]
fn suppressed_persistence_failures_and_fencing_shortfalls_warn_once_at_the_decision_layer() {
    let output = Arc::new(Mutex::new(Vec::new()));

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_target(true)
        .without_time()
        .with_writer(SharedWriter(Arc::clone(&output)))
        .finish();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| panic!("test runtime failed: {error}"));

    tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(async {
            complete_warning(COMPLETE_NONE, 0).await;
            complete_warning(COMPLETE_TRANSIENT_ONCE, 0).await;
            complete_warning(COMPLETE_ALL, 20).await;
            fail_warning(FAIL_NONE, 0).await;
            fail_warning(FAIL_TRANSIENT, 0).await;
            fail_warning(FAIL_ALL, 20).await;
            release_warning(RELEASE_NONE, 0).await;
            release_warning(RELEASE_TRANSIENT, 0).await;
            release_warning(RELEASE_ALL, 20).await;
            rejected_release_warning(RELEASE_NONE, 0).await;
            rejected_release_warning(RELEASE_TRANSIENT, 0).await;
            rejected_release_warning(RELEASE_ALL, 20).await;
            cleanup_release_warning(RELEASE_NONE, 0).await;
            cleanup_release_warning(RELEASE_TRANSIENT, 0).await;
            cleanup_release_warning(RELEASE_ALL, 20).await;
            permanent_cleanup_release_warning().await;
        });
    });

    let rendered = String::from_utf8(
        output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
    .unwrap_or_else(|error| panic!("subscriber emitted invalid UTF-8: {error}"));

    for operation in [
        "complete",
        "fail",
        "release",
        "rejected_release",
        "cleanup_release",
    ] {
        for category in ["fencing_shortfall", "transient_failure", "timeout"] {
            let evidence = format!("operation=\"{operation}\" category=\"{category}\" count=1");

            assert!(
                rendered.contains(&evidence),
                "missing bounded warning {evidence}: {rendered}"
            );

            assert_eq!(
                rendered.matches(&evidence).count(),
                1,
                "warning was emitted more than once: {evidence}: {rendered}"
            );
        }
    }

    let permanent_cleanup = "operation=\"cleanup_release\" category=\"permanent_failure\" count=1";

    assert_eq!(
        rendered.matches(permanent_cleanup).count(),
        1,
        "missing or duplicated bounded warning {permanent_cleanup}: {rendered}"
    );

    assert!(
        !rendered.contains("category=\"fencing_shortfall\" count=0"),
        "zero-count fencing warning was emitted: {rendered}"
    );

    for forbidden in FOREIGN_DISPLAY_MARKERS.into_iter().chain([
        STORE_ERROR_DISPLAY_MARKER,
        "payload",
        "header",
        "credential",
        "token",
        "http://",
        "https://",
    ]) {
        assert!(!rendered.contains(forbidden), "leaked {forbidden}");
    }
}
