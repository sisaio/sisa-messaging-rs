use std::sync::{Arc, Mutex};

use sisa_messaging_outbox::OutboxDispatcher;
use tokio_util::sync::CancellationToken;

use super::support::*;

const FOREIGN_DISPLAY_MARKERS: [&str; 3] = [
    "foreign-display-marker-alpha",
    "foreign-display-marker-beta",
    "foreign-display-marker-gamma",
];

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
    tracing::subscriber::set_global_default(subscriber)
        .unwrap_or_else(|error| panic!("test subscriber failed: {error}"));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| panic!("test runtime failed: {error}"));

    runtime.block_on(async {
        let mut secret_record = record(1, 0);
        secret_record.envelope.payload = b"TOP_SECRET_PAYLOAD".to_vec();
        let store = FakeStore::new(vec![secret_record]);
        let cancellation = CancellationToken::new();
        let dispatcher = OutboxDispatcher::new(store.clone(), SecretPublisher, settings(1))
            .unwrap_or_else(|error| panic!("settings rejected: {error}"));
        let run_cancel = cancellation.clone();
        let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

        wait_for(|| !store.lock().failures.is_empty()).await;
        cancellation.cancel();
        task.await
            .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
            .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
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
    for forbidden in FOREIGN_DISPLAY_MARKERS
        .into_iter()
        .chain(["TOP_SECRET_PAYLOAD"])
    {
        assert!(!rendered.contains(forbidden), "leaked {forbidden}");
    }
}
