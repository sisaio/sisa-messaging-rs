use std::error::Error;
use std::fmt;
use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sisa_messaging::{
    ContentType, ErrorClassifier, FailureKind, MessageId, MessageType, Metadata, SerializedEnvelope,
};
use sisa_messaging_outbox::{
    Claim, ClaimToken, ClaimedRecord, DispatcherSettings, ExponentialBackoff, OutboxId,
};
use uuid::Uuid;

#[path = "support/publisher.rs"]
pub(crate) mod publisher;
#[path = "support/store.rs"]
pub(crate) mod store;

pub(crate) use publisher::*;
pub(crate) use store::*;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProtocolError {
    pub(crate) kind: FailureKind,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("safe protocol failure")
    }
}

impl Error for ProtocolError {}

impl ErrorClassifier for ProtocolError {
    fn classify(&self) -> FailureKind {
        self.kind
    }
}

#[derive(Clone)]
pub(crate) struct SharedWriter(pub(crate) Arc<Mutex<Vec<u8>>>);

pub(crate) struct SharedWriteGuard(Arc<Mutex<Vec<u8>>>);

impl io::Write for SharedWriteGuard {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for SharedWriter {
    type Writer = SharedWriteGuard;

    fn make_writer(&'writer self) -> Self::Writer {
        SharedWriteGuard(Arc::clone(&self.0))
    }
}

pub(crate) fn record(index: u128, attempts: u32) -> ClaimedRecord {
    ClaimedRecord {
        claim: Claim {
            id: OutboxId::from_uuid(Uuid::from_u128(index.saturating_add(1))),
            token: ClaimToken::from_uuid(Uuid::from_u128(index.saturating_add(10_000))),
        },
        envelope: SerializedEnvelope {
            message_id: MessageId::from_uuid(Uuid::from_u128(index.saturating_add(20_000))),
            message_type: MessageType::new("test.message")
                .unwrap_or_else(|error| panic!("static message type rejected: {error}")),
            message_version: 1,
            content_type: ContentType::new("application/test")
                .unwrap_or_else(|error| panic!("static content type rejected: {error}")),
            payload: vec![u8::try_from(index % 255).unwrap_or(0)],
            metadata: Metadata::default(),
            ordering_key: None,
        },
        attempts,
    }
}

pub(crate) fn settings(capacity: usize) -> DispatcherSettings<ExponentialBackoff> {
    DispatcherSettings {
        worker_id: "test-worker".to_owned(),
        max_in_flight: NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN),
        lease: Duration::from_millis(100),
        poll_interval: Duration::from_millis(2),
        idle_poll_interval: Duration::from_millis(5),
        publish_timeout: Duration::from_secs(2),
        store_timeout: Duration::from_millis(10),
        drain_timeout: Duration::from_millis(80),
        retry_policy: ExponentialBackoff::new(
            Duration::from_millis(5),
            Duration::from_millis(20),
            NonZeroU32::new(3).unwrap_or(NonZeroU32::MIN),
        )
        .unwrap_or_else(|error| panic!("valid retry rejected: {error}")),
    }
}

pub(crate) async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("protocol condition timed out"));
}
