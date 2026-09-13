use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use sisa_messaging::{ErrorClassifier, FailureKind, Publisher, SerializedEnvelope};
use tokio::sync::watch;

use super::ProtocolError;

pub(crate) const PUBLISH_SUCCESS: u8 = 0;
pub(crate) const PUBLISH_TRANSIENT: u8 = 1;
pub(crate) const PUBLISH_PERMANENT: u8 = 2;
pub(crate) const PUBLISH_GATE: u8 = 3;
pub(crate) const PUBLISH_PANIC: u8 = 4;
pub(crate) const PUBLISH_MIXED_GATE: u8 = 5;

#[derive(Clone, Copy, Debug)]
pub(crate) struct SecretPublisherError;

impl fmt::Display for SecretPublisherError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "foreign-display-marker-alpha; foreign-display-marker-beta; foreign-display-marker-gamma",
        )
    }
}

impl Error for SecretPublisherError {}

impl ErrorClassifier for SecretPublisherError {
    fn classify(&self) -> FailureKind {
        FailureKind::Transient
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SecretPublisher;

impl Publisher for SecretPublisher {
    type Error = SecretPublisherError;

    async fn publish(&self, _envelope: &SerializedEnvelope) -> Result<(), Self::Error> {
        Err(SecretPublisherError)
    }
}

struct ActiveGuard {
    active: Arc<AtomicUsize>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub(crate) struct FakePublisher {
    behavior: Arc<AtomicU8>,
    pub(crate) calls: Arc<AtomicUsize>,
    pub(crate) active: Arc<AtomicUsize>,
    pub(crate) maximum: Arc<AtomicUsize>,
    gate: Arc<watch::Sender<bool>>,
}

impl FakePublisher {
    pub(crate) fn new(behavior: u8) -> Self {
        Self {
            behavior: Arc::new(AtomicU8::new(behavior)),
            calls: Arc::new(AtomicUsize::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
            maximum: Arc::new(AtomicUsize::new(0)),
            gate: Arc::new(watch::channel(false).0),
        }
    }

    pub(crate) fn release(&self) {
        self.gate.send_replace(true);
    }

    pub(crate) fn gate_waiter_count(&self) -> usize {
        self.gate.receiver_count()
    }

    async fn wait_until_released(&self) {
        let mut gate = self.gate.subscribe();
        while !*gate.borrow() {
            if gate.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Publisher for FakePublisher {
    type Error = ProtocolError;

    async fn publish(&self, envelope: &SerializedEnvelope) -> Result<(), Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        let _guard = ActiveGuard {
            active: Arc::clone(&self.active),
        };

        match self.behavior.load(Ordering::SeqCst) {
            PUBLISH_SUCCESS => Ok(()),
            PUBLISH_TRANSIENT => Err(ProtocolError {
                kind: FailureKind::Transient,
            }),
            PUBLISH_PERMANENT => Err(ProtocolError {
                kind: FailureKind::Permanent,
            }),
            PUBLISH_GATE => {
                self.wait_until_released().await;
                Ok(())
            }
            PUBLISH_PANIC => panic!("intentional publisher panic"),
            PUBLISH_MIXED_GATE => {
                self.wait_until_released().await;
                match envelope.payload.first().copied().unwrap_or_default() {
                    0 => Ok(()),
                    1 => Err(ProtocolError {
                        kind: FailureKind::Transient,
                    }),
                    _ => std::future::pending().await,
                }
            }
            _ => unreachable!("test behavior is a closed constant"),
        }
    }
}
