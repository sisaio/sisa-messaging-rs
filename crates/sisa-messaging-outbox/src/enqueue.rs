//! Transactional enqueue capability.

use std::error::Error;
use std::future::Future;
use std::time::SystemTime;

use sisa_messaging::{Envelope, ErrorClassifier, Message, Serializer};

use crate::OutboxId;

/// Optional enqueue policy interpreted using database time by the store.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EnqueueOptions {
    /// Deadline after which a new claim must not start.
    ///
    /// An attempt claimed before this time may still finish afterward.
    pub expires_at: Option<SystemTime>,
}

/// Writes a typed envelope into a transaction owned by the caller.
///
/// Implementations serialize before awaiting persistence, execute one insert, and never commit,
/// roll back, retry, sleep, or publish. A duplicate message identity is an error and the caller
/// remains responsible for rolling back the transaction.
pub trait OutboxEnqueue<Tx>: Send + Sync {
    /// Store error whose display and source chain are safe to return without payload data.
    type Error: Error + ErrorClassifier + Send + Sync + 'static;

    /// Serializer owned by the concrete store.
    type Serializer: Send + Sync;

    /// Serializes and inserts one envelope through the caller's transaction.
    fn enqueue<M>(
        &self,
        transaction: &mut Tx,
        envelope: &Envelope<M>,
        options: EnqueueOptions,
    ) -> impl Future<Output = Result<OutboxId, Self::Error>> + Send
    where
        M: Message,
        Self::Serializer: Serializer<M>;
}
