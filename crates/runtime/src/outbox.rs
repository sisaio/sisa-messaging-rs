//! Publishing through the outbox, in the caller's transaction.

use std::fmt;

use sisa_eventing_core::{JsonCodec, Message, OutboxStore, Publisher, Source};

use crate::publish::{PublishError, encode};

/// Publishes by writing outbox rows in the caller's transaction; the dispatcher sends them after
/// the commit, so a message goes out if and only if the transaction commits.
///
/// Get one from [`Bus::outbox`](crate::Bus::outbox). It borrows the transaction only while it
/// lives, so the application keeps using the transaction between publishes.
pub struct OutboxPublisher<'call, Store: OutboxStore, Codec = JsonCodec> {
    store: &'call Store,
    source: &'call Source,
    codec: &'call Codec,
    connection: &'call mut Store::Connection,
}

impl<'call, Store: OutboxStore, Codec> OutboxPublisher<'call, Store, Codec> {
    pub(crate) fn new(
        store: &'call Store,
        source: &'call Source,
        codec: &'call Codec,
        connection: &'call mut Store::Connection,
    ) -> Self {
        Self {
            store,
            source,
            codec,
            connection,
        }
    }
}

// Writes the message as a row in the caller's transaction.
impl<Store: OutboxStore, Codec: sisa_eventing_core::Codec> Publisher
    for OutboxPublisher<'_, Store, Codec>
{
    type Error = PublishError;

    async fn publish<Msg: Message>(&mut self, message: &Msg) -> Result<(), PublishError> {
        let envelope = encode(self.source, self.codec, message)?;
        self.store.insert(self.connection, &[envelope]).await?;
        Ok(())
    }
}

// Shows the source, never the connection.
impl<Store: OutboxStore, Codec> fmt::Debug for OutboxPublisher<'_, Store, Codec> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboxPublisher")
            .field("source", self.source)
            .finish_non_exhaustive()
    }
}
