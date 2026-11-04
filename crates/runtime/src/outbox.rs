//! Publishing through the outbox, in the caller's transaction.

use std::fmt;

use sisa_messaging_core::{Message, OutboxStore, Publisher, Source};

use crate::publish::{PublishError, encode};

/// Publishes by writing outbox rows in the caller's transaction; the dispatcher sends them after
/// the commit, so a message goes out if and only if the transaction commits.
///
/// Get one from [`Bus::outbox`](crate::Bus::outbox). It borrows the transaction only while it
/// lives, so the application keeps using the transaction between publishes.
pub struct OutboxPublisher<'call, Store: OutboxStore> {
    store: &'call Store,
    source: &'call Source,
    connection: &'call mut Store::Connection,
}

impl<'call, Store: OutboxStore> OutboxPublisher<'call, Store> {
    pub(crate) fn new(
        store: &'call Store,
        source: &'call Source,
        connection: &'call mut Store::Connection,
    ) -> Self {
        Self {
            store,
            source,
            connection,
        }
    }
}

// Writes the message as a row in the caller's transaction.
impl<Store: OutboxStore> Publisher for OutboxPublisher<'_, Store> {
    type Error = PublishError;

    async fn publish<Msg: Message>(&mut self, message: &Msg) -> Result<(), PublishError> {
        let envelope = encode(self.source, message)?;
        self.store.insert(self.connection, &[envelope]).await?;
        Ok(())
    }
}

// Shows the source, never the connection.
impl<Store: OutboxStore> fmt::Debug for OutboxPublisher<'_, Store> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboxPublisher")
            .field("source", self.source)
            .finish_non_exhaustive()
    }
}
