//! The store contracts: what a database crate implements so the runtime can keep an outbox.

use std::error::Error;

use crate::Envelope;

/// Why a store couldn't do what was asked.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// The database refused the statement, or couldn't be reached.
    #[error("{context}: {source}")]
    Database {
        /// What the store was doing, such as `inserting outbox rows`.
        context: &'static str,
        /// The client's error.
        source: Box<dyn Error + Send + Sync>,
    },
}

/// The outbox table of one database: rows written in the caller's transaction, which the
/// dispatcher publishes after the commit.
///
/// Every store keeps these promises:
///
/// - [`insert`](OutboxStore::insert) writes on the connection it is given, never on one of its
///   own, so the rows commit or roll back with the caller's transaction.
/// - A row holds the whole envelope, so the dispatcher publishes it as it was written.
pub trait OutboxStore: Send + Sync + 'static {
    /// The connection the caller's transaction runs on, such as `sqlx::PgConnection`. A bus
    /// with this store takes only this type, so a transaction from another database doesn't
    /// compile.
    type Connection: ?Sized + Send;

    /// Writes a pending row for each envelope on `connection`, inside the caller's transaction.
    ///
    /// # Errors
    ///
    /// [`StoreError`] when the database didn't take the rows. The caller's transaction may then
    /// be unusable, as the database decides.
    fn insert(
        &self,
        connection: &mut Self::Connection,
        envelopes: &[Envelope],
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}
