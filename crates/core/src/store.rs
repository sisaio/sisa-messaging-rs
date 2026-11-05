//! The store contracts: what a database crate implements so the runtime can keep an outbox and
//! an inbox; the partition contract is in `partition.rs`.

use std::error::Error;
use std::ops::DerefMut;

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

/// The inbox table of one database: a receipt per endpoint and message handled, written in the
/// handler's transaction, so a redelivered message is acknowledged without running its handler
/// again.
///
/// Every store keeps these promises:
///
/// - The receipt and the handler's work share the transaction from [`begin`](InboxStore::begin),
///   so they commit or roll back together.
/// - Dropping a transaction without [`commit`](InboxStore::commit) rolls it back.
/// - [`commit`](InboxStore::commit) fails when the transaction can't commit, such as one a failed
///   statement left aborted, rather than rolling it back without an error.
/// - [`record`](InboxStore::record) finds a duplicate however long after the first delivery it
///   comes, while the first receipt is kept.
pub trait InboxStore: Send + Sync + 'static {
    /// A transaction on the store's database, such as `sqlx::PgTransaction<'static>`, which the
    /// handler gets for its own work. It dereferences to the connection the handler's queries run
    /// on, such as `sqlx::PgConnection`.
    type Transaction: DerefMut + Send;

    /// Begins a transaction on a connection of the store's own.
    ///
    /// # Errors
    ///
    /// [`StoreError`] when the database can't be reached.
    fn begin(&self) -> impl Future<Output = Result<Self::Transaction, StoreError>> + Send;

    /// Writes the receipt of `envelope` on `endpoint` in `transaction`. Returns `false`, and
    /// writes nothing, when the endpoint has a receipt for this message already.
    ///
    /// # Errors
    ///
    /// [`StoreError`] when the database didn't take the receipt.
    fn record(
        &self,
        transaction: &mut Self::Transaction,
        endpoint: &str,
        envelope: &Envelope,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Commits `transaction`: the receipt with the handler's work.
    ///
    /// # Errors
    ///
    /// [`StoreError`] when the commit failed. Its outcome is then unknown: the database may have
    /// committed before the error, such as when the connection drops after the commit was sent.
    fn commit(
        &self,
        transaction: Self::Transaction,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}
