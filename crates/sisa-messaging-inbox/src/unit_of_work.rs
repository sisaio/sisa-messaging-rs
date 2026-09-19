//! Provider-owned transaction lifecycle capability.

use std::error::Error;
use std::future::Future;

use sisa_messaging::ErrorClassifier;

/// Begins and consumes provider transactions for framework-managed inbox processing.
///
/// The consumer framework must commit before acknowledgement and roll back before recording a
/// classified handler failure. Consuming terminal operations prevent a transaction from being
/// reused after either outcome.
pub trait InboxUnitOfWork: Send + Sync + 'static {
    /// Provider transaction used with its matching [`crate::InboxStore`] implementation.
    type Transaction: Send + 'static;

    /// Provider error with structured retry classification and safe rendering.
    type Error: Error + ErrorClassifier + Send + Sync + 'static;

    /// Begins one provider transaction.
    fn begin(&self) -> impl Future<Output = Result<Self::Transaction, Self::Error>> + Send;

    /// Consumes and commits a transaction before broker settlement.
    fn commit(
        &self,
        transaction: Self::Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Consumes and rolls back a transaction before failure recording.
    fn rollback(
        &self,
        transaction: Self::Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
