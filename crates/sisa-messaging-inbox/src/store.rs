//! Transactional inbox persistence capability.

use std::error::Error;
use std::future::Future;
use std::num::NonZeroU32;

use sisa_messaging::ErrorClassifier;

use crate::{InboxClaimOutcome, InboxFailure, InboxFailureOutcome, InboxReceipt, InboxRecord};

/// Persistence operations used by manual inbox callers and the consumer framework.
///
/// `claim` and `complete` operate in the caller's transaction. On handler failure, the caller
/// must roll that transaction back before invoking `fail`, which records the classified outcome
/// through the provider's independent operation. Implementations must not hold the caller's
/// transaction across broker or other network I/O.
pub trait InboxStore<Tx: Send>: Send + Sync + 'static {
    /// Provider error with structured retry classification and safe rendering.
    type Error: Error + ErrorClassifier + Send + Sync + 'static;

    /// Provider-controlled evidence returned by a successful claim.
    ///
    /// Its fields and construction remain provider-controlled; completion accepts only this exact
    /// associated receipt type and consumes it.
    type Receipt: InboxReceipt;

    /// Configured bound used when recording transient failures.
    fn max_attempts(&self) -> NonZeroU32;

    /// Claims the record inside `transaction` without increasing its recorded failure count.
    fn claim(
        &self,
        transaction: &mut Tx,
        record: &InboxRecord,
    ) -> impl Future<Output = Result<InboxClaimOutcome<Self::Receipt>, Self::Error>> + Send;

    /// Marks the exact claimed receipt complete inside the transaction owning its business effects.
    ///
    /// Completion consumes the provider's associated receipt. If completion returns an error, the
    /// caller must roll back the transaction and leave the delivery available for redelivery.
    fn complete(
        &self,
        transaction: &mut Tx,
        receipt: Self::Receipt,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Atomically records a classified handler failure after the caller rolled back its transaction.
    ///
    /// Providers leave completed and already-dead receipts immutable. For an active receipt they
    /// increment the recorded failure count exactly once with saturating arithmetic, make a
    /// permanent failure dead immediately, and make a transient failure dead only when the
    /// incremented count reaches [`Self::max_attempts`].
    fn fail(
        &self,
        record: &InboxRecord,
        failure: InboxFailure,
    ) -> impl Future<Output = Result<InboxFailureOutcome, Self::Error>> + Send;
}
