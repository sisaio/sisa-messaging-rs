//! Claim outcomes and transaction-bound completion evidence.

use crate::{DeadReason, InboxId};

/// Provider-controlled evidence that one transaction may mark a receipt complete.
///
/// Providers define the receipt fields and construction. Implementations should not make a receipt
/// `Clone`, `Copy`, `Default`, deserializable, or constructible from [`InboxId`]; callers obtain
/// it only from [`InboxClaimOutcome::Claimed`] and completion consumes that exact value.
pub trait InboxReceipt: Send + 'static {
    /// Returns the provider-minted durable receipt identity.
    fn id(&self) -> InboxId;

    /// Returns the number of failures recorded before this claim.
    fn recorded_failures(&self) -> u32;
}

/// Result of claiming a delivery inside the caller-owned transaction.
#[derive(Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InboxClaimOutcome<R: InboxReceipt> {
    /// The caller owns processing in this transaction and may complete the supplied receipt.
    Claimed(R),

    /// A committed transaction has already completed this delivery.
    CompletedDuplicate,

    /// Another live transaction is processing this delivery.
    InProgressDuplicate,

    /// A prior classified failure made this delivery terminal.
    DeadDuplicate {
        /// Stable terminal classification retained by the provider.
        reason: DeadReason,
    },
}
