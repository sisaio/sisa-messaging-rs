//! Claim outcomes and transaction-bound completion evidence.

use crate::{DeadReason, InboxId};

/// Evidence that this transaction may mark one receipt complete.
///
/// Providers mint a receipt only from a successful [`InboxClaimOutcome::Claimed`] result. It is
/// intentionally non-`Clone`: completion consumes the evidence so callers cannot accidentally
/// complete the same claim twice.
#[derive(Debug, Eq, PartialEq)]
pub struct ClaimedReceipt {
    /// Provider-minted receipt identity.
    pub id: InboxId,

    /// Number of failures recorded before this claim.
    pub recorded_failures: u32,
}

/// Result of claiming a delivery inside the caller-owned transaction.
#[derive(Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InboxClaimOutcome {
    /// The caller owns processing in this transaction and may complete the supplied receipt.
    Claimed(ClaimedReceipt),

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
