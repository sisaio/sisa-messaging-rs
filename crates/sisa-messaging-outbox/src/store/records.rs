//! Claimed records, fencing results, and failure transitions.

use std::num::NonZeroU32;
use std::time::Duration;

use sisa_messaging::{ErrorSummary, FailureKind, SerializedEnvelope};

use super::{ClaimToken, OutboxId};

/// A durable row and the token that fences its current lease.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Claim {
    /// Durable provider-owned row identity.
    pub id: OutboxId,

    /// Token minted afresh by the provider for this lease.
    pub token: ClaimToken,
}

/// One decoded record returned from a successful claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedRecord {
    /// Fenced ownership used for every later worker write.
    pub claim: Claim,

    /// Serialized envelope published without holding a store operation open.
    pub envelope: SerializedEnvelope,

    /// Number of publication outcomes already recorded for the row.
    pub attempts: u32,
}

/// Bounded request for one atomic claim pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimRequest {
    /// Stable diagnostic worker identity; correctness uses the claim token instead.
    pub worker_id: String,

    /// Maximum rows returned, already limited to available local capacity.
    pub limit: NonZeroU32,

    /// Lease duration applied using database time.
    pub lease: Duration,
}

/// Aggregate poison-row handling performed inside one timed claim operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PoisonReport {
    /// Unreadable rows observed in the bounded claim pass.
    pub observed: u32,

    /// Observed rows successfully transitioned to `undecodable` dead state.
    pub marked_dead: u32,
}

/// Healthy claim results plus bounded poison reporting.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClaimBatch {
    /// Healthy decoded records ready for publication.
    pub records: Vec<ClaimedRecord>,

    /// Aggregate result for unreadable rows isolated by the provider.
    pub poison: PoisonReport,
}

/// The exact claims confirmed by a fenced store operation.
///
/// A missing requested claim is a benign ownership loss, not a store error.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FencedClaims {
    /// Requested claims whose ID and token still matched.
    pub confirmed: Vec<Claim>,
}

/// Stable terminal category stored for a dead outbox row.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum DeadReason {
    /// Its new-claim deadline elapsed before another attempt began.
    Expired,

    /// Its persisted representation could not be decoded safely.
    Undecodable,

    /// Publication failed permanently.
    Permanent,

    /// The configured retry policy denied another attempt.
    Exhausted,
}

impl DeadReason {
    /// Returns the stable value used for persistence and telemetry.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::Undecodable => "undecodable",
            Self::Permanent => "permanent",
            Self::Exhausted => "exhausted",
        }
    }
}

/// Database-time transition requested after one failed publication attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FailureAction {
    /// Clear the claim and schedule another attempt after this delay.
    Retry {
        /// Database-time delay before the record becomes eligible again.
        delay: Duration,
    },

    /// Clear the claim and make the row terminal.
    Dead {
        /// Stable reason recorded for the terminal transition.
        reason: DeadReason,
    },
}

/// Classified, bounded failure data persisted with a fenced claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureRecord {
    /// Claim whose ownership must still match.
    pub claim: Claim,

    /// Explicit retry classification; stores never parse rendered text.
    pub failure_kind: FailureKind,

    /// Caller-reviewed, UTF-8-boundary-bounded diagnostic text.
    pub error: ErrorSummary,

    /// Retry or dead transition decided by the dispatcher.
    pub action: FailureAction,
}
