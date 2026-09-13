//! Focused dispatcher storage capability.

mod ids;
mod records;

use std::error::Error;
use std::future::Future;
use std::time::Duration;

use sisa_messaging::ErrorClassifier;

pub use ids::{ClaimToken, OutboxId};
pub use records::{
    Claim, ClaimBatch, ClaimRequest, ClaimedRecord, DeadReason, FailureAction, FailureRecord,
    FencedClaims, PoisonReport,
};

/// Hot-path persistence operations required by [`crate::OutboxDispatcher`].
///
/// Each implementation bounds its query by the supplied slice/request, uses short internal
/// transactions, and returns only after those transactions finish. Publication is never passed
/// into this capability.
pub trait OutboxStore: Send + Sync + 'static {
    /// Store error with structured retry classification and safe rendering.
    type Error: Error + ErrorClassifier + Send + Sync + 'static;

    /// Claims no more than the requested capacity and isolates poison rows.
    fn claim(
        &self,
        request: ClaimRequest,
    ) -> impl Future<Output = Result<ClaimBatch, Self::Error>> + Send;

    /// Marks broker-acknowledged claims published and returns exact fenced matches.
    fn complete(
        &self,
        claims: &[Claim],
    ) -> impl Future<Output = Result<FencedClaims, Self::Error>> + Send;

    /// Records classified retry/dead transitions and returns exact fenced matches.
    fn fail(
        &self,
        failures: &[FailureRecord],
    ) -> impl Future<Output = Result<FencedClaims, Self::Error>> + Send;

    /// Makes still-owned claims immediately available without consuming an attempt.
    fn release(
        &self,
        claims: &[Claim],
    ) -> impl Future<Output = Result<FencedClaims, Self::Error>> + Send;

    /// Extends still-owned leases using database time without rotating their tokens.
    fn extend_lease(
        &self,
        claims: &[Claim],
        lease: Duration,
    ) -> impl Future<Output = Result<FencedClaims, Self::Error>> + Send;
}
