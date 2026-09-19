//! Explicit bounded terminal receipt maintenance.

use std::error::Error;
use std::future::Future;
use std::num::NonZeroU32;
use std::time::Duration;

use sisa_messaging::ErrorClassifier;

/// One bounded pass that may delete only terminal inbox receipts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InboxPurgeRequest {
    /// Completed receipts older than this age may be deleted; `None` disables this phase.
    pub completed_retention: Option<Duration>,

    /// Dead receipts older than this age may be deleted; `None` disables this phase.
    pub dead_retention: Option<Duration>,

    /// Maximum deletions in each terminal-state phase.
    pub batch_size: NonZeroU32,
}

impl Default for InboxPurgeRequest {
    fn default() -> Self {
        Self {
            completed_retention: Some(Duration::from_secs(7 * 24 * 60 * 60)),
            dead_retention: Some(Duration::from_secs(30 * 24 * 60 * 60)),
            batch_size: NonZeroU32::new(500).unwrap_or(NonZeroU32::MIN),
        }
    }
}

/// Confirmed deletions from one bounded terminal-maintenance pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InboxPurgeReport {
    /// Completed receipts deleted in this pass.
    pub completed_deleted: u64,

    /// Dead receipts deleted in this pass.
    pub dead_deleted: u64,
}

/// Database-authoritative inbox state levels from one observation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InboxStats {
    /// Receipts observed but not yet failed or completed.
    pub pending: u64,

    /// Receipts with one or more recorded transient failures.
    pub retrying: u64,

    /// Receipts completed with their caller's business effects.
    pub completed: u64,

    /// Receipts made terminal by classified failure.
    pub dead: u64,
}

/// Explicit terminal retention and diagnostic operations.
///
/// This capability does not schedule work. Applications choose when to invoke bounded passes,
/// yield between full passes, and honor their own cancellation policy.
pub trait InboxMaintenance: Send + Sync + 'static {
    /// Provider error with structured retry classification and safe rendering.
    type Error: Error + ErrorClassifier + Send + Sync + 'static;

    /// Deletes only eligible completed and dead receipts in one bounded pass.
    fn purge(
        &self,
        request: InboxPurgeRequest,
    ) -> impl Future<Output = Result<InboxPurgeReport, Self::Error>> + Send;

    /// Reads authoritative inbox state levels without hidden polling or scheduling.
    fn stats(&self) -> impl Future<Output = Result<InboxStats, Self::Error>> + Send;
}
