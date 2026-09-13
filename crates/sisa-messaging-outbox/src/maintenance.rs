//! Explicit outbox maintenance and database-authoritative statistics.

use std::error::Error;
use std::future::Future;
use std::num::NonZeroU32;
use std::time::Duration;

use sisa_messaging::ErrorClassifier;

/// One bounded expiry and retention pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboxPurgeRequest {
    /// Published rows older than this database-time age may be removed.
    pub published_retention: Duration,

    /// Dead rows older than this database-time age may be removed.
    pub dead_retention: Duration,

    /// Maximum rows transitioned or deleted in each phase.
    pub batch_size: NonZeroU32,
}

impl Default for OutboxPurgeRequest {
    fn default() -> Self {
        Self {
            published_retention: Duration::from_secs(7 * 24 * 60 * 60),
            dead_retention: Duration::from_secs(30 * 24 * 60 * 60),
            batch_size: NonZeroU32::new(500).unwrap_or(NonZeroU32::MIN),
        }
    }
}

/// Confirmed work from one bounded maintenance pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OutboxPurgeReport {
    /// Non-terminal expired rows transitioned to dead.
    pub expired: u64,

    /// Old published rows deleted.
    pub published_deleted: u64,

    /// Old dead rows deleted.
    pub dead_deleted: u64,
}

/// Database-authoritative outbox levels from one observation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OutboxStats {
    /// Rows ready now or backing off.
    pub pending: u64,

    /// Expired rows awaiting maintenance transition.
    pub expired: u64,

    /// Terminal dead rows.
    pub dead: u64,

    /// Age of the oldest currently claimable row, or zero when none exists.
    pub oldest_pending_age: Duration,
}

/// Explicit bounded maintenance operations, separate from dispatcher storage.
pub trait OutboxMaintenance: Send + Sync {
    /// Maintenance error with structured retry classification and safe rendering.
    type Error: Error + ErrorClassifier + Send + Sync + 'static;

    /// Expires eligible rows and purges old terminal rows in one bounded pass.
    fn purge(
        &self,
        request: OutboxPurgeRequest,
    ) -> impl Future<Output = Result<OutboxPurgeReport, Self::Error>> + Send;

    /// Reads authoritative queue levels without scheduling background polling.
    fn stats(&self) -> impl Future<Output = Result<OutboxStats, Self::Error>> + Send;
}
