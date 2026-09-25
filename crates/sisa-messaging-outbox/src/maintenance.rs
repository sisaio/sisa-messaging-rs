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

impl OutboxStats {
    /// Records this database-authoritative snapshot through the configured global meter provider.
    ///
    /// Applications should designate one observer per database/schema to fetch [`Self`] through
    /// [`OutboxMaintenance::stats`] and invoke this method. This library never polls, schedules a
    /// background task, or installs a provider, preventing duplicate application-owned observers.
    pub fn record_metrics(&self) {
        let snapshot = metric_snapshot(self);

        crate::telemetry::record_stats(
            snapshot.message_counts,
            snapshot.oldest_pending_age_seconds,
        );
    }
}

#[derive(Debug, PartialEq)]
struct MetricSnapshot {
    message_counts: [(&'static str, u64); 3],

    oldest_pending_age_seconds: f64,
}

fn metric_snapshot(stats: &OutboxStats) -> MetricSnapshot {
    let oldest_pending_age_seconds = if stats.pending == 0 {
        0.0
    } else {
        stats.oldest_pending_age.as_secs_f64()
    };

    MetricSnapshot {
        message_counts: [
            ("pending", stats.pending),
            ("expired", stats.expired),
            ("dead", stats.dead),
        ],
        oldest_pending_age_seconds,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_snapshot_maps_every_state_and_resets_empty_pending_age() {
        let populated = OutboxStats {
            pending: 3,
            expired: 5,
            dead: 7,
            oldest_pending_age: Duration::from_millis(1_250),
        };

        assert_eq!(
            metric_snapshot(&populated),
            MetricSnapshot {
                message_counts: [("pending", 3), ("expired", 5), ("dead", 7)],
                oldest_pending_age_seconds: 1.25,
            }
        );

        let inconsistent_empty = OutboxStats {
            pending: 0,
            oldest_pending_age: Duration::from_secs(99),
            ..populated
        };

        assert_eq!(
            metric_snapshot(&inconsistent_empty),
            MetricSnapshot {
                message_counts: [("pending", 0), ("expired", 5), ("dead", 7)],
                oldest_pending_age_seconds: 0.0,
            }
        );

        populated.record_metrics();
        inconsistent_empty.record_metrics();
    }
}
