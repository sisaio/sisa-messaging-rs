//! The partition maintenance worker: partitions ahead, and the retention.

use std::time::Duration;

use sisa_eventing_core::{Date, Expiry, PartitionInterval, PartitionStore, StoreError, Timestamp};
use tokio::sync::watch;

// How often the worker runs.
const EVERY: Duration = Duration::from_secs(3_600);

/// Who keeps a table's partitions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum PartitionMode {
    /// The bus's maintenance worker, the default.
    #[default]
    Managed,
    /// Someone else, such as pg_partman or the team's own jobs: the bus starts no worker.
    External,
}

/// How the bus keeps the partitions of one table, such as the outbox or the inbox.
///
/// Once an hour, while no other instance does, the worker creates the partitions for the current
/// interval and the [`ahead`](PartitionSettings::ahead) ones after it. With a
/// [`retention_days`](PartitionSettings::retention_days), it also expires the partitions that
/// hold only older days, as [`on_expiry`](PartitionSettings::on_expiry) says, except an outbox
/// partition that still holds pending or failed rows: it warns about those instead, and about a
/// default partition that holds rows.
///
/// ```
/// use sisa_eventing_runtime::PartitionSettings;
///
/// let mut outbox = PartitionSettings::default();
/// outbox.retention_days = Some(90);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PartitionSettings {
    /// Who keeps the partitions: the bus by default.
    pub partitions: PartitionMode,
    /// How much time one new partition holds: a day by default. Partitions that exist keep
    /// theirs; new ones start where they end.
    pub interval: PartitionInterval,
    /// How many partitions exist after the current one: 7 by default.
    pub ahead: u32,
    /// After how many days a partition expires; `None`, the default, keeps every partition.
    /// Keep the inbox's longer than the longest redelivery, or a very late one is handled again.
    pub retention_days: Option<u32>,
    /// What expiring does: detach the partition by default, or drop it.
    pub on_expiry: Expiry,
}

// Kept by the bus, a partition a day, 7 ahead, none expired.
impl Default for PartitionSettings {
    fn default() -> Self {
        Self {
            partitions: PartitionMode::Managed,
            interval: PartitionInterval::Day,
            ahead: 7,
            retention_days: None,
            on_expiry: Expiry::Detach,
        }
    }
}

// One run, under the store's lock: partitions ahead, then the retention.
async fn maintain<Store: PartitionStore>(
    store: &Store,
    settings: &PartitionSettings,
    today: Date,
) -> Result<(), StoreError> {
    let Some(_lock) = store.try_lock().await? else {
        tracing::debug!("another instance maintains the partitions");
        return Ok(());
    };

    let count = settings.ahead.saturating_add(1);
    if let Err(error) = store
        .ensure_partitions(settings.interval, today, count)
        .await
    {
        tracing::error!(%error, "could not create every partition ahead");
    }
    let rows = store.default_rows().await?;
    if rows > 0 {
        tracing::warn!(
            rows,
            "the default partition holds rows whose time has no partition; move them to create one"
        );
    }

    let Some(days) = settings.retention_days else {
        return Ok(());
    };
    for partition in store.expirable(today.add_days(-i64::from(days))).await? {
        if partition.pending > 0 || partition.failed > 0 {
            tracing::warn!(
                partition = %partition.name,
                pending = partition.pending,
                failed = partition.failed,
                "the partition is past its retention but still holds pending or failed rows; it is kept"
            );
            continue;
        }
        match store.expire(&partition, settings.on_expiry).await {
            Ok(()) => {
                tracing::info!(partition = %partition.name, mode = ?settings.on_expiry, "expired a partition");
            }
            Err(error) => {
                tracing::error!(partition = %partition.name, %error, "could not expire a partition");
            }
        }
    }
    Ok(())
}

// Maintains the store's partitions now and every hour after, until the bus stops: `stop`
// changes on shutdown and closes when the last clone of the bus drops.
pub(crate) async fn run<Store: PartitionStore>(
    store: Store,
    settings: PartitionSettings,
    mut stop: watch::Receiver<()>,
) {
    loop {
        if let Err(error) = maintain(&store, &settings, Timestamp::now().date()).await {
            tracing::error!(%error, "could not maintain the partitions");
        }
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            () = tokio::time::sleep(EVERY) => {}
        }
    }
}
