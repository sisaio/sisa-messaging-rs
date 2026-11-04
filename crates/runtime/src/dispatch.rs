//! The dispatcher: the worker that publishes committed outbox rows to the broker.

use std::sync::Arc;
use std::time::Duration;

use sisa_messaging_core::{OutboxRecord, OutboxStore, TransportPublisher};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::RetryPolicy;

/// How the dispatcher publishes the outbox of a bus.
///
/// Every [`poll_interval`](DispatchSettings::poll_interval), it claims up to
/// [`batch_size`](DispatchSettings::batch_size) due rows for a
/// [`lease`](DispatchSettings::lease), publishes them in id order, waiting for the broker each
/// time, and marks them dispatched. A full batch is followed by the next one at once. A failed
/// publish puts its row back as [`retry`](DispatchSettings::retry) says, and after the last
/// attempt marks it failed.
///
/// ```
/// use std::time::Duration;
///
/// use sisa_messaging_runtime::DispatchSettings;
///
/// let mut settings = DispatchSettings::default();
/// settings.poll_interval = Duration::from_millis(200);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct DispatchSettings {
    /// How long the dispatcher waits after a round that found fewer rows than a batch: one
    /// second by default. This, the batch size and the lease must not be zero, or the bus
    /// doesn't start.
    pub poll_interval: Duration,
    /// How many rows one claim takes at most: 100 by default.
    pub batch_size: usize,
    /// How long a claimed row stays with its dispatcher: thirty seconds by default. A row that
    /// isn't marked by then, because its dispatcher stopped, is claimed again and published
    /// again with the same id; the dispatcher publishes no row of a batch whose lease ran out.
    pub lease: Duration,
    /// How many publishes a row gets, and how long a failed one waits: 10 attempts, one second
    /// apart at first and at most a minute apart, by default, about five minutes in all.
    pub retry: RetryPolicy,
}

// Rounds a second apart, 100 rows each, leased for thirty seconds, retried for about five minutes.
impl DispatchSettings {
    // Why these settings can't run: a zero would claim nothing, or claim rows it can't publish,
    // or poll the database without a pause.
    pub(crate) fn invalid(&self) -> Option<&'static str> {
        if self.batch_size == 0 {
            Some("`batch_size` is 0")
        } else if self.lease.is_zero() {
            Some("`lease` is 0")
        } else if self.poll_interval.is_zero() {
            Some("`poll_interval` is 0")
        } else {
            None
        }
    }
}

impl Default for DispatchSettings {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            batch_size: 100,
            lease: Duration::from_secs(30),
            retry: RetryPolicy {
                attempts: 10,
                ..RetryPolicy::default()
            },
        }
    }
}

// Whether the bus asked its workers to stop: a shutdown, or its last clone dropped.
fn stopping(stop: &watch::Receiver<()>) -> bool {
    stop.has_changed().unwrap_or(true)
}

// After a failed publish: back with a later due time, or failed after the last attempt.
async fn put_back<Store: OutboxStore>(
    store: &Store,
    settings: &DispatchSettings,
    record: &OutboxRecord,
    error: &str,
) {
    let attempt = record.attempts.saturating_add(1);
    let id = record.envelope.id;
    let marked = if settings.retry.is_last(attempt) {
        tracing::error!(%id, attempt, error, "could not publish an outbox row; it is marked failed");
        store.mark_failed(record, error).await
    } else {
        tracing::warn!(%id, attempt, error, "could not publish an outbox row; it is retried");
        store
            .mark_retry(record, error, settings.retry.delay(attempt))
            .await
    };
    if let Err(mark_error) = marked {
        tracing::warn!(%id, error = %mark_error, "could not mark an outbox row; it comes back after its lease");
    }
}

// One round: claim, publish in id order, mark. True when it published a full batch, so more
// rows may be due.
async fn round<Store, Transport>(
    store: &Store,
    transport: &Transport,
    settings: &DispatchSettings,
    stop: &watch::Receiver<()>,
) -> bool
where
    Store: OutboxStore,
    Transport: TransportPublisher,
{
    // Measured from before the claim, so the lease runs out here no later than in the database.
    let claimed_at = Instant::now();
    let records = match store.claim(settings.batch_size, settings.lease).await {
        Ok(records) => records,
        Err(error) => {
            tracing::warn!(%error, "could not claim outbox rows");
            return false;
        }
    };

    // The lease runs out here before it does in the database, so a publish still waiting then is
    // given up: another dispatcher may hold the row by now.
    let deadline = claimed_at + settings.lease;
    let mut published = Vec::with_capacity(records.len());
    for record in &records {
        // Past the lease another dispatcher may hold the rest, and on a stop they wait for
        // theirs to run out.
        if stopping(stop) || claimed_at.elapsed() >= settings.lease {
            break;
        }
        match tokio::time::timeout_at(deadline, transport.publish(&record.envelope)).await {
            Ok(Ok(())) => published.push(record.clone()),
            Ok(Err(error)) => put_back(store, settings, record, &error.to_string()).await,
            Err(_elapsed) => break,
        }
    }

    if !published.is_empty()
        && let Err(error) = store.mark_dispatched(&published).await
    {
        tracing::warn!(%error, rows = published.len(), "could not mark outbox rows dispatched; they are published again after their lease");
    }
    published.len() == settings.batch_size
}

/// Runs rounds until the bus stops.
pub(crate) async fn run<Store, Transport>(
    store: Arc<Store>,
    transport: Arc<Transport>,
    settings: DispatchSettings,
    mut stop: watch::Receiver<()>,
) where
    Store: OutboxStore,
    Transport: TransportPublisher,
{
    loop {
        if round(&*store, &*transport, &settings, &stop).await && !stopping(&stop) {
            continue;
        }
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            () = tokio::time::sleep(settings.poll_interval) => {}
        }
    }
}
