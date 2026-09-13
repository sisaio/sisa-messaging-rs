//! Timed bounded claims and claim scheduling.

use std::num::NonZeroU32;
use std::time::Duration;
use tokio::time::Instant;
use tracing::Instrument;

use sisa_messaging::ErrorClassifier;

use crate::{ClaimBatch, ClaimRequest, DeadReason, DispatcherSettings, OutboxStore, RetryPolicy};

use super::OutboxRunReport;
use super::state::State;

pub(crate) enum ClaimCall<E> {
    Completed(ClaimBatch),
    Failed(E),
    TimedOut,
}

pub(crate) async fn claim<S: OutboxStore>(
    store: &S,
    worker_id: &str,
    available: usize,
    lease: Duration,
    timeout: Duration,
) -> (Instant, ClaimCall<S::Error>) {
    let started = Instant::now();
    let limit = u32::try_from(available).unwrap_or(u32::MAX).max(1);
    let request = ClaimRequest {
        worker_id: worker_id.to_owned(),
        limit: NonZeroU32::new(limit).unwrap_or(NonZeroU32::MIN),
        lease,
    };

    let result = tokio::time::timeout(timeout, store.claim(request))
        .instrument(tracing::debug_span!(
            target: "messaging.outbox",
            "outbox.claim"
        ))
        .await;
    let call = match result {
        Ok(Ok(batch)) => ClaimCall::Completed(batch),
        Ok(Err(error)) => ClaimCall::Failed(error),
        Err(_) => ClaimCall::TimedOut,
    };

    (started, call)
}

pub(crate) fn finish<E, R>(
    started: Instant,
    result: ClaimCall<E>,
    settings: &DispatcherSettings<R>,
    state: &mut State,
    report: &mut OutboxRunReport,
    next_claim: &mut Instant,
) -> Option<E>
where
    E: ErrorClassifier,
    R: RetryPolicy,
{
    match result {
        ClaimCall::Completed(batch) => {
            let completed = Instant::now();
            let returned = batch.records.len();
            let delay = if batch.records.is_empty() {
                settings.idle_poll_interval
            } else {
                settings.poll_interval
            };
            *next_claim = completed.checked_add(delay).unwrap_or(completed);
            report.poisoned += u64::from(batch.poison.observed);
            report.dead += u64::from(batch.poison.marked_dead);
            if batch.poison.marked_dead > 0 {
                crate::telemetry::dead(
                    usize::try_from(batch.poison.marked_dead).unwrap_or(usize::MAX),
                    DeadReason::Undecodable,
                );
            }
            if batch.poison.observed > 0 {
                tracing::warn!(
                    target: "messaging.outbox",
                    observed = batch.poison.observed,
                    marked_dead = batch.poison.marked_dead,
                    "poison rows isolated during claim"
                );
            }
            let renewal_at = completed
                .checked_add(settings.renewal_offset())
                .unwrap_or(completed);
            let lease_safe_until = started.checked_add(settings.lease).unwrap_or(started);
            let insertion = state.insert_claimed(batch.records, renewal_at, lease_safe_until);
            report.claimed += insertion.inserted as u64;
            crate::telemetry::claimed(insertion.inserted);
            if insertion.inserted < returned {
                report.store_failures += 1;
                tracing::error!(
                    target: "messaging.outbox",
                    inserted = insertion.inserted,
                    retained_rejected = insertion.retained_rejected,
                    dropped_to_expiry = insertion.dropped_to_expiry,
                    duplicates = insertion.duplicates,
                    "store returned duplicate or excess claims"
                );
            }
            None
        }
        ClaimCall::Failed(error) => {
            report.store_failures += 1;
            *next_claim = Instant::now() + settings.poll_interval;
            if error.classify().is_retryable() {
                tracing::warn!(
                    target: "messaging.outbox",
                    operation = "claim",
                    "transient store failure; claim backed off"
                );
                None
            } else {
                Some(error)
            }
        }
        ClaimCall::TimedOut => {
            report.store_failures += 1;
            *next_claim = Instant::now() + settings.poll_interval;
            tracing::warn!(
                target: "messaging.outbox",
                operation = "claim",
                "transient store failure; claim backed off"
            );
            None
        }
    }
}

pub(crate) async fn available<S, R>(
    store: &S,
    settings: &DispatcherSettings<R>,
    state: &mut State,
    report: &mut OutboxRunReport,
    next_claim: &mut Instant,
) -> Option<S::Error>
where
    S: OutboxStore,
    R: RetryPolicy,
{
    let (started, result) = claim(
        store,
        &settings.worker_id,
        state.available(),
        settings.lease,
        settings.store_timeout,
    )
    .await;
    finish(started, result, settings, state, report, next_claim)
}
