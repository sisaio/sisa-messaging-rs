//! Renewal timing and fenced lease extension.

use sisa_messaging::ErrorClassifier;
use std::time::Duration;
use tokio::time::Instant;
use tracing::Instrument;

use crate::{Claim, OutboxStore};
use crate::{DispatcherSettings, RetryPolicy};

use super::OutboxRunReport;
use super::outcomes::StoreCall;
use super::state::State;

pub(crate) enum RenewalOutcome<E> {
    Completed { lost: usize },
    Failed { permanent_error: Option<E> },
}

pub(crate) enum RenewalTurn<E> {
    NotDue,
    Finished(RenewalOutcome<E>),
}

pub(crate) async fn extend<S: OutboxStore>(
    store: &S,
    claims: &[Claim],
    lease: Duration,
    timeout: Duration,
) -> (Instant, StoreCall<S::Error>) {
    let started = Instant::now();
    let result = tokio::time::timeout(timeout, store.extend_lease(claims, lease))
        .instrument(tracing::debug_span!(
            target: "messaging.outbox",
            "outbox.persist_outcome",
            operation = "extend_lease"
        ))
        .await;
    let call = match result {
        Ok(Ok(matches)) => StoreCall::Completed(matches),
        Ok(Err(error)) => StoreCall::Failed(error),
        Err(_) => StoreCall::TimedOut,
    };

    (started, call)
}

pub(crate) fn renewal_deadline(started: Instant, offset: Duration) -> Instant {
    started + offset
}

pub(crate) fn finish<E: ErrorClassifier>(
    started: Instant,
    result: StoreCall<E>,
    claims: &[Claim],
    renewal_offset: Duration,
    state: &mut State,
    report: &mut OutboxRunReport,
) -> RenewalOutcome<E> {
    match result {
        StoreCall::Completed(matches) => {
            let lost = state.apply_renewal(
                claims,
                &matches.confirmed,
                renewal_deadline(started, renewal_offset),
            );
            report.fenced += lost as u64;
            report.aborted += lost as u64;
            RenewalOutcome::Completed { lost }
        }
        StoreCall::Failed(error) => {
            let permanent = !error.classify().is_retryable();
            report.store_failures += 1;
            report.aborted += claims.len() as u64;
            state.mark_release(claims);
            RenewalOutcome::Failed {
                permanent_error: permanent.then_some(error),
            }
        }
        StoreCall::TimedOut => {
            report.store_failures += 1;
            report.aborted += claims.len() as u64;
            state.mark_release(claims);
            RenewalOutcome::Failed {
                permanent_error: None,
            }
        }
    }
}

pub(crate) async fn renew_due<S, R>(
    store: &S,
    settings: &DispatcherSettings<R>,
    state: &mut State,
    report: &mut OutboxRunReport,
) -> RenewalTurn<S::Error>
where
    S: OutboxStore,
    R: RetryPolicy,
{
    let due = state.due_renewals(Instant::now());
    if due.is_empty() {
        return RenewalTurn::NotDue;
    }
    let (started, result) = extend(store, &due, settings.lease, settings.store_timeout).await;
    RenewalTurn::Finished(finish(
        started,
        result,
        &due,
        settings.renewal_offset(),
        state,
        report,
    ))
}
