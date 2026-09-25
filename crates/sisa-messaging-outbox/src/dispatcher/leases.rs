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

pub(crate) enum StoreSafety<E> {
    Safe,
    Renewed(RenewalOutcome<E>),
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

pub(crate) fn finish<E: ErrorClassifier>(
    started: Instant,
    result: StoreCall<E>,
    claims: &[Claim],
    lease: Duration,
    renewal_offset: Duration,
    state: &mut State,
    report: &mut OutboxRunReport,
) -> RenewalOutcome<E> {
    match result {
        StoreCall::Completed(matches) => {
            let completed = Instant::now();
            let renewal_at = completed.checked_add(renewal_offset).unwrap_or(completed);
            let lease_safe_until = started.checked_add(lease).unwrap_or(started);

            let loss =
                state.apply_renewal(claims, &matches.confirmed, renewal_at, lease_safe_until);

            report.fenced += loss.total as u64;
            report.aborted += loss.retired_publishers as u64;

            RenewalOutcome::Completed { lost: loss.total }
        }
        StoreCall::Failed(error) => {
            let permanent = !error.classify().is_retryable();
            report.store_failures += 1;
            report.aborted += state.mark_release(claims) as u64;

            RenewalOutcome::Failed {
                permanent_error: permanent.then_some(error),
            }
        }
        StoreCall::TimedOut => {
            report.store_failures += 1;
            report.aborted += state.mark_release(claims) as u64;

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
        settings.lease,
        settings.renewal_offset(),
        state,
        report,
    ))
}

pub(crate) async fn protect_store_call<S, R>(
    store: &S,
    settings: &DispatcherSettings<R>,
    state: &mut State,
    report: &mut OutboxRunReport,
) -> StoreSafety<S::Error>
where
    S: OutboxStore,
    R: RetryPolicy,
{
    let blocking = state.store_call_blockers(Instant::now(), settings.store_timeout);

    if blocking.is_empty() {
        return StoreSafety::Safe;
    }

    let (started, result) = extend(store, &blocking, settings.lease, settings.store_timeout).await;

    let outcome = finish(
        started,
        result,
        &blocking,
        settings.lease,
        settings.renewal_offset(),
        state,
        report,
    );

    if matches!(outcome, RenewalOutcome::Completed { .. }) {
        let still_blocking = state.store_call_blockers(Instant::now(), settings.store_timeout);

        if !still_blocking.is_empty() {
            let retiring = state.retire_publishers(&still_blocking);

            tracing::warn!(
                target: "messaging.outbox",
                blocking = still_blocking.len(),
                retiring,
                "publisher claims retired after insufficient lease headroom"
            );
        }
    }

    StoreSafety::Renewed(outcome)
}
