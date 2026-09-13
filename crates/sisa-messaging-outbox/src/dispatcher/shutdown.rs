//! Finite cancellation drain, abort, and release behavior.

mod persistence;

use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::{DispatcherError, DispatcherSettings, OutboxStore, RetryPolicy};

use self::persistence::{persist_one, persist_rejected_one};
use super::OutboxRunReport;
use super::leases::{self, RenewalOutcome};
use super::outcomes::{self, StoreCall};
use super::publish::{self, PublishResult};
use super::state::State;

pub(crate) async fn graceful<S, R>(
    store: &S,
    settings: &DispatcherSettings<R>,
    state: &mut State,
    tasks: &mut JoinSet<PublishResult>,
    report: &mut OutboxRunReport,
) -> Result<(), DispatcherError<S::Error>>
where
    S: OutboxStore,
    R: RetryPolicy,
{
    let mut permanent_error = None;
    let mut publisher_error = None;

    let unstarted = state.unstarted_claims();
    state.mark_release(&unstarted);

    let deadline = Instant::now() + settings.drain_timeout;
    loop {
        while let Some(joined) = tasks.try_join_next_with_id() {
            if let Some(error) = resolve_join(joined, settings, state, report) {
                publisher_error.get_or_insert(error);
            }
        }

        if Instant::now() >= deadline {
            break;
        }

        let due = state.due_renewals(Instant::now());
        if !due.is_empty() {
            let (started, result) =
                leases::extend(store, &due, settings.lease, settings.store_timeout).await;
            if let RenewalOutcome::Failed {
                permanent_error: Some(error),
            } = leases::finish(
                started,
                result,
                &due,
                settings.renewal_offset(),
                state,
                report,
            ) && permanent_error.is_none()
            {
                permanent_error = Some(error);
            }
            continue;
        }

        if persist_one(store, settings, state, report, &mut permanent_error).await {
            continue;
        }
        if persist_rejected_one(
            store,
            settings.store_timeout,
            state,
            report,
            &mut permanent_error,
        )
        .await
        {
            continue;
        }
        if !state.has_tasks() {
            break;
        }

        let wake_at = state
            .next_renewal()
            .map_or(deadline, |renewal| renewal.min(deadline));
        tokio::select! {
            joined = tasks.join_next_with_id() => {
                if let Some(joined) = joined
                    && let Some(error) = resolve_join(joined, settings, state, report)
                {
                    publisher_error.get_or_insert(error);
                }
            }
            () = tokio::time::sleep_until(wake_at) => {},
        }
    }

    let unresolved = state.unresolved_claims();
    if !unresolved.is_empty() {
        tracing::warn!(
            target: "messaging.outbox",
            unresolved = unresolved.len(),
            "dispatcher drain deadline reached"
        );
    }

    tasks.abort_all();
    while let Some(joined) = tasks.join_next_with_id().await {
        if let Some(error) = resolve_join(joined, settings, state, report) {
            publisher_error.get_or_insert(error);
        }
    }

    while persist_one(store, settings, state, report, &mut permanent_error).await {}
    while persist_rejected_one(
        store,
        settings.store_timeout,
        state,
        report,
        &mut permanent_error,
    )
    .await
    {}

    if let Some(error) = publisher_error {
        return Err(DispatcherError::PublisherTask(error));
    }
    permanent_error.map_or(Ok(()), |error| Err(DispatcherError::Store(error)))
}

pub(crate) async fn cleanup_after_fatal<S: OutboxStore>(
    store: &S,
    state: &mut State,
    tasks: &mut JoinSet<PublishResult>,
    report: &mut OutboxRunReport,
    timeout: std::time::Duration,
) {
    let claims = state.all_claims();
    report.aborted += state.unresolved_claims().len() as u64;
    state.mark_release(&claims);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}

    match outcomes::release(store, &claims, timeout).await {
        StoreCall::Completed(matches) => {
            report.released += matches.confirmed.len() as u64;
            report.fenced += claims.len().saturating_sub(matches.confirmed.len()) as u64;
        }
        StoreCall::Failed(_) | StoreCall::TimedOut => report.store_failures += 1,
    }
    state.remove(&claims);

    while state.has_rejected() {
        match super::outcomes::accounting::persist_rejected_ready(store, timeout, state, report)
            .await
        {
            super::outcomes::accounting::PersistenceTurn::Idle => break,
            super::outcomes::accounting::PersistenceTurn::Progressed
            | super::outcomes::accounting::PersistenceTurn::Permanent(_) => {}
        }
    }
}

pub(crate) async fn store_failure<S: OutboxStore>(
    store: &S,
    state: &mut State,
    tasks: &mut JoinSet<PublishResult>,
    report: &mut OutboxRunReport,
    timeout: std::time::Duration,
    error: S::Error,
) -> DispatcherError<S::Error> {
    cleanup_after_fatal(store, state, tasks, report, timeout).await;
    DispatcherError::Store(error)
}

pub(crate) async fn publisher_failure<S: OutboxStore>(
    store: &S,
    state: &mut State,
    tasks: &mut JoinSet<PublishResult>,
    report: &mut OutboxRunReport,
    timeout: std::time::Duration,
    error: tokio::task::JoinError,
) -> DispatcherError<S::Error> {
    cleanup_after_fatal(store, state, tasks, report, timeout).await;
    DispatcherError::PublisherTask(error)
}

fn resolve_join<R: RetryPolicy>(
    joined: Result<(tokio::task::Id, PublishResult), tokio::task::JoinError>,
    settings: &DispatcherSettings<R>,
    state: &mut State,
    report: &mut OutboxRunReport,
) -> Option<tokio::task::JoinError> {
    match joined {
        Err(error) if error.is_cancelled() => {
            if state.publisher_task_failed(error.id()).is_some() {
                report.aborted += 1;
            }
            None
        }
        joined => match publish::finish_join(joined, &settings.retry_policy, state) {
            Ok(()) => None,
            Err(error) => {
                if state.publisher_task_failed(error.id()).is_some() {
                    report.aborted += 1;
                }
                Some(error)
            }
        },
    }
}
