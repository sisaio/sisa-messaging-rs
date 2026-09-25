//! One-call-at-a-time shutdown persistence and error capture.

use crate::{DispatcherSettings, OutboxStore, RetryPolicy};

use super::super::OutboxRunReport;
use super::super::outcomes::{self, accounting};
use super::super::state::State;

pub(super) async fn persist_one<S, R>(
    store: &S,
    settings: &DispatcherSettings<R>,
    state: &mut State,
    report: &mut OutboxRunReport,
    permanent_error: &mut Option<S::Error>,
) -> bool
where
    S: OutboxStore,
    R: RetryPolicy,
{
    let completions = state.completion_batch();

    if !completions.is_empty() {
        let result = outcomes::complete(store, &completions, settings.store_timeout).await;

        if let Some(error) = accounting::finish_completions(result, &completions, state, report) {
            capture_permanent(error, permanent_error);
        }

        return true;
    }

    let failures = state.failure_batch();

    if !failures.is_empty() {
        let claims = failures
            .iter()
            .map(|failure| failure.claim)
            .collect::<Vec<_>>();

        let result = outcomes::fail(store, &failures, settings.store_timeout).await;

        if let Some(error) = accounting::finish_failures(result, &failures, &claims, state, report)
        {
            capture_permanent(error, permanent_error);
        }

        return true;
    }

    let releases = state.release_batch();

    if !releases.is_empty() {
        let result = outcomes::release(store, &releases, settings.store_timeout).await;

        if let Some(error) = accounting::finish_releases(result, &releases, state, report) {
            capture_permanent(error, permanent_error);
        }

        return true;
    }

    false
}

pub(super) async fn persist_rejected_one<S: OutboxStore>(
    store: &S,
    timeout: std::time::Duration,
    state: &mut State,
    report: &mut OutboxRunReport,
    permanent_error: &mut Option<S::Error>,
) -> bool {
    match accounting::persist_rejected_ready(store, timeout, state, report).await {
        accounting::PersistenceTurn::Idle => false,
        accounting::PersistenceTurn::Progressed => true,
        accounting::PersistenceTurn::Permanent(error) => {
            if permanent_error.is_none() {
                *permanent_error = Some(error);
            }

            true
        }
    }
}

fn capture_permanent<E>(error: E, permanent_error: &mut Option<E>) {
    if permanent_error.is_none() {
        *permanent_error = Some(error);
    }
}
