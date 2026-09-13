//! One-call-at-a-time shutdown persistence and error capture.

use sisa_messaging::ErrorClassifier;

use crate::{DispatcherSettings, OutboxStore, RetryPolicy};

use super::super::OutboxRunReport;
use super::super::outcomes::{self, StoreCall, accounting};
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
    let releases = state.release_batch();
    if !releases.is_empty() {
        match outcomes::release(store, &releases, settings.store_timeout).await {
            StoreCall::Completed(matches) => {
                report.released += matches.confirmed.len() as u64;
                report.fenced += releases.len().saturating_sub(matches.confirmed.len()) as u64;
            }
            StoreCall::Failed(error) => capture_permanent(error, permanent_error, report),
            StoreCall::TimedOut => report.store_failures += 1,
        }
        state.remove(&releases);
        return true;
    }

    let completions = state.completion_batch();
    if !completions.is_empty() {
        match outcomes::complete(store, &completions, settings.store_timeout).await {
            StoreCall::Completed(matches) => {
                accounting::confirmed_completions(&matches, &completions, state, report);
            }
            StoreCall::Failed(error) => {
                capture_permanent(error, permanent_error, report);
                state.mark_release(&completions);
            }
            StoreCall::TimedOut => {
                report.store_failures += 1;
                state.mark_release(&completions);
            }
        }
        return true;
    }

    let failures = state.failure_batch();
    if !failures.is_empty() {
        let claims = failures
            .iter()
            .map(|failure| failure.claim)
            .collect::<Vec<_>>();
        match outcomes::fail(store, &failures, settings.store_timeout).await {
            StoreCall::Completed(matches) => {
                accounting::confirmed_failures(&matches, &failures, &claims, state, report);
            }
            StoreCall::Failed(error) => {
                capture_permanent(error, permanent_error, report);
                state.mark_release(&claims);
            }
            StoreCall::TimedOut => {
                report.store_failures += 1;
                state.mark_release(&claims);
            }
        }
        return true;
    }

    false
}

pub(super) fn capture_permanent<E: ErrorClassifier>(
    error: E,
    permanent_error: &mut Option<E>,
    report: &mut OutboxRunReport,
) {
    report.store_failures += 1;
    if !error.classify().is_retryable() && permanent_error.is_none() {
        *permanent_error = Some(error);
    }
}
