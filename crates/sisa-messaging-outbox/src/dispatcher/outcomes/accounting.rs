//! Confirmed transition accounting and ambiguous-outcome handling.

use std::collections::HashSet;
use std::time::Duration;

use sisa_messaging::ErrorClassifier;

use crate::{Claim, DeadReason, FailureAction, FailureRecord, FencedClaims, OutboxStore};

use super::StoreCall;
use crate::dispatcher::OutboxRunReport;
use crate::dispatcher::state::State;

pub(crate) enum PersistenceTurn<E> {
    Idle,
    Progressed,
    Permanent(E),
}

pub(crate) fn warn_suppressed(operation: &'static str, category: &'static str, count: usize) {
    if count > 0 {
        tracing::warn!(
            target: "messaging.outbox",
            operation,
            category,
            count,
            "outbox persistence outcome suppressed"
        );
    }
}

pub(crate) fn warn_fencing_shortfall(operation: &'static str, requested: usize, confirmed: usize) {
    warn_suppressed(
        operation,
        "fencing_shortfall",
        requested.saturating_sub(confirmed),
    );
}

pub(crate) async fn persist_ready<S: OutboxStore>(
    store: &S,
    timeout: Duration,
    state: &mut State,
    report: &mut OutboxRunReport,
) -> PersistenceTurn<S::Error> {
    let releases = state.release_batch();
    if !releases.is_empty() {
        return turn(persist_releases(store, &releases, timeout, state, report).await);
    }
    let completions = state.completion_batch();
    if !completions.is_empty() {
        return turn(persist_completions(store, &completions, timeout, state, report).await);
    }
    let failures = state.failure_batch();
    if !failures.is_empty() {
        return turn(persist_failures(store, &failures, timeout, state, report).await);
    }
    PersistenceTurn::Idle
}

pub(crate) async fn persist_rejected_ready<S: OutboxStore>(
    store: &S,
    timeout: Duration,
    state: &mut State,
    report: &mut OutboxRunReport,
) -> PersistenceTurn<S::Error> {
    let requested = state.rejected_batch();
    if requested.is_empty() {
        return PersistenceTurn::Idle;
    }

    let result = super::release(store, &requested, timeout).await;
    let error = match result {
        StoreCall::Completed(matches) => {
            report.released += matches.confirmed.len() as u64;
            let shortfall = requested.len().saturating_sub(matches.confirmed.len());
            report.fenced += shortfall as u64;
            warn_fencing_shortfall("rejected_release", requested.len(), matches.confirmed.len());
            None
        }
        StoreCall::Failed(error) => {
            report.store_failures += 1;
            let retryable = error.classify().is_retryable();
            if retryable {
                warn_suppressed("rejected_release", "transient_failure", requested.len());
            }
            (!retryable).then_some(error)
        }
        StoreCall::TimedOut => {
            report.store_failures += 1;
            warn_suppressed("rejected_release", "timeout", requested.len());
            None
        }
    };
    state.retire_rejected(requested.len());
    turn(error)
}

fn turn<E>(error: Option<E>) -> PersistenceTurn<E> {
    error.map_or(PersistenceTurn::Progressed, PersistenceTurn::Permanent)
}

pub(crate) async fn persist_releases<S: OutboxStore>(
    store: &S,
    requested: &[Claim],
    timeout: Duration,
    state: &mut State,
    report: &mut OutboxRunReport,
) -> Option<S::Error> {
    finish_releases(
        super::release(store, requested, timeout).await,
        requested,
        state,
        report,
    )
}

pub(crate) async fn persist_completions<S: OutboxStore>(
    store: &S,
    requested: &[Claim],
    timeout: Duration,
    state: &mut State,
    report: &mut OutboxRunReport,
) -> Option<S::Error> {
    finish_completions(
        super::complete(store, requested, timeout).await,
        requested,
        state,
        report,
    )
}

pub(crate) async fn persist_failures<S: OutboxStore>(
    store: &S,
    failures: &[FailureRecord],
    timeout: Duration,
    state: &mut State,
    report: &mut OutboxRunReport,
) -> Option<S::Error> {
    let claims = failures
        .iter()
        .map(|failure| failure.claim)
        .collect::<Vec<_>>();
    finish_failures(
        super::fail(store, failures, timeout).await,
        failures,
        &claims,
        state,
        report,
    )
}

pub(crate) fn finish_completions<E: ErrorClassifier>(
    result: StoreCall<E>,
    requested: &[Claim],
    state: &mut State,
    report: &mut OutboxRunReport,
) -> Option<E> {
    match result {
        StoreCall::Completed(matches) => {
            confirmed_completions(&matches, requested, state, report);
            None
        }
        StoreCall::Failed(error) => {
            let permanent = !error.classify().is_retryable();
            report.store_failures += 1;
            state.mark_release(requested);
            if !permanent {
                warn_suppressed("complete", "transient_failure", requested.len());
            }
            permanent.then_some(error)
        }
        StoreCall::TimedOut => {
            report.store_failures += 1;
            state.mark_release(requested);
            warn_suppressed("complete", "timeout", requested.len());
            None
        }
    }
}

pub(crate) fn finish_releases<E: ErrorClassifier>(
    result: StoreCall<E>,
    requested: &[Claim],
    state: &mut State,
    report: &mut OutboxRunReport,
) -> Option<E> {
    match result {
        StoreCall::Completed(matches) => {
            report.released += matches.confirmed.len() as u64;
            let shortfall = requested.len().saturating_sub(matches.confirmed.len());
            report.fenced += shortfall as u64;
            warn_fencing_shortfall("release", requested.len(), matches.confirmed.len());
            state.remove(requested);
            None
        }
        StoreCall::Failed(error) => {
            let permanent = !error.classify().is_retryable();
            report.store_failures += 1;
            state.remove(requested);
            if !permanent {
                warn_suppressed("release", "transient_failure", requested.len());
            }
            permanent.then_some(error)
        }
        StoreCall::TimedOut => {
            report.store_failures += 1;
            state.remove(requested);
            warn_suppressed("release", "timeout", requested.len());
            None
        }
    }
}

pub(crate) fn finish_failures<E: ErrorClassifier>(
    result: StoreCall<E>,
    failures: &[FailureRecord],
    requested: &[Claim],
    state: &mut State,
    report: &mut OutboxRunReport,
) -> Option<E> {
    match result {
        StoreCall::Completed(matches) => {
            confirmed_failures(&matches, failures, requested, state, report);
            None
        }
        StoreCall::Failed(error) => {
            let permanent = !error.classify().is_retryable();
            report.store_failures += 1;
            state.mark_release(requested);
            if !permanent {
                warn_suppressed("fail", "transient_failure", requested.len());
            }
            permanent.then_some(error)
        }
        StoreCall::TimedOut => {
            report.store_failures += 1;
            state.mark_release(requested);
            warn_suppressed("fail", "timeout", requested.len());
            None
        }
    }
}

pub(crate) fn confirmed_completions(
    matches: &FencedClaims,
    requested: &[Claim],
    state: &mut State,
    report: &mut OutboxRunReport,
) {
    report.completed += matches.confirmed.len() as u64;
    if !matches.confirmed.is_empty() {
        crate::telemetry::published(matches.confirmed.len());
    }
    let shortfall = requested.len().saturating_sub(matches.confirmed.len());
    report.fenced += shortfall as u64;
    warn_fencing_shortfall("complete", requested.len(), matches.confirmed.len());
    state.remove(requested);
}

pub(crate) fn confirmed_failures(
    matches: &FencedClaims,
    failures: &[FailureRecord],
    requested: &[Claim],
    state: &mut State,
    report: &mut OutboxRunReport,
) {
    let confirmed = matches.confirmed.iter().copied().collect::<HashSet<_>>();
    observe_confirmed_failures(failures, &confirmed, report);
    let shortfall = requested.len().saturating_sub(confirmed.len());
    report.fenced += shortfall as u64;
    warn_fencing_shortfall("fail", requested.len(), confirmed.len());
    state.remove(requested);
}

fn observe_confirmed_failures(
    failures: &[FailureRecord],
    confirmed: &HashSet<Claim>,
    report: &mut OutboxRunReport,
) {
    let mut retried_transient = 0_usize;
    let mut dead_permanent = 0_usize;
    let mut dead_exhausted = 0_usize;

    for failure in failures
        .iter()
        .filter(|failure| confirmed.contains(&failure.claim))
    {
        match failure.action {
            FailureAction::Retry { .. } => retried_transient += 1,
            FailureAction::Dead {
                reason: DeadReason::Permanent,
            } => dead_permanent += 1,
            FailureAction::Dead {
                reason: DeadReason::Exhausted,
            } => dead_exhausted += 1,
            FailureAction::Dead { .. } => {}
        }
    }

    if retried_transient > 0 {
        report.retried += retried_transient as u64;
        crate::telemetry::retried(retried_transient, sisa_messaging::FailureKind::Transient);
        tracing::warn!(
            target: "messaging.outbox",
            { "failure.kind" = "transient", count = retried_transient },
            "publication failures scheduled for retry"
        );
    }
    for (count, reason) in [
        (dead_permanent, DeadReason::Permanent),
        (dead_exhausted, DeadReason::Exhausted),
    ] {
        if count > 0 {
            report.dead += count as u64;
            crate::telemetry::dead(count, reason);
            tracing::warn!(
                target: "messaging.outbox",
                { "dead.reason" = reason.as_str(), count },
                "publication failures transitioned to dead"
            );
        }
    }
}
