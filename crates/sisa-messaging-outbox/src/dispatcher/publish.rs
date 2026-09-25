//! Bounded publisher-task spawning and result classification.

use std::sync::Arc;
use std::time::Duration;

use sisa_messaging::{ErrorClassifier, ErrorSummary, FailureKind, Publisher};
use tokio::task::{AbortHandle, Id, JoinSet};
use tokio::time::Instant;
use tracing::Instrument;

use crate::{Claim, DeadReason, FailureAction, RetryPolicy};

use super::OutboxRunReport;
use super::state::{PublishWork, ResolvedOutcome};

pub(crate) struct PublishResult {
    pub(crate) claim: Claim,

    pub(crate) attempts: u32,

    pub(crate) elapsed: Duration,

    pub(crate) failure: Option<ClassifiedFailure>,
}

pub(crate) struct ClassifiedFailure {
    pub(crate) kind: FailureKind,

    pub(crate) summary: ErrorSummary,

    pub(crate) error_type: &'static str,
}

pub(crate) enum JoinOutcome {
    Finished,
    Retired,
}

struct InFlightGuard;

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        crate::telemetry::publish_stopped();
    }
}

pub(crate) fn spawn<P>(
    tasks: &mut JoinSet<PublishResult>,
    publisher: Arc<P>,
    work: PublishWork,
    timeout: Duration,
) -> (Id, AbortHandle)
where
    P: Publisher + 'static,
{
    crate::telemetry::publish_started();
    let in_flight = InFlightGuard;

    let task = async move {
        let _in_flight = in_flight;
        let started = Instant::now();
        let result = tokio::time::timeout(timeout, publisher.publish(&work.envelope)).await;
        let elapsed = started.elapsed();

        let failure = match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(ClassifiedFailure {
                kind: error.classify(),
                summary: safe_publisher_summary(error.classify()),
                error_type: "publisher",
            }),
            Err(_) => Some(ClassifiedFailure {
                kind: FailureKind::Transient,
                summary: ErrorSummary::from_safe_text("publisher acknowledgement timed out"),
                error_type: "timeout",
            }),
        };

        PublishResult {
            claim: work.claim,
            attempts: work.attempts,
            elapsed,
            failure,
        }
    }
    .instrument(tracing::debug_span!(
        target: "messaging.outbox",
        "outbox.publish"
    ));

    let handle = tasks.spawn(task);
    let id = handle.id();

    (id, handle)
}

fn safe_publisher_summary(kind: FailureKind) -> ErrorSummary {
    let text = match kind {
        FailureKind::Transient => "publisher failed transiently",
        FailureKind::Permanent => "publisher failed permanently",
        _ => "publisher failed",
    };

    ErrorSummary::from_safe_text(text)
}

pub(crate) fn resolve<R: RetryPolicy>(result: &PublishResult, retry: &R) -> ResolvedOutcome {
    let Some(failure) = &result.failure else {
        return ResolvedOutcome::Complete;
    };

    let attempt = result.attempts.saturating_add(1);
    let attempt = std::num::NonZeroU32::new(attempt).unwrap_or(std::num::NonZeroU32::MAX);

    let action = match failure.kind {
        FailureKind::Permanent => FailureAction::Dead {
            reason: DeadReason::Permanent,
        },
        FailureKind::Transient => retry
            .retry_delay(attempt)
            .map(|delay| FailureAction::Retry { delay })
            .unwrap_or(FailureAction::Dead {
                reason: DeadReason::Exhausted,
            }),
        _ => FailureAction::Dead {
            reason: DeadReason::Permanent,
        },
    };

    ResolvedOutcome::Failure {
        kind: failure.kind,
        summary: failure.summary.clone(),
        action,
    }
}

pub(crate) fn start_pending<P: Publisher + 'static>(
    publisher: &Arc<P>,
    publish_timeout: Duration,
    state: &mut super::state::State,
    tasks: &mut JoinSet<PublishResult>,
) {
    while let Some(work) = state.next_publish() {
        let claim = work.claim;
        let (task_id, abort) = spawn(tasks, Arc::clone(publisher), work, publish_timeout);
        state.publishing(claim, task_id, abort);
    }
}

pub(crate) fn advance_ready<P, R>(
    publisher: &Arc<P>,
    publish_timeout: Duration,
    retry: &R,
    state: &mut super::state::State,
    tasks: &mut JoinSet<PublishResult>,
    report: &mut OutboxRunReport,
) -> Result<(), tokio::task::JoinError>
where
    P: Publisher + 'static,
    R: RetryPolicy,
{
    start_pending(publisher, publish_timeout, state, tasks);

    while let Some(joined) = tasks.try_join_next_with_id() {
        if matches!(finish_join(joined, retry, state)?, JoinOutcome::Retired) {
            report.aborted += 1;
        }
    }

    Ok(())
}

pub(crate) fn finish_join<R: RetryPolicy>(
    joined: Result<(Id, PublishResult), tokio::task::JoinError>,
    retry: &R,
    state: &mut super::state::State,
) -> Result<JoinOutcome, tokio::task::JoinError> {
    match joined {
        Ok((task_id, result)) => {
            let error_type = result.failure.as_ref().map(|failure| failure.error_type);
            crate::telemetry::publish_finished(result.elapsed, error_type);

            let Some(active_claim) = state.task_claim(task_id) else {
                return Ok(JoinOutcome::Finished);
            };

            debug_assert_eq!(active_claim, result.claim);
            state.resolve(task_id, resolve(&result, retry));

            Ok(JoinOutcome::Finished)
        }
        Err(error)
            if error.is_cancelled() && state.finish_retiring_cancelled(error.id()).is_some() =>
        {
            Ok(JoinOutcome::Retired)
        }
        Err(error) if error.is_cancelled() && state.task_claim(error.id()).is_none() => {
            Ok(JoinOutcome::Finished)
        }
        Err(error) => {
            crate::telemetry::publish_finished(Duration::ZERO, Some("publisher.panic"));

            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future;

    use sisa_messaging::{ContentType, MessageId, MessageType, Metadata, SerializedEnvelope};
    use uuid::Uuid;

    use super::*;
    use crate::{ClaimToken, ClaimedRecord, ExponentialBackoff, OutboxId};

    fn record() -> ClaimedRecord {
        ClaimedRecord {
            claim: Claim {
                id: OutboxId::from_uuid(Uuid::from_u128(1)),
                token: ClaimToken::from_uuid(Uuid::from_u128(2)),
            },
            envelope: SerializedEnvelope {
                message_id: MessageId::from_uuid(Uuid::from_u128(3)),
                message_type: MessageType::new("test.message")
                    .unwrap_or_else(|error| panic!("static message type rejected: {error}")),
                message_version: 1,
                content_type: ContentType::new("application/test")
                    .unwrap_or_else(|error| panic!("static content type rejected: {error}")),
                payload: vec![1],
                metadata: Metadata::default(),
                ordering_key: None,
            },
            attempts: 0,
        }
    }

    async fn cancelled_join(retiring: bool) -> Result<JoinOutcome, tokio::task::JoinError> {
        let mut state = super::super::state::State::new(1);
        let now = Instant::now();
        let claimed = record();
        let claim = claimed.claim;
        state.insert_claimed(vec![claimed], now, now);

        let _work = state
            .next_publish()
            .unwrap_or_else(|| panic!("record was not queued for publication"));

        let mut tasks = JoinSet::new();
        let abort = tasks.spawn(async { future::pending::<PublishResult>().await });
        let task_id = abort.id();
        let control = abort.clone();
        state.publishing(claim, task_id, abort);

        if retiring {
            assert_eq!(state.retire_publishers(&[claim]), 1);
        } else {
            control.abort();
        }

        let joined = tasks
            .join_next_with_id()
            .await
            .unwrap_or_else(|| panic!("cancelled task was not joined"));

        let retry = ExponentialBackoff::new(
            Duration::from_millis(1),
            Duration::from_millis(1),
            std::num::NonZeroU32::MIN,
        )
        .unwrap_or_else(|error| panic!("retry policy rejected: {error}"));

        finish_join(joined, &retry, &mut state)
    }

    #[tokio::test]
    async fn only_tracked_retirement_makes_task_cancellation_benign() {
        let retired = cancelled_join(true)
            .await
            .unwrap_or_else(|error| panic!("retiring cancellation was terminal: {error}"));

        assert!(matches!(retired, JoinOutcome::Retired));

        let active = cancelled_join(false).await;
        assert!(active.is_err(), "unexpected active cancellation was benign");
    }
}
