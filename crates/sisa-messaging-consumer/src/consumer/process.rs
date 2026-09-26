//! The per-delivery database and handler workflow.
//!
//! This task alone owns the delivery's transaction. It maps and decodes the wire value, claims
//! the inbox receipt, runs the handler, completes and commits, or rolls back and records the
//! classified failure. It never performs broker I/O and returns a profile-neutral resolution
//! without the transaction.

use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use opentelemetry::context::FutureExt;
use opentelemetry::trace::SpanKind;
use sisa_messaging::{
    EnvelopeMapper, ErrorClassifier, ErrorSummary, FailureKind, Message, MessageId, Serializer,
};
use sisa_messaging_inbox::{
    DeadReason, InboxClaimOutcome, InboxFailure, InboxFailureOutcome, InboxReceipt, InboxRecord,
    InboxStore, InboxUnitOfWork,
};

use crate::ConsumerHandler;
use crate::telemetry::{self, DeliveryLabels};

use super::Shared;
use super::settlement::Resolution;

/// Fixed failure summary recorded for an undecodable body; codec text is never persisted.
const DECODE_FAILURE_SUMMARY: &str = "message body could not be decoded";

pub(super) type ProviderSource = Box<dyn Error + Send + Sync + 'static>;

/// Where a workflow established its resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Stage {
    Map,
    Decode,
    Begin,
    Claim,
    Complete,
    Commit,
    Rollback,
    Fail,
}

impl Stage {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Map => "map",
            Self::Decode => "decode",
            Self::Begin => "begin",
            Self::Claim => "claim",
            Self::Complete => "complete",
            Self::Commit => "commit",
            Self::Rollback => "rollback",
            Self::Fail => "fail",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClaimMetric {
    Duplicate(&'static str),
    Dead(DeadReason),
}

fn claim_metric<R: InboxReceipt>(outcome: &InboxClaimOutcome<R>) -> Option<ClaimMetric> {
    match outcome {
        InboxClaimOutcome::CompletedDuplicate => Some(ClaimMetric::Duplicate("completed")),
        InboxClaimOutcome::InProgressDuplicate => Some(ClaimMetric::Duplicate("in_progress")),
        InboxClaimOutcome::DeadDuplicate { reason } => Some(ClaimMetric::Dead(*reason)),
        _ => None,
    }
}

fn emit_claim_metric<R: InboxReceipt>(outcome: &InboxClaimOutcome<R>) {
    match claim_metric(outcome) {
        Some(ClaimMetric::Duplicate(state)) => telemetry::duplicate(state),
        Some(ClaimMetric::Dead(reason)) => telemetry::dead(reason),
        None => {}
    }
}

fn process_error_type(processed: &Processed) -> Option<&'static str> {
    match processed.resolution {
        Resolution::Completed | Resolution::InProgress if processed.stage == Stage::Claim => None,
        Resolution::Completed if processed.stage == Stage::Commit => None,
        Resolution::Dead(_) if processed.stage == Stage::Claim => None,
        _ => Some(processed.stage.as_str()),
    }
}

/// A finished workflow; it never carries the transaction.
pub(super) struct Processed {
    pub(super) resolution: Resolution,

    pub(super) stage: Stage,

    /// Typed provider error behind a failure resolution, retained only for `ConsumerError`.
    pub(super) error: Option<ProviderSource>,

    pub(super) message_id: Option<MessageId>,

    pub(super) attempt: Option<u32>,
}

/// A framework-owned database operation that did not succeed.
enum DatabaseFailure {
    TimedOut,

    Failed(FailureKind, ProviderSource),
}

impl DatabaseFailure {
    const fn kind(&self) -> FailureKind {
        match self {
            Self::TimedOut => FailureKind::Transient,
            Self::Failed(kind, _) => *kind,
        }
    }

    fn into_source(self) -> Option<ProviderSource> {
        match self {
            Self::TimedOut => None,
            Self::Failed(_, source) => Some(source),
        }
    }
}

async fn bounded<T, E>(
    timeout: Duration,
    operation: impl Future<Output = Result<T, E>>,
) -> Result<T, DatabaseFailure>
where
    E: Error + ErrorClassifier + Send + Sync + 'static,
{
    match tokio::time::timeout(timeout, operation).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(DatabaseFailure::Failed(error.classify(), Box::new(error))),
        Err(_elapsed) => Err(DatabaseFailure::TimedOut),
    }
}

struct Workflow<'a, Map, Codec, Inbox, H> {
    shared: &'a Shared<Map, Codec, Inbox, H>,

    message_id: Option<MessageId>,

    attempt: Option<u32>,
}

impl<Map, Codec, Inbox, H> Workflow<'_, Map, Codec, Inbox, H>
where
    Inbox: InboxUnitOfWork + InboxStore<Inbox::Transaction>,
{
    fn timeout(&self) -> Duration {
        self.shared.settings.database_timeout
    }

    fn finish(&self, resolution: Resolution, stage: Stage) -> Processed {
        Processed {
            resolution,
            stage,
            error: None,
            message_id: self.message_id,
            attempt: self.attempt,
        }
    }

    fn failed(
        &self,
        stage: Stage,
        failure: DatabaseFailure,
        resolution: impl FnOnce(FailureKind) -> Resolution,
    ) -> Processed {
        Processed {
            resolution: resolution(failure.kind()),
            stage,
            error: failure.into_source(),
            message_id: self.message_id,
            attempt: self.attempt,
        }
    }

    /// Rolls back a transaction whose claim did not lead to a handler success or failure record.
    ///
    /// The transaction is dropped either way. A transient rollback failure or timeout is logged
    /// and keeps the caller's resolution. A permanent or unknown rollback failure is a permanent
    /// unit-of-work error, so it replaces the resolution with a permanent unresolved result that
    /// leaves the delivery unsettled and stops the consumer, retaining the rollback error.
    async fn discard(&self, transaction: Inbox::Transaction, stage: Stage) -> Option<Processed> {
        let rollback = self.shared.inbox.rollback(transaction);

        let failure = bounded(self.timeout(), rollback).await.err()?;

        telemetry::cleanup_failed(self.labels(), stage.as_str(), failure.kind());

        if failure.kind().is_retryable() {
            return None;
        }

        Some(
            self.failed(Stage::Rollback, failure, |_| Resolution::Unresolved {
                kind: FailureKind::Permanent,
            }),
        )
    }

    fn labels(&self) -> DeliveryLabels {
        DeliveryLabels {
            message: self.shared.labels,
            message_id: self.message_id,
            attempt: self.attempt,
        }
    }

    /// Records a classified failure after any transaction was rolled back.
    async fn record_failure(
        &self,
        record: &InboxRecord,
        failure: InboxFailure,
        stage: Stage,
    ) -> Processed {
        match bounded(self.timeout(), self.shared.inbox.fail(record, failure)).await {
            Ok(InboxFailureOutcome::Retry { .. }) => self.finish(Resolution::RetryRecorded, stage),
            Ok(InboxFailureOutcome::Dead { reason, .. }) => {
                telemetry::dead(reason);

                self.finish(Resolution::Dead(reason), stage)
            }
            Ok(InboxFailureOutcome::CompletedDuplicate) => {
                self.finish(Resolution::Completed, stage)
            }
            // An unknown outcome fails closed.
            Ok(_) => self.finish(
                Resolution::Unresolved {
                    kind: FailureKind::Permanent,
                },
                stage,
            ),
            Err(failure) => self.failed(stage, failure, |kind| Resolution::NotRecorded { kind }),
        }
    }
}

/// Runs one delivery from wire value to resolution.
///
/// `in_handler` is set only while the application handler future is being awaited, so the
/// coordinator can attribute a panic of this task to the handler or to a provider call.
pub(super) async fn process<M, W, Map, Codec, Inbox, H>(
    shared: Arc<Shared<Map, Codec, Inbox, H>>,
    wire: W,
    in_handler: Arc<AtomicBool>,
) -> Processed
where
    M: Message,
    W: Send + 'static,
    Map: EnvelopeMapper<W>,
    Codec: Serializer<M>,
    Inbox: InboxUnitOfWork + InboxStore<Inbox::Transaction>,
    H: ConsumerHandler<M, Inbox::Transaction>,
{
    let timer = telemetry::ProcessingTimer::new();

    // A mapper can panic before metadata exists. Give that delivery an ambient-parented process
    // span, then preserve the panic for the coordinator's typed failure classification.
    let mapped =
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| shared.mapper.decode(wire)))
        {
            Ok(mapped) => mapped.map_err(|_| ()),
            Err(payload) => {
                let _span = telemetry::ProcessingSpan::new(shared.labels, None, &shared.ambient);

                std::panic::resume_unwind(payload);
            }
        };

    let span = telemetry::ProcessingSpan::new(
        shared.labels,
        mapped.as_ref().ok().map(|serialized| &serialized.metadata),
        &shared.ambient,
    );

    let processed = process_inner::<M, W, _, _, _, _>(shared, mapped, in_handler)
        .with_context(span.context())
        .await;

    timer.finish(process_error_type(&processed));

    processed
}

async fn process_inner<M, W, Map, Codec, Inbox, H>(
    shared: Arc<Shared<Map, Codec, Inbox, H>>,
    mapped: Result<sisa_messaging::SerializedEnvelope, ()>,
    in_handler: Arc<AtomicBool>,
) -> Processed
where
    M: Message,
    W: Send + 'static,
    Map: EnvelopeMapper<W>,
    Codec: Serializer<M>,
    Inbox: InboxUnitOfWork + InboxStore<Inbox::Transaction>,
    H: ConsumerHandler<M, Inbox::Transaction>,
{
    let mut workflow = Workflow {
        shared: &shared,
        message_id: None,
        attempt: None,
    };

    // Mapper errors may render wire bytes; they are dropped without being retained or emitted.
    let Ok(serialized) = mapped else {
        return workflow.finish(Resolution::Malformed, Stage::Map);
    };

    workflow.message_id = Some(serialized.message_id);

    let record = InboxRecord {
        scope: shared.scope.clone(),
        message_id: serialized.message_id,
        message_type: serialized.message_type.clone(),
        version: serialized.message_version,
        metadata: serialized.metadata.clone(),
    };

    // The identity is trustworthy, so a type, version, or body failure is recorded as permanent
    // with fixed text; codec errors are never rendered.
    let Ok(envelope) = shared.codec.deserialize(serialized) else {
        let failure = InboxFailure {
            kind: FailureKind::Permanent,
            error: ErrorSummary::from_safe_text(DECODE_FAILURE_SUMMARY),
        };

        return workflow
            .record_failure(&record, failure, Stage::Decode)
            .await;
    };

    let timeout = workflow.timeout();

    let mut transaction = match bounded(timeout, shared.inbox.begin()).await {
        Ok(transaction) => transaction,
        Err(failure) => {
            return workflow.failed(Stage::Begin, failure, |kind| Resolution::Unresolved {
                kind,
            });
        }
    };

    let claim_span = telemetry::OperationSpan::child("inbox.claim", SpanKind::Internal);

    let claimed = bounded(timeout, shared.inbox.claim(&mut transaction, &record))
        .with_context(claim_span.context())
        .await;

    drop(claim_span);

    let receipt = match claimed {
        Ok(InboxClaimOutcome::Claimed(receipt)) => receipt,
        Ok(outcome) => {
            emit_claim_metric(&outcome);

            let resolution = match outcome {
                InboxClaimOutcome::CompletedDuplicate => Resolution::Completed,
                InboxClaimOutcome::InProgressDuplicate => Resolution::InProgress,
                InboxClaimOutcome::DeadDuplicate { reason } => Resolution::Dead(reason),
                // Unknown outcomes fail closed.
                _ => Resolution::Unresolved {
                    kind: FailureKind::Permanent,
                },
            };

            if let Some(escalated) = workflow.discard(transaction, Stage::Claim).await {
                return escalated;
            }

            return workflow.finish(resolution, Stage::Claim);
        }
        Err(failure) => {
            if let Some(escalated) = workflow.discard(transaction, Stage::Claim).await {
                return escalated;
            }

            return workflow.failed(Stage::Claim, failure, |kind| Resolution::Unresolved {
                kind,
            });
        }
    };

    workflow.attempt = Some(receipt.recorded_failures().saturating_add(1));

    in_handler.store(true, Ordering::Release);

    let handler_span = telemetry::OperationSpan::child("handler", SpanKind::Internal);

    let handled = shared
        .handler
        .handle(&mut transaction, &envelope)
        .with_context(handler_span.context())
        .await;

    drop(handler_span);

    in_handler.store(false, Ordering::Release);

    match handled {
        Ok(()) => {
            let completed =
                bounded(timeout, shared.inbox.complete(&mut transaction, receipt)).await;

            if let Err(failure) = completed {
                if let Some(escalated) = workflow.discard(transaction, Stage::Complete).await {
                    return escalated;
                }

                return workflow.failed(Stage::Complete, failure, |kind| Resolution::Unresolved {
                    kind,
                });
            }

            let commit_span = telemetry::OperationSpan::child("inbox.commit", SpanKind::Internal);

            let committed = bounded(timeout, shared.inbox.commit(transaction))
                .with_context(commit_span.context())
                .await;

            drop(commit_span);

            match committed {
                Ok(()) => {
                    telemetry::processed();

                    workflow.finish(Resolution::Completed, Stage::Commit)
                }
                // A commit error or timeout is ambiguous; no failure is recorded.
                Err(failure) => workflow.failed(Stage::Commit, failure, |kind| {
                    Resolution::CommitAmbiguous { kind }
                }),
            }
        }
        Err(error) => {
            let failure = InboxFailure {
                kind: error.classify(),
                error: ErrorSummary::from_safe_error(&error),
            };

            drop(error);

            if let Err(rollback) = bounded(timeout, shared.inbox.rollback(transaction)).await {
                return workflow.failed(Stage::Rollback, rollback, |kind| {
                    Resolution::NotRecorded { kind }
                });
            }

            workflow.record_failure(&record, failure, Stage::Fail).await
        }
    }
}

#[cfg(test)]
mod tests {
    use sisa_messaging_inbox::{DeadReason, InboxClaimOutcome, InboxId, InboxReceipt};

    use super::{ClaimMetric, Processed, Stage, claim_metric, process_error_type};
    use crate::consumer::settlement::Resolution;

    struct Receipt;

    impl InboxReceipt for Receipt {
        fn id(&self) -> InboxId {
            unreachable!("classification never reads a receipt")
        }

        fn recorded_failures(&self) -> u32 {
            unreachable!("classification never reads a receipt")
        }
    }

    #[test]
    fn claim_metrics_only_cover_handler_free_duplicate_and_dead_outcomes() {
        assert_eq!(
            claim_metric(&InboxClaimOutcome::<Receipt>::CompletedDuplicate),
            Some(ClaimMetric::Duplicate("completed"))
        );

        assert_eq!(
            claim_metric(&InboxClaimOutcome::<Receipt>::InProgressDuplicate),
            Some(ClaimMetric::Duplicate("in_progress"))
        );

        assert_eq!(
            claim_metric(&InboxClaimOutcome::<Receipt>::DeadDuplicate {
                reason: DeadReason::Permanent
            }),
            Some(ClaimMetric::Dead(DeadReason::Permanent))
        );
    }

    #[test]
    fn process_duration_uses_only_closed_error_categories() {
        let mut processed = Processed {
            resolution: Resolution::Completed,
            stage: Stage::Commit,
            error: None,
            message_id: None,
            attempt: None,
        };

        assert_eq!(process_error_type(&processed), None);
        processed.stage = Stage::Claim;
        assert_eq!(process_error_type(&processed), None);
        processed.resolution = Resolution::InProgress;
        assert_eq!(process_error_type(&processed), None);
        processed.resolution = Resolution::Dead(DeadReason::Exhausted);
        assert_eq!(process_error_type(&processed), None);
        processed.stage = Stage::Fail;
        assert_eq!(process_error_type(&processed), Some("fail"));
    }
}
