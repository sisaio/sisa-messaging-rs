//! The per-delivery database and handler workflow.
//!
//! This task alone owns the delivery's transaction. It maps and decodes the wire value, claims
//! the inbox receipt, runs the handler, completes and commits, or rolls back and records the
//! classified failure. It never performs broker I/O and returns a profile-neutral resolution
//! without the transaction.

use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use sisa_messaging::{
    EnvelopeMapper, ErrorClassifier, ErrorSummary, FailureKind, Message, MessageId, Serializer,
};
use sisa_messaging_inbox::{
    InboxClaimOutcome, InboxFailure, InboxFailureOutcome, InboxReceipt, InboxRecord, InboxStore,
    InboxUnitOfWork,
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
    /// The resolution is already decided; a failed rollback is logged and the transaction is
    /// dropped either way.
    async fn discard(&self, transaction: Inbox::Transaction, stage: Stage) {
        let rollback = self.shared.inbox.rollback(transaction);

        if let Err(failure) = bounded(self.timeout(), rollback).await {
            telemetry::cleanup_failed(self.labels(), stage.as_str(), failure.kind());
        }
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
pub(super) async fn process<M, W, Map, Codec, Inbox, H>(
    shared: Arc<Shared<Map, Codec, Inbox, H>>,
    wire: W,
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
    let Ok(serialized) = shared.mapper.decode(wire) else {
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

    let claimed = bounded(timeout, shared.inbox.claim(&mut transaction, &record)).await;

    let receipt = match claimed {
        Ok(InboxClaimOutcome::Claimed(receipt)) => receipt,
        Ok(outcome) => {
            let resolution = match outcome {
                InboxClaimOutcome::CompletedDuplicate => Resolution::Completed,
                InboxClaimOutcome::InProgressDuplicate => Resolution::InProgress,
                InboxClaimOutcome::DeadDuplicate { reason } => Resolution::Dead(reason),
                // Unknown outcomes fail closed.
                _ => Resolution::Unresolved {
                    kind: FailureKind::Permanent,
                },
            };

            workflow.discard(transaction, Stage::Claim).await;

            return workflow.finish(resolution, Stage::Claim);
        }
        Err(failure) => {
            workflow.discard(transaction, Stage::Claim).await;

            return workflow.failed(Stage::Claim, failure, |kind| Resolution::Unresolved {
                kind,
            });
        }
    };

    workflow.attempt = Some(receipt.recorded_failures().saturating_add(1));

    match shared.handler.handle(&mut transaction, &envelope).await {
        Ok(()) => {
            let completed =
                bounded(timeout, shared.inbox.complete(&mut transaction, receipt)).await;

            if let Err(failure) = completed {
                workflow.discard(transaction, Stage::Complete).await;

                return workflow.failed(Stage::Complete, failure, |kind| Resolution::Unresolved {
                    kind,
                });
            }

            match bounded(timeout, shared.inbox.commit(transaction)).await {
                Ok(()) => workflow.finish(Resolution::Completed, Stage::Commit),
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
