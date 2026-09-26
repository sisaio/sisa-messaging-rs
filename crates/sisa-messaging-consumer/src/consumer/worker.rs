//! Bounded per-delivery coordinators and their accounting.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use sisa_messaging::{
    Delivery, EnvelopeMapper, ErrorClassifier, FailureKind, IndividualSettlement,
    IndividualSettlementError, Message, Serializer,
};
use sisa_messaging_inbox::{InboxStore, InboxUnitOfWork};
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tokio_util::task::{AbortOnDropHandle, TaskTracker};

use crate::telemetry::{self, DeliveryLabels, MessageLabels};
use crate::{ConsumerError, ConsumerErrorKind, ConsumerExit, ConsumerHandler};

use super::Shared;
use super::process::{self, ProviderSource};
use super::settlement::{self, IndividualAction, SettlementFailure};

type CoordinatorResult = Result<(), ConsumerError>;

/// Live coordinators, their workflow tasks, the internal stop signal, and the first failure.
///
/// One admitted delivery occupies one permit from dispatch until its coordinator is reaped, and
/// its workflow task, the only owner of its transaction, is tracked separately so shutdown can
/// wait until every transaction has been dropped.
pub(super) struct Workers {
    pub(super) coordinators: JoinSet<CoordinatorResult>,

    pub(super) tracker: TaskTracker,

    stop: CancellationToken,

    capacity: usize,

    first_error: Option<ConsumerError>,

    labels: MessageLabels,
}

impl Workers {
    pub(super) fn new(capacity: usize, labels: MessageLabels) -> Self {
        Self {
            coordinators: JoinSet::new(),
            tracker: TaskTracker::new(),
            stop: CancellationToken::new(),
            capacity,
            first_error: None,
            labels,
        }
    }

    /// Reports whether another delivery may be admitted.
    pub(super) fn has_capacity(&self) -> bool {
        self.coordinators.len().max(self.tracker.len()) < self.capacity
    }

    pub(super) fn stop_token(&self) -> CancellationToken {
        self.stop.clone()
    }

    pub(super) fn tracker(&self) -> TaskTracker {
        self.tracker.clone()
    }

    pub(super) fn spawn<F>(&mut self, coordinator: F)
    where
        F: Future<Output = CoordinatorResult> + Send + 'static,
    {
        self.coordinators.spawn(coordinator);
    }

    /// Records every coordinator that has already finished without waiting.
    pub(super) fn reap_ready(&mut self) {
        while let Some(joined) = self.coordinators.try_join_next() {
            self.record(joined);
        }
    }

    /// Waits for one coordinator to finish and records it.
    pub(super) async fn next_finished(&mut self) {
        match self.coordinators.join_next().await {
            Some(joined) => self.record(joined),
            // Only transiently reachable while an aborted workflow task is still being dropped.
            None => tokio::task::yield_now().await,
        }
    }

    pub(super) fn record(&mut self, joined: Result<CoordinatorResult, JoinError>) {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(error)) => self.fail(error),
            // Coordinators are cancelled only by the drain deadline, which leaves them unsettled.
            Err(error) if error.is_cancelled() => {}
            Err(_) => {
                telemetry::task_failed(self.labels, ConsumerErrorKind::Runtime);

                self.fail(ConsumerError::new(
                    ConsumerErrorKind::Runtime,
                    FailureKind::Permanent,
                    None,
                ));
            }
        }
    }

    /// Keeps the first fatal failure and stops receiving.
    pub(super) fn fail(&mut self, error: ConsumerError) {
        if self.first_error.is_none() {
            telemetry::stopping(self.labels, error.kind(), error.failure_kind());

            self.first_error = Some(error);
        }

        self.stop.cancel();
    }

    /// Resolves the run result: the first failure wins over the first clean exit.
    pub(super) fn finish(self, exit: Option<ConsumerExit>) -> Result<ConsumerExit, ConsumerError> {
        match (self.first_error, exit) {
            (Some(error), _) => Err(error),
            (None, Some(exit)) => Ok(exit),
            (None, None) => Err(ConsumerError::new(
                ConsumerErrorKind::Runtime,
                FailureKind::Permanent,
                None,
            )),
        }
    }
}

/// Owns one delivery's settlement handle while its workflow runs in a tracked task.
///
/// The transaction exists only inside the workflow task. The coordinator settles only after
/// joining that task, so no transaction is ever held across broker I/O, and it acknowledges only
/// a completed resolution. Dropping the coordinator aborts the workflow.
pub(super) async fn coordinate<M, D, Map, Codec, Inbox, H>(
    shared: Arc<Shared<Map, Codec, Inbox, H>>,
    delivery: D,
    tracker: TaskTracker,
    stop: CancellationToken,
) -> CoordinatorResult
where
    M: Message,
    D: Delivery<Settlement: IndividualSettlement>,
    Map: EnvelopeMapper<D::Wire> + 'static,
    Codec: Serializer<M> + 'static,
    Inbox: InboxUnitOfWork + InboxStore<Inbox::Transaction>,
    H: ConsumerHandler<M, Inbox::Transaction> + 'static,
{
    // A panic anywhere in this coordinator, including settlement or handle drop, raises the stop
    // signal at once instead of only when the join set is next reaped.
    let _panic_stop = StopOnPanic(stop.clone());

    let (wire, settlement) = delivery.into_parts();

    let workflow = AbortOnDropHandle::new(tracker.spawn(process::process::<M, _, _, _, _, _>(
        Arc::clone(&shared),
        wire,
    )));

    let processed = match workflow.await {
        Ok(processed) => processed,
        Err(error) => {
            // Never render the panic payload; the settlement handle is dropped unsettled.
            let kind = if error.is_panic() {
                ConsumerErrorKind::HandlerPanicked
            } else {
                ConsumerErrorKind::Runtime
            };

            drop(settlement);
            telemetry::task_failed(shared.labels, kind);
            stop.cancel();

            return Err(ConsumerError::new(kind, FailureKind::Permanent, None));
        }
    };

    let plan = settlement::decide_individual(
        shared.settings.mode,
        shared.settings.nak_delay,
        processed.resolution,
    );

    let labels = DeliveryLabels {
        message: shared.labels,
        message_id: processed.message_id,
        attempt: processed.attempt,
    };

    telemetry::resolved(
        labels,
        processed.resolution,
        processed.stage.as_str(),
        plan.action,
    );

    if let Err((failure, source)) =
        settle(plan.action, settlement, shared.settings.settlement_timeout).await
    {
        telemetry::settlement_failed(
            labels,
            plan.action,
            failure,
            std::any::type_name::<<D::Settlement as IndividualSettlement>::Error>(),
        );

        if failure.stops() {
            stop.cancel();

            return Err(ConsumerError::new(
                ConsumerErrorKind::Settlement,
                failure.failure_kind(),
                source,
            ));
        }
    } else if plan.action != IndividualAction::Leave {
        telemetry::settled(labels, plan.action);
    }

    if let Some(cause) = plan.stop {
        stop.cancel();

        return Err(ConsumerError::new(
            cause.kind(),
            processed
                .resolution
                .failure_kind()
                .unwrap_or(FailureKind::Permanent),
            processed.error,
        ));
    }

    Ok(())
}

/// Cancels the stop signal when dropped during a panic unwind.
struct StopOnPanic(CancellationToken);

impl Drop for StopOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.cancel();
        }
    }
}

/// Performs one bounded settlement operation; `Leave` drops the handle without broker I/O.
async fn settle<St: IndividualSettlement>(
    action: IndividualAction,
    settlement: St,
    timeout: Duration,
) -> Result<(), (SettlementFailure, Option<ProviderSource>)> {
    let result = match action {
        IndividualAction::Ack => tokio::time::timeout(timeout, settlement.ack()).await,
        IndividualAction::Nak { delay } => {
            tokio::time::timeout(timeout, settlement.nak(delay)).await
        }
        IndividualAction::Terminate => tokio::time::timeout(timeout, settlement.terminate()).await,
        IndividualAction::Leave => {
            drop(settlement);

            return Ok(());
        }
    };

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            let failure = if matches!(error, IndividualSettlementError::Unsupported(_)) {
                SettlementFailure::Unsupported
            } else {
                SettlementFailure::Failed(error.classify())
            };

            Err((failure, Some(Box::new(error))))
        }
        Err(_elapsed) => Err((SettlementFailure::TimedOut, None)),
    }
}
