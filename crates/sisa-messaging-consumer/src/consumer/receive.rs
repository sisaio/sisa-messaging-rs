//! Source opening, startup validation, and profile-specific intake.

use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::sync::Arc;

use sisa_messaging::{
    Delivery, EnvelopeMapper, ErrorClassifier, FailureKind, IndividualDeliverySource,
    IndividualSourceOpenError, Message, Serializer,
};
use sisa_messaging_inbox::{InboxStore, InboxUnitOfWork};
use tokio_util::sync::CancellationToken;

use crate::telemetry::MessageLabels;
use crate::{ConsumerError, ConsumerErrorKind, ConsumerHandler, ConsumerSettings};

use super::Shared;
use super::worker::{self, Workers};

/// One delivery profile's cancel-safe receive and per-item coordinator dispatch.
///
/// The receive loop is generic over this trait so each profile adds only its intake and decision
/// table, not a second runtime.
pub(super) trait Intake: Send {
    /// One received unit of work.
    type Item: Send + 'static;

    /// Waits cancel-safely for the next item; `None` is a clean close and every error is fatal.
    fn receive(&mut self)
    -> impl Future<Output = Result<Option<Self::Item>, ConsumerError>> + Send;

    /// Starts the coordinator that owns this item until it is settled or left.
    fn dispatch(&self, item: Self::Item, workers: &mut Workers);
}

/// The individual-delivery profile.
pub(super) struct IndividualIntake<M, S, Map, Codec, Inbox, H> {
    source: S,

    shared: Arc<Shared<Map, Codec, Inbox, H>>,

    message: PhantomData<fn() -> M>,
}

impl<M, S, Map, Codec, Inbox, H> IndividualIntake<M, S, Map, Codec, Inbox, H> {
    pub(super) fn new(source: S, shared: Arc<Shared<Map, Codec, Inbox, H>>) -> Self {
        Self {
            source,
            shared,
            message: PhantomData,
        }
    }
}

impl<M, S, Map, Codec, Inbox, H> Intake for IndividualIntake<M, S, Map, Codec, Inbox, H>
where
    M: Message,
    S: IndividualDeliverySource,
    Map: EnvelopeMapper<<S::Delivery as Delivery>::Wire> + 'static,
    Codec: Serializer<M> + 'static,
    Inbox: InboxUnitOfWork + InboxStore<Inbox::Transaction>,
    H: ConsumerHandler<M, Inbox::Transaction> + 'static,
{
    type Item = S::Delivery;

    async fn receive(&mut self) -> Result<Option<Self::Item>, ConsumerError> {
        self.source.receive().await.map_err(|error| {
            let failure = error.classify();

            ConsumerError::new(ConsumerErrorKind::Source, failure, Some(Box::new(error)))
        })
    }

    fn dispatch(&self, item: Self::Item, workers: &mut Workers) {
        let shared = Arc::clone(&self.shared);
        let tracker = workers.tracker();
        let stop = workers.stop_token();

        workers.spawn(worker::coordinate::<M, _, _, _, _, _>(
            shared, item, tracker, stop,
        ));
    }
}

/// Whether opening finished or was cancelled first.
pub(super) enum Opened {
    Ready,
    Cancelled,
}

/// Opens the source under `source_timeout` and validates descriptor-dependent limits.
pub(super) async fn open<S: IndividualDeliverySource>(
    source: &mut S,
    settings: &ConsumerSettings,
    max_attempts: NonZeroU32,
    cancel: &CancellationToken,
    labels: MessageLabels,
) -> Result<Opened, ConsumerError> {
    let requirements = settings.mode.requirements();

    let opened = tokio::select! {
        biased;
        () = cancel.cancelled() => return Ok(Opened::Cancelled),
        opened = tokio::time::timeout(settings.source_timeout, source.open(requirements)) => opened,
    };

    let descriptor = match opened {
        Ok(Ok(descriptor)) => descriptor,
        Ok(Err(IndividualSourceOpenError::Unsupported(unsupported))) => {
            return Err(unsupported_error(labels, unsupported));
        }
        Ok(Err(IndividualSourceOpenError::Source(error))) => {
            let failure = error.classify();

            return Err(startup_error(
                labels,
                ConsumerErrorKind::SourceOpen,
                failure,
                Some(Box::new(error)),
            ));
        }
        Ok(Err(_)) => {
            return Err(startup_error(
                labels,
                ConsumerErrorKind::SourceOpen,
                FailureKind::Permanent,
                None,
            ));
        }
        Err(_elapsed) => {
            return Err(startup_error(
                labels,
                ConsumerErrorKind::SourceOpenTimeout,
                FailureKind::Transient,
                None,
            ));
        }
    };

    // Re-check the descriptor so a source that skipped validation still fails closed.
    descriptor
        .validate(requirements)
        .map_err(|unsupported| unsupported_error(labels, unsupported))?;

    if let Some(max_deliver) = descriptor.max_deliver()
        && max_deliver.get() < u64::from(max_attempts.get())
    {
        return Err(startup_error(
            labels,
            ConsumerErrorKind::AttemptBoundExceedsMaxDeliver,
            FailureKind::Permanent,
            None,
        ));
    }

    Ok(Opened::Ready)
}

fn unsupported_error(
    labels: MessageLabels,
    unsupported: sisa_messaging::UnsupportedIndividualRequirement,
) -> ConsumerError {
    startup_error(
        labels,
        ConsumerErrorKind::Unsupported(unsupported.requirement()),
        FailureKind::Permanent,
        Some(Box::new(unsupported)),
    )
}

fn startup_error(
    labels: MessageLabels,
    kind: ConsumerErrorKind,
    failure: FailureKind,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
) -> ConsumerError {
    crate::telemetry::stopping(labels, kind, failure);

    ConsumerError::new(kind, failure, source)
}
