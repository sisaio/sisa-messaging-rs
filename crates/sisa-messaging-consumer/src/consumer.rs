//! Public consumer façade and the profile-generic receive loop.

mod partitioned;
mod process;
mod receive;
pub(crate) mod settlement;
mod shutdown;
mod worker;

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use sisa_messaging::{
    Delivery, EnvelopeMapper, IndividualDeliverySource, Message, PartitionedLogDeliverySource,
    Serializer,
};
use sisa_messaging_inbox::{InboxScope, InboxStore, InboxUnitOfWork};
use tokio_util::sync::CancellationToken;

use crate::telemetry::{self, MessageLabels};
use crate::{ConsumerConfigError, ConsumerError, ConsumerExit, ConsumerHandler, ConsumerSettings};

use self::receive::{IndividualIntake, Intake, Opened};
use self::worker::Workers;

/// Components shared by every delivery of one consumer; cloned once per delivery as an `Arc`.
pub(crate) struct Shared<Map, Codec, Inbox, H> {
    pub(crate) mapper: Map,

    pub(crate) codec: Codec,

    pub(crate) inbox: Inbox,

    pub(crate) handler: H,

    pub(crate) scope: InboxScope,

    pub(crate) settings: ConsumerSettings,

    pub(crate) labels: MessageLabels,

    /// Caller context captured before workflow tasks are spawned.
    pub(crate) ambient: opentelemetry::Context,
}

/// A typed, bounded individual-delivery consumer for one message type.
///
/// The component tuple is private so call sites annotate only `Consumer<Message, _>`. Every
/// component is statically dispatched.
pub struct Consumer<M, Components> {
    components: Components,

    scope: InboxScope,

    settings: ConsumerSettings,

    message: PhantomData<fn() -> M>,
}

impl<M, S, Map, Codec, Inbox, H> Consumer<M, (S, Map, Codec, Inbox, H)>
where
    M: Message,
    S: IndividualDeliverySource,
    Map: EnvelopeMapper<<S::Delivery as Delivery>::Wire> + 'static,
    Codec: Serializer<M> + 'static,
    Inbox: InboxUnitOfWork + InboxStore<Inbox::Transaction>,
    H: ConsumerHandler<M, Inbox::Transaction> + 'static,
{
    /// Validates settings once and constructs an idle consumer without performing I/O.
    ///
    /// Rejects a zero source, database, settlement, or drain timeout. Broker mode requires a
    /// nonzero `nak_delay`; immediate-requeue mode requires zero.
    pub fn new(
        source: S,
        mapper: Map,
        codec: Codec,
        inbox: Inbox,
        scope: InboxScope,
        handler: H,
        settings: ConsumerSettings,
    ) -> Result<Self, ConsumerConfigError> {
        settings.validate()?;

        Ok(Self {
            components: (source, mapper, codec, inbox, handler),
            scope,
            settings,
            message: PhantomData,
        })
    }

    /// Opens the source and processes deliveries until cancellation, source close, or a fatal
    /// failure.
    ///
    /// Opening requests the selected mode's source requirements under `source_timeout`, and a
    /// finite source delivery bound below the inbox attempt bound is rejected before the first
    /// receive. Once receiving stops for any cause, in-flight deliveries drain for at most
    /// `drain_timeout`; remaining work is then aborted, dropping its transactions and leaving its
    /// deliveries unsettled. The call returns only after every transaction has been dropped. The
    /// first fatal failure, including one observed during the drain, takes precedence over a
    /// cancellation or clean close.
    pub async fn run(self, cancel: CancellationToken) -> Result<ConsumerExit, ConsumerError> {
        let labels = MessageLabels {
            message_type: M::TYPE,
            version: M::VERSION,
        };

        telemetry::started(labels);
        let ambient = opentelemetry::Context::current();

        let (mut source, mapper, codec, inbox, handler) = self.components;
        let settings = self.settings;
        let capacity = settings.max_in_flight.get();
        let drain_timeout = settings.drain_timeout;
        let max_attempts = <Inbox as InboxStore<Inbox::Transaction>>::max_attempts(&inbox);

        let result =
            match receive::open(&mut source, &settings, max_attempts, &cancel, labels).await {
                Ok(Opened::Ready) => {
                    let shared = Arc::new(Shared {
                        mapper,
                        codec,
                        inbox,
                        handler,
                        scope: self.scope,
                        settings,
                        labels,
                        ambient,
                    });

                    let intake = IndividualIntake::<M, _, _, _, _, _>::new(source, shared);
                    let workers = Workers::new(capacity, labels);

                    run_loop(intake, workers, &cancel, drain_timeout).await
                }
                Ok(Opened::Cancelled) => Ok(ConsumerExit::Cancelled),
                Err(error) => Err(error),
            };

        telemetry::stopped(
            labels,
            match &result {
                Ok(ConsumerExit::Cancelled) => "cancelled",
                Ok(ConsumerExit::SourceClosed) => "source_closed",
                Err(_) => "failed",
            },
        );

        result
    }
}

impl<M, S, Map, Codec, Inbox, H> Consumer<M, (S, Map, Codec, Inbox, H)>
where
    M: Message,
    S: PartitionedLogDeliverySource,
    Map: EnvelopeMapper<<S::Delivery as Delivery>::Wire> + 'static,
    Codec: Serializer<M> + 'static,
    Inbox: InboxUnitOfWork + InboxStore<Inbox::Transaction>,
    H: ConsumerHandler<M, Inbox::Transaction> + 'static,
{
    /// Constructs a partitioned-log consumer with one active record per partition.
    ///
    /// Heartbeat settings are rejected: partition liveness belongs to the source's group
    /// ownership protocol, not to individual delivery acknowledgement deadlines.
    pub fn new_partitioned(
        source: S,
        mapper: Map,
        codec: Codec,
        inbox: Inbox,
        scope: InboxScope,
        handler: H,
        settings: ConsumerSettings,
    ) -> Result<Self, ConsumerConfigError> {
        settings.validate()?;

        if settings.heartbeat_interval.is_some() {
            return Err(ConsumerConfigError::HeartbeatRequiresIndividualSource);
        }

        Ok(Self {
            components: (source, mapper, codec, inbox, handler),
            scope,
            settings,
            message: PhantomData,
        })
    }

    /// Opens and runs the partitioned source until cancellation, close, or a fatal failure.
    ///
    /// A partition advances only after a committed success or durable terminal dead record.
    /// A timed-out, transient, or otherwise ambiguous advance pauses only that partition without
    /// failing the run; unrelated partitions may continue. A returned permanent provider error
    /// stops the run with [`ConsumerErrorKind::Settlement`]. An overlapping live record stops the
    /// run. On every exit, active work drains for at most the configured `drain_timeout` before
    /// its transactions are released.
    pub async fn run_partitioned(
        self,
        cancel: CancellationToken,
    ) -> Result<ConsumerExit, ConsumerError> {
        partitioned::run::<M, _, _, _, _, _>(self, cancel).await
    }
}

/// Receives while capacity allows, then drains for every stop cause.
///
/// The source is polled only below capacity, and a finishing delivery never cancels a pending
/// receive. Cancellation, an internal stop, and receive readiness are the only select branches;
/// no workflow or settlement I/O runs inside one.
async fn run_loop<I: Intake>(
    mut intake: I,
    mut workers: Workers,
    cancel: &CancellationToken,
    drain_timeout: Duration,
) -> Result<ConsumerExit, ConsumerError> {
    let stop = workers.stop_token();

    let exit = loop {
        workers.reap_ready();

        if stop.is_cancelled() {
            break None;
        }

        if workers.has_capacity() {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break Some(ConsumerExit::Cancelled),
                () = stop.cancelled() => break None,
                received = intake.receive() => match received {
                    Ok(Some(item)) => if intake.dispatch(item, &mut workers) {
                        break Some(ConsumerExit::SourceClosed);
                    },
                    Ok(None) => break Some(ConsumerExit::SourceClosed),
                    Err(error) => {
                        workers.fail(error);

                        break None;
                    }
                },
            }
        } else {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break Some(ConsumerExit::Cancelled),
                () = stop.cancelled() => break None,
                () = workers.next_finished() => {}
            }
        }
    };

    intake.stop();

    // The source stays alive until the drain ends because settlement handles may depend on it.
    shutdown::drain(&mut workers, drain_timeout).await;

    drop(intake);

    workers.finish(exit)
}
