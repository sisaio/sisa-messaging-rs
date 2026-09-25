//! Cancel-safe individual delivery source over one dedicated channel.

use crate::{
    error::RabbitMqError, mapper::RabbitMqWire, settings::RabbitMqSourceSettings,
    settlement::RabbitMqSettlement, telemetry,
};
use futures_util::StreamExt;
use lapin::{
    Channel, Consumer,
    options::{BasicCancelOptions, BasicConsumeOptions, BasicQosOptions},
    types::{FieldTable, ShortString},
};
use sisa_messaging::{
    Delivery, IndividualDeliverySource, IndividualSourceDescriptor, IndividualSourceOpenError,
    IndividualSourceRequirements,
};
use std::time::Instant;

/// One broker delivery with its settlement handle.
pub struct RabbitMqDelivery {
    wire: RabbitMqWire,

    settlement: RabbitMqSettlement,
}

impl Delivery for RabbitMqDelivery {
    type Wire = RabbitMqWire;
    type Settlement = RabbitMqSettlement;

    fn into_parts(self) -> (Self::Wire, Self::Settlement) {
        (self.wire, self.settlement)
    }
}

/// Source consuming one application-declared queue on a dedicated application-owned channel.
///
/// The provider never declares topology. `open` reports a descriptor with no acknowledgement
/// deadline, no delivery bound, no delayed retry, and no heartbeat, but with terminal discard;
/// it validates caller requirements before any broker I/O, so an unsatisfied requirement never
/// starts a consumer. It then sets the per-consumer prefetch with `basic.qos` and starts a
/// manual-acknowledgement consumer.
///
/// Receiving never settles a delivery, and dropping a pending receive loses nothing. A consumer
/// stream error, or a broker-initiated cancel such as a deleted queue, is
/// [`RabbitMqError::Source`] and closes the source. A successful open happens at most once;
/// opening again after a successful open, or after the source closed, is
/// [`RabbitMqError::Settings`]. A failed or cancelled open is not recorded and may already have
/// started a broker consumer, so the application must discard that channel rather than retry
/// `open` on it: an orphaned consumer may hold up to `prefetch` unacknowledged deliveries until
/// the channel closes.
///
/// Use one channel per source: settlement confirmation issues `basic.qos` on this channel. The
/// connection must not enable lapin automatic recovery.
pub struct RabbitMqDeliverySource {
    channel: Channel,

    queue: ShortString,

    prefetch: u16,

    consumer: Option<Consumer>,

    closed: bool,
}

impl RabbitMqDeliverySource {
    /// Creates a source without network I/O.
    ///
    /// Returns [`RabbitMqError::Settings`] when the queue name is empty, longer than 255 bytes,
    /// or contains an ASCII control byte.
    pub fn new(channel: Channel, settings: RabbitMqSourceSettings) -> Result<Self, RabbitMqError> {
        if settings.queue.is_empty() || settings.queue.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(RabbitMqError::Settings);
        }

        let queue = ShortString::try_new(settings.queue).map_err(|_| RabbitMqError::Settings)?;

        Ok(Self {
            channel,
            queue,
            prefetch: settings.prefetch.get(),
            consumer: None,
            closed: false,
        })
    }

    /// Stops receiving and cancels the broker consumer. Closing is terminal.
    ///
    /// The source is marked closed before `basic.cancel` is sent, so later receives return
    /// `Ok(None)` and later opens return [`RabbitMqError::Settings`], even when cancellation
    /// fails or this future is dropped. Deliveries already received stay valid and can still be
    /// settled. Deliveries the client buffered but `receive` never returned stay unacknowledged
    /// until the application closes the channel, at which point the broker requeues them.
    /// Returns [`RabbitMqError::Source`] when the cancel is not confirmed.
    ///
    /// After a failed or cancelled `open` or `close`, the application must discard the channel:
    /// an orphaned broker consumer may still hold up to `prefetch` deliveries until the channel
    /// closes.
    pub async fn close(&mut self) -> Result<(), RabbitMqError> {
        self.closed = true;

        let Some(consumer) = self.consumer.take() else {
            return Ok(());
        };

        if !self.channel.status().connected() {
            return Ok(());
        }

        self.channel
            .basic_cancel(consumer.tag(), BasicCancelOptions { nowait: false })
            .await
            .map_err(|_| RabbitMqError::Source)
    }

    fn descriptor() -> Result<IndividualSourceDescriptor, RabbitMqError> {
        // Arguments: no ack wait, no max deliver, no delayed retry, terminal discard, no heartbeat.
        IndividualSourceDescriptor::new(None, None, false, true, false)
            .map_err(|_| RabbitMqError::Settings)
    }

    async fn start(&mut self) -> Result<(), RabbitMqError> {
        if self.closed || self.consumer.is_some() {
            return Err(RabbitMqError::Settings);
        }

        self.channel
            .basic_qos(self.prefetch, BasicQosOptions { global: false })
            .await
            .map_err(|_| RabbitMqError::Source)?;

        let options = BasicConsumeOptions {
            no_local: false,
            no_ack: false,
            exclusive: false,
            nowait: false,
        };

        let consumer = self
            .channel
            .basic_consume(
                self.queue.clone(),
                ShortString::default(),
                options,
                FieldTable::default(),
            )
            .await
            .map_err(|_| RabbitMqError::Source)?;

        self.consumer = Some(consumer);

        Ok(())
    }

    async fn receive_inner(&mut self) -> Result<Option<RabbitMqDelivery>, RabbitMqError> {
        if self.closed {
            return Ok(None);
        }

        let consumer = self.consumer.as_mut().ok_or(RabbitMqError::Settings)?;

        let delivery = match consumer.next().await {
            Some(Ok(delivery)) => delivery,
            Some(Err(_)) | None => {
                // A local close returns early above, so a stream end here is broker-initiated.
                self.consumer = None;
                self.closed = true;

                return Err(RabbitMqError::Source);
            }
        };

        let wire = RabbitMqWire {
            exchange: delivery.exchange.into(),
            routing_key: delivery.routing_key.into(),
            properties: delivery.properties,
            payload: delivery.data,
        };

        let settlement =
            RabbitMqSettlement::new(self.channel.clone(), delivery.acker, self.prefetch);

        Ok(Some(RabbitMqDelivery { wire, settlement }))
    }
}

impl IndividualDeliverySource for RabbitMqDeliverySource {
    type Delivery = RabbitMqDelivery;
    type Error = RabbitMqError;

    /// Validates requirements, then sets the prefetch and starts the consumer.
    ///
    /// Succeeds at most once per source. After a failed or cancelled open, discard the channel:
    /// a broker consumer may already exist and hold up to `prefetch` unacknowledged deliveries
    /// until the channel closes.
    async fn open(
        &mut self,
        requirements: IndividualSourceRequirements,
    ) -> Result<IndividualSourceDescriptor, IndividualSourceOpenError<Self::Error>> {
        let descriptor = Self::descriptor().map_err(IndividualSourceOpenError::Source)?;

        descriptor
            .validate(requirements)
            .map_err(IndividualSourceOpenError::Unsupported)?;

        self.start()
            .await
            .map_err(IndividualSourceOpenError::Source)?;

        Ok(descriptor)
    }

    #[tracing::instrument(
        name = "receive",
        target = "messaging.rabbitmq",
        level = "debug",
        skip_all
    )]
    async fn receive(&mut self) -> Result<Option<Self::Delivery>, Self::Error> {
        let started = Instant::now();
        let result = self.receive_inner().await;

        match &result {
            Ok(Some(_)) => telemetry::finished("receive", started.elapsed(), Ok(())),
            Ok(None) => telemetry::receive_closed(started.elapsed()),
            Err(error) => telemetry::finished("receive", started.elapsed(), Err(*error)),
        }

        result
    }
}
