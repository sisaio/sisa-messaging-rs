//! Cancel-safe individual JetStream delivery source.

use crate::{error::NatsError, mapper::NatsWire, settlement::NatsSettlement, telemetry};
use async_nats::jetstream::{
    self,
    consumer::{AckPolicy, PullConsumer, pull::MessagesErrorKind},
};
use futures_util::StreamExt;
use sisa_messaging::{
    Delivery, IndividualDeliverySource, IndividualSourceDescriptor, IndividualSourceOpenError,
    IndividualSourceRequirements,
};
use std::{num::NonZeroU64, time::Instant};

/// One broker delivery with its settlement handle.
pub struct NatsDelivery {
    wire: NatsWire,
    settlement: NatsSettlement,
}

impl Delivery for NatsDelivery {
    type Wire = NatsWire;
    type Settlement = NatsSettlement;
    fn into_parts(self) -> (Self::Wire, Self::Settlement) {
        (self.wire, self.settlement)
    }
}

/// Source over a caller-provisioned pull consumer.
pub struct NatsDeliverySource {
    consumer: PullConsumer,
    stream: Option<jetstream::consumer::pull::Stream>,
    closed: bool,
}

impl NatsDeliverySource {
    /// Creates a source without network I/O or consumer provisioning.
    pub fn new(consumer: PullConsumer) -> Self {
        Self {
            consumer,
            stream: None,
            closed: false,
        }
    }

    /// Stops receiving locally and releases the active pull stream.
    pub fn close(&mut self) {
        self.stream = None;
        self.closed = true;
    }

    fn descriptor(&self) -> Result<IndividualSourceDescriptor, NatsError> {
        let config = &self.consumer.cached_info().config;
        let ack_wait = (!config.ack_wait.is_zero()).then_some(config.ack_wait);
        let max_deliver = u64::try_from(config.max_deliver)
            .ok()
            .and_then(NonZeroU64::new);
        // The pull consumer uses explicit acknowledgements only. See `open` validation.
        IndividualSourceDescriptor::new(ack_wait, max_deliver, true, true, true)
            .map_err(|_| NatsError::Settings)
    }
}

impl IndividualDeliverySource for NatsDeliverySource {
    type Delivery = NatsDelivery;
    type Error = NatsError;

    async fn open(
        &mut self,
        requirements: IndividualSourceRequirements,
    ) -> Result<IndividualSourceDescriptor, IndividualSourceOpenError<Self::Error>> {
        self.consumer
            .info()
            .await
            .map_err(|_| IndividualSourceOpenError::Source(NatsError::Source))?;
        if self.consumer.cached_info().config.ack_policy != AckPolicy::Explicit {
            return Err(IndividualSourceOpenError::Source(NatsError::Settings));
        }
        let descriptor = self
            .descriptor()
            .map_err(IndividualSourceOpenError::Source)?;
        descriptor
            .validate(requirements)
            .map_err(IndividualSourceOpenError::Unsupported)?;
        let stream = self
            .consumer
            .stream()
            .max_messages_per_batch(1)
            .messages()
            .await
            .map_err(|_| IndividualSourceOpenError::Source(NatsError::Source))?;
        self.stream = Some(stream);
        self.closed = false;
        Ok(descriptor)
    }

    #[tracing::instrument(name = "receive", target = "messaging.nats", level = "debug", skip_all)]
    async fn receive(&mut self) -> Result<Option<Self::Delivery>, Self::Error> {
        let started = Instant::now();
        let result = self.receive_inner().await;
        telemetry::finished(
            "receive",
            started.elapsed(),
            match &result {
                Ok(Some(_)) => Ok(()),
                Ok(None) => return result,
                Err(error) => Err(*error),
            },
        );
        result
    }
}

impl NatsDeliverySource {
    async fn receive_inner(&mut self) -> Result<Option<NatsDelivery>, NatsError> {
        if self.closed {
            return Ok(None);
        }
        let stream = self.stream.as_mut().ok_or(NatsError::Settings)?;
        let message = match stream.next().await {
            Some(Ok(message)) => message,
            Some(Err(error)) => {
                if matches!(
                    error.kind(),
                    MessagesErrorKind::ConsumerDeleted | MessagesErrorKind::PushBasedConsumer
                ) {
                    self.stream = None;
                    self.closed = true;
                }
                return Err(NatsError::Source);
            }
            None => {
                self.stream = None;
                self.closed = true;
                return Ok(None);
            }
        };
        let wire = NatsWire {
            subject: message.message.subject.to_string(),
            headers: message.message.headers.clone().unwrap_or_default(),
            payload: message.message.payload.to_vec(),
        };
        let settlement = NatsSettlement { message };
        Ok(Some(NatsDelivery { wire, settlement }))
    }
}
