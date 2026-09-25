//! Confirmed, mandatory AMQP publication over an application-owned channel.

use crate::{
    error::RabbitMqError,
    mapper::{RabbitMqMapper, RouteResolver},
    settings::RabbitMqPublisherSettings,
    telemetry,
};
use lapin::{Channel, Confirmation, options::BasicPublishOptions};
use sisa_messaging::{Publisher, SerializedEnvelope};
use std::time::Instant;

/// Publisher over an application-owned channel in publisher-confirm mode.
///
/// Every publish is mandatory and awaits the broker confirm:
///
/// - a positive confirm without a returned message is success;
/// - a positive confirm carrying a returned message is [`RabbitMqError::Unroutable`], and the
///   returned payload is dropped immediately;
/// - a negative confirm is [`RabbitMqError::Rejected`];
/// - a client failure or a closed channel or connection is [`RabbitMqError::Publish`], and an
///   elapsed deadline is [`RabbitMqError::Timeout`]; in both cases the broker outcome is unknown.
///
/// Concurrent publishes pipeline on the one channel. Dropping a publish future before the
/// publication is handed to the client sends nothing; dropping it afterwards leaves the outcome
/// unknown.
///
/// # Channel lifecycle
///
/// The provider never declares topology and never enables confirms. Publishing to a missing
/// exchange makes the broker close the channel; every later publish then fails with
/// [`RabbitMqError::Publish`] and the application must open a new channel and build a new
/// publisher. The client retains the confirms and returns of abandoned publishes until the
/// channel is drained with `wait_for_confirms`, so recycle the channel after a
/// [`RabbitMqError::Timeout`] or [`RabbitMqError::Publish`] rather than publishing on it
/// indefinitely.
pub struct RabbitMqPublisher<R> {
    channel: Channel,

    mapper: RabbitMqMapper<R>,

    settings: RabbitMqPublisherSettings,
}

impl<R> RabbitMqPublisher<R> {
    /// Creates a publisher without network I/O.
    ///
    /// Returns [`RabbitMqError::Settings`] when the publish timeout is zero or when `channel` is
    /// not in publisher-confirm mode; the application enables confirms with `confirm_select`
    /// before handing over the channel.
    pub fn new(
        channel: Channel,
        resolver: R,
        settings: RabbitMqPublisherSettings,
    ) -> Result<Self, RabbitMqError> {
        let settings = settings.validate()?;

        if !channel.status().confirm() {
            return Err(RabbitMqError::Settings);
        }

        Ok(Self {
            channel,
            mapper: RabbitMqMapper::new(resolver),
            settings,
        })
    }
}

impl<R: RouteResolver> RabbitMqPublisher<R> {
    async fn publish_inner(&self, envelope: &SerializedEnvelope) -> Result<(), RabbitMqError> {
        let limit = usize::try_from(self.settings.max_message_size.get()).unwrap_or(usize::MAX);

        if envelope.payload.len() > limit {
            return Err(RabbitMqError::PayloadTooLarge);
        }

        let (route, properties) = self
            .mapper
            .project(envelope)
            .map_err(|_| RabbitMqError::Mapping)?;

        let options = BasicPublishOptions {
            mandatory: true,
            immediate: false,
        };

        let operation = async {
            telemetry::sent_attempted();

            let confirm = self
                .channel
                .basic_publish(
                    route.exchange.into_short_string(),
                    route.routing_key.into_short_string(),
                    options,
                    &envelope.payload,
                    properties,
                )
                .await
                .map_err(|_| RabbitMqError::Publish)?;

            match confirm.await.map_err(|_| RabbitMqError::Publish)? {
                Confirmation::Ack(None) => Ok(()),
                Confirmation::Ack(Some(returned)) => {
                    drop(returned);

                    Err(RabbitMqError::Unroutable)
                }
                Confirmation::Nack(_) => Err(RabbitMqError::Rejected),
                Confirmation::NotRequested => Err(RabbitMqError::Settings),
            }
        };

        tokio::time::timeout(self.settings.publish_timeout, operation)
            .await
            .map_err(|_| RabbitMqError::Timeout)?
    }
}

impl<R: RouteResolver> Publisher for RabbitMqPublisher<R> {
    type Error = RabbitMqError;

    #[tracing::instrument(
        name = "publish",
        target = "messaging.rabbitmq",
        level = "debug",
        skip_all
    )]
    async fn publish(&self, envelope: &SerializedEnvelope) -> Result<(), Self::Error> {
        let started = Instant::now();
        let result = self.publish_inner(envelope).await;
        telemetry::finished("publish", started.elapsed(), result);

        result
    }
}
