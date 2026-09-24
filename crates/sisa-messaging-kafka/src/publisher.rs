//! Kafka publication under application-configured acknowledgement policy.

mod delivery;
mod resolver;

use sisa_messaging::{EnvelopeMapper, Publisher, SerializedEnvelope};

use crate::{KafkaClient, KafkaPublishError, KafkaPublisherSettings};

pub use resolver::{KafkaTopicResolver, RoutingDestinationResolver};

/// Publishes shared envelopes through an application-created Kafka producer.
///
/// The producer's acknowledgement policy is configured by the application and is reflected in
/// successful delivery reports. The publisher does not override or inspect that policy.
pub struct KafkaPublisher<R> {
    client: KafkaClient,

    resolver: R,

    settings: KafkaPublisherSettings,
}

impl<R> KafkaPublisher<R> {
    /// Constructs a publisher without performing network I/O.
    ///
    /// The client was constructed from application-owned settings.
    pub fn new(client: KafkaClient, resolver: R, settings: KafkaPublisherSettings) -> Self {
        Self {
            client,
            resolver,
            settings,
        }
    }
}

impl<R> Publisher for KafkaPublisher<R>
where
    R: KafkaTopicResolver,
{
    type Error = KafkaPublishError;

    async fn publish(&self, envelope: &SerializedEnvelope) -> Result<(), Self::Error> {
        let topic = self
            .resolver
            .resolve(envelope)
            .map_err(delivery::map_error)?;

        let record = crate::KafkaEnvelopeMapper
            .encode(envelope)
            .map_err(delivery::map_mapping_error)?;

        delivery::deliver(&self.client, &topic, &record, self.settings.enqueue_timeout).await
    }
}
