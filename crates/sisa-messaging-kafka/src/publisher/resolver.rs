use std::error::Error;

use sisa_messaging::{ErrorClassifier, SerializedEnvelope};

use crate::RoutingDestinationError;

/// Error raised by a Kafka topic resolver.
pub trait KafkaTopicResolver: Send + Sync {
    /// Error returned when a topic cannot be selected for an envelope.
    type Error: Error + Send + Sync + 'static + ErrorClassifier;

    /// Resolves the Kafka topic for one envelope.
    fn resolve(&self, envelope: &SerializedEnvelope) -> Result<String, Self::Error>;
}

/// Resolves the topic from the shared logical destination metadata.
#[derive(Clone, Copy, Debug, Default)]
pub struct RoutingDestinationResolver;

impl KafkaTopicResolver for RoutingDestinationResolver {
    type Error = RoutingDestinationError;

    fn resolve(&self, envelope: &SerializedEnvelope) -> Result<String, Self::Error> {
        envelope
            .metadata
            .routing
            .destination
            .as_ref()
            .map(|destination| destination.as_str().to_owned())
            .ok_or(RoutingDestinationError)
    }
}
