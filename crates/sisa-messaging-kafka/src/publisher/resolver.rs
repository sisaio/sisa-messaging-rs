use std::error::Error;

use sisa_messaging::{ErrorClassifier, SerializedEnvelope};

use crate::RoutingDestinationError;

/// Resolves a Kafka topic synchronously for each publish operation.
///
/// Implementations should be quick and nonblocking because resolution runs in the publish
/// path. The resolver error's [`ErrorClassifier`] classification is preserved by the publisher:
/// transient errors are retryable, while permanent errors are not expected to succeed if retried.
pub trait KafkaTopicResolver: Send + Sync {
    /// Error returned when a topic cannot be selected for an envelope.
    type Error: Error + Send + Sync + 'static + ErrorClassifier;

    /// Resolves the Kafka topic for one envelope. This method runs synchronously during each
    /// publish operation and should not block on I/O.
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
