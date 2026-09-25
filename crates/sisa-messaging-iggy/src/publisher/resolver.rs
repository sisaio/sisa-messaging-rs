use std::error::Error;

use iggy::prelude::Identifier;
use sisa_messaging::{ErrorClassifier, SerializedEnvelope};

use crate::RoutingDestinationError;

/// Resolves a validated Iggy stream and topic [`Identifier`] pair synchronously for each publish
/// operation.
///
/// Implementations should be quick and nonblocking because resolution runs in the publish path.
/// The resolver error's [`ErrorClassifier`] classification is preserved by the publisher: transient
/// errors are retryable, while permanent errors are not expected to succeed if retried.
pub trait IggyDestinationResolver: Send + Sync {
    /// Error returned when a stream and topic cannot be selected for an envelope.
    type Error: Error + Send + Sync + 'static + ErrorClassifier;

    /// Resolves the Iggy stream and topic identifiers for one envelope. This method runs
    /// synchronously during each publish operation and should not block on I/O.
    fn resolve(
        &self,
        envelope: &SerializedEnvelope,
    ) -> Result<(Identifier, Identifier), Self::Error>;
}

/// Resolves the stream and topic from the shared logical destination metadata, formatted as
/// `<stream>/<topic>` where each side is either a numeric id or a name up to 255 bytes.
#[derive(Clone, Copy, Debug, Default)]
pub struct RoutingDestinationResolver;

impl IggyDestinationResolver for RoutingDestinationResolver {
    type Error = RoutingDestinationError;

    fn resolve(
        &self,
        envelope: &SerializedEnvelope,
    ) -> Result<(Identifier, Identifier), Self::Error> {
        let destination = envelope
            .metadata
            .routing
            .destination
            .as_ref()
            .ok_or(RoutingDestinationError)?;

        let (stream, topic) = destination
            .as_str()
            .split_once('/')
            .ok_or(RoutingDestinationError)?;

        if topic.contains('/') {
            return Err(RoutingDestinationError);
        }

        let stream = Identifier::from_str_value(stream).map_err(|_| RoutingDestinationError)?;
        let topic = Identifier::from_str_value(topic).map_err(|_| RoutingDestinationError)?;

        Ok((stream, topic))
    }
}
