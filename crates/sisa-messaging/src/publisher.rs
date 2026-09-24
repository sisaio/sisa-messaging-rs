//! Transport-configured publication capability.

use std::error::Error;
use std::future::Future;

use crate::{ErrorClassifier, SerializedEnvelope};

/// Performs one publish attempt according to the transport's configured delivery policy.
pub trait Publisher: Send + Sync {
    /// Transport error with an explicit retry decision.
    type Error: Error + Send + Sync + 'static + ErrorClassifier;

    /// Publishes once and completes after the transport reports success or failure.
    ///
    /// The meaning of success, including its acknowledgement strength, is defined by the
    /// transport and its application-owned configuration.
    fn publish(
        &self,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
