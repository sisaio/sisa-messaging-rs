//! Acknowledged publication capability.

use std::error::Error;
use std::future::Future;

use crate::{Classify, SerializedEnvelope};

/// Performs one acknowledged publish attempt for a serialized envelope.
pub trait Publisher: Send + Sync {
    /// Transport error with an explicit retry decision.
    type Error: Error + Send + Sync + 'static + Classify;

    /// Publishes once and completes only after acknowledgement or failure.
    fn publish(
        &self,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
