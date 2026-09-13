//! Pure transport-wire envelope mapping.

use std::error::Error;

use crate::{Classify, SerializedEnvelope};

/// Converts serialized envelopes to and from one owned transport wire representation.
pub trait EnvelopeMapper<Wire>: Send + Sync
where
    Wire: Send + 'static,
{
    /// Mapping error with an explicit retry decision.
    type Error: Error + Send + Sync + 'static + Classify;

    /// Projects a shared envelope into transport-owned wire data.
    fn encode(&self, envelope: &SerializedEnvelope) -> Result<Wire, Self::Error>;

    /// Reconstructs a validated shared envelope from transport-owned wire data.
    fn decode(&self, wire: Wire) -> Result<SerializedEnvelope, Self::Error>;
}
