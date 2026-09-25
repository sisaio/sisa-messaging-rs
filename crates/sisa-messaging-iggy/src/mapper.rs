//! Shared-envelope projection into Iggy messages.

use sisa_messaging::{EnvelopeMapper, SerializedEnvelope};

use crate::IggyMappingError;

mod decode;
mod encode;
mod headers;

/// An owned Iggy header used by [`IggyRecord`].
///
/// Unlike a raw wire header, this value is always a concrete byte string: Iggy's header TLV
/// format has no representation for a null value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IggyHeader {
    /// Header name as projected on the Iggy message.
    pub name: String,

    /// Header value bytes.
    pub value: Vec<u8>,
}

/// The Iggy-specific wire representation of a serialized envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IggyRecord {
    /// Stable message identity projected as the SDK's `u128` message id.
    pub id: u128,

    /// Serialized message body.
    pub payload: Vec<u8>,

    /// Framework and application headers in deterministic order.
    pub headers: Vec<IggyHeader>,

    /// Ordering key bytes used as the Iggy messages-key partitioning input, when present.
    pub key: Option<Vec<u8>>,
}

/// Maps a serialized envelope to Iggy headers and back.
#[derive(Clone, Copy, Debug, Default)]
pub struct IggyEnvelopeMapper;

impl EnvelopeMapper<IggyRecord> for IggyEnvelopeMapper {
    type Error = IggyMappingError;

    fn encode(&self, envelope: &SerializedEnvelope) -> Result<IggyRecord, Self::Error> {
        encode::encode(envelope)
    }

    fn decode(&self, wire: IggyRecord) -> Result<SerializedEnvelope, Self::Error> {
        decode::decode(wire)
    }
}
