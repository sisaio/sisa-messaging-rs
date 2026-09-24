//! Shared-envelope projection into Kafka records.

use sisa_messaging::{EnvelopeMapper, SerializedEnvelope};

use crate::KafkaMappingError;

mod decode;
mod encode;
mod headers;

/// An owned Kafka header used by [`KafkaRecord`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaHeader {
    /// Header name as projected on the Kafka record.
    pub name: String,

    /// Header value; `None` is retained so malformed broker records are rejected on decode.
    pub value: Option<Vec<u8>>,
}

/// The Kafka-specific wire representation of a serialized envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaRecord {
    /// Kafka record key, projected from the optional shared ordering key.
    pub key: Option<Vec<u8>>,

    /// Serialized message body.
    pub payload: Vec<u8>,

    /// Framework and application headers in deterministic order.
    pub headers: Vec<KafkaHeader>,
}

/// Maps a serialized envelope to Kafka headers and back.
#[derive(Clone, Copy, Debug, Default)]
pub struct KafkaEnvelopeMapper;

impl EnvelopeMapper<KafkaRecord> for KafkaEnvelopeMapper {
    type Error = KafkaMappingError;

    fn encode(&self, envelope: &SerializedEnvelope) -> Result<KafkaRecord, Self::Error> {
        encode::encode(envelope)
    }

    fn decode(&self, wire: KafkaRecord) -> Result<SerializedEnvelope, Self::Error> {
        decode::decode(wire)
    }
}
