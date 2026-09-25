//! Pure, versioned envelope projection.

use crate::RedisMappingError;
use serde::{Deserialize, Serialize};
use sisa_messaging::{
    ContentType, EnvelopeMapper, MessageId, MessageType, Metadata, OrderingKey, SerializedEnvelope,
};

const VERSION: &[u8] = b"1";
// Validated string content totals under 100 KiB: 65,536 custom-header bytes, two 8,192-byte
// trace values, six 1,024-byte metadata values, two 255-byte identifiers, a 512-byte ordering
// key, and four UUIDs. Even sixfold JSON escaping plus field syntax fits within 1 MiB.
const MAX_REDIS_HEADER_JSON_BYTES: usize = 1024 * 1024;

/// Owned Redis stream fields. `payload` is binary; `envelope` is bounded JSON metadata.
#[derive(Clone)]
pub struct RedisWire {
    /// Format version.
    pub version: Vec<u8>,

    /// Serialized identity and metadata.
    pub envelope: Vec<u8>,

    /// Original serialized body.
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    message_id: MessageId,

    message_type: MessageType,

    message_version: u32,

    content_type: ContentType,

    metadata: Metadata,

    ordering_key: Option<OrderingKey>,
}

/// Maps serialized envelopes to the three Redis stream fields and back.
#[derive(Clone, Copy, Debug, Default)]
pub struct RedisMapper;

impl EnvelopeMapper<RedisWire> for RedisMapper {
    type Error = RedisMappingError;

    fn encode(&self, envelope: &SerializedEnvelope) -> Result<RedisWire, Self::Error> {
        let header = Header {
            message_id: envelope.message_id,
            message_type: envelope.message_type.clone(),
            message_version: envelope.message_version,
            content_type: envelope.content_type.clone(),
            metadata: envelope.metadata.clone(),
            ordering_key: envelope.ordering_key.clone(),
        };

        let bytes = serde_json::to_vec(&header).map_err(|_| RedisMappingError)?;

        if bytes.len() > MAX_REDIS_HEADER_JSON_BYTES {
            return Err(RedisMappingError);
        }

        Ok(RedisWire {
            version: VERSION.to_vec(),
            envelope: bytes,
            payload: envelope.payload.clone(),
        })
    }

    fn decode(&self, wire: RedisWire) -> Result<SerializedEnvelope, Self::Error> {
        if wire.version != VERSION || wire.envelope.len() > MAX_REDIS_HEADER_JSON_BYTES {
            return Err(RedisMappingError);
        }

        let header: Header =
            serde_json::from_slice(&wire.envelope).map_err(|_| RedisMappingError)?;

        Ok(SerializedEnvelope {
            message_id: header.message_id,
            message_type: header.message_type,
            message_version: header.message_version,
            content_type: header.content_type,
            payload: wire.payload,
            metadata: header.metadata,
            ordering_key: header.ordering_key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sisa_messaging::{HeaderName, HeaderValue, Headers, MAX_CUSTOM_HEADER_BYTES};

    #[test]
    fn maximum_valid_headers_with_escaped_values_round_trip() {
        let mut headers = Headers::new();

        for index in 0..64 {
            let name = HeaderName::new(format!("k{index:03}")).unwrap();
            let value = HeaderValue::new("\\\"".repeat(510)).unwrap();
            headers.insert(name, value).unwrap();
        }

        assert_eq!(64 * (4 + 1020), MAX_CUSTOM_HEADER_BYTES);

        let envelope = SerializedEnvelope {
            message_id: MessageId::new(),
            message_type: MessageType::new("redis_boundary").unwrap(),
            message_version: 1,
            content_type: ContentType::new("application/octet-stream").unwrap(),
            payload: vec![0, 1, 255],
            metadata: Metadata {
                headers,
                ..Metadata::default()
            },
            ordering_key: None,
        };

        let wire = RedisMapper.encode(&envelope).unwrap();
        assert!(wire.envelope.len() > 64 * 1024);
        assert!(wire.envelope.len() <= MAX_REDIS_HEADER_JSON_BYTES);
        assert_eq!(RedisMapper.decode(wire).unwrap(), envelope);
    }

    #[test]
    fn malformed_wire_has_bounded_error() {
        let bad = RedisWire {
            version: b"2".to_vec(),
            envelope: b"sensitive raw header".to_vec(),
            payload: b"sensitive payload".to_vec(),
        };

        let error = RedisMapper.decode(bad).err().unwrap();
        assert!(!error.to_string().contains("sensitive"));
    }
}
