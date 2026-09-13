//! Static-dispatch serialization contracts and optional JSON implementation.

use std::error::Error;

use crate::{Envelope, ErrorClassifier, Message, SerializedEnvelope};

#[cfg(feature = "json")]
use crate::FailureKind;

#[cfg(feature = "json")]
const JSON_CONTENT_TYPE: &str = "application/json";

/// Serializes and reconstructs typed envelopes without introducing transport types.
pub trait Serializer<M: Message>: Send + Sync {
    /// Codec error type. Its display representation must not expose payload bytes.
    type Error: Error + Send + Sync + 'static + ErrorClassifier;

    /// Serializes a typed envelope.
    fn serialize(&self, envelope: &Envelope<M>) -> Result<SerializedEnvelope, Self::Error>;

    /// Validates the type/version/ordering contract and reconstructs a typed envelope.
    fn deserialize(&self, envelope: SerializedEnvelope) -> Result<Envelope<M>, Self::Error>;
}

/// Stateless JSON envelope serializer.
#[cfg(feature = "json")]
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonSerializer;

/// A safely rendered JSON serialization failure.
#[cfg(feature = "json")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JsonSerializerError {
    /// JSON encoding failed. Payload and foreign error text are intentionally omitted.
    Encode,

    /// JSON decoding failed. Payload and foreign error text are intentionally omitted.
    Decode,

    /// The serialized contract name did not match the requested message type.
    MessageTypeMismatch,

    /// The serialized contract version did not match the requested message type.
    MessageVersionMismatch,

    /// The serialized content type was not JSON.
    ContentTypeMismatch,

    /// The decoded message resolved a different ordering key.
    OrderingKeyMismatch,

    /// The requested message type has an invalid static contract name.
    InvalidMessageType,
}

#[cfg(feature = "json")]
impl std::fmt::Display for JsonSerializerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode => formatter.write_str("JSON encoding failed"),
            Self::Decode => formatter.write_str("JSON decoding failed"),
            Self::MessageTypeMismatch => formatter.write_str("message type does not match"),
            Self::MessageVersionMismatch => formatter.write_str("message version does not match"),
            Self::ContentTypeMismatch => formatter.write_str("message content type does not match"),
            Self::OrderingKeyMismatch => formatter.write_str("message ordering key does not match"),
            Self::InvalidMessageType => formatter.write_str("message type is invalid"),
        }
    }
}

#[cfg(feature = "json")]
impl Error for JsonSerializerError {}

#[cfg(feature = "json")]
impl ErrorClassifier for JsonSerializerError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

#[cfg(feature = "json")]
impl<M> Serializer<M> for JsonSerializer
where
    M: Message + serde::Serialize + serde::de::DeserializeOwned,
{
    type Error = JsonSerializerError;

    fn serialize(&self, envelope: &Envelope<M>) -> Result<SerializedEnvelope, Self::Error> {
        let payload = serde_json::to_vec(envelope.payload()).map_err(|_| Self::Error::Encode)?;

        let content_type = crate::ContentType::new(JSON_CONTENT_TYPE)
            .map_err(|_| Self::Error::InvalidMessageType)?;

        Ok(SerializedEnvelope {
            message_id: envelope.message_id(),
            message_type: envelope.message_type().clone(),
            message_version: envelope.message_version(),

            content_type,
            payload,

            metadata: envelope.metadata().clone(),
            ordering_key: envelope.ordering_key().cloned(),
        })
    }

    fn deserialize(&self, envelope: SerializedEnvelope) -> Result<Envelope<M>, Self::Error> {
        if envelope.message_type.as_str() != M::TYPE {
            return Err(Self::Error::MessageTypeMismatch);
        }

        if envelope.message_version != M::VERSION {
            return Err(Self::Error::MessageVersionMismatch);
        }

        if envelope.content_type.as_str() != JSON_CONTENT_TYPE {
            return Err(Self::Error::ContentTypeMismatch);
        }

        let message = serde_json::from_slice(&envelope.payload).map_err(|_| Self::Error::Decode)?;

        let typed = Envelope::new(envelope.message_id, message, envelope.metadata)
            .map_err(|_| Self::Error::InvalidMessageType)?;

        if typed.ordering_key() != envelope.ordering_key.as_ref() {
            return Err(Self::Error::OrderingKeyMismatch);
        }

        Ok(typed)
    }
}
