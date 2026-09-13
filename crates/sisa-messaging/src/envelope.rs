//! Typed and serialized envelope representations.

use std::fmt;

use crate::{ContentType, Message, MessageId, MessageType, Metadata, OrderingKey, ValidationError};

/// A typed application message plus its stable identity and metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope<T> {
    message_id: MessageId,

    message_type: MessageType,

    message_version: u32,

    payload: T,

    metadata: Metadata,

    ordering_key: Option<OrderingKey>,
}

impl<T: Message> Envelope<T> {
    /// Constructs an envelope and freezes the message's type, version, and ordering key.
    pub fn new(
        message_id: MessageId,
        payload: T,
        metadata: Metadata,
    ) -> Result<Self, EnvelopeError> {
        let message_type =
            MessageType::new(T::TYPE).map_err(EnvelopeError::invalid_message_type)?;
        let ordering_key = payload.order_by();

        Ok(Self {
            message_id,
            message_type,
            message_version: T::VERSION,

            payload,

            metadata,
            ordering_key,
        })
    }

    /// Returns the stable logical message identity.
    #[must_use]
    pub const fn message_id(&self) -> MessageId {
        self.message_id
    }

    /// Returns the validated stable contract name.
    #[must_use]
    pub const fn message_type(&self) -> &MessageType {
        &self.message_type
    }

    /// Returns the stable contract version.
    #[must_use]
    pub const fn message_version(&self) -> u32 {
        self.message_version
    }

    /// Borrows the typed body.
    #[must_use]
    pub const fn payload(&self) -> &T {
        &self.payload
    }

    /// Borrows the transport-independent metadata.
    #[must_use]
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Borrows the ordering key resolved during construction.
    #[must_use]
    pub fn ordering_key(&self) -> Option<&OrderingKey> {
        self.ordering_key.as_ref()
    }
}

/// A typed-envelope construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EnvelopeError {
    /// The message's static type name was not a valid wire identifier.
    InvalidMessageType(ValidationError),
}

impl EnvelopeError {
    fn invalid_message_type(source: ValidationError) -> Self {
        Self::InvalidMessageType(source)
    }
}

impl fmt::Display for EnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMessageType(_) => formatter.write_str("message type is invalid"),
        }
    }
}

impl std::error::Error for EnvelopeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidMessageType(source) => Some(source),
        }
    }
}

/// A transport-independent serialized envelope.
///
/// Its fields contain only validated shared values and owned bytes. Broker-specific subjects,
/// acknowledgements, and headers belong in a transport mapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SerializedEnvelope {
    /// Stable logical message identity.
    pub message_id: MessageId,

    /// Stable validated contract name.
    pub message_type: MessageType,

    /// Stable contract version.
    pub message_version: u32,

    /// Serialized body content type.
    pub content_type: ContentType,

    /// Serialized body bytes.
    pub payload: Vec<u8>,

    /// Additive transport-independent metadata.
    pub metadata: Metadata,

    /// Ordering key resolved before serialization.
    pub ordering_key: Option<OrderingKey>,
}
