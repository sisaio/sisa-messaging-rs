//! Turning a message into an envelope, and why a publish fails.

use sisa_eventing_core::{
    Codec, CodecError, ContentType, Envelope, EnvelopeError, Message, MessageType, Source,
    StoreError, TransportError,
};

/// Why a [`Bus`](crate::Bus), an [`OutboxPublisher`](crate::OutboxPublisher) or a
/// [`HandlerContext`](crate::HandlerContext) couldn't publish a message.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PublishError {
    /// The message's [`Message::TYPE`] isn't a valid message type, or the codec's content type
    /// isn't a valid media type.
    #[error(transparent)]
    MessageType(#[from] EnvelopeError),
    /// The codec couldn't encode the message.
    #[error(transparent)]
    Encode(#[from] CodecError),
    /// The broker didn't confirm it has the envelope.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// The outbox didn't take the row; the caller's transaction may be unusable.
    #[error(transparent)]
    Store(#[from] StoreError),
}

// A new envelope for a message from this source, encoded with the bus's codec.
pub(crate) fn encode<Msg: Message>(
    source: &Source,
    codec: &impl Codec,
    message: &Msg,
) -> Result<Envelope, PublishError> {
    let message_type = MessageType::of::<Msg>()?;
    let content_type = ContentType::new(codec.content_type())?;
    let data = codec.encode(message)?;
    Ok(Envelope::new(
        message_type,
        source.clone(),
        content_type,
        data,
    ))
}
