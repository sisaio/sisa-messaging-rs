use super::NatsWire;
use super::constants::*;
use super::headers::{parsed, required};
use crate::error::MappingError;
use sisa_messaging::{
    ContentType, HeaderName, HeaderValue, Headers, MessageId, MessageType, Metadata, OrderingKey,
    SerializedEnvelope,
};

pub(super) fn decode(wire: NatsWire) -> Result<SerializedEnvelope, MappingError> {
    let h = &wire.headers;

    let message_id: MessageId = required(h, MESSAGE_ID)?
        .parse()
        .map_err(|_| MappingError::InvalidEnvelope)?;

    let message_type: MessageType = required(h, MESSAGE_TYPE)?
        .parse()
        .map_err(|_| MappingError::InvalidEnvelope)?;

    let message_version: u32 = required(h, MESSAGE_VERSION)?
        .parse()
        .map_err(|_| MappingError::InvalidEnvelope)?;

    let content_type: ContentType = required(h, CONTENT_TYPE)?
        .parse()
        .map_err(|_| MappingError::InvalidEnvelope)?;

    let ordering_key = parsed::<OrderingKey>(h, ORDERING_KEY)?;

    let mut metadata = Metadata::default();
    metadata.correlation.correlation_id = parsed(h, CORRELATION_ID)?;
    metadata.correlation.conversation_id = parsed(h, CONVERSATION_ID)?;
    metadata.correlation.causation_id = parsed(h, CAUSATION_ID)?;
    metadata.correlation.request_id = parsed(h, REQUEST_ID)?;

    metadata.routing.source = parsed(h, SOURCE)?;
    metadata.routing.destination = parsed(h, DESTINATION)?;
    metadata.routing.reply_to = parsed(h, REPLY_TO)?;

    metadata.delivery.sent_at_ms = parsed(h, SENT_AT_MS)?;
    metadata.delivery.deduplication_id = parsed(h, DEDUPLICATION_ID)?;

    let expected_dedup_id = metadata
        .delivery
        .deduplication_id
        .as_ref()
        .map_or_else(|| message_id.to_string(), |value| value.as_str().to_owned());

    if required(h, NATS_MSG_ID)? != expected_dedup_id {
        return Err(MappingError::InvalidEnvelope);
    }

    metadata.tenant_id = parsed(h, TENANT_ID)?;
    metadata.trace.traceparent = parsed(h, TRACEPARENT)?;
    metadata.trace.tracestate = parsed(h, TRACESTATE)?;

    let mut custom = Headers::new();

    for (name, values) in h.iter() {
        if let Some(suffix) = name
            .to_string()
            .to_ascii_lowercase()
            .strip_prefix(CUSTOM_PREFIX_LOWERCASE)
        {
            if values.len() != 1 {
                return Err(MappingError::InvalidHeaders);
            }

            let key = HeaderName::new(suffix).map_err(|_| MappingError::InvalidHeaders)?;

            let value =
                HeaderValue::new(values[0].as_str()).map_err(|_| MappingError::InvalidHeaders)?;

            if custom
                .insert(key, value)
                .map_err(|_| MappingError::InvalidHeaders)?
                .is_some()
            {
                return Err(MappingError::InvalidHeaders);
            }
        }
    }

    metadata.headers = custom;

    Ok(SerializedEnvelope {
        message_id,
        message_type,
        message_version,
        content_type,
        payload: wire.payload,
        metadata,
        ordering_key,
    })
}
