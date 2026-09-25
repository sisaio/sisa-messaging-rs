use sisa_messaging::{
    ContentType, ConversationId, CorrelationMetadata, DeliveryMetadata, FrameworkHeader,
    HeaderName, HeaderValue, Headers, MessageId, MessageType, Metadata, MetadataValue, OrderingKey,
    RequestId, RoutingMetadata, SerializedEnvelope, TraceMetadata,
};

use super::headers::{normalize_headers, parse_required, take_optional, take_required};
use super::{IggyMappingError, IggyRecord};

pub(super) fn decode(wire: IggyRecord) -> Result<SerializedEnvelope, IggyMappingError> {
    let mut values = normalize_headers(wire.headers)?;
    let message_id = parse_required::<MessageId>(&mut values, FrameworkHeader::MessageId)?;

    if message_id.as_uuid().as_u128() != wire.id {
        return Err(IggyMappingError::InvalidRecordId);
    }

    let message_type = MessageType::new(take_required(&mut values, FrameworkHeader::MessageType)?)
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let message_version = take_required(&mut values, FrameworkHeader::MessageVersion)?
        .parse::<u32>()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let content_type = ContentType::new(take_required(&mut values, FrameworkHeader::ContentType)?)
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let ordering_key = take_optional(&mut values, FrameworkHeader::OrderingKey)?
        .map(OrderingKey::new)
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    // Iggy does not return the messages-key on reads, so a record with no wire key is always
    // accepted regardless of the header: the ordering key comes from the header alone in that
    // case. A record that does carry a wire key must have that key match the ordering-key header
    // exactly; a wire key with no header, or one that disagrees with the header, is an
    // inconsistent record.
    if let Some(key) = wire.key.as_deref() {
        let matches_header = ordering_key
            .as_ref()
            .is_some_and(|value| value.as_str().as_bytes() == key);

        if !matches_header {
            return Err(IggyMappingError::InvalidRecordKey);
        }
    }

    let correlation_id = take_optional(&mut values, FrameworkHeader::CorrelationId)?
        .map(MetadataValue::new)
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let conversation_id = take_optional(&mut values, FrameworkHeader::ConversationId)?
        .map(|value| value.parse::<ConversationId>())
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let causation_id = take_optional(&mut values, FrameworkHeader::CausationId)?
        .map(|value| value.parse::<MessageId>())
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let request_id = take_optional(&mut values, FrameworkHeader::RequestId)?
        .map(|value| value.parse::<RequestId>())
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let source = take_optional(&mut values, FrameworkHeader::Source)?
        .map(MetadataValue::new)
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let destination = take_optional(&mut values, FrameworkHeader::Destination)?
        .map(MetadataValue::new)
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let reply_to = take_optional(&mut values, FrameworkHeader::ReplyTo)?
        .map(MetadataValue::new)
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let sent_at_ms = take_optional(&mut values, FrameworkHeader::SentAtMs)?
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let deduplication_id = take_optional(&mut values, FrameworkHeader::DeduplicationId)?
        .map(MetadataValue::new)
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let tenant_id = take_optional(&mut values, FrameworkHeader::TenantId)?
        .map(MetadataValue::new)
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let traceparent = take_optional(&mut values, FrameworkHeader::Traceparent)?
        .map(HeaderValue::new)
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let tracestate = take_optional(&mut values, FrameworkHeader::Tracestate)?
        .map(HeaderValue::new)
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let mut custom_headers = Headers::new();

    for (name, value) in values {
        let name = HeaderName::new(name).map_err(|_| IggyMappingError::InvalidHeader)?;
        let value = String::from_utf8(value).map_err(|_| IggyMappingError::InvalidHeader)?;
        let value = HeaderValue::new(value).map_err(|_| IggyMappingError::InvalidHeader)?;

        custom_headers
            .insert(name, value)
            .map_err(|_| IggyMappingError::HeaderBoundsExceeded)?;
    }

    Ok(SerializedEnvelope {
        message_id,
        message_type,
        message_version,
        content_type,
        payload: wire.payload,
        metadata: Metadata {
            correlation: CorrelationMetadata {
                correlation_id,
                conversation_id,
                causation_id,
                request_id,
            },
            trace: TraceMetadata {
                traceparent,
                tracestate,
            },
            routing: RoutingMetadata {
                source,
                destination,
                reply_to,
            },
            delivery: DeliveryMetadata {
                sent_at_ms,
                deduplication_id,
            },
            tenant_id,
            headers: custom_headers,
        },
        ordering_key,
    })
}
