use sisa_messaging::{
    ContentType, ConversationId, CorrelationMetadata, DeliveryMetadata, FrameworkHeader,
    HeaderName, HeaderValue, Headers, MessageId, MessageType, Metadata, MetadataValue, OrderingKey,
    RequestId, RoutingMetadata, SerializedEnvelope, TraceMetadata,
};

use super::headers::{normalize_headers, parse_required, take_optional, take_required};
use super::{KafkaMappingError, KafkaRecord};

pub(super) fn decode(wire: KafkaRecord) -> Result<SerializedEnvelope, KafkaMappingError> {
    let mut values = normalize_headers(wire.headers)?;
    let message_id = parse_required::<MessageId>(&mut values, FrameworkHeader::MessageId)?;

    let message_type = MessageType::new(take_required(&mut values, FrameworkHeader::MessageType)?)
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let message_version = take_required(&mut values, FrameworkHeader::MessageVersion)?
        .parse::<u32>()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let content_type = ContentType::new(take_required(&mut values, FrameworkHeader::ContentType)?)
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let ordering_key = take_optional(&mut values, FrameworkHeader::OrderingKey)?
        .map(OrderingKey::new)
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let key_matches = match (&ordering_key, wire.key.as_deref()) {
        (None, None) => true,
        (Some(ordering_key), Some(key)) => key == ordering_key.as_str().as_bytes(),
        _ => false,
    };

    if !key_matches {
        return Err(KafkaMappingError::InvalidRecordKey);
    }

    let correlation_id = take_optional(&mut values, FrameworkHeader::CorrelationId)?
        .map(MetadataValue::new)
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let conversation_id = take_optional(&mut values, FrameworkHeader::ConversationId)?
        .map(|value| value.parse::<ConversationId>())
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let causation_id = take_optional(&mut values, FrameworkHeader::CausationId)?
        .map(|value| value.parse::<MessageId>())
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let request_id = take_optional(&mut values, FrameworkHeader::RequestId)?
        .map(|value| value.parse::<RequestId>())
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let source = take_optional(&mut values, FrameworkHeader::Source)?
        .map(MetadataValue::new)
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let destination = take_optional(&mut values, FrameworkHeader::Destination)?
        .map(MetadataValue::new)
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let reply_to = take_optional(&mut values, FrameworkHeader::ReplyTo)?
        .map(MetadataValue::new)
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let sent_at_ms = take_optional(&mut values, FrameworkHeader::SentAtMs)?
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let deduplication_id = take_optional(&mut values, FrameworkHeader::DeduplicationId)?
        .map(MetadataValue::new)
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let tenant_id = take_optional(&mut values, FrameworkHeader::TenantId)?
        .map(MetadataValue::new)
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let traceparent = take_optional(&mut values, FrameworkHeader::Traceparent)?
        .map(HeaderValue::new)
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let tracestate = take_optional(&mut values, FrameworkHeader::Tracestate)?
        .map(HeaderValue::new)
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)?;

    let mut custom_headers = Headers::new();

    for (name, value) in values {
        let name = HeaderName::new(name).map_err(|_| KafkaMappingError::InvalidHeader)?;
        let value = String::from_utf8(value).map_err(|_| KafkaMappingError::InvalidHeader)?;
        let value = HeaderValue::new(value).map_err(|_| KafkaMappingError::InvalidHeader)?;

        custom_headers
            .insert(name, value)
            .map_err(|_| KafkaMappingError::HeaderBoundsExceeded)?;
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
