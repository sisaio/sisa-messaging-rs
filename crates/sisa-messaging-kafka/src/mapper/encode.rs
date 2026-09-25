use sisa_messaging::{FrameworkHeader, HeaderValue, MetadataValue, SerializedEnvelope};

use super::headers::push_header;
use super::{KafkaHeader, KafkaMappingError, KafkaRecord};

pub(super) fn encode(envelope: &SerializedEnvelope) -> Result<KafkaRecord, KafkaMappingError> {
    let mut headers =
        Vec::with_capacity(FrameworkHeader::ALL.len() + envelope.metadata.headers.len());

    push_header(
        &mut headers,
        FrameworkHeader::MessageId,
        Some(envelope.message_id.to_string()),
    );

    push_header(
        &mut headers,
        FrameworkHeader::MessageType,
        Some(envelope.message_type.as_str()),
    );

    push_header(
        &mut headers,
        FrameworkHeader::MessageVersion,
        Some(envelope.message_version.to_string()),
    );

    push_header(
        &mut headers,
        FrameworkHeader::ContentType,
        Some(envelope.content_type.as_str()),
    );

    push_header(
        &mut headers,
        FrameworkHeader::OrderingKey,
        envelope.ordering_key.as_ref().map(|value| value.as_str()),
    );

    let metadata = &envelope.metadata;

    push_header(
        &mut headers,
        FrameworkHeader::CorrelationId,
        metadata
            .correlation
            .correlation_id
            .as_ref()
            .map(MetadataValue::as_str),
    );

    push_header(
        &mut headers,
        FrameworkHeader::ConversationId,
        metadata
            .correlation
            .conversation_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
    );

    push_header(
        &mut headers,
        FrameworkHeader::CausationId,
        metadata
            .correlation
            .causation_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
    );

    push_header(
        &mut headers,
        FrameworkHeader::RequestId,
        metadata
            .correlation
            .request_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
    );

    push_header(
        &mut headers,
        FrameworkHeader::Source,
        metadata.routing.source.as_ref().map(MetadataValue::as_str),
    );

    push_header(
        &mut headers,
        FrameworkHeader::Destination,
        metadata
            .routing
            .destination
            .as_ref()
            .map(MetadataValue::as_str),
    );

    push_header(
        &mut headers,
        FrameworkHeader::ReplyTo,
        metadata
            .routing
            .reply_to
            .as_ref()
            .map(MetadataValue::as_str),
    );

    push_header(
        &mut headers,
        FrameworkHeader::SentAtMs,
        metadata
            .delivery
            .sent_at_ms
            .map(|value| value.to_string())
            .as_deref(),
    );

    push_header(
        &mut headers,
        FrameworkHeader::DeduplicationId,
        metadata
            .delivery
            .deduplication_id
            .as_ref()
            .map(MetadataValue::as_str),
    );

    push_header(
        &mut headers,
        FrameworkHeader::TenantId,
        metadata.tenant_id.as_ref().map(MetadataValue::as_str),
    );

    push_header(
        &mut headers,
        FrameworkHeader::Traceparent,
        metadata.trace.traceparent.as_ref().map(HeaderValue::as_str),
    );

    push_header(
        &mut headers,
        FrameworkHeader::Tracestate,
        metadata.trace.tracestate.as_ref().map(HeaderValue::as_str),
    );

    for (name, value) in metadata.headers.iter() {
        headers.push(KafkaHeader {
            name: name.as_str().to_owned(),
            value: Some(value.as_str().as_bytes().to_vec()),
        });
    }

    Ok(KafkaRecord {
        key: envelope
            .ordering_key
            .as_ref()
            .map(|key| key.as_str().as_bytes().to_vec()),
        payload: envelope.payload.clone(),
        headers,
    })
}
