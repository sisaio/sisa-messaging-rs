use iggy::prelude::MAX_PAYLOAD_SIZE;
use sisa_messaging::{FrameworkHeader, HeaderValue, MetadataValue, SerializedEnvelope};

use super::headers::{MAX_HEADER_FIELD_BYTES, push_header, push_optional_header};
use super::{IggyHeader, IggyMappingError, IggyRecord};

pub(super) fn encode(envelope: &SerializedEnvelope) -> Result<IggyRecord, IggyMappingError> {
    if envelope.payload.is_empty() {
        return Err(IggyMappingError::EmptyPayload);
    }

    if envelope.payload.len() > MAX_PAYLOAD_SIZE as usize {
        return Err(IggyMappingError::PayloadTooLarge);
    }

    let mut headers =
        Vec::with_capacity(FrameworkHeader::ALL.len() + envelope.metadata.headers.len());

    push_header(
        &mut headers,
        FrameworkHeader::MessageId,
        Some(envelope.message_id.to_string()),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::MessageType,
        Some(envelope.message_type.as_str()),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::MessageVersion,
        Some(envelope.message_version.to_string()),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::ContentType,
        Some(envelope.content_type.as_str()),
    )?;

    let key = match &envelope.ordering_key {
        Some(ordering_key) => {
            let bytes = ordering_key.as_str().as_bytes();

            if bytes.is_empty() || bytes.len() > MAX_HEADER_FIELD_BYTES {
                return Err(IggyMappingError::InvalidOrderingKey);
            }

            push_header(
                &mut headers,
                FrameworkHeader::OrderingKey,
                Some(ordering_key.as_str()),
            )?;

            Some(bytes.to_vec())
        }
        None => None,
    };

    let metadata = &envelope.metadata;

    push_header(
        &mut headers,
        FrameworkHeader::CorrelationId,
        metadata
            .correlation
            .correlation_id
            .as_ref()
            .map(MetadataValue::as_str),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::ConversationId,
        metadata
            .correlation
            .conversation_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::CausationId,
        metadata
            .correlation
            .causation_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::RequestId,
        metadata
            .correlation
            .request_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::Source,
        metadata.routing.source.as_ref().map(MetadataValue::as_str),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::Destination,
        metadata
            .routing
            .destination
            .as_ref()
            .map(MetadataValue::as_str),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::ReplyTo,
        metadata
            .routing
            .reply_to
            .as_ref()
            .map(MetadataValue::as_str),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::SentAtMs,
        metadata
            .delivery
            .sent_at_ms
            .map(|value| value.to_string())
            .as_deref(),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::DeduplicationId,
        metadata
            .delivery
            .deduplication_id
            .as_ref()
            .map(MetadataValue::as_str),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::TenantId,
        metadata.tenant_id.as_ref().map(MetadataValue::as_str),
    )?;

    push_header(
        &mut headers,
        FrameworkHeader::Traceparent,
        metadata.trace.traceparent.as_ref().map(HeaderValue::as_str),
    )?;

    // `tracestate` may legitimately exceed the Iggy header-value bound (the shared contract
    // allows up to 8,192 bytes; Iggy caps each header value at 255). W3C Trace Context permits a
    // participant to drop `tracestate` under vendor size constraints, so an oversized value is
    // omitted from the wire record here rather than rejected. `traceparent` has no such
    // allowance and stays required through `push_header`.
    push_optional_header(
        &mut headers,
        FrameworkHeader::Tracestate,
        metadata.trace.tracestate.as_ref().map(HeaderValue::as_str),
    );

    for (name, value) in metadata.headers.iter() {
        let value_bytes = value.as_str().as_bytes();

        if value_bytes.is_empty() || value_bytes.len() > MAX_HEADER_FIELD_BYTES {
            return Err(IggyMappingError::InvalidHeader);
        }

        headers.push(IggyHeader {
            name: name.as_str().to_owned(),
            value: value_bytes.to_vec(),
        });
    }

    Ok(IggyRecord {
        id: envelope.message_id.as_uuid().as_u128(),
        payload: envelope.payload.clone(),
        headers,
        key,
    })
}
