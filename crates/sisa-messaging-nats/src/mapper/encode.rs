use super::constants::*;
use super::headers::push_header;
use super::{NatsWire, SubjectResolver};
use crate::error::MappingError;
use async_nats::HeaderMap;
use sisa_messaging::{HeaderValue, Metadata, OrderingKey, SerializedEnvelope};

pub(super) fn encode<R: SubjectResolver>(
    resolver: &R,
    envelope: &SerializedEnvelope,
) -> Result<NatsWire, MappingError> {
    let subject = resolver.resolve(envelope)?;

    let mut headers = HeaderMap::new();

    push_identity(&mut headers, envelope);
    push_correlation(&mut headers, &envelope.metadata);
    push_routing(&mut headers, &envelope.metadata);
    push_delivery_and_custom(&mut headers, &envelope.metadata);

    Ok(NatsWire {
        subject: subject.into_string(),
        headers,
        payload: envelope.payload.clone(),
    })
}

fn push_identity(h: &mut HeaderMap, envelope: &SerializedEnvelope) {
    push_header(h, MESSAGE_ID, Some(&envelope.message_id.to_string()));

    let dedup_id = envelope
        .metadata
        .delivery
        .deduplication_id
        .as_ref()
        .map_or_else(
            || envelope.message_id.to_string(),
            |value| value.as_str().to_owned(),
        );

    push_header(h, NATS_MSG_ID, Some(&dedup_id));
    push_header(h, MESSAGE_TYPE, Some(envelope.message_type.as_str()));

    push_header(
        h,
        MESSAGE_VERSION,
        Some(envelope.message_version.to_string()),
    );

    push_header(h, CONTENT_TYPE, Some(envelope.content_type.as_str()));

    push_header(
        h,
        ORDERING_KEY,
        envelope.ordering_key.as_ref().map(OrderingKey::as_str),
    );
}

fn push_correlation(h: &mut HeaderMap, m: &Metadata) {
    push_header(
        h,
        CORRELATION_ID,
        m.correlation.correlation_id.as_ref().map(|v| v.as_str()),
    );

    push_header(
        h,
        CONVERSATION_ID,
        m.correlation
            .conversation_id
            .as_ref()
            .map(|v| v.to_string())
            .as_deref(),
    );

    push_header(
        h,
        CAUSATION_ID,
        m.correlation
            .causation_id
            .as_ref()
            .map(|v| v.to_string())
            .as_deref(),
    );

    push_header(
        h,
        REQUEST_ID,
        m.correlation
            .request_id
            .as_ref()
            .map(|v| v.to_string())
            .as_deref(),
    );
}

fn push_routing(h: &mut HeaderMap, m: &Metadata) {
    push_header(h, SOURCE, m.routing.source.as_ref().map(|v| v.as_str()));

    push_header(
        h,
        DESTINATION,
        m.routing.destination.as_ref().map(|v| v.as_str()),
    );

    push_header(h, REPLY_TO, m.routing.reply_to.as_ref().map(|v| v.as_str()));
}

fn push_delivery_and_custom(h: &mut HeaderMap, m: &Metadata) {
    push_header(
        h,
        SENT_AT_MS,
        m.delivery.sent_at_ms.map(|v| v.to_string()).as_deref(),
    );

    push_header(
        h,
        DEDUPLICATION_ID,
        m.delivery.deduplication_id.as_ref().map(|v| v.as_str()),
    );

    push_header(h, TENANT_ID, m.tenant_id.as_ref().map(|v| v.as_str()));

    push_header(
        h,
        TRACEPARENT,
        m.trace.traceparent.as_ref().map(HeaderValue::as_str),
    );

    push_header(
        h,
        TRACESTATE,
        m.trace.tracestate.as_ref().map(HeaderValue::as_str),
    );

    for (name, value) in m.headers.iter() {
        push_header(
            h,
            &format!("{CUSTOM_PREFIX}{}", name.as_str()),
            Some(value.as_str()),
        );
    }
}
