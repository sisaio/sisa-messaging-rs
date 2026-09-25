//! Header-table decoding that ignores broker-owned headers and basic properties.

use super::{
    CAUSATION_ID, CONTENT_TYPE, CONVERSATION_ID, CORRELATION_ID, CUSTOM_PREFIX, DEDUPLICATION_ID,
    DESTINATION, FRAMEWORK_NAMES, FRAMEWORK_PREFIX, MESSAGE_ID, MESSAGE_TYPE, MESSAGE_VERSION,
    ORDERING_KEY, REPLY_TO, REQUEST_ID, RabbitMqWire, SENT_AT_MS, SOURCE, TENANT_ID, TRACEPARENT,
    TRACESTATE,
};
use crate::error::MappingError;
use lapin::types::AMQPValue;
use sisa_messaging::{HeaderName, HeaderValue, Headers, Metadata, SerializedEnvelope};
use std::{borrow::Cow, str::FromStr};

type Slots<'a> = [Option<&'a str>; FRAMEWORK_NAMES.len()];

fn required<T: FromStr>(slots: &Slots<'_>, index: usize) -> Result<T, MappingError> {
    slots[index]
        .ok_or(MappingError::InvalidEnvelope)?
        .parse()
        .map_err(|_| MappingError::InvalidEnvelope)
}

fn optional<T: FromStr>(slots: &Slots<'_>, index: usize) -> Result<Option<T>, MappingError> {
    slots[index]
        .map(|value| value.parse().map_err(|_| MappingError::InvalidEnvelope))
        .transpose()
}

pub(super) fn decode(wire: RabbitMqWire) -> Result<SerializedEnvelope, MappingError> {
    let table = wire
        .properties
        .headers()
        .as_ref()
        .ok_or(MappingError::InvalidEnvelope)?;

    let mut slots: Slots<'_> = [None; FRAMEWORK_NAMES.len()];
    let mut custom = Headers::new();

    for (name, value) in table {
        let name = name.as_str();

        let in_namespace = name
            .as_bytes()
            .get(..FRAMEWORK_PREFIX.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(FRAMEWORK_PREFIX));

        // Broker and application headers such as `x-death` are outside the projection.
        if !in_namespace {
            continue;
        }

        let AMQPValue::LongString(value) = value else {
            return Err(MappingError::InvalidHeaders);
        };

        let value =
            std::str::from_utf8(value.as_bytes()).map_err(|_| MappingError::InvalidHeaders)?;

        let name = if name.bytes().any(|byte| byte.is_ascii_uppercase()) {
            Cow::Owned(name.to_ascii_lowercase())
        } else {
            Cow::Borrowed(name)
        };

        if let Some(suffix) = name.strip_prefix(CUSTOM_PREFIX) {
            let key = HeaderName::new(suffix).map_err(|_| MappingError::InvalidHeaders)?;
            let value = HeaderValue::new(value).map_err(|_| MappingError::InvalidHeaders)?;

            if custom
                .insert(key, value)
                .map_err(|_| MappingError::InvalidHeaders)?
                .is_some()
            {
                return Err(MappingError::InvalidHeaders);
            }
        } else if let Some(index) = FRAMEWORK_NAMES.iter().position(|known| *known == name)
            && slots[index].replace(value).is_some()
        {
            return Err(MappingError::InvalidHeaders);
        }

        // Unknown `sisa-` names are ignored so a newer producer can add framework headers.
    }

    let mut metadata = Metadata::default();
    metadata.correlation.correlation_id = optional(&slots, CORRELATION_ID)?;
    metadata.correlation.conversation_id = optional(&slots, CONVERSATION_ID)?;
    metadata.correlation.causation_id = optional(&slots, CAUSATION_ID)?;
    metadata.correlation.request_id = optional(&slots, REQUEST_ID)?;
    metadata.routing.source = optional(&slots, SOURCE)?;
    metadata.routing.destination = optional(&slots, DESTINATION)?;
    metadata.routing.reply_to = optional(&slots, REPLY_TO)?;
    metadata.delivery.sent_at_ms = optional(&slots, SENT_AT_MS)?;
    metadata.delivery.deduplication_id = optional(&slots, DEDUPLICATION_ID)?;
    metadata.tenant_id = optional(&slots, TENANT_ID)?;
    metadata.trace.traceparent = optional(&slots, TRACEPARENT)?;
    metadata.trace.tracestate = optional(&slots, TRACESTATE)?;
    metadata.headers = custom;

    Ok(SerializedEnvelope {
        message_id: required(&slots, MESSAGE_ID)?,
        message_type: required(&slots, MESSAGE_TYPE)?,
        message_version: required(&slots, MESSAGE_VERSION)?,
        content_type: required(&slots, CONTENT_TYPE)?,
        ordering_key: optional(&slots, ORDERING_KEY)?,
        metadata,
        payload: wire.payload,
    })
}
