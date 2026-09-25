//! Deterministic AMQP 0-9-1 projection with validated routes and redacted errors.
//!
//! The headers table is the only authoritative projection. Framework fields travel as `sisa-*`
//! long-string headers and custom headers as `sisa-custom-*` long-string headers. The message
//! identity, content type, and persistent delivery mode are mirrored into basic properties for
//! broker tooling, but decoding reads only the headers table.

use crate::error::MappingError;
use lapin::{
    BasicProperties,
    types::{AMQPValue, FieldTable, LongString, ShortString},
};
use sisa_messaging::{EnvelopeMapper, HeaderValue, OrderingKey, SerializedEnvelope};
use std::{collections::BTreeMap, fmt};

mod decode;

/// Encoded content-header frame payload bound: the AMQP minimum `frame_max` of 4,096 bytes less
/// the 8 bytes of frame type, channel, size, and end octet.
const MAX_CONTENT_HEADER_BYTES: usize = 4_088;

/// Class id, weight, body size, and property flags preceding the property list.
const CONTENT_HEADER_FIXED_BYTES: usize = 2 + 2 + 8 + 2;

/// Persistent AMQP delivery mode.
const PERSISTENT: u8 = 2;

const FRAMEWORK_PREFIX: &[u8] = b"sisa-";
const CUSTOM_PREFIX: &str = "sisa-custom-";

const MESSAGE_ID: usize = 0;
const MESSAGE_TYPE: usize = 1;
const MESSAGE_VERSION: usize = 2;
const CONTENT_TYPE: usize = 3;
const ORDERING_KEY: usize = 4;
const CORRELATION_ID: usize = 5;
const CONVERSATION_ID: usize = 6;
const CAUSATION_ID: usize = 7;
const REQUEST_ID: usize = 8;
const SOURCE: usize = 9;
const DESTINATION: usize = 10;
const REPLY_TO: usize = 11;
const SENT_AT_MS: usize = 12;
const DEDUPLICATION_ID: usize = 13;
const TENANT_ID: usize = 14;
const TRACEPARENT: usize = 15;
const TRACESTATE: usize = 16;

/// Framework header names indexed by the constants above, in `FrameworkHeader::ALL` order.
const FRAMEWORK_NAMES: [&str; 17] = [
    "sisa-message-id",
    "sisa-message-type",
    "sisa-message-version",
    "sisa-content-type",
    "sisa-ordering-key",
    "sisa-correlation-id",
    "sisa-conversation-id",
    "sisa-causation-id",
    "sisa-request-id",
    "sisa-source",
    "sisa-destination",
    "sisa-reply-to",
    "sisa-sent-at-ms",
    "sisa-deduplication-id",
    "sisa-tenant-id",
    "sisa-traceparent",
    "sisa-tracestate",
];

fn short_string(value: &str) -> Option<ShortString> {
    if value.bytes().any(|byte| byte.is_ascii_control()) {
        return None;
    }

    ShortString::try_new(value).ok()
}

/// A validated AMQP exchange name.
///
/// `Debug` and `Display` never render the name.
#[derive(Clone, Eq, PartialEq)]
pub struct ExchangeName(ShortString);

impl ExchangeName {
    /// Validates and owns a named exchange.
    ///
    /// The name must be nonempty, at most 255 bytes, and free of ASCII control bytes. Use
    /// [`ExchangeName::default_exchange`] to publish through the default exchange.
    pub fn new(value: impl Into<String>) -> Result<Self, MappingError> {
        let value = value.into();

        if value.is_empty() {
            return Err(MappingError::InvalidRoute);
        }

        short_string(&value)
            .map(Self)
            .ok_or(MappingError::InvalidRoute)
    }

    /// Selects the broker's default exchange, which routes by queue name.
    #[must_use]
    pub fn default_exchange() -> Self {
        Self(ShortString::default())
    }

    /// Borrows the validated name; the default exchange is the empty string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn into_short_string(self) -> ShortString {
        self.0
    }
}

impl fmt::Debug for ExchangeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExchangeName(<redacted>)")
    }
}

impl fmt::Display for ExchangeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted exchange>")
    }
}

/// A validated AMQP routing key.
///
/// `Debug` and `Display` never render the key.
#[derive(Clone, Eq, PartialEq)]
pub struct RoutingKey(ShortString);

impl RoutingKey {
    /// Validates and owns a routing key.
    ///
    /// The key must be at most 255 bytes and free of ASCII control bytes. An empty key is valid,
    /// for example for fanout exchanges.
    pub fn new(value: impl Into<String>) -> Result<Self, MappingError> {
        short_string(&value.into())
            .map(Self)
            .ok_or(MappingError::InvalidRoute)
    }

    /// Borrows the validated key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn into_short_string(self) -> ShortString {
        self.0
    }
}

impl fmt::Debug for RoutingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RoutingKey(<redacted>)")
    }
}

impl fmt::Display for RoutingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted routing key>")
    }
}

/// A concrete outbound destination: an exchange and the routing key it routes by.
///
/// `Debug` and `Display` never render either component.
#[derive(Clone, Eq, PartialEq)]
pub struct Route {
    /// Exchange receiving the publication.
    pub exchange: ExchangeName,

    /// Routing key the exchange applies.
    pub routing_key: RoutingKey,
}

impl fmt::Debug for Route {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Route(<redacted>)")
    }
}

impl fmt::Display for Route {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted route>")
    }
}

/// Resolves the concrete outbound route for an envelope.
pub trait RouteResolver: Send + Sync {
    /// Computes a route from validated logical fields.
    fn resolve(&self, envelope: &SerializedEnvelope) -> Result<Route, MappingError>;
}

/// Routes to one exchange with the routing key `{message_type}.v{message_version}`.
#[derive(Clone, Debug)]
pub struct TypeRouteResolver {
    exchange: ExchangeName,
}

impl TypeRouteResolver {
    /// Constructs a resolver without network I/O.
    pub fn new(exchange: ExchangeName) -> Self {
        Self { exchange }
    }
}

impl RouteResolver for TypeRouteResolver {
    fn resolve(&self, envelope: &SerializedEnvelope) -> Result<Route, MappingError> {
        let routing_key = RoutingKey::new(format!(
            "{}.v{}",
            envelope.message_type.as_str(),
            envelope.message_version
        ))?;

        Ok(Route {
            exchange: self.exchange.clone(),
            routing_key,
        })
    }
}

/// Owned AMQP wire projection.
///
/// `Debug` renders only the payload length.
#[derive(Clone)]
pub struct RabbitMqWire {
    /// Outbound exchange, or the exchange a delivery was published to.
    pub exchange: String,

    /// Outbound or delivered routing key.
    pub routing_key: String,

    /// Basic properties; only their headers table is read when decoding.
    pub properties: BasicProperties,

    /// Serialized body.
    pub payload: Vec<u8>,
}

impl fmt::Debug for RabbitMqWire {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RabbitMqWire")
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

/// Pure mapper using `sisa-*` framework headers and `sisa-custom-*` custom headers.
///
/// Encoding fails with [`MappingError::HeadersTooLarge`] when the content-header frame would
/// exceed 4,088 bytes. Decoding ignores basic properties, the route, and headers outside the
/// `sisa-` namespace such as `x-death`; it rejects missing, case-variant duplicate, or
/// non-long-string framework headers. The AMQP client collapses byte-identical duplicate table
/// keys before the mapper observes them.
pub struct RabbitMqMapper<R> {
    resolver: R,
}

impl<R> RabbitMqMapper<R> {
    /// Constructs a mapper without network I/O.
    pub fn new(resolver: R) -> Self {
        Self { resolver }
    }
}

impl<R: RouteResolver> RabbitMqMapper<R> {
    /// Resolves the route and builds properties without copying the payload.
    pub(crate) fn project(
        &self,
        envelope: &SerializedEnvelope,
    ) -> Result<(Route, BasicProperties), MappingError> {
        let route = self.resolver.resolve(envelope)?;
        let properties = properties(envelope)?;

        Ok((route, properties))
    }
}

impl<R: RouteResolver> EnvelopeMapper<RabbitMqWire> for RabbitMqMapper<R> {
    type Error = MappingError;

    fn encode(&self, envelope: &SerializedEnvelope) -> Result<RabbitMqWire, Self::Error> {
        let (route, properties) = self.project(envelope)?;

        Ok(RabbitMqWire {
            exchange: route.exchange.as_str().to_owned(),
            routing_key: route.routing_key.as_str().to_owned(),
            properties,
            payload: envelope.payload.clone(),
        })
    }

    fn decode(&self, wire: RabbitMqWire) -> Result<SerializedEnvelope, Self::Error> {
        decode::decode(wire)
    }
}

/// Accumulates the headers table while tracking its encoded size.
struct TableBuilder {
    entries: BTreeMap<ShortString, AMQPValue>,

    encoded: usize,

    budget: usize,
}

impl TableBuilder {
    fn push(&mut self, name: &str, value: Option<&str>) -> Result<(), MappingError> {
        let Some(value) = value else {
            return Ok(());
        };

        // Name length octet, name, type octet, value length, and value.
        self.encoded = self
            .encoded
            .saturating_add(1 + name.len() + 1 + 4)
            .saturating_add(value.len());

        if self.encoded > self.budget {
            return Err(MappingError::HeadersTooLarge);
        }

        let name = ShortString::try_new(name).map_err(|_| MappingError::InvalidHeaders)?;

        let value = AMQPValue::LongString(LongString::from(value.as_bytes()));

        if self.entries.insert(name, value).is_some() {
            return Err(MappingError::InvalidHeaders);
        }

        Ok(())
    }
}

fn properties(envelope: &SerializedEnvelope) -> Result<BasicProperties, MappingError> {
    let message_id = envelope.message_id.to_string();

    let content_type =
        short_string(envelope.content_type.as_str()).ok_or(MappingError::InvalidEnvelope)?;

    let message_id_property = short_string(&message_id).ok_or(MappingError::InvalidEnvelope)?;

    // Content type and message id short strings, the table length prefix, and delivery mode.
    let properties_fixed = (1 + content_type.as_str().len()) + (1 + message_id.len()) + 4 + 1;

    let budget = MAX_CONTENT_HEADER_BYTES
        .checked_sub(CONTENT_HEADER_FIXED_BYTES + properties_fixed)
        .ok_or(MappingError::HeadersTooLarge)?;

    let mut table = TableBuilder {
        entries: BTreeMap::new(),
        encoded: 0,
        budget,
    };

    push_framework(&mut table, envelope, &message_id)?;

    for (name, value) in envelope.metadata.headers.iter() {
        table.push(
            &format!("{CUSTOM_PREFIX}{}", name.as_str()),
            Some(value.as_str()),
        )?;
    }

    Ok(BasicProperties::default()
        .with_content_type(content_type)
        .with_headers(FieldTable::from(table.entries))
        .with_delivery_mode(PERSISTENT)
        .with_message_id(message_id_property))
}

fn push_framework(
    table: &mut TableBuilder,
    envelope: &SerializedEnvelope,
    message_id: &str,
) -> Result<(), MappingError> {
    let m = &envelope.metadata;
    let version = envelope.message_version.to_string();

    let conversation_id = m
        .correlation
        .conversation_id
        .as_ref()
        .map(|v| v.to_string());

    let causation_id = m.correlation.causation_id.as_ref().map(|v| v.to_string());
    let request_id = m.correlation.request_id.as_ref().map(|v| v.to_string());
    let sent_at_ms = m.delivery.sent_at_ms.map(|v| v.to_string());

    let fields = [
        (MESSAGE_ID, Some(message_id)),
        (MESSAGE_TYPE, Some(envelope.message_type.as_str())),
        (MESSAGE_VERSION, Some(version.as_str())),
        (CONTENT_TYPE, Some(envelope.content_type.as_str())),
        (
            ORDERING_KEY,
            envelope.ordering_key.as_ref().map(OrderingKey::as_str),
        ),
        (
            CORRELATION_ID,
            m.correlation.correlation_id.as_ref().map(|v| v.as_str()),
        ),
        (CONVERSATION_ID, conversation_id.as_deref()),
        (CAUSATION_ID, causation_id.as_deref()),
        (REQUEST_ID, request_id.as_deref()),
        (SOURCE, m.routing.source.as_ref().map(|v| v.as_str())),
        (
            DESTINATION,
            m.routing.destination.as_ref().map(|v| v.as_str()),
        ),
        (REPLY_TO, m.routing.reply_to.as_ref().map(|v| v.as_str())),
        (SENT_AT_MS, sent_at_ms.as_deref()),
        (
            DEDUPLICATION_ID,
            m.delivery.deduplication_id.as_ref().map(|v| v.as_str()),
        ),
        (TENANT_ID, m.tenant_id.as_ref().map(|v| v.as_str())),
        (
            TRACEPARENT,
            m.trace.traceparent.as_ref().map(HeaderValue::as_str),
        ),
        (
            TRACESTATE,
            m.trace.tracestate.as_ref().map(HeaderValue::as_str),
        ),
    ];

    for (index, value) in fields {
        table.push(FRAMEWORK_NAMES[index], value)?;
    }

    Ok(())
}
