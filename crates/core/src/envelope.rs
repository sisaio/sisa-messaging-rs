//! The envelope: a message on its way, as a CloudEvent.

use std::collections::BTreeMap;
use std::fmt;

use bytes::Bytes;

use crate::{EnvelopeError, MessageId, MessageType, Timestamp};

// The attributes the envelope has fields for, which an extension can't take. `data` is the
// body's member in the structured mode, so it is no extension either.
const RESERVED: [&str; 14] = [
    "specversion",
    "id",
    "source",
    "type",
    "time",
    "datacontenttype",
    "subject",
    "dataschema",
    "partitionkey",
    "traceparent",
    "tracestate",
    "correlationid",
    "causationid",
    "data",
];

fn has_control_or_space(value: &str) -> bool {
    value
        .chars()
        .any(|character| character.is_control() || character.is_whitespace())
}

/// The publishing service: the CloudEvents `source`, a URI reference such as `/orders`.
///
/// A bus sets it once for everything it publishes. It must not be empty or hold spaces or
/// control characters; beyond that it isn't checked against the full URI grammar, so keep it a
/// simple path such as `/orders` or an absolute URI.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Source(String);

impl Source {
    /// Checks a source.
    ///
    /// # Errors
    ///
    /// [`EnvelopeError::InvalidAttribute`] when it is empty or holds a space or control
    /// character.
    pub fn new(value: impl Into<String>) -> Result<Self, EnvelopeError> {
        let value = value.into();
        if value.is_empty() || has_control_or_space(&value) {
            return Err(EnvelopeError::invalid("source", "not a URI reference"));
        }
        Ok(Self(value))
    }

    /// The source as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// Writes the source as it goes on the wire.
impl fmt::Display for Source {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The media type of the envelope's data, such as `application/json`: the CloudEvents
/// `datacontenttype`. The codec that encoded the data names it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentType(String);

impl ContentType {
    /// Checks a content type.
    ///
    /// # Errors
    ///
    /// [`EnvelopeError::InvalidAttribute`] when it is empty or holds a control character.
    pub fn new(value: impl Into<String>) -> Result<Self, EnvelopeError> {
        let value = value.into();
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(EnvelopeError::invalid(
                "datacontenttype",
                "not a media type",
            ));
        }
        Ok(Self(value))
    }

    /// The content type as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// Writes the content type as it goes on the wire.
impl fmt::Display for ContentType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Ties messages together: the `correlationid` and `causationid` extensions.
///
/// A handler's publishes carry the handled message's correlation id and have that message as
/// their cause; the runtime sets both.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Correlation {
    /// The id that ties a conversation together.
    pub correlation_id: Option<MessageId>,
    /// The id of the message whose handler published this one.
    pub causation_id: Option<MessageId>,
}

impl Correlation {
    /// A correlation with both ids.
    #[must_use]
    pub fn new(correlation_id: Option<MessageId>, causation_id: Option<MessageId>) -> Self {
        Self {
            correlation_id,
            causation_id,
        }
    }
}

/// W3C Trace Context: the `traceparent` and `tracestate` attributes of the CloudEvents
/// Distributed Tracing extension.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct TraceContext {
    /// The trace and parent span, such as
    /// `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`.
    pub traceparent: String,
    /// Vendor data, passed on unchanged as W3C Trace Context requires.
    pub tracestate: Option<String>,
}

impl TraceContext {
    /// A trace context from its two headers.
    #[must_use]
    pub fn new(traceparent: impl Into<String>, tracestate: Option<String>) -> Self {
        Self {
            traceparent: traceparent.into(),
            tracestate,
        }
    }
}

/// The CloudEvents attributes the envelope has no field for, kept as they came.
///
/// A name is 1 to 20 lowercase ASCII letters and digits, as CloudEvents asks, and can't be one of
/// the attributes the envelope has a field for.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Extensions(BTreeMap<String, String>);

impl Extensions {
    /// No extensions.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets an extension, and returns the value it replaced.
    ///
    /// # Errors
    ///
    /// [`EnvelopeError::InvalidAttribute`] when the name isn't lowercase letters and digits, is
    /// longer than 20 characters, or is an attribute the envelope has a field for.
    pub fn insert(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, EnvelopeError> {
        let name = name.into();
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        {
            return Err(EnvelopeError::invalid(
                name,
                "not lowercase letters and digits",
            ));
        }
        if name.len() > 20 {
            return Err(EnvelopeError::invalid(name, "longer than 20 characters"));
        }
        if RESERVED.contains(&name.as_str()) {
            return Err(EnvelopeError::invalid(
                name,
                "has a field of its own in the envelope",
            ));
        }
        Ok(self.0.insert(name, value.into()))
    }

    /// The value of an extension.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    /// Removes an extension, and returns its value.
    pub fn remove(&mut self, name: &str) -> Option<String> {
        self.0.remove(name)
    }

    /// The extensions in name order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// How many extensions there are.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

fn required(
    attributes: &mut BTreeMap<String, String>,
    name: &'static str,
) -> Result<String, EnvelopeError> {
    attributes
        .remove(name)
        .ok_or(EnvelopeError::MissingAttribute(name))
}

fn optional_id(
    attributes: &mut BTreeMap<String, String>,
    name: &'static str,
) -> Result<Option<MessageId>, EnvelopeError> {
    let Some(text) = attributes.remove(name) else {
        return Ok(None);
    };
    let id = text
        .parse()
        .map_err(|_| EnvelopeError::invalid(name, "not a UUID"))?;
    Ok(Some(id))
}

/// A message on its way, as a CloudEvent 1.0: its attributes and its encoded data.
///
/// The fields are grouped here and flat on the wire, where a transport writes each attribute as
/// a header of its own. [`to_attributes`](Envelope::to_attributes) and
/// [`from_attributes`](Envelope::from_attributes) map the envelope to the attributes and back;
/// a transport adds its binding's prefix, such as `ce-` on NATS.
///
/// ```
/// use bytes::Bytes;
/// use sisa_eventing_core::{ContentType, Envelope, MessageType, Source};
///
/// let envelope = Envelope::new(
///     MessageType::new("orders.order_placed.v1")?,
///     Source::new("/orders")?,
///     ContentType::new("application/json")?,
///     Bytes::from_static(br#"{"id":42}"#),
/// );
/// let attributes = envelope.to_attributes();
/// assert_eq!(attributes["type"], "orders.order_placed.v1");
/// assert_eq!(Envelope::from_attributes(attributes, envelope.data.clone())?, envelope);
/// # Ok::<(), sisa_eventing_core::EnvelopeError>(())
/// ```
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Envelope {
    /// `id`: unique per message, and the same on every retry and redelivery.
    pub id: MessageId,
    /// `type`: the [`Message::TYPE`](crate::Message::TYPE).
    pub message_type: MessageType,
    /// `source`: the publishing service.
    pub source: Source,
    /// `time`: when the message was published.
    pub time: Timestamp,
    /// `subject`: what the message is about within the source, such as an order id.
    pub subject: Option<String>,
    /// `datacontenttype`: the media type of [`data`](Envelope::data), from the codec.
    pub content_type: ContentType,
    /// `dataschema`: the URI of the data's schema, for schema codecs such as Avro.
    pub data_schema: Option<String>,
    /// `partitionkey`: messages with one key keep their order, where the transport can.
    pub partition_key: Option<String>,
    /// `correlationid` and `causationid`.
    pub correlation: Correlation,
    /// `traceparent` and `tracestate`.
    pub trace: Option<TraceContext>,
    /// Any other attribute, kept as it came.
    pub extensions: Extensions,
    /// The encoded message.
    pub data: Bytes,
}

impl Envelope {
    /// An envelope with a new id, the current time, and no optional attributes.
    #[must_use]
    pub fn new(
        message_type: MessageType,
        source: Source,
        content_type: ContentType,
        data: Bytes,
    ) -> Self {
        Self {
            id: MessageId::new(),
            message_type,
            source,
            time: Timestamp::now(),
            subject: None,
            content_type,
            data_schema: None,
            partition_key: None,
            correlation: Correlation::default(),
            trace: None,
            extensions: Extensions::new(),
            data,
        }
    }

    /// The CloudEvents attributes, by name without a transport's prefix: `specversion` `1.0`,
    /// every field that is set, and the extensions.
    #[must_use]
    pub fn to_attributes(&self) -> BTreeMap<String, String> {
        let required = [
            ("specversion", "1.0".to_owned()),
            ("id", self.id.to_string()),
            ("source", self.source.to_string()),
            ("type", self.message_type.to_string()),
            ("time", self.time.to_string()),
            ("datacontenttype", self.content_type.to_string()),
        ];
        let optional = [
            ("subject", self.subject.clone()),
            ("dataschema", self.data_schema.clone()),
            ("partitionkey", self.partition_key.clone()),
            (
                "correlationid",
                self.correlation.correlation_id.map(|id| id.to_string()),
            ),
            (
                "causationid",
                self.correlation.causation_id.map(|id| id.to_string()),
            ),
            (
                "traceparent",
                self.trace.as_ref().map(|trace| trace.traceparent.clone()),
            ),
            (
                "tracestate",
                self.trace
                    .as_ref()
                    .and_then(|trace| trace.tracestate.clone()),
            ),
        ];

        let mut attributes: BTreeMap<String, String> = required
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect();
        for (name, value) in optional {
            if let Some(value) = value {
                attributes.insert(name.to_owned(), value);
            }
        }
        for (name, value) in self.extensions.iter() {
            attributes.insert(name.to_owned(), value.to_owned());
        }
        attributes
    }

    /// An envelope from CloudEvents attributes, named without a transport's prefix, and its
    /// data. Attributes the envelope has no field for become extensions.
    ///
    /// A `tracestate` without a `traceparent` is dropped, as W3C Trace Context says.
    ///
    /// # Errors
    ///
    /// [`EnvelopeError::MissingAttribute`] when `specversion`, `id`, `source`, `type`, `time` or
    /// `datacontenttype` is missing, and [`EnvelopeError::InvalidAttribute`] or
    /// [`EnvelopeError::InvalidMessageType`] when an attribute's value or an extension's name is
    /// invalid, or `specversion` isn't `1.0`.
    pub fn from_attributes(
        mut attributes: BTreeMap<String, String>,
        data: Bytes,
    ) -> Result<Self, EnvelopeError> {
        if required(&mut attributes, "specversion")? != "1.0" {
            return Err(EnvelopeError::invalid(
                "specversion",
                "only CloudEvents 1.0 is supported",
            ));
        }
        let id = required(&mut attributes, "id")?.parse()?;
        let source = Source::new(required(&mut attributes, "source")?)?;
        let message_type = MessageType::new(required(&mut attributes, "type")?)?;
        let time = required(&mut attributes, "time")?.parse()?;
        let content_type = ContentType::new(required(&mut attributes, "datacontenttype")?)?;
        let subject = attributes.remove("subject");
        let data_schema = attributes.remove("dataschema");
        let partition_key = attributes.remove("partitionkey");
        let correlation = Correlation::new(
            optional_id(&mut attributes, "correlationid")?,
            optional_id(&mut attributes, "causationid")?,
        );
        let tracestate = attributes.remove("tracestate");
        let trace = attributes
            .remove("traceparent")
            .map(|traceparent| TraceContext::new(traceparent, tracestate));

        let mut extensions = Extensions::new();
        for (name, value) in attributes {
            extensions.insert(name, value)?;
        }

        Ok(Self {
            id,
            message_type,
            source,
            time,
            subject,
            content_type,
            data_schema,
            partition_key,
            correlation,
            trace,
            extensions,
            data,
        })
    }
}

// Shows the attributes and only the data's length, since the data may be private.
impl fmt::Debug for Envelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Envelope")
            .field("id", &self.id)
            .field("message_type", &self.message_type)
            .field("source", &self.source)
            .field("time", &self.time)
            .field("subject", &self.subject)
            .field("content_type", &self.content_type)
            .field("data_schema", &self.data_schema)
            .field("partition_key", &self.partition_key)
            .field("correlation", &self.correlation)
            .field("trace", &self.trace)
            .field("extensions", &self.extensions)
            .field("data", &format_args!("{} bytes", self.data.len()))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attributes(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|&(name, value)| (name.to_owned(), value.to_owned()))
            .collect()
    }

    fn required() -> BTreeMap<String, String> {
        attributes(&[
            ("specversion", "1.0"),
            ("id", "0190f5a4-8c3b-7d2e-9f10-1a2b3c4d5e6f"),
            ("source", "/orders"),
            ("type", "orders.order_placed.v1"),
            ("time", "2026-11-02T10:30:00.5Z"),
            ("datacontenttype", "application/json"),
        ])
    }

    fn envelope() -> Envelope {
        Envelope::new(
            MessageType::new("orders.order_placed.v1").unwrap(),
            Source::new("/orders").unwrap(),
            ContentType::new("application/json").unwrap(),
            Bytes::from_static(b"secret"),
        )
    }

    #[test]
    fn writes_only_the_required_attributes_of_a_new_envelope() {
        let envelope = envelope();

        let names: Vec<String> = envelope.to_attributes().into_keys().collect();

        assert_eq!(
            names,
            [
                "datacontenttype",
                "id",
                "source",
                "specversion",
                "time",
                "type"
            ]
        );
    }

    #[test]
    fn round_trips_every_attribute_tracestate_and_extensions_included() {
        let mut envelope = envelope();
        envelope.subject = Some("order-42".into());
        envelope.data_schema = Some("https://schemas.example.com/order_placed.avsc".into());
        envelope.partition_key = Some("customer-7".into());
        envelope.correlation = Correlation::new(Some(MessageId::new()), Some(MessageId::new()));
        envelope.trace = Some(TraceContext::new(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            Some("congo=t61rcWkgMzE".into()),
        ));
        envelope.extensions.insert("tenant", "acme").unwrap();

        let attributes = envelope.to_attributes();

        assert_eq!(attributes["tracestate"], "congo=t61rcWkgMzE");
        assert_eq!(attributes["tenant"], "acme");
        assert_eq!(
            Envelope::from_attributes(attributes, envelope.data.clone()),
            Ok(envelope)
        );
    }

    #[test]
    fn reads_unknown_attributes_as_extensions() {
        let mut attributes = required();
        attributes.insert("errorendpoint".into(), "billing".into());

        let envelope = Envelope::from_attributes(attributes, Bytes::new()).unwrap();

        assert_eq!(envelope.extensions.get("errorendpoint"), Some("billing"));
    }

    #[test]
    fn drops_a_tracestate_without_a_traceparent() {
        let mut attributes = required();
        attributes.insert("tracestate".into(), "congo=t61rcWkgMzE".into());

        let envelope = Envelope::from_attributes(attributes, Bytes::new()).unwrap();

        assert_eq!(envelope.trace, None);
        assert!(envelope.extensions.is_empty());
    }

    #[test]
    fn rejects_a_missing_required_attribute() {
        for name in [
            "specversion",
            "id",
            "source",
            "type",
            "time",
            "datacontenttype",
        ] {
            let mut attributes = required();
            attributes.remove(name);

            assert_eq!(
                Envelope::from_attributes(attributes, Bytes::new()),
                Err(EnvelopeError::MissingAttribute(name)),
            );
        }
    }

    #[test]
    fn rejects_an_invalid_attribute_value() {
        let cases = [
            ("specversion", "0.3", "only CloudEvents 1.0 is supported"),
            ("id", "42", "not a UUID"),
            ("source", "", "not a URI reference"),
            ("time", "yesterday", "not an RFC 3339 date-time"),
            ("datacontenttype", "", "not a media type"),
            ("correlationid", "42", "not a UUID"),
            ("causationid", "42", "not a UUID"),
        ];
        for (name, value, reason) in cases {
            let mut attributes = required();
            attributes.insert(name.into(), value.into());

            assert_eq!(
                Envelope::from_attributes(attributes, Bytes::new()),
                Err(EnvelopeError::invalid(name, reason)),
            );
        }
    }

    #[test]
    fn rejects_an_invalid_message_type() {
        let mut attributes = required();
        attributes.insert("type".into(), "OrderPlaced".into());

        assert!(matches!(
            Envelope::from_attributes(attributes, Bytes::new()),
            Err(EnvelopeError::InvalidMessageType { .. })
        ));
    }

    #[test]
    fn rejects_an_extension_name_cloudevents_does_not_allow() {
        let mut attributes = required();
        attributes.insert("Tenant_Id".into(), "acme".into());

        assert_eq!(
            Envelope::from_attributes(attributes, Bytes::new()),
            Err(EnvelopeError::invalid(
                "Tenant_Id",
                "not lowercase letters and digits"
            )),
        );
    }

    #[test]
    fn keeps_attributes_with_a_field_out_of_the_extensions() {
        let mut extensions = Extensions::new();

        assert_eq!(
            extensions.insert("correlationid", "x"),
            Err(EnvelopeError::invalid(
                "correlationid",
                "has a field of its own in the envelope"
            )),
        );
        assert_eq!(
            extensions.insert("", "x"),
            Err(EnvelopeError::invalid(
                "",
                "not lowercase letters and digits"
            ))
        );
    }

    #[test]
    fn rejects_an_extension_name_longer_than_20_characters() {
        let mut extensions = Extensions::new();

        assert!(extensions.insert("a".repeat(20), "x").is_ok());
        assert_eq!(
            extensions.insert("a".repeat(21), "x"),
            Err(EnvelopeError::invalid(
                "a".repeat(21),
                "longer than 20 characters"
            )),
        );
    }

    #[test]
    fn rejects_a_source_with_spaces() {
        assert_eq!(
            Source::new("/my orders"),
            Err(EnvelopeError::invalid("source", "not a URI reference"))
        );
    }

    #[test]
    fn shows_the_data_length_not_the_data_in_debug() {
        let shown = format!("{:?}", envelope());

        assert!(shown.contains("data: 6 bytes"), "{shown}");
        assert!(!shown.contains("secret"), "{shown}");
    }
}
