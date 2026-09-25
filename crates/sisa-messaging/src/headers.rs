//! Validated custom headers and the frozen framework header namespace.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

const MAX_HEADER_NAME_BYTES: usize = 255;

const MAX_HEADER_VALUE_BYTES: usize = 8_192;

/// Maximum number of distinct canonical custom headers retained by [`Headers`].
pub const MAX_CUSTOM_HEADER_COUNT: usize = 64;

/// Maximum aggregate bytes retained by custom header names and values.
///
/// The bound counts the UTF-8 bytes of each canonical name and its value exactly once. It excludes
/// serialization syntax, allocator overhead, framework headers, and other metadata fields.
pub const MAX_CUSTOM_HEADER_BYTES: usize = 65_536;

/// A framework-owned wire header.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum FrameworkHeader {
    /// Stable message identity.
    MessageId,

    /// Stable message contract name.
    MessageType,

    /// Stable message contract version.
    MessageVersion,

    /// Serialized body content type.
    ContentType,

    /// Optional ordering key.
    OrderingKey,

    /// Correlation identifier.
    CorrelationId,

    /// Conversation identifier.
    ConversationId,

    /// Causation message identifier.
    CausationId,

    /// Request identifier.
    RequestId,

    /// Logical source.
    Source,

    /// Logical destination.
    Destination,

    /// Logical reply destination.
    ReplyTo,

    /// Send timestamp in Unix milliseconds.
    SentAtMs,

    /// Transport deduplication identity.
    DeduplicationId,

    /// Tenant identity.
    TenantId,

    /// W3C trace parent.
    Traceparent,

    /// W3C trace state.
    Tracestate,
}

impl FrameworkHeader {
    /// Every framework header in deterministic projection order.
    pub const ALL: [Self; 17] = [
        Self::MessageId,
        Self::MessageType,
        Self::MessageVersion,
        Self::ContentType,
        Self::OrderingKey,
        Self::CorrelationId,
        Self::ConversationId,
        Self::CausationId,
        Self::RequestId,
        Self::Source,
        Self::Destination,
        Self::ReplyTo,
        Self::SentAtMs,
        Self::DeduplicationId,
        Self::TenantId,
        Self::Traceparent,
        Self::Tracestate,
    ];

    /// Returns the stable lowercase wire name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::MessageId => "message-id",
            Self::MessageType => "message-type",
            Self::MessageVersion => "message-version",
            Self::ContentType => "content-type",
            Self::OrderingKey => "ordering-key",
            Self::CorrelationId => "correlation-id",
            Self::ConversationId => "conversation-id",
            Self::CausationId => "causation-id",
            Self::RequestId => "request-id",
            Self::Source => "source",
            Self::Destination => "destination",
            Self::ReplyTo => "reply-to",
            Self::SentAtMs => "sent-at-ms",
            Self::DeduplicationId => "deduplication-id",
            Self::TenantId => "tenant-id",
            Self::Traceparent => "traceparent",
            Self::Tracestate => "tracestate",
        }
    }

    pub(crate) fn is_reserved(name: &str) -> bool {
        Self::ALL
            .iter()
            .any(|header| name.eq_ignore_ascii_case(header.name()))
    }
}

/// A custom header-name validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HeaderNameError {
    /// The name was empty.
    Empty,

    /// The name exceeded the byte bound.
    TooLong,

    /// The name was not an ASCII transport-neutral token.
    InvalidCharacter,

    /// The name collided with a framework-owned header.
    Reserved,
}

impl fmt::Display for HeaderNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("header name must not be empty"),
            Self::TooLong => formatter.write_str("header name exceeds the 255-byte limit"),
            Self::InvalidCharacter => formatter.write_str("header name contains an invalid byte"),
            Self::Reserved => formatter.write_str("header name is reserved by the framework"),
        }
    }
}

impl std::error::Error for HeaderNameError {}

/// A validated custom header name stored in canonical lowercase form.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HeaderName(String);

impl HeaderName {
    /// Validates and owns a custom header name.
    pub fn new(value: impl Into<String>) -> Result<Self, HeaderNameError> {
        let mut value = value.into();

        Self::validate(&value)?;
        value.make_ascii_lowercase();

        Ok(Self(value))
    }

    fn validate(value: &str) -> Result<(), HeaderNameError> {
        if value.is_empty() {
            return Err(HeaderNameError::Empty);
        }

        if value.len() > MAX_HEADER_NAME_BYTES {
            return Err(HeaderNameError::TooLong);
        }

        if !value.as_bytes().iter().all(|byte| is_token_byte(*byte)) {
            return Err(HeaderNameError::InvalidCharacter);
        }

        if FrameworkHeader::is_reserved(value) {
            return Err(HeaderNameError::Reserved);
        }

        Ok(())
    }

    /// Borrows the validated name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the owned validated name.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

impl AsRef<str> for HeaderName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for HeaderName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for HeaderName {
    type Err = HeaderNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

/// A custom header-value validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HeaderValueError {
    /// The value exceeded the byte bound.
    TooLong,

    /// The value contained an ASCII control character.
    ControlCharacter,

    /// The value contained CR or LF.
    Newline,
}

impl fmt::Display for HeaderValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLong => formatter.write_str("header value exceeds the 8192-byte limit"),
            Self::ControlCharacter => {
                formatter.write_str("header value contains a forbidden control character")
            }
            Self::Newline => formatter.write_str("header value contains a forbidden newline"),
        }
    }
}

impl std::error::Error for HeaderValueError {}

/// A validated, bounded UTF-8 custom header value.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HeaderValue(String);

impl HeaderValue {
    /// Validates and owns a custom header value.
    pub fn new(value: impl Into<String>) -> Result<Self, HeaderValueError> {
        let value = value.into();

        Self::validate(&value)?;

        Ok(Self(value))
    }

    fn validate(value: &str) -> Result<(), HeaderValueError> {
        if value.len() > MAX_HEADER_VALUE_BYTES {
            return Err(HeaderValueError::TooLong);
        }

        if value.contains(['\r', '\n']) {
            return Err(HeaderValueError::Newline);
        }

        if value.as_bytes().iter().any(|byte| byte.is_ascii_control()) {
            return Err(HeaderValueError::ControlCharacter);
        }

        Ok(())
    }

    /// Borrows the validated value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the owned validated value.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for HeaderValue {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for HeaderValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for HeaderValue {
    type Err = HeaderValueError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

/// A custom-header collection validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HeadersError {
    /// The number of distinct canonical names exceeded [`MAX_CUSTOM_HEADER_COUNT`].
    TooManyHeaders,

    /// The retained canonical names and values exceeded [`MAX_CUSTOM_HEADER_BYTES`].
    TooManyBytes,
}

impl fmt::Display for HeadersError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyHeaders => formatter.write_str("too many custom headers"),
            Self::TooManyBytes => formatter.write_str("custom headers exceed the byte limit"),
        }
    }
}

impl std::error::Error for HeadersError {}

/// A deterministic, bounded collection of validated custom headers.
///
/// The collection retains at most [`MAX_CUSTOM_HEADER_COUNT`] distinct canonical names and at most
/// [`MAX_CUSTOM_HEADER_BYTES`] UTF-8 bytes across those names and their values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Headers {
    values: BTreeMap<HeaderName, HeaderValue>,

    aggregate_bytes: usize,
}

impl Headers {
    /// Creates an empty collection.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            values: BTreeMap::new(),
            aggregate_bytes: 0,
        }
    }

    /// Inserts a validated value, returning the previous value when present.
    ///
    /// Replacement uses the canonical name and does not consume another count slot. If the
    /// resulting retained aggregate would exceed a collection bound, the collection is unchanged.
    pub fn insert(
        &mut self,
        name: HeaderName,
        value: HeaderValue,
    ) -> Result<Option<HeaderValue>, HeadersError> {
        let previous = self.values.get(&name);
        let previous_bytes = previous.map_or(0, |previous| name.0.len() + previous.0.len());

        if previous.is_none() && self.values.len() >= MAX_CUSTOM_HEADER_COUNT {
            return Err(HeadersError::TooManyHeaders);
        }

        let candidate_bytes = name
            .0
            .len()
            .checked_add(value.0.len())
            .and_then(|new_bytes| {
                self.aggregate_bytes
                    .checked_sub(previous_bytes)?
                    .checked_add(new_bytes)
            })
            .ok_or(HeadersError::TooManyBytes)?;

        if candidate_bytes > MAX_CUSTOM_HEADER_BYTES {
            return Err(HeadersError::TooManyBytes);
        }

        let previous = self.values.insert(name, value);
        self.aggregate_bytes = candidate_bytes;

        Ok(previous)
    }

    /// Gets a value by its validated canonical-lowercase name.
    #[must_use]
    pub fn get(&self, name: &HeaderName) -> Option<&HeaderValue> {
        self.values.get(name)
    }

    /// Iterates in deterministic name order.
    pub fn iter(&self) -> impl Iterator<Item = (&HeaderName, &HeaderValue)> {
        self.values.iter()
    }

    /// Returns the number of custom headers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Reports whether the collection is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for HeaderName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for HeaderName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(HeaderNameVisitor)
    }
}

#[cfg(feature = "serde")]
struct HeaderNameVisitor;

#[cfg(feature = "serde")]
impl serde::de::Visitor<'_> for HeaderNameVisitor {
    type Value = HeaderName;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a valid custom header name")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        HeaderName::validate(value).map_err(E::custom)?;
        let mut value = value.to_owned();
        value.make_ascii_lowercase();

        Ok(HeaderName(value))
    }

    fn visit_borrowed_str<E>(self, value: &'_ str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(value)
    }

    fn visit_string<E>(self, mut value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        HeaderName::validate(&value).map_err(E::custom)?;
        value.make_ascii_lowercase();

        Ok(HeaderName(value))
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for HeaderValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for HeaderValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(HeaderValueVisitor)
    }
}

#[cfg(feature = "serde")]
struct HeaderValueVisitor;

#[cfg(feature = "serde")]
impl serde::de::Visitor<'_> for HeaderValueVisitor {
    type Value = HeaderValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a valid custom header value")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        HeaderValue::validate(value).map_err(E::custom)?;

        Ok(HeaderValue(value.to_owned()))
    }

    fn visit_borrowed_str<E>(self, value: &'_ str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(value)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        HeaderValue::validate(&value).map_err(E::custom)?;

        Ok(HeaderValue(value))
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Headers {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.values.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Headers {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(HeadersVisitor)
    }
}

#[cfg(feature = "serde")]
struct HeadersVisitor;

#[cfg(feature = "serde")]
impl<'de> serde::de::Visitor<'de> for HeadersVisitor {
    type Value = Headers;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded custom header map")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut headers = Headers::new();

        while let Some(name) = map.next_key::<HeaderName>()? {
            if !headers.values.contains_key(&name)
                && headers.values.len() >= MAX_CUSTOM_HEADER_COUNT
            {
                return Err(serde::de::Error::custom(HeadersError::TooManyHeaders));
            }

            let value = map.next_value::<HeaderValue>()?;

            headers
                .insert(name, value)
                .map_err(serde::de::Error::custom)?;
        }

        Ok(headers)
    }
}

#[cfg(all(test, feature = "serde"))]
mod tests {
    use serde::de::{DeserializeSeed, MapAccess, Visitor};

    use super::Headers;

    struct HugeSizeHintDeserializer;

    impl<'de> serde::Deserializer<'de> for HugeSizeHintDeserializer {
        type Error = serde::de::value::Error;

        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_map(HugeSizeHintMap)
        }

        fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_map(HugeSizeHintMap)
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes
            byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct struct enum
            identifier ignored_any
        }
    }

    struct HugeSizeHintMap;

    impl<'de> MapAccess<'de> for HugeSizeHintMap {
        type Error = serde::de::value::Error;

        fn next_key_seed<K>(&mut self, _seed: K) -> Result<Option<K::Value>, Self::Error>
        where
            K: DeserializeSeed<'de>,
        {
            Ok(None)
        }

        fn next_value_seed<V>(&mut self, _seed: V) -> Result<V::Value, Self::Error>
        where
            V: DeserializeSeed<'de>,
        {
            unreachable!("the empty test map never requests a value")
        }

        fn size_hint(&self) -> Option<usize> {
            Some(usize::MAX)
        }
    }

    #[test]
    fn deserialization_ignores_untrusted_map_size_hints() {
        let headers = <Headers as serde::Deserialize>::deserialize(HugeSizeHintDeserializer)
            .expect("empty header map must deserialize");

        assert!(headers.is_empty());
    }
}
