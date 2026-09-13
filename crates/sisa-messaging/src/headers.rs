//! Validated custom headers and the frozen framework header namespace.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

const MAX_HEADER_NAME_BYTES: usize = 255;

const MAX_HEADER_VALUE_BYTES: usize = 8_192;

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

        if value.is_empty() {
            return Err(HeaderNameError::Empty);
        }

        if value.len() > MAX_HEADER_NAME_BYTES {
            return Err(HeaderNameError::TooLong);
        }

        if !value.as_bytes().iter().all(|byte| is_token_byte(*byte)) {
            return Err(HeaderNameError::InvalidCharacter);
        }

        if FrameworkHeader::is_reserved(&value) {
            return Err(HeaderNameError::Reserved);
        }

        value.make_ascii_lowercase();
        Ok(Self(value))
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

    /// The value contained CR or LF.
    Newline,
}

impl fmt::Display for HeaderValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLong => formatter.write_str("header value exceeds the 8192-byte limit"),
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

        if value.len() > MAX_HEADER_VALUE_BYTES {
            return Err(HeaderValueError::TooLong);
        }

        if value.contains(['\r', '\n']) {
            return Err(HeaderValueError::Newline);
        }

        Ok(Self(value))
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

/// A deterministic collection of validated custom headers.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Headers(BTreeMap<HeaderName, HeaderValue>);

impl Headers {
    /// Creates an empty collection.
    #[must_use]
    pub const fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Inserts a validated value, returning the previous value when present.
    pub fn insert(&mut self, name: HeaderName, value: HeaderValue) -> Option<HeaderValue> {
        self.0.insert(name, value)
    }

    /// Gets a value by its case-preserving validated name.
    #[must_use]
    pub fn get(&self, name: &HeaderName) -> Option<&HeaderValue> {
        self.0.get(name)
    }

    /// Iterates in deterministic name order.
    pub fn iter(&self) -> impl Iterator<Item = (&HeaderName, &HeaderValue)> {
        self.0.iter()
    }

    /// Returns the number of custom headers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Reports whether the collection is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
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
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
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
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}
