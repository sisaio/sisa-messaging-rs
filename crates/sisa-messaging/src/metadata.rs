//! Additive transport-independent message metadata.

use crate::{ConversationId, HeaderValue, Headers, MessageId, MetadataValue, RequestId};

/// Correlation and causal identities.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct CorrelationMetadata {
    /// Application correlation identity.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub correlation_id: Option<MetadataValue>,

    /// Conversation identity.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub conversation_id: Option<ConversationId>,

    /// Identity of the message that caused this one.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub causation_id: Option<MessageId>,

    /// Request identity.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub request_id: Option<RequestId>,
}

impl CorrelationMetadata {
    #[cfg(feature = "serde")]
    fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// W3C trace-context values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct TraceMetadata {
    /// W3C `traceparent` value.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub traceparent: Option<HeaderValue>,

    /// W3C `tracestate` value.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub tracestate: Option<HeaderValue>,
}

impl TraceMetadata {
    #[cfg(feature = "serde")]
    fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// Logical routing metadata, independent of a concrete broker subject.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct RoutingMetadata {
    /// Logical publisher source.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub source: Option<MetadataValue>,

    /// Logical destination template or name.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub destination: Option<MetadataValue>,

    /// Logical reply destination.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub reply_to: Option<MetadataValue>,
}

impl RoutingMetadata {
    #[cfg(feature = "serde")]
    fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// Metadata established by a publisher or mapper at delivery time.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct DeliveryMetadata {
    /// Unix timestamp in milliseconds at which publication was attempted.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub sent_at_ms: Option<u64>,

    /// Transport deduplication identity.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub deduplication_id: Option<MetadataValue>,
}

impl DeliveryMetadata {
    #[cfg(feature = "serde")]
    fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// Additive metadata persisted and projected alongside an envelope.
///
/// Unknown JSON fields are ignored when the `serde` feature is enabled so rolling upgrades can
/// safely add metadata. Missing fields use their empty defaults.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct Metadata {
    /// Correlation and causal identities.
    #[cfg_attr(
        feature = "serde",
        serde(skip_serializing_if = "CorrelationMetadata::is_empty")
    )]
    pub correlation: CorrelationMetadata,

    /// W3C trace context.
    #[cfg_attr(
        feature = "serde",
        serde(skip_serializing_if = "TraceMetadata::is_empty")
    )]
    pub trace: TraceMetadata,

    /// Logical routing metadata.
    #[cfg_attr(
        feature = "serde",
        serde(skip_serializing_if = "RoutingMetadata::is_empty")
    )]
    pub routing: RoutingMetadata,

    /// Publication/delivery metadata.
    #[cfg_attr(
        feature = "serde",
        serde(skip_serializing_if = "DeliveryMetadata::is_empty")
    )]
    pub delivery: DeliveryMetadata,

    /// Optional tenant identity.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub tenant_id: Option<MetadataValue>,

    /// Validated application-owned custom headers.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Headers::is_empty"))]
    pub headers: Headers,
}
