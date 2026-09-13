//! Transport-independent messaging values and capability contracts.
//!
//! The crate owns validated message identities, envelopes, metadata, serialization boundaries,
//! publication, inbound delivery, settlement, mapping, and explicit failure classification. It
//! owns no runtime, persistence, transport, configuration loading, or telemetry exporter policy.
//!
//! Enable the `json` feature for `JsonSerializer`. The lower-level values remain available
//! without Serde or a concrete codec.

#![forbid(unsafe_code)]

mod delivery;
mod envelope;
mod error;
mod failure;
mod headers;
mod ids;
mod mapper;
mod message;
mod metadata;
mod publisher;
mod serializer;

pub use delivery::{Delivery, DeliverySource, Settlement};
pub use envelope::{Envelope, EnvelopeError, SerializedEnvelope};
pub use error::{ErrorSummary, MAX_ERROR_SUMMARY_BYTES};
pub use failure::{Classify, FailureKind};
pub use headers::{
    FrameworkHeader, HeaderName, HeaderNameError, HeaderValue, HeaderValueError, Headers,
};
pub use ids::{ConversationId, MessageId, RequestId};
pub use mapper::EnvelopeMapper;
pub use message::{ContentType, Message, MessageType, MetadataValue, OrderingKey, ValidationError};
pub use metadata::{
    CorrelationMetadata, DeliveryMetadata, Metadata, RoutingMetadata, TraceMetadata,
};
pub use publisher::Publisher;
pub use serializer::Serializer;
#[cfg(feature = "json")]
pub use serializer::{JsonSerializer, JsonSerializerError};
