//! Redacted RabbitMQ provider and wire mapping errors.

use sisa_messaging::{ErrorClassifier, FailureKind};
use std::fmt;

/// Bounded mapping error category.
///
/// Neither `Debug` nor `Display` renders exchange names, routing keys, header names or values, or
/// payload bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MappingError {
    /// Exchange name or routing key is too long or contains a control byte.
    InvalidRoute,

    /// Logical envelope fields are missing or cannot be projected to AMQP wire form.
    InvalidEnvelope,

    /// Framework or custom headers are duplicated, not long strings, or otherwise invalid.
    InvalidHeaders,

    /// The encoded content-header frame exceeds the 4,088-byte bound that fits the AMQP minimum
    /// frame size.
    HeadersTooLarge,
}

impl fmt::Display for MappingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RabbitMQ wire mapping failed")
    }
}

impl std::error::Error for MappingError {}

impl ErrorClassifier for MappingError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// Bounded provider error category. Client errors are deliberately not exposed in the error chain.
///
/// `Settings`, `Mapping`, and `PayloadTooLarge` are permanent; every other variant is transient.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RabbitMqError {
    /// Invalid local settings, a channel without publisher confirms, or a source that was not
    /// opened.
    Settings,

    /// Invalid outbound wire data.
    Mapping,

    /// Payload exceeds the configured maximum message size; nothing was sent.
    PayloadTooLarge,

    /// The broker confirmed a mandatory publication it could not route to any queue.
    Unroutable,

    /// The broker negatively confirmed the publication, for example because a queue rejected it
    /// on overflow.
    Rejected,

    /// The client failed or the channel or connection closed; the broker outcome is unknown.
    Publish,

    /// The publish deadline elapsed; the broker outcome is unknown.
    Timeout,

    /// Opening, reading, or closing the source failed.
    Source,

    /// Settlement failed or was not confirmed; the broker outcome is unknown.
    Settlement,
}

impl fmt::Display for RabbitMqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RabbitMQ provider operation failed")
    }
}

impl std::error::Error for RabbitMqError {}

impl ErrorClassifier for RabbitMqError {
    fn classify(&self) -> FailureKind {
        match self {
            Self::Settings | Self::Mapping | Self::PayloadTooLarge => FailureKind::Permanent,
            Self::Unroutable
            | Self::Rejected
            | Self::Publish
            | Self::Timeout
            | Self::Source
            | Self::Settlement => FailureKind::Transient,
        }
    }
}
