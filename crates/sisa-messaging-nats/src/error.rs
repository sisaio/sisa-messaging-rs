//! Redacted NATS provider and wire mapping errors.

use sisa_messaging::{ErrorClassifier, FailureKind};
use std::fmt;

/// Bounded mapping error category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MappingError {
    /// Subject is empty, too long, or outside the concrete outbound grammar.
    InvalidSubject,

    /// Logical envelope fields cannot be projected to NATS wire form.
    InvalidEnvelope,

    /// Framework or custom headers are missing, duplicated, or invalid.
    InvalidHeaders,
}

impl fmt::Display for MappingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NATS wire mapping failed")
    }
}

impl std::error::Error for MappingError {}
impl ErrorClassifier for MappingError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// Bounded provider error category. SDK errors are deliberately not exposed in the error chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NatsError {
    /// Invalid local settings.
    Settings,

    /// Invalid outbound wire data.
    Mapping,

    /// Outbound frame exceeds the current negotiated limit.
    PayloadTooLarge,

    /// Broker publication failed or its outcome is unknown.
    Publish,

    /// Operation deadline elapsed; broker outcome may be unknown.
    Timeout,

    /// Opening or reading a source failed.
    Source,

    /// Settlement failed; broker outcome may be unknown.
    Settlement,
}

impl fmt::Display for NatsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NATS provider operation failed")
    }
}

impl std::error::Error for NatsError {}
impl ErrorClassifier for NatsError {
    fn classify(&self) -> FailureKind {
        match self {
            Self::Settings | Self::Mapping | Self::PayloadTooLarge => FailureKind::Permanent,
            Self::Publish | Self::Timeout | Self::Source | Self::Settlement => {
                FailureKind::Transient
            }
        }
    }
}
