//! Redacted, structured Kafka provider errors.

use std::error::Error;
use std::fmt;

use sisa_messaging::{ErrorClassifier, FailureKind};

/// A redacted reason why Kafka client configuration could not be applied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum KafkaClientErrorKind {
    /// No bootstrap broker was configured.
    EmptyBootstrapBrokers,

    /// An advanced property attempted to replace a typed setting.
    TypedPropertyOverride,

    /// An advanced property name was empty.
    EmptyPropertyName,

    /// librdkafka could not initialize the producer from the supplied configuration.
    ProducerInitialization,
}

/// A redacted Kafka client construction error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaClientError {
    kind: KafkaClientErrorKind,
}

impl KafkaClientError {
    pub(crate) const fn new(kind: KafkaClientErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the structured client construction failure.
    #[must_use]
    pub const fn kind(self) -> KafkaClientErrorKind {
        self.kind
    }
}

impl fmt::Display for KafkaClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            KafkaClientErrorKind::EmptyBootstrapBrokers => {
                "Kafka bootstrap brokers must be configured"
            }
            KafkaClientErrorKind::TypedPropertyOverride => {
                "Kafka advanced property conflicts with a typed setting"
            }
            KafkaClientErrorKind::EmptyPropertyName => {
                "Kafka advanced property name must not be empty"
            }
            KafkaClientErrorKind::ProducerInitialization => "Kafka producer initialization failed",
        })
    }
}

impl Error for KafkaClientError {}

/// A malformed or unrepresentable Kafka envelope projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum KafkaMappingError {
    /// A required framework header was absent.
    MissingRequiredHeader,

    /// A header name or value was malformed or outside shared bounds.
    InvalidHeader,

    /// Two wire headers mapped to the same case-insensitive name.
    DuplicateHeader,

    /// A framework header had an invalid value.
    InvalidFrameworkValue,

    /// The Kafka record key did not match the shared ordering key.
    InvalidRecordKey,

    /// The wire record exceeded shared header bounds.
    HeaderBoundsExceeded,
}

impl fmt::Display for KafkaMappingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingRequiredHeader => "Kafka record is missing a required framework header",
            Self::InvalidHeader => "Kafka record contains an invalid header",
            Self::DuplicateHeader => "Kafka record contains duplicate headers",
            Self::InvalidFrameworkValue => "Kafka record contains an invalid framework value",
            Self::InvalidRecordKey => "Kafka record key does not match its ordering-key header",
            Self::HeaderBoundsExceeded => "Kafka record headers exceed shared envelope bounds",
        })
    }
}

impl Error for KafkaMappingError {}

impl ErrorClassifier for KafkaMappingError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// The envelope did not contain a routing destination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutingDestinationError;

impl fmt::Display for RoutingDestinationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Kafka destination is missing from routing metadata")
    }
}

impl Error for RoutingDestinationError {}

impl ErrorClassifier for RoutingDestinationError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// Safe, structured class of a Kafka publication failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum KafkaPublishErrorKind {
    /// The application topic resolver rejected the envelope.
    TopicResolution,

    /// The envelope could not be projected into a Kafka record.
    Mapping,

    /// The local producer queue did not accept the record.
    Enqueue,

    /// The configured producer reported unsuccessful delivery.
    Delivery,
}

/// A redacted Kafka publication failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaPublishError {
    kind: KafkaPublishErrorKind,

    failure_kind: FailureKind,
}

impl KafkaPublishError {
    /// Returns the structured operation stage that failed.
    #[must_use]
    pub const fn kind(self) -> KafkaPublishErrorKind {
        self.kind
    }
}

impl fmt::Display for KafkaPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            KafkaPublishErrorKind::TopicResolution => "Kafka topic resolution failed",
            KafkaPublishErrorKind::Mapping => "Kafka envelope mapping failed",
            KafkaPublishErrorKind::Enqueue => "Kafka producer queue rejected the record",
            KafkaPublishErrorKind::Delivery => "Kafka producer reported an unsuccessful delivery",
        })
    }
}

impl Error for KafkaPublishError {}

impl ErrorClassifier for KafkaPublishError {
    fn classify(&self) -> FailureKind {
        self.failure_kind
    }
}
