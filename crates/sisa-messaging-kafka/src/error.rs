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

    /// A consumer group identifier was empty.
    EmptyGroupId,

    /// A static group instance identifier was empty.
    EmptyGroupInstanceId,

    /// No topic, or an empty topic name, was configured for a consumer.
    EmptyTopic,

    /// A consumer operation or shutdown timeout was zero.
    ZeroTimeout,
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
            KafkaClientErrorKind::EmptyGroupId => {
                "Kafka consumer group identifier must not be empty"
            }
            KafkaClientErrorKind::EmptyGroupInstanceId => {
                "Kafka static group instance identifier must not be empty"
            }
            KafkaClientErrorKind::EmptyTopic => "Kafka consumer topics must be non-empty",
            KafkaClientErrorKind::ZeroTimeout => "Kafka consumer timeouts must be positive",
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
    pub(crate) fn new(kind: KafkaPublishErrorKind, failure_kind: FailureKind) -> Self {
        Self { kind, failure_kind }
    }

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

/// Safe, structured class of a Kafka partitioned-source failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum KafkaSourceErrorKind {
    /// The source was opened more than once.
    AlreadyOpened,

    /// librdkafka could not create the consumer, producer, or member thread.
    Initialization,

    /// The transactional offset-commit producer could not be initialized.
    TransactionInitialization,

    /// A configured topic does not exist; the source never creates topics.
    MissingTopic,

    /// Topic metadata could not be read.
    Metadata,

    /// The consumer could not subscribe to its topics.
    Subscription,

    /// Another live member with the same static instance identity fenced this source.
    InstanceFenced,

    /// The broker rejected the member's group, topic, or transactional authorization.
    Authorization,

    /// The consumer reported a fatal error.
    Fatal,

    /// The consumer reported a rebalance error.
    Rebalance,

    /// The consumer-group metadata for a new assignment was unavailable.
    GroupMetadataUnavailable,

    /// A partition could not be paused, sought, or resumed safely.
    PartitionControl,

    /// The committed cursor of an indeterminate advance could not be established; the
    /// partition stays paused.
    Reconciliation,

    /// The member thread stopped.
    MemberStopped,
}

/// A redacted Kafka partitioned-source failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaSourceError {
    kind: KafkaSourceErrorKind,

    failure_kind: FailureKind,
}

impl KafkaSourceError {
    pub(crate) const fn new(kind: KafkaSourceErrorKind, failure_kind: FailureKind) -> Self {
        Self { kind, failure_kind }
    }

    /// Returns the structured failure class.
    #[must_use]
    pub const fn kind(self) -> KafkaSourceErrorKind {
        self.kind
    }
}

impl fmt::Display for KafkaSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            KafkaSourceErrorKind::AlreadyOpened => "Kafka source was already opened",
            KafkaSourceErrorKind::Initialization => "Kafka source initialization failed",
            KafkaSourceErrorKind::TransactionInitialization => {
                "Kafka offset-commit producer initialization failed"
            }
            KafkaSourceErrorKind::MissingTopic => "Kafka source topic does not exist",
            KafkaSourceErrorKind::Metadata => "Kafka topic metadata could not be read",
            KafkaSourceErrorKind::Subscription => "Kafka source subscription failed",
            KafkaSourceErrorKind::InstanceFenced => {
                "Kafka source was fenced by another member with the same instance identity"
            }
            KafkaSourceErrorKind::Authorization => "Kafka source authorization failed",
            KafkaSourceErrorKind::Fatal => "Kafka consumer reported a fatal error",
            KafkaSourceErrorKind::Rebalance => "Kafka consumer rebalance failed",
            KafkaSourceErrorKind::GroupMetadataUnavailable => {
                "Kafka consumer-group metadata was unavailable for an assignment"
            }
            KafkaSourceErrorKind::PartitionControl => "Kafka partition control failed",
            KafkaSourceErrorKind::Reconciliation => {
                "Kafka committed cursor of an indeterminate advance could not be established"
            }
            KafkaSourceErrorKind::MemberStopped => "Kafka source member thread stopped",
        })
    }
}

impl Error for KafkaSourceError {}

impl ErrorClassifier for KafkaSourceError {
    fn classify(&self) -> FailureKind {
        self.failure_kind
    }
}

/// Safe, structured class of a Kafka partition advance failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum KafkaSettlementErrorKind {
    /// The advance outcome is unknown; the source pauses and reconciles the partition.
    Indeterminate,

    /// The broker rejected the transactional offset commit's authorization.
    Authorization,

    /// The member thread stopped before the advance resolved.
    MemberStopped,
}

/// A redacted Kafka partition advance failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaSettlementError {
    kind: KafkaSettlementErrorKind,

    failure_kind: FailureKind,
}

impl KafkaSettlementError {
    pub(crate) const fn new(kind: KafkaSettlementErrorKind, failure_kind: FailureKind) -> Self {
        Self { kind, failure_kind }
    }

    /// Returns the structured failure class.
    #[must_use]
    pub const fn kind(self) -> KafkaSettlementErrorKind {
        self.kind
    }
}

impl fmt::Display for KafkaSettlementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            KafkaSettlementErrorKind::Indeterminate => "Kafka partition advance is indeterminate",
            KafkaSettlementErrorKind::Authorization => {
                "Kafka transactional offset commit was not authorized"
            }
            KafkaSettlementErrorKind::MemberStopped => {
                "Kafka source member thread stopped before the advance resolved"
            }
        })
    }
}

impl Error for KafkaSettlementError {}

impl ErrorClassifier for KafkaSettlementError {
    fn classify(&self) -> FailureKind {
        self.failure_kind
    }
}
