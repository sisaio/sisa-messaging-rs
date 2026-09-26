use std::fmt;
use std::time::Duration;

use crate::{KafkaClientError, KafkaClientErrorKind};

const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Consumer-group identity and bounds for a Kafka partitioned delivery source.
///
/// The group, static instance, and topics are validated at construction without I/O. The
/// provider derives the stable transactional identity from them (see
/// [`Self::transactional_id`]), so every instance must use a distinct static identity within its
/// group.
///
/// A group without a committed offset starts at the earliest retained record, so no record
/// published before the first assignment is skipped. An application that wants a new group to
/// start at the log end commits that position for the group before the source opens.
#[derive(Clone)]
pub struct KafkaConsumerSettings {
    pub(crate) group_id: String,

    pub(crate) group_instance_id: String,

    pub(crate) topics: Vec<String>,

    pub(crate) operation_timeout: Duration,

    pub(crate) shutdown_timeout: Duration,
}

impl KafkaConsumerSettings {
    /// Creates settings for one static member of `group_id` subscribed to `topics`.
    ///
    /// Every value must be non-empty after trimming, and at least one topic is required.
    pub fn new<I, T>(
        group_id: impl Into<String>,
        group_instance_id: impl Into<String>,
        topics: I,
    ) -> Result<Self, KafkaClientError>
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let group_id = group_id.into();
        let group_instance_id = group_instance_id.into();
        let topics: Vec<String> = topics.into_iter().map(Into::into).collect();

        if group_id.trim().is_empty() {
            return Err(KafkaClientError::new(KafkaClientErrorKind::EmptyGroupId));
        }

        if group_instance_id.trim().is_empty() {
            return Err(KafkaClientError::new(
                KafkaClientErrorKind::EmptyGroupInstanceId,
            ));
        }

        if topics.is_empty() || topics.iter().any(|topic| topic.trim().is_empty()) {
            return Err(KafkaClientError::new(KafkaClientErrorKind::EmptyTopic));
        }

        Ok(Self {
            group_id,
            group_instance_id,
            topics,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        })
    }

    /// Bounds each blocking broker operation: transaction steps, metadata, seeks, and
    /// committed-cursor reads. Reconciliation retries within three times this bound.
    ///
    /// The default is ten seconds. A zero timeout returns
    /// [`KafkaClientErrorKind::ZeroTimeout`], because every broker operation would fail at once.
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Result<Self, KafkaClientError> {
        if timeout.is_zero() {
            return Err(KafkaClientError::new(KafkaClientErrorKind::ZeroTimeout));
        }

        self.operation_timeout = timeout;

        Ok(self)
    }

    /// Bounds the consumer close performed after the source is dropped.
    ///
    /// The default is ten seconds. A zero timeout returns
    /// [`KafkaClientErrorKind::ZeroTimeout`], because the close could never complete.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Result<Self, KafkaClientError> {
        if timeout.is_zero() {
            return Err(KafkaClientError::new(KafkaClientErrorKind::ZeroTimeout));
        }

        self.shutdown_timeout = timeout;

        Ok(self)
    }

    /// Returns the stable transactional identity the source's offset-commit producer uses.
    ///
    /// The format is `sisa.{group_len}.{group_id}.{group_instance_id}`, where `group_len` is the
    /// group identifier's length in bytes, in decimal. The length prefix keeps distinct
    /// group and instance pairs distinct even when either contains dots. Operators grant
    /// transactional-identity ACLs for this value. Two live instances with the same identity
    /// fence each other.
    #[must_use]
    pub fn transactional_id(&self) -> String {
        format!(
            "sisa.{}.{}.{}",
            self.group_id.len(),
            self.group_id,
            self.group_instance_id
        )
    }
}

impl fmt::Debug for KafkaConsumerSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaConsumerSettings")
            .field("group_id", &"<redacted>")
            .field("group_instance_id", &"<redacted>")
            .field("topic_count", &self.topics.len())
            .field("operation_timeout", &self.operation_timeout)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish()
    }
}
