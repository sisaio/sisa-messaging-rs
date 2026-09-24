use std::time::Duration;

/// Queueing policy for Kafka publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaPublisherSettings {
    /// How long to wait for capacity in librdkafka's local producer queue.
    ///
    /// This controls only the local queue wait. It does not set the broker delivery timeout,
    /// retries, or acknowledgement policy. Those remain producer configuration owned by the
    /// application.
    pub enqueue_timeout: Duration,
}

impl Default for KafkaPublisherSettings {
    fn default() -> Self {
        Self {
            enqueue_timeout: Duration::from_secs(5),
        }
    }
}
