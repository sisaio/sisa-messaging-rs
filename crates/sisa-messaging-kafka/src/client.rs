//! Cloneable handle to an application-configured Kafka producer.

use std::fmt;

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, Producer};

use crate::{KafkaClientError, KafkaClientErrorKind, KafkaClientSettings};

/// Cloneable handle to an application-configured Kafka producer.
#[derive(Clone)]
pub struct KafkaClient {
    producer: FutureProducer,
}

impl KafkaClient {
    /// Initializes a local producer handle from settings.
    ///
    /// This does not check broker availability, authentication, topic existence, or readiness.
    /// Those conditions are observed by subsequent producer operations.
    pub fn start(settings: KafkaClientSettings) -> Result<Self, KafkaClientError> {
        if settings.bootstrap_brokers.is_empty()
            || settings
                .bootstrap_brokers
                .iter()
                .any(|broker| broker.trim().is_empty())
        {
            return Err(KafkaClientError::new(
                KafkaClientErrorKind::EmptyBootstrapBrokers,
            ));
        }

        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", settings.bootstrap_brokers.join(","))
            .set("acks", settings.acks.as_str());

        for (name, value) in settings.advanced_properties {
            client_config.set(name, value);
        }

        let producer = client_config
            .create()
            .map_err(|_| KafkaClientError::new(KafkaClientErrorKind::ProducerInitialization))?;

        Ok(Self { producer })
    }

    /// Returns the number of records currently queued or in flight in the local producer.
    #[must_use]
    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }
}

impl fmt::Debug for KafkaClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaClient")
            .field("producer", &"<redacted>")
            .finish()
    }
}
