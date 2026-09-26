//! Cloneable handle to an application-configured Kafka producer.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, Producer};

use crate::{
    KafkaClientError, KafkaClientErrorKind, KafkaClientSettings, KafkaConsumerSettings,
    KafkaDeliverySource,
};

/// Broker and advanced properties retained so consumer sources inherit auth and TLS settings.
pub(crate) struct BaseConfig {
    pub(crate) bootstrap_servers: String,

    pub(crate) advanced_properties: BTreeMap<String, String>,
}

/// Cloneable handle to an application-configured Kafka producer.
///
/// The handle also retains the application's broker and advanced properties, which
/// [`KafkaClient::delivery_source`] applies to each partitioned consumer source.
#[derive(Clone)]
pub struct KafkaClient {
    producer: FutureProducer,

    base: Arc<BaseConfig>,
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

        let base = BaseConfig {
            bootstrap_servers: settings.bootstrap_brokers.join(","),
            advanced_properties: settings.advanced_properties,
        };

        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", &base.bootstrap_servers)
            .set("acks", settings.acks.as_str());

        for (name, value) in &base.advanced_properties {
            client_config.set(name, value);
        }

        let producer = client_config
            .create()
            .map_err(|_| KafkaClientError::new(KafkaClientErrorKind::ProducerInitialization))?;

        Ok(Self {
            producer,
            base: Arc::new(base),
        })
    }

    /// Prepares a partitioned delivery source for one static consumer-group member.
    ///
    /// The source inherits this client's brokers and advanced properties and forces the
    /// properties its fencing and at-least-once delivery depend on: `group.id`,
    /// `group.instance.id`, and a stable `transactional.id`, `isolation.level=read_committed`,
    /// `auto.offset.reset=earliest`, the eager `range` assignor over the `classic` group
    /// protocol, disabled automatic commits and offset stores, `allow.auto.create.topics=false`,
    /// and an idempotent `acks=all` offset-commit producer. A group without a committed offset
    /// therefore starts at the earliest retained record; an application that wants a new group
    /// to start at the log end commits that position for the group before the source opens.
    /// An advanced property that names an identity property, or sets a forced property to a
    /// different value, returns [`KafkaClientErrorKind::TypedPropertyOverride`]. Unless an
    /// advanced property sets them, the consumer uses `fetch.wait.max.ms=10` and the offset-commit
    /// producer `retry.backoff.ms=10`, because each record pauses and later resumes its partition.
    ///
    /// This performs no I/O; the consumer and producer are created when the source opens.
    pub fn delivery_source(
        &self,
        settings: KafkaConsumerSettings,
    ) -> Result<KafkaDeliverySource, KafkaClientError> {
        KafkaDeliverySource::new(&self.base, settings)
    }

    /// Returns the number of records currently queued or in flight in the local producer.
    #[must_use]
    pub fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }

    pub(crate) fn producer(&self) -> &FutureProducer {
        &self.producer
    }
}

impl fmt::Debug for KafkaClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaClient")
            .field("producer", &"<redacted>")
            .field("bootstrap_servers", &"<redacted>")
            .field(
                "advanced_property_count",
                &self.base.advanced_properties.len(),
            )
            .finish()
    }
}
