use std::collections::BTreeMap;
use std::fmt;

use crate::{KafkaClientError, KafkaClientErrorKind};

/// Kafka producer acknowledgement policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum KafkaAcks {
    /// Send without waiting for a broker acknowledgement (`acks=0`).
    None,

    /// Wait for the partition leader (`acks=1`).
    Leader,

    /// Wait for all in-sync replicas (`acks=all`).
    #[default]
    All,
}

impl KafkaAcks {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::None => "0",
            Self::Leader => "1",
            Self::All => "all",
        }
    }
}

/// Typed Kafka producer configuration.
pub struct KafkaClientSettings {
    pub(crate) bootstrap_brokers: Vec<String>,

    pub(crate) acks: KafkaAcks,

    pub(crate) advanced_properties: BTreeMap<String, String>,
}

impl KafkaClientSettings {
    /// Creates configuration for the supplied bootstrap brokers.
    ///
    /// The default acknowledgement policy is [`KafkaAcks::All`]. Broker values are redacted
    /// from `Debug` output.
    pub fn new<I, B>(bootstrap_brokers: I) -> Self
    where
        I: IntoIterator<Item = B>,
        B: Into<String>,
    {
        Self {
            bootstrap_brokers: bootstrap_brokers.into_iter().map(Into::into).collect(),
            acks: KafkaAcks::default(),
            advanced_properties: BTreeMap::new(),
        }
    }

    /// Sets the producer acknowledgement policy.
    #[must_use]
    pub const fn with_acks(mut self, acks: KafkaAcks) -> Self {
        self.acks = acks;

        self
    }

    /// Adds an advanced librdkafka property, such as auth, TLS, compression, or timeout settings.
    ///
    /// Broker and acknowledgement settings are typed and cannot be overridden. Property names
    /// and values are redacted from configuration debug output and construction errors.
    pub fn with_advanced_property(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, KafkaClientError> {
        let name = name.into().trim().to_ascii_lowercase();

        if name.is_empty() {
            return Err(KafkaClientError::new(
                KafkaClientErrorKind::EmptyPropertyName,
            ));
        }

        if matches!(
            name.as_str(),
            "acks"
                | "request.required.acks"
                | "bootstrap.servers"
                | "metadata.broker.list"
                | "delivery.report.only.error"
        ) {
            return Err(KafkaClientError::new(
                KafkaClientErrorKind::TypedPropertyOverride,
            ));
        }

        self.advanced_properties.insert(name, value.into());

        Ok(self)
    }
}

impl fmt::Debug for KafkaClientSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaClientSettings")
            .field("bootstrap_brokers", &"<redacted>")
            .field("acks", &self.acks)
            .field("advanced_property_count", &self.advanced_properties.len())
            .field("advanced_property_values", &"<redacted>")
            .finish()
    }
}
