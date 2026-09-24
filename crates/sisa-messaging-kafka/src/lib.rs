//! Kafka transport provider for Sisa messaging contracts.
//!
//! Applications configure brokers, acknowledgements, and advanced properties through
//! [`KafkaClientSettings`]. [`KafkaClient::start`] creates the local producer handle, but does
//! not verify broker readiness. The provider maps shared envelopes into Kafka
//! records and reports success when librdkafka returns a successful delivery report under the
//! configured acknowledgement policy. `acks=0` requests no broker
//! acknowledgement and can report success without broker receipt, `acks=1` waits for the leader,
//! and `acks=all` waits for all in-sync replicas.
//! Applications that rely on broker-backed outbox durability should use `acks=all` with topic
//! replication and `min.insync.replicas` settings suited to their durability target.
//! Enable the crate's `tls` feature to use TLS properties. Advanced properties pass through to the
//! compiled rdkafka capabilities; accepting a property does not enable a protocol mechanism that
//! was not compiled into the selected build.

#![forbid(unsafe_code)]

mod client;
mod error;
mod mapper;
mod publisher;
mod settings;

pub use client::KafkaClient;
pub use error::{
    KafkaClientError, KafkaClientErrorKind, KafkaMappingError, KafkaPublishError,
    KafkaPublishErrorKind, RoutingDestinationError,
};
pub use mapper::{KafkaEnvelopeMapper, KafkaHeader, KafkaRecord};
pub use publisher::{KafkaPublisher, KafkaTopicResolver, RoutingDestinationResolver};
pub use settings::{KafkaAcks, KafkaClientSettings, KafkaPublisherSettings};
