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
//!
//! [`KafkaClient::delivery_source`] composes a static consumer-group member with the generic
//! partitioned consumer. Offsets advance only through a transactional producer's
//! `send_offsets_to_transaction`, bound to the consumer-group generation captured when each
//! assignment arrives, so a member that lost its partitions cannot advance them. Records are
//! read with `read_committed` isolation; each partition has at most one record outstanding and
//! resumes at its exact next position, so accepted offsets never skip a record.

#![forbid(unsafe_code)]

mod client;
mod error;
mod mapper;
mod publisher;
mod settings;
mod source;

pub use client::KafkaClient;
pub use error::{
    KafkaClientError, KafkaClientErrorKind, KafkaMappingError, KafkaPublishError,
    KafkaPublishErrorKind, KafkaSettlementError, KafkaSettlementErrorKind, KafkaSourceError,
    KafkaSourceErrorKind, RoutingDestinationError,
};
pub use mapper::{KafkaEnvelopeMapper, KafkaHeader, KafkaRecord};
pub use publisher::{KafkaPublisher, KafkaTopicResolver, RoutingDestinationResolver};
pub use settings::{KafkaAcks, KafkaClientSettings, KafkaConsumerSettings, KafkaPublisherSettings};
pub use source::{
    KafkaDelivery, KafkaDeliverySource, KafkaPartition, KafkaSettlement, KafkaShutdownOutcome,
    KafkaSourceShutdown,
};
