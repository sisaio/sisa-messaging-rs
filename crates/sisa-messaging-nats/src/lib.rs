//! NATS JetStream transport provider for outbound and inbound messaging.
//!
//! This crate maps transport-independent envelopes to JetStream publication and delivery.

#![forbid(unsafe_code)]

mod error;
mod mapper;
mod publisher;
mod settings;
mod settlement;
mod source;
mod telemetry;

pub use error::{MappingError, NatsError};
pub use mapper::{NatsMapper, NatsWire, Subject, SubjectResolver, TypeSubjectResolver};
pub use publisher::NatsPublisher;
pub use settings::NatsPublisherSettings;
pub use settlement::NatsSettlement;
pub use source::{NatsDelivery, NatsDeliverySource};
