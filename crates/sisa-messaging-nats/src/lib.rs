//! NATS JetStream transport provider for outbound and inbound messaging.
//!
//! This crate maps transport-independent envelopes to JetStream publication and delivery.
//!
//! For typed inbound processing, pass [`NatsDeliverySource`] and [`NatsMapper`] to the generic
//! `sisa-messaging-consumer` `Consumer::new`; this crate adds no consumer façade. The composition,
//! its call site, and the guarantees it provides are described in
//! [`docs/consumer-framework.md` section 4, "Application integration"][integration]; the
//! [`nats-postgres-consumer` example][example] is a runnable application with a PostgreSQL
//! inbox.
//!
//! [integration]: https://github.com/sisaio/sisa-messaging-rs/blob/main/docs/consumer-framework.md#4-application-integration
//! [example]: https://github.com/sisaio/sisa-messaging-rs/blob/main/examples/nats-postgres-consumer/src/main.rs

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
