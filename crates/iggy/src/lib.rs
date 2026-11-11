//! The Apache Iggy transport of Sisa Messaging: publishes envelopes to Iggy topics and consumes
//! them through consumer groups, committing each partition's offset as deliveries are settled.
//!
//! [`IggyTransport`] connects with [`IggySettings`], creates the stream when it is missing, and
//! publishes each envelope to the topic named after its message type, such as
//! `orders.order_placed.v1`, with its CloudEvents attributes in `ce_` user headers. Each endpoint
//! is a consumer group of its name on each topic it handles.
//!
//! Iggy is a log, like Kafka: it acknowledges a position in a partition, not one message. So this
//! crate settles a delivery by committing its offset, and retries one by holding its partition
//! and reading it again from that offset once the delay has passed. The messages behind it on the
//! same partition wait, which keeps the partition's order. The attempt is counted in memory, so
//! it starts from 1 again after a restart or when the partition moves to another instance.
//!
//! Iggy has no acknowledgement timeout either: a delivery that is held long keeps its partition
//! waiting, and the message goes to another instance only once this one leaves the group, when it
//! stops or its session times out.
//!
//! Iggy is younger than the other brokers and changes between releases: this crate follows the
//! `iggy` 0.12 client and is tested against Iggy server 0.10.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]

mod settings;
mod subscribe;
mod transport;

pub use crate::{settings::IggySettings, subscribe::IggyDelivery, transport::IggyTransport};
