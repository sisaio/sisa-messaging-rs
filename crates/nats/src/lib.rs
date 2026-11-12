//! The NATS transport of Sisa Eventing: publishes envelopes to JetStream subjects and consumes
//! them through durable consumers, with JetStream's acknowledgements and redelivery.
//!
//! [`NatsTransport`] connects with [`NatsSettings`], creates or updates the stream that captures
//! every subject under the prefix, and publishes each envelope to `{prefix}.{message type}` with
//! its CloudEvents attributes in `ce-` headers.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]

mod binding;
mod settings;
mod subscribe;
mod transport;

pub use crate::{settings::NatsSettings, subscribe::NatsDelivery, transport::NatsTransport};
