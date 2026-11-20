//! The Redis Streams transport of Sisa Messaging, for Redis and the servers that speak its
//! protocol and its streams commands: Valkey and Dragonfly.
//!
//! [`RedisTransport`] connects with [`RedisSettings`] and appends each envelope to the stream
//! named after its prefix and message type, such as `sisa:orders.order_placed.v1`, with its
//! CloudEvents attributes in `ce_` fields and its data in the `data` field. Each endpoint is a
//! consumer group of its name on the stream of each type it handles, and each instance of the
//! endpoint reads as a consumer of its own.
//!
//! A stream entry stays pending in its group until it is acknowledged, and Redis has no
//! negative acknowledgement with a delay. So this crate builds the redelivery from the pending
//! entries: a delivery left unsettled for [`RedisSettings::ack_wait`], such as one of an instance
//! that stopped, is taken over by any instance of the endpoint, and a retry is held by its
//! instance, which keeps the entry claimed and delivers it again once its delay has passed. The
//! server counts the deliveries, so the attempt number survives a restart.
//!
//! It uses only commands that Redis, Valkey and Dragonfly all have, and is tested against the
//! newest release of each. It connects to one server, or the primary of a replicated one;
//! Redis Cluster isn't supported yet, and neither is TLS. Garnet has no consumer groups yet, so
//! it can't run this transport.

mod settings;
mod subscribe;
mod transport;

pub use crate::{settings::RedisSettings, subscribe::RedisDelivery, transport::RedisTransport};
