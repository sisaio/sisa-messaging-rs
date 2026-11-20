//! The NATS transport against a NATS server with JetStream in Docker.

#![expect(
    clippy::unwrap_used,
    reason = "tests and their helpers unwrap: a failure should panic"
)]

mod common;
mod consume;
mod publish;
