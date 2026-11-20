//! The Redis transport against a Redis, Valkey or Dragonfly server in Docker.

#![expect(
    clippy::unwrap_used,
    reason = "tests and their helpers unwrap: a failure should panic"
)]

mod bus;
mod common;
mod consume;
mod publish;
