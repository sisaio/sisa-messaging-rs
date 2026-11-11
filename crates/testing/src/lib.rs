//! Test helpers for the Sisa Messaging crates. Not published.
//!
//! - [`Nats`]: a fresh NATS server with JetStream in Docker for one test, removed when the test
//!   ends.
//! - [`Postgres`]: the same for a PostgreSQL server, with an empty database.
//! - [`Iggy`]: the same for an Apache Iggy server.
//! - [`eventually`]: waits for something another task does.
//!
//! `NATS_IMAGE` picks the NATS server, `nats:2.15-alpine` by default, and `POSTGRES_IMAGE` the
//! database, `postgres:18-alpine` by default, and `IGGY_IMAGE` the Iggy server, `apache/iggy:0.10.0`
//! by default.

mod iggy;
mod nats;
mod postgres;
mod server;

use std::time::Duration;

pub use crate::{iggy::Iggy, nats::Nats, postgres::Postgres};

/// Polls `check` until it holds, and fails the test with `what` after five seconds.
pub async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    while !check().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");

        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
