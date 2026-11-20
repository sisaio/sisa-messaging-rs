//! The PostgreSQL stores against a PostgreSQL server in Docker.

#![expect(
    clippy::unwrap_used,
    reason = "tests and their helpers unwrap: a failure should panic"
)]

mod dispatch;
mod inbox;
mod migrate;
mod outbox;
mod partitions;
