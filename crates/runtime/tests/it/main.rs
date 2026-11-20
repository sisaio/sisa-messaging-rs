//! The bus with its endpoints, on the in-memory transport.

#![expect(
    clippy::unwrap_used,
    reason = "tests and their helpers unwrap: a failure should panic"
)]

mod codec;
mod partitions;
mod publish;
mod receive;
mod support;
mod workers;
