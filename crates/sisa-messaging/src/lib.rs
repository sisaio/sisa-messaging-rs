//! Messaging for Rust services: publish and consume messages over a transport, with an optional
//! transactional outbox and inbox in the application's database.
//!
//! No transport or persistence is on by default; turn on the ones you use:
//!
//! | Feature | Adds |
//! | --- | --- |
//! | `nats` | The NATS JetStream transport |
//! | `postgres` | The PostgreSQL outbox and inbox, on sqlx |
//!
//! Each part is also its own crate (`sisa-messaging-core`, `-nats`, `-postgres`), for an
//! application that wants only one.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]

// The core types are re-exported here by name once they land, as sisa-caching does.

#[cfg(feature = "nats")]
pub use sisa_messaging_nats as nats;

#[cfg(feature = "postgres")]
pub use sisa_messaging_postgres as postgres;
