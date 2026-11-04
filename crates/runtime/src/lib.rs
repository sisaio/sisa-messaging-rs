//! The engine of Sisa Messaging, the same for every transport and database: the bus and its
//! publishers, the receive pipeline with the inbox step, the outbox dispatcher, the partition
//! maintenance scheduler, retries, and an in-memory transport for tests.
//!
//! Database crates implement only the store traits from `sisa-messaging-core`; this crate holds
//! the logic around them. Applications depend on `sisa-messaging`, which re-exports it.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]

mod in_memory;

pub use in_memory::{InMemoryDelivery, InMemoryTransport};
