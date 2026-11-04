//! The engine of Sisa Messaging, the same for every transport and database: the bus and its
//! publishers, the receive pipeline with the inbox step, the outbox dispatcher, the partition
//! maintenance scheduler, retries, and an in-memory transport for tests.
//!
//! Database crates implement only the store traits from `sisa-messaging-core`; this crate holds
//! the logic around them. Applications depend on `sisa-messaging`, which re-exports it.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]

mod bus;
mod endpoint;
mod handler;
mod in_memory;
mod inbox;
mod maintenance;
mod outbox;
mod publish;
mod retry;

pub use bus::{Bus, BusBuilder, NoOutbox, StartError};
pub use endpoint::EndpointBuilder;
#[doc(hidden)]
pub use handler::HandlerFn;
pub use handler::{Handler, HandlerContext, HandlerError};
pub use in_memory::{InMemoryDelivery, InMemoryTransport};
pub use inbox::NoInbox;
#[doc(hidden)]
pub use inbox::{EndpointInbox, HandlerOutbox, InboxPipeline};
pub use maintenance::{PartitionMode, PartitionSettings};
pub use outbox::OutboxPublisher;
pub use publish::PublishError;
pub use retry::RetryPolicy;
