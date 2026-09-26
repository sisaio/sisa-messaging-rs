//! Redis Streams publication and individual delivery for RESP-compatible servers.
//!
//! Applications own the clients, independent read and command connections, stream and group
//! provisioning, credentials, TLS, and source supervision. Each live source needs a distinct
//! consumer name. A successful `XADD` confirms an append and returns its stream ID; a timeout
//! may still have appended, so retries may create duplicates. A delivery must be acknowledged
//! only after the application transaction commits.
//!
//! Compose [`RedisDeliverySource`] and [`RedisMapper`] directly with the typed consumer runtime.
//! The application provisions the stream and group, chooses a stable inbox scope, and creates
//! independent read and command connections. Redis supports pending recovery, but neither
//! delayed negative acknowledgement nor terminal discard. For example, after constructing an
//! inbox store and a handler for `OrderCreated`:
//!
//! ```rust,ignore
//! use sisa_messaging_consumer::{Consumer, ConsumerSettings, SettlementMode};
//! use sisa_messaging_redis::{RedisDeliverySource, RedisMapper, SourceSettings};
//! use sisa_messaging::JsonSerializer;
//! use sisa_messaging_inbox::InboxScope;
//!
//! let source = RedisDeliverySource::new(
//!     read_connection, command_connection, stream, group, consumer_name,
//!     SourceSettings::default(),
//! )?;
//! let mut settings = ConsumerSettings::default();
//! settings.mode = SettlementMode::PendingRecovery;
//! let consumer = Consumer::<OrderCreated, _>::new(
//!     source, RedisMapper, JsonSerializer, inbox,
//!     InboxScope::new("orders-projection")?, handler, settings,
//! )?;
//! consumer.run(cancel).await?;
//! ```
//!
//! On failure, the runtime leaves the entry pending for a later bounded reclaim. A malformed
//! entry or durable dead result stops the consumer for operator action; it is not acknowledged.
#![forbid(unsafe_code)]

mod error;
mod mapper;
mod publisher;
mod source;

pub use error::{RedisError, RedisMappingError};
pub use mapper::{RedisMapper, RedisWire};
pub use publisher::RedisPublisher;
pub use source::{RedisDelivery, RedisDeliverySource, RedisSettlement, SourceSettings};
