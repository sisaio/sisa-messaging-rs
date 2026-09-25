//! Redis Streams publication and individual delivery for RESP-compatible servers.
//!
//! Applications own the clients, independent read and command connections, stream and group
//! provisioning, credentials, TLS, and source supervision. Each live source needs a distinct
//! consumer name. A successful `XADD` confirms an append and returns its stream ID; a timeout
//! may still have appended, so retries may create duplicates. A delivery must be acknowledged
//! only after the application transaction commits.
#![forbid(unsafe_code)]

mod error;
mod mapper;
mod publisher;
mod source;

pub use error::{RedisError, RedisMappingError};
pub use mapper::{RedisMapper, RedisWire};
pub use publisher::RedisPublisher;
pub use source::{RedisDelivery, RedisDeliverySource, RedisSettlement, SourceSettings};
