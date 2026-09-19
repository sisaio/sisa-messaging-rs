//! PostgreSQL runtime providers for transactional outbox and inbox capabilities.
//!
//! The application owns pool construction, connection policy, migrations, and `search_path`.
//! This crate only executes static, parameterized runtime statements against the fixed tables.

#![forbid(unsafe_code)]

mod error;
mod inbox;
mod metadata;
mod outbox;

pub use error::PostgresError;
pub use inbox::{PostgresInboxReceipt, PostgresInboxStore, PostgresInboxTransaction};
pub use outbox::PostgresOutboxStore;
