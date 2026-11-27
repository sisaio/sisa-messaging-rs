//! The PostgreSQL persistence of Sisa Messaging, on sqlx: an outbox written in the application's
//! own transaction and dispatched by a worker, and an inbox that records each consumed message so
//! a redelivery is skipped.
//!
//! # Tables
//!
//! The tables come from the SQL migrations in this crate's
//! [`migrations`](https://github.com/sisaio/sisa-messaging-rs/tree/main/crates/postgres/migrations)
//! folder, which the application runs itself, for example in its deploy pipeline, after creating
//! the schema once with `CREATE SCHEMA messaging`:
//!
//! ```sh
//! DATABASE_URL='postgres://app@db/orders?options=-c%20search_path%3Dmessaging' \
//!     sqlx migrate run --source crates/postgres/migrations
//! ```
//!
//! or from Rust with `migrator()` (feature `migrate`), or with psql. Pick one way per schema: a psql run leaves no row in `_sqlx_migrations`, so a
//! later `sqlx migrate run` would run the migration again and fail. The SQL names no schema, so the
//! connection's `search_path` picks it, both for the migrations and for this crate's queries. A
//! schema of its own, such as `messaging`, also keeps sqlx's `_sqlx_migrations` table for these
//! migrations apart from the application's.
//!
//! # Partitions
//!
//! Both tables are partitioned by time. [`PostgresPartitions`] lets the bus's maintenance worker
//! create partitions ahead and expire old ones.

mod inbox;
#[cfg(feature = "migrate")]
mod migrator;
mod outbox;
mod partitions;

pub use inbox::PostgresInbox;
#[cfg(feature = "migrate")]
pub use migrator::migrator;
pub use outbox::PostgresOutbox;
pub use partitions::PostgresPartitions;
