//! The PostgreSQL persistence of Sisa Messaging, on sqlx: an outbox written in the application's
//! own transaction and dispatched by a worker, and an inbox that records each consumed message so
//! a redelivery is skipped.
//!
//! # Tables
//!
//! The tables come from the SQL migrations in the repository's
//! [`migrations/postgresql`](https://github.com/sisaio/sisa-messaging-rs/tree/main/migrations/postgresql)
//! folder, which the application runs itself, for example in its deploy pipeline:
//!
//! ```sh
//! DATABASE_URL='postgres://app@db/orders?options=-c%20search_path%3Dmessaging' \
//!     sqlx migrate run --source migrations/postgresql
//! ```
//!
//! or with psql. The SQL names no schema, so the connection's `search_path` picks it, both for the
//! migrations and for this crate's queries. A schema of its own, such as `messaging`, also keeps
//! sqlx's `_sqlx_migrations` table for these migrations apart from the application's.
//!
//! The outbox and inbox land with the first milestone.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]
