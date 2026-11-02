//! The PostgreSQL persistence of Sisa Messaging, on sqlx: an outbox written in the application's
//! own transaction and dispatched by a worker, and an inbox that records each consumed message so
//! a redelivery is skipped.
//!
//! The outbox and inbox land with the first milestone.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]
