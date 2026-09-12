//! PostgreSQL runtime providers for transactional outbox and inbox capabilities.
//!
//! This crate will implement the portable contracts against application-supplied
//! PostgreSQL resources. Versioned migration execution remains outside the Rust crates.

#![forbid(unsafe_code)]
