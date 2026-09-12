//! Transactional inbox and unit-of-work contracts.
//!
//! This crate will own portable claim, completion, failure, maintenance, dead-letter,
//! and delegated transaction capabilities without selecting a concrete database.

#![forbid(unsafe_code)]
