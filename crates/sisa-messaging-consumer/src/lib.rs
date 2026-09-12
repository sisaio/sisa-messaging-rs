//! Typed, bounded inbound message processing and settlement coordination.
//!
//! This crate will own the transport-neutral receive, transactional processing, and
//! settlement state machine. Applications explicitly delegate per-delivery transactions.

#![forbid(unsafe_code)]
