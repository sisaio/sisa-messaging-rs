//! Transactional outbox contracts and dispatch coordination.
//!
//! This crate will own portable enqueue, store, dispatch, retry, maintenance, and
//! dead-letter capabilities. Concrete persistence and transport remain separate providers.

#![forbid(unsafe_code)]
