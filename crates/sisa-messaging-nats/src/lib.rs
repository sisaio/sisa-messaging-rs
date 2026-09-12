//! NATS JetStream transport provider for outbound and inbound messaging.
//!
//! This crate will map transport-independent envelopes to NATS subjects, headers,
//! publication acknowledgements, deliveries, and settlement operations.

#![forbid(unsafe_code)]
