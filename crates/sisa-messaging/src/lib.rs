//! Transport-independent messaging contracts.
//!
//! This crate will define envelopes, metadata, serialization, publication, delivery,
//! settlement, mapping, and failure-classification capabilities. It owns no runtime,
//! persistence, transport, configuration-loading, or telemetry-exporter policy.

#![forbid(unsafe_code)]
