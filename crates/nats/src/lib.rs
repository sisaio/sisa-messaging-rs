//! The NATS transport of Sisa Messaging: publishes envelopes to JetStream subjects and consumes
//! them through durable consumers, with JetStream's acknowledgements and redelivery.
//!
//! The transport lands with the first milestone.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]
