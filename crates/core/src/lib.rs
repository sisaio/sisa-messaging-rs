//! The shared parts of Sisa Messaging: messages and their envelope, codecs, and the contracts that
//! transports, outboxes and inboxes implement.
//!
//! Applications depend on `sisa-messaging`, which re-exports this crate's types, or on this crate
//! and the transport and persistence crates they use. The types land with the first milestone.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]
