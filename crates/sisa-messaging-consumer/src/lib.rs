//! Typed, bounded inbound message processing and settlement coordination.
//!
//! [`Consumer`] receives individual deliveries, decodes one message type, and runs each delivery
//! through one framework-owned transaction: claim the inbox receipt, invoke the
//! [`ConsumerHandler`], complete the receipt, and commit before any acknowledgement. A handler
//! failure is rolled back before it is recorded. Every path reduces to one centralized
//! settlement decision under the explicitly selected [`SettlementMode`]:
//!
//! - [`SettlementMode::Broker`] (the default) acknowledges completed work, negatively
//!   acknowledges retryable work with a delay, and terminally discards poison or dead deliveries.
//! - [`SettlementMode::PendingRecovery`] acknowledges completed work and otherwise leaves the
//!   delivery unsettled for the source's bounded pending recovery, stopping for operator action
//!   on malformed input or durable dead results.
//!
//! Delivery is at least once: the inbox deduplicates redelivery, and acknowledgement ambiguity is
//! resolved by a later completed duplicate. [`ConsumerSettings::max_in_flight`] bounds received but
//! unsettled deliveries and open transactions; the source is not polled while every permit is
//! taken.

#![forbid(unsafe_code)]

mod consumer;
mod error;
mod handler;
mod settings;
mod telemetry;

pub use consumer::Consumer;
pub use error::{
    ConsumerConfigError, ConsumerError, ConsumerErrorKind, ConsumerExit, OperatorReason,
    SettingsField,
};
pub use handler::ConsumerHandler;
pub use settings::{ConsumerSettings, SettlementMode};
