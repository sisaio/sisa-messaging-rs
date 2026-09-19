//! Transactional inbox and unit-of-work contracts.
//!
//! This crate will own portable claim, completion, failure, maintenance, dead-letter,
//! and delegated transaction capabilities without selecting a concrete database.

#![forbid(unsafe_code)]

mod claim;
mod error;
mod failure;
mod record;
mod settings;
mod store;
mod unit_of_work;

pub use claim::{ClaimedReceipt, InboxClaimOutcome};
pub use error::{InboxScopeError, InboxSettingsError};
pub use failure::{DeadReason, InboxFailure, InboxFailureOutcome};
pub use record::{InboxId, InboxRecord, InboxScope, MAX_INBOX_SCOPE_BYTES};
pub use settings::InboxSettings;
pub use store::InboxStore;
pub use unit_of_work::InboxUnitOfWork;
