//! Transactional inbox and unit-of-work contracts.
//!
//! This crate will own portable claim, completion, failure, maintenance, dead-letter,
//! and delegated transaction capabilities without selecting a concrete database.

#![forbid(unsafe_code)]

mod claim;
mod dead_letters;
mod error;
mod failure;
mod maintenance;
mod record;
mod settings;
mod store;
mod unit_of_work;

pub use claim::{InboxClaimOutcome, InboxReceipt};
pub use dead_letters::{
    DeadLetterBatch, DeadLetterBatchError, DeadLetterCursor, DeadLetterQuery, DeadLetterRecord,
    InboxDeadLetters, MAX_DEAD_LETTER_BATCH_SIZE,
};
pub use error::{InboxScopeError, InboxSettingsError};
pub use failure::{DeadReason, InboxFailure, InboxFailureOutcome};
pub use maintenance::{InboxMaintenance, InboxPurgeReport, InboxPurgeRequest, InboxStats};
pub use record::{InboxId, InboxRecord, InboxScope, MAX_INBOX_SCOPE_BYTES};
pub use settings::InboxSettings;
pub use store::InboxStore;
pub use unit_of_work::InboxUnitOfWork;
