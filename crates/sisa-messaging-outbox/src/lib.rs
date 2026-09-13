//! Transport-independent transactional outbox contracts and dispatch coordination.
//!
//! Applications enqueue typed envelopes through [`OutboxEnqueue`] inside their own transaction.
//! [`OutboxDispatcher`] later claims records, publishes outside store operations, and persists each
//! acknowledged or classified outcome with claim-token fencing. Publication is at least once:
//! broker acknowledgement ambiguity, process failure, lease expiry, and shutdown can duplicate a
//! message, so consumers remain responsible for idempotency.

#![forbid(unsafe_code)]

mod dead_letters;
mod dispatcher;
mod enqueue;
mod error;
mod maintenance;
mod retry;
mod settings;
mod store;
mod telemetry;

pub use dead_letters::{
    DeadLetterBatch, DeadLetterBatchError, DeadLetterCursor, DeadLetterQuery, DeadLetterRecord,
    MAX_DEAD_LETTER_BATCH_SIZE, OutboxDeadLetters,
};
pub use dispatcher::{OutboxDispatcher, OutboxRunReport};
pub use enqueue::{EnqueueOptions, OutboxEnqueue};
pub use error::{DispatcherError, SettingsError};
pub use maintenance::{OutboxMaintenance, OutboxPurgeReport, OutboxPurgeRequest, OutboxStats};
pub use retry::{ExponentialBackoff, RetryPolicy, RetryPolicyError};
pub use settings::DispatcherSettings;
pub use store::{
    Claim, ClaimBatch, ClaimRequest, ClaimToken, ClaimedRecord, DeadReason, FailureAction,
    FailureRecord, FencedClaims, OutboxId, OutboxStore, PoisonReport,
};
