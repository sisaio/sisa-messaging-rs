//! Bounded dead-letter operator capability.

use std::error::Error;
use std::future::Future;
use std::num::NonZeroU32;
use std::time::SystemTime;

use sisa_messaging::{ErrorClassifier, ErrorSummary, MessageId, SerializedEnvelope};

use crate::{DeadReason, OutboxId};

/// Maximum number of dead-letter identities accepted by one mutation.
pub const MAX_DEAD_LETTER_BATCH_SIZE: usize = 1_024;

/// Invalid dead-letter mutation batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum DeadLetterBatchError {
    /// At least one durable identity is required.
    #[error("dead-letter batch must not be empty")]
    Empty,

    /// The request exceeded [`MAX_DEAD_LETTER_BATCH_SIZE`].
    #[error("dead-letter batch exceeds the maximum size")]
    TooLarge,
}

impl ErrorClassifier for DeadLetterBatchError {
    fn classify(&self) -> sisa_messaging::FailureKind {
        sisa_messaging::FailureKind::Permanent
    }
}

/// Borrowed, validated set of identities for one dead-letter mutation.
///
/// Construction performs no allocation. Duplicate identities are permitted and count toward
/// [`MAX_DEAD_LETTER_BATCH_SIZE`]. Providers process the batch in one bounded pass and return a
/// distinct confirmed subset; the confirmed ordering is unspecified.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeadLetterBatch<'a> {
    ids: &'a [OutboxId],
}

impl<'a> DeadLetterBatch<'a> {
    /// Validates a borrowed mutation batch before any provider operation can start.
    pub fn new(ids: &'a [OutboxId]) -> Result<Self, DeadLetterBatchError> {
        if ids.is_empty() {
            return Err(DeadLetterBatchError::Empty);
        }
        if ids.len() > MAX_DEAD_LETTER_BATCH_SIZE {
            return Err(DeadLetterBatchError::TooLarge);
        }
        Ok(Self { ids })
    }

    /// Returns the original borrowed identities without allocation or normalization.
    #[must_use]
    pub fn ids(&self) -> &'a [OutboxId] {
        self.ids
    }
}

/// Stable keyset cursor ordered by death time then durable row identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeadLetterCursor {
    /// Database death timestamp of the last returned row.
    pub dead_at: SystemTime,

    /// Durable tiebreaker of the last returned row.
    pub id: OutboxId,
}

/// One bounded dead-letter page request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeadLetterQuery {
    /// Exclusive timestamp-and-ID cursor.
    pub after: Option<DeadLetterCursor>,

    /// Maximum rows returned.
    pub limit: NonZeroU32,
}

impl Default for DeadLetterQuery {
    fn default() -> Self {
        Self {
            after: None,
            limit: NonZeroU32::new(100).unwrap_or(NonZeroU32::MIN),
        }
    }
}

/// Dead row returned for operator inspection.
///
/// Durable identities and terminal diagnostics remain available even when the persisted envelope
/// cannot be decoded and `envelope` is therefore absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetterRecord {
    /// Durable provider-owned row identity.
    pub id: OutboxId,

    /// Stable logical message identity retained independently of envelope decoding.
    pub message_id: MessageId,

    /// Decoded envelope when its persisted representation was readable.
    pub envelope: Option<SerializedEnvelope>,

    /// Number of publication outcomes recorded before death.
    pub attempts: u32,

    /// Database death timestamp used by keyset pagination.
    pub dead_at: SystemTime,

    /// Stable terminal reason.
    pub reason: DeadReason,

    /// Last safe bounded diagnostic summary, when available.
    pub last_error: Option<ErrorSummary>,
}

/// Explicit bounded dead-letter inspection and mutation operations.
pub trait OutboxDeadLetters: Send + Sync {
    /// Operator error with structured retry classification and safe rendering.
    type Error: Error + ErrorClassifier + Send + Sync + 'static;

    /// Lists one stable keyset page.
    fn list(
        &self,
        query: DeadLetterQuery,
    ) -> impl Future<Output = Result<Vec<DeadLetterRecord>, Self::Error>> + Send;

    /// Resurrects a caller-bounded set of dead identities and returns exact confirmed matches.
    ///
    /// Each confirmed row preserves its durable row and message identities.
    fn retry(
        &self,
        batch: DeadLetterBatch<'_>,
    ) -> impl Future<Output = Result<Vec<OutboxId>, Self::Error>> + Send;

    /// Deletes a caller-bounded set and returns the exact identities found in dead state.
    fn delete(
        &self,
        batch: DeadLetterBatch<'_>,
    ) -> impl Future<Output = Result<Vec<OutboxId>, Self::Error>> + Send;
}
