//! Bounded dead-receipt operator capability.

use std::error::Error;
use std::future::Future;
use std::num::NonZeroU32;
use std::time::SystemTime;

use sisa_messaging::{ErrorClassifier, ErrorSummary, MessageId, MessageType, Metadata};

use crate::{DeadReason, InboxId, InboxScope};

/// Maximum identities accepted by one dead-letter mutation.
pub const MAX_DEAD_LETTER_BATCH_SIZE: usize = 1_024;

/// Invalid dead-letter mutation batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum DeadLetterBatchError {
    /// At least one receipt identity is required.
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

/// Borrowed, validated receipt identities for one bounded dead-letter mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeadLetterBatch<'a> {
    ids: &'a [InboxId],
}

impl<'a> DeadLetterBatch<'a> {
    /// Validates a borrowed batch before a provider operation can start.
    pub fn new(ids: &'a [InboxId]) -> Result<Self, DeadLetterBatchError> {
        if ids.is_empty() {
            return Err(DeadLetterBatchError::Empty);
        }

        if ids.len() > MAX_DEAD_LETTER_BATCH_SIZE {
            return Err(DeadLetterBatchError::TooLarge);
        }

        Ok(Self { ids })
    }

    /// Returns the original identities without allocation or normalization.
    #[must_use]
    pub fn ids(&self) -> &'a [InboxId] {
        self.ids
    }
}

/// Stable exclusive keyset cursor ordered by death time then receipt identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeadLetterCursor {
    /// Database death timestamp of the last returned receipt.
    pub dead_at: SystemTime,

    /// Durable tiebreaker of the last returned receipt.
    pub id: InboxId,
}

/// One bounded dead-letter inspection request with no scope filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeadLetterQuery {
    /// Exclusive death-time-and-ID cursor.
    pub after: Option<DeadLetterCursor>,

    /// Maximum records returned.
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

/// One dead receipt returned for explicit operator inspection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetterRecord {
    /// Provider-minted durable receipt identity.
    pub id: InboxId,

    /// Consumer scope participating in the deduplication key.
    pub scope: InboxScope,

    /// Stable logical message identity.
    pub message_id: MessageId,

    /// Stable message contract identity.
    pub message_type: MessageType,

    /// Stable message contract version.
    pub version: u32,

    /// Persisted metadata, or `None` when provider decoding found poisoned metadata.
    pub metadata: Option<Metadata>,

    /// Recorded failures when this receipt became terminal.
    pub attempts: u32,

    /// Database time of the first persisted observation.
    pub received_at: SystemTime,

    /// Database time at which the receipt became terminal.
    pub dead_at: SystemTime,

    /// Stable terminal category.
    pub reason: DeadReason,

    /// Last caller-reviewed bounded failure summary, when available.
    pub last_error: Option<ErrorSummary>,
}

/// Explicit bounded inspection and mutation operations for dead inbox receipts.
pub trait InboxDeadLetters: Send + Sync + 'static {
    /// Provider error with structured retry classification and safe rendering.
    type Error: Error + ErrorClassifier + Send + Sync + 'static;

    /// Lists one stable, exclusive-cursor page across all consumer scopes.
    fn list(
        &self,
        query: DeadLetterQuery,
    ) -> impl Future<Output = Result<Vec<DeadLetterRecord>, Self::Error>> + Send;

    /// Resurrects dead receipts and returns exactly the identities confirmed by the provider.
    ///
    /// Confirmed rows retain their identity while clearing `dead_at`, `dead_reason`, `last_error`,
    /// and recorded attempts so the next failure starts a fresh retry budget.
    fn retry(
        &self,
        batch: DeadLetterBatch<'_>,
    ) -> impl Future<Output = Result<Vec<InboxId>, Self::Error>> + Send;

    /// Deletes only receipt identities found in dead state and returns exact confirmed matches.
    fn delete(
        &self,
        batch: DeadLetterBatch<'_>,
    ) -> impl Future<Output = Result<Vec<InboxId>, Self::Error>> + Send;
}
