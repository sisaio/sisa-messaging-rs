use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

use sisa_messaging::{ErrorClassifier, FailureKind};
use sisa_messaging_inbox::{
    DeadLetterBatch, DeadLetterQuery, DeadLetterRecord, InboxClaimOutcome, InboxDeadLetters,
    InboxFailure, InboxFailureOutcome, InboxId, InboxMaintenance, InboxPurgeReport,
    InboxPurgeRequest, InboxReceipt, InboxRecord, InboxStats, InboxStore, InboxUnitOfWork,
};

#[derive(Debug)]
pub(crate) struct SafeError;

impl fmt::Display for SafeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("safe test error")
    }
}

impl Error for SafeError {}

impl ErrorClassifier for SafeError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

pub(crate) struct CompileCapabilities;

pub(crate) struct CompileReceipt {
    id: InboxId,

    recorded_failures: u32,
}

impl InboxReceipt for CompileReceipt {
    fn id(&self) -> InboxId {
        self.id
    }

    fn recorded_failures(&self) -> u32 {
        self.recorded_failures
    }
}

impl InboxStore<()> for CompileCapabilities {
    type Error = SafeError;

    type Receipt = CompileReceipt;

    fn max_attempts(&self) -> NonZeroU32 {
        NonZeroU32::MIN
    }

    async fn claim(
        &self,
        _transaction: &mut (),
        _record: &InboxRecord,
    ) -> Result<InboxClaimOutcome<Self::Receipt>, Self::Error> {
        Ok(InboxClaimOutcome::Claimed(CompileReceipt {
            id: InboxId::from_uuid(uuid::Uuid::from_u128(1)),
            recorded_failures: 2,
        }))
    }

    async fn complete(
        &self,
        _transaction: &mut (),
        _receipt: Self::Receipt,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn fail(
        &self,
        _record: &InboxRecord,
        _failure: InboxFailure,
    ) -> Result<InboxFailureOutcome, Self::Error> {
        Ok(InboxFailureOutcome::CompletedDuplicate)
    }
}

impl InboxUnitOfWork for CompileCapabilities {
    type Transaction = ();
    type Error = SafeError;

    async fn begin(&self) -> Result<Self::Transaction, Self::Error> {
        Ok(())
    }

    async fn commit(&self, _transaction: Self::Transaction) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn rollback(&self, _transaction: Self::Transaction) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl InboxMaintenance for CompileCapabilities {
    type Error = SafeError;

    async fn purge(&self, _request: InboxPurgeRequest) -> Result<InboxPurgeReport, Self::Error> {
        Ok(InboxPurgeReport::default())
    }

    async fn stats(&self) -> Result<InboxStats, Self::Error> {
        Ok(InboxStats::default())
    }
}

impl InboxDeadLetters for CompileCapabilities {
    type Error = SafeError;

    async fn list(&self, _query: DeadLetterQuery) -> Result<Vec<DeadLetterRecord>, Self::Error> {
        Ok(Vec::new())
    }

    async fn retry(
        &self,
        batch: DeadLetterBatch<'_>,
    ) -> Result<Vec<sisa_messaging_inbox::InboxId>, Self::Error> {
        Ok(batch.ids().to_vec())
    }

    async fn delete(
        &self,
        batch: DeadLetterBatch<'_>,
    ) -> Result<Vec<sisa_messaging_inbox::InboxId>, Self::Error> {
        Ok(batch.ids().to_vec())
    }
}
