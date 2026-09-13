use std::error::Error;
use std::fmt;
use std::time::Duration;

use sisa_messaging::{
    ContentType, Envelope, ErrorClassifier, FailureKind, Message, MessageId, Metadata,
    SerializedEnvelope, Serializer,
};
use sisa_messaging_outbox::{
    Claim, ClaimBatch, ClaimRequest, DeadLetterBatch, DeadLetterQuery, DeadLetterRecord,
    EnqueueOptions, FailureRecord, FencedClaims, OutboxDeadLetters, OutboxEnqueue, OutboxId,
    OutboxMaintenance, OutboxPurgeReport, OutboxPurgeRequest, OutboxStats, OutboxStore,
};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct TestMessage;

impl Message for TestMessage {
    const TYPE: &'static str = "test.message";
    const VERSION: u32 = 1;
}

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

pub(crate) struct TestSerializer;

impl Serializer<TestMessage> for TestSerializer {
    type Error = SafeError;

    fn serialize(
        &self,
        envelope: &Envelope<TestMessage>,
    ) -> Result<SerializedEnvelope, Self::Error> {
        Ok(SerializedEnvelope {
            message_id: envelope.message_id(),
            message_type: envelope.message_type().clone(),
            message_version: envelope.message_version(),
            content_type: ContentType::new("application/test").map_err(|_| SafeError)?,
            payload: Vec::new(),
            metadata: envelope.metadata().clone(),
            ordering_key: None,
        })
    }

    fn deserialize(
        &self,
        _envelope: SerializedEnvelope,
    ) -> Result<Envelope<TestMessage>, Self::Error> {
        Envelope::new(MessageId::new(), TestMessage, Metadata::default()).map_err(|_| SafeError)
    }
}

pub(crate) struct CompileCapabilities;

impl OutboxEnqueue<()> for CompileCapabilities {
    type Error = SafeError;
    type Serializer = TestSerializer;

    async fn enqueue<M>(
        &self,
        _transaction: &mut (),
        _envelope: &Envelope<M>,
        _options: EnqueueOptions,
    ) -> Result<OutboxId, Self::Error>
    where
        M: Message,
        Self::Serializer: Serializer<M>,
    {
        Ok(OutboxId::from_uuid(Uuid::from_u128(1)))
    }
}

impl OutboxStore for CompileCapabilities {
    type Error = SafeError;

    async fn claim(&self, _request: ClaimRequest) -> Result<ClaimBatch, Self::Error> {
        Ok(ClaimBatch::default())
    }

    async fn complete(&self, _claims: &[Claim]) -> Result<FencedClaims, Self::Error> {
        Ok(FencedClaims::default())
    }

    async fn fail(&self, _failures: &[FailureRecord]) -> Result<FencedClaims, Self::Error> {
        Ok(FencedClaims::default())
    }

    async fn release(&self, _claims: &[Claim]) -> Result<FencedClaims, Self::Error> {
        Ok(FencedClaims::default())
    }

    async fn extend_lease(
        &self,
        _claims: &[Claim],
        _lease: Duration,
    ) -> Result<FencedClaims, Self::Error> {
        Ok(FencedClaims::default())
    }
}

impl OutboxMaintenance for CompileCapabilities {
    type Error = SafeError;

    async fn purge(&self, _request: OutboxPurgeRequest) -> Result<OutboxPurgeReport, Self::Error> {
        Ok(OutboxPurgeReport::default())
    }

    async fn stats(&self) -> Result<OutboxStats, Self::Error> {
        Ok(OutboxStats::default())
    }
}

impl OutboxDeadLetters for CompileCapabilities {
    type Error = SafeError;

    async fn list(&self, _query: DeadLetterQuery) -> Result<Vec<DeadLetterRecord>, Self::Error> {
        Ok(Vec::new())
    }

    async fn retry(&self, batch: DeadLetterBatch<'_>) -> Result<Vec<OutboxId>, Self::Error> {
        let mut confirmed = batch.ids().to_vec();
        confirmed.sort_unstable();
        confirmed.dedup();
        Ok(confirmed)
    }

    async fn delete(&self, batch: DeadLetterBatch<'_>) -> Result<Vec<OutboxId>, Self::Error> {
        self.retry(batch).await
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CompilePublisher;

impl sisa_messaging::Publisher for CompilePublisher {
    type Error = SafeError;

    async fn publish(&self, _envelope: &SerializedEnvelope) -> Result<(), Self::Error> {
        Ok(())
    }
}
