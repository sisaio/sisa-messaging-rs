//! Timed complete/fail persistence and confirmed transition accounting.

pub(crate) mod accounting;

use std::time::Duration;
use tracing::Instrument;

use crate::{Claim, FailureRecord, FencedClaims, OutboxStore};

pub(crate) enum StoreCall<E> {
    Completed(FencedClaims),
    Failed(E),
    TimedOut,
}

pub(crate) async fn complete<S: OutboxStore>(
    store: &S,
    claims: &[Claim],
    timeout: Duration,
) -> StoreCall<S::Error> {
    match tokio::time::timeout(timeout, store.complete(claims))
        .instrument(tracing::debug_span!(
            target: "messaging.outbox",
            "outbox.persist_outcome",
            operation = "complete"
        ))
        .await
    {
        Ok(Ok(matches)) => StoreCall::Completed(matches),
        Ok(Err(error)) => StoreCall::Failed(error),
        Err(_) => StoreCall::TimedOut,
    }
}

pub(crate) async fn fail<S: OutboxStore>(
    store: &S,
    failures: &[FailureRecord],
    timeout: Duration,
) -> StoreCall<S::Error> {
    match tokio::time::timeout(timeout, store.fail(failures))
        .instrument(tracing::debug_span!(
            target: "messaging.outbox",
            "outbox.persist_outcome",
            operation = "fail"
        ))
        .await
    {
        Ok(Ok(matches)) => StoreCall::Completed(matches),
        Ok(Err(error)) => StoreCall::Failed(error),
        Err(_) => StoreCall::TimedOut,
    }
}

pub(crate) async fn release<S: OutboxStore>(
    store: &S,
    claims: &[Claim],
    timeout: Duration,
) -> StoreCall<S::Error> {
    match tokio::time::timeout(timeout, store.release(claims))
        .instrument(tracing::debug_span!(
            target: "messaging.outbox",
            "outbox.persist_outcome",
            operation = "release"
        ))
        .await
    {
        Ok(Ok(matches)) => StoreCall::Completed(matches),
        Ok(Err(error)) => StoreCall::Failed(error),
        Err(_) => StoreCall::TimedOut,
    }
}
