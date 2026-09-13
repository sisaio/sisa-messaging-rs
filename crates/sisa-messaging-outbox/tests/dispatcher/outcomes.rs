use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use sisa_messaging::{FailureKind, Publisher, SerializedEnvelope};
use sisa_messaging_outbox::{DeadReason, FailureAction, OutboxDispatcher};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::support::*;

#[derive(Clone)]
struct RacingPublisher {
    first: Arc<watch::Sender<bool>>,
    second: Arc<watch::Sender<bool>>,
}

impl RacingPublisher {
    fn new() -> Self {
        Self {
            first: Arc::new(watch::channel(false).0),
            second: Arc::new(watch::channel(false).0),
        }
    }

    async fn wait(sender: &watch::Sender<bool>) {
        let mut receiver = sender.subscribe();
        while !*receiver.borrow() {
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Publisher for RacingPublisher {
    type Error = ProtocolError;

    async fn publish(&self, envelope: &SerializedEnvelope) -> Result<(), Self::Error> {
        if envelope.payload.first().copied().unwrap_or_default() == 0 {
            Self::wait(&self.first).await;
        } else {
            Self::wait(&self.second).await;
        }
        Ok(())
    }
}

async fn completion_report(
    mode: u8,
    records: Vec<sisa_messaging_outbox::ClaimedRecord>,
) -> sisa_messaging_outbox::OutboxRunReport {
    let capacity = records.len();
    let store = FakeStore::new(records);
    store.complete_mode.store(mode, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(
        store.clone(),
        FakePublisher::new(PUBLISH_SUCCESS),
        settings(capacity),
    )
    .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });
    wait_for(|| !store.lock().completes.is_empty()).await;
    cancellation.cancel();
    task.await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"))
}

#[tokio::test]
async fn complete_shortfall_is_a_benign_fencing_outcome() {
    let report = completion_report(COMPLETE_FIRST, vec![record(1, 0), record(2, 0)]).await;
    assert_eq!(report.completed, 1);
    assert_eq!(report.fenced, 1);
}

#[tokio::test]
async fn empty_complete_confirmation_records_only_fencing() {
    let report = completion_report(COMPLETE_NONE, vec![record(1, 0)]).await;
    assert_eq!(report.completed, 0);
    assert_eq!(report.fenced, 1);
}

#[tokio::test]
async fn transient_permanent_and_exhausted_failures_choose_explicit_actions() {
    for (behavior, attempts, expected_kind, expected) in [
        (
            PUBLISH_TRANSIENT,
            0,
            FailureKind::Transient,
            FailureAction::Retry {
                delay: Duration::from_millis(5),
            },
        ),
        (
            PUBLISH_PERMANENT,
            0,
            FailureKind::Permanent,
            FailureAction::Dead {
                reason: DeadReason::Permanent,
            },
        ),
        (
            PUBLISH_TRANSIENT,
            2,
            FailureKind::Transient,
            FailureAction::Dead {
                reason: DeadReason::Exhausted,
            },
        ),
    ] {
        let store = FakeStore::new(vec![record(1, attempts)]);
        let cancellation = CancellationToken::new();
        let dispatcher =
            OutboxDispatcher::new(store.clone(), FakePublisher::new(behavior), settings(1))
                .unwrap_or_else(|error| panic!("settings rejected: {error}"));
        let run_cancel = cancellation.clone();
        let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });
        wait_for(|| !store.lock().failures.is_empty()).await;
        cancellation.cancel();
        task.await
            .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
            .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

        let failures = &store.lock().failures[0];
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].failure_kind, expected_kind);
        assert_eq!(failures[0].action, expected);
    }
}

#[tokio::test]
async fn ambiguous_complete_is_released_without_local_republish() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store
        .complete_mode
        .store(COMPLETE_TRANSIENT_ONCE, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_SUCCESS);
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), settings(1))
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });
    wait_for(|| !store.lock().releases.is_empty()).await;
    cancellation.cancel();
    task.await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert_eq!(publisher.calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.lock().completes.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolved_outcome_is_persisted_while_only_publishing_claim_is_renewed() {
    let completed = record(0, 0);
    let publishing = record(2, 0);
    let store = FakeStore::new(vec![completed.clone(), publishing.clone()]);
    store.renew_mode.store(RENEW_NONE, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_MIXED_GATE);
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), settings(2))
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| publisher.active.load(Ordering::SeqCst) == 2).await;
    publisher.release();
    wait_for(|| !store.lock().completes.is_empty()).await;
    wait_for(|| !store.lock().renewals.is_empty()).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    assert_eq!(report.completed, 1);
    assert_eq!(report.fenced, 1);
    assert_eq!(report.aborted, 1);
    assert_eq!(store.lock().completes, vec![vec![completed.claim]]);
    assert_eq!(store.lock().renewals, vec![vec![publishing.claim]]);
}

#[tokio::test(start_paused = true)]
async fn fast_store_path_persists_resolved_work_without_retiring_live_publisher() {
    let completed = record(0, 0);
    let publishing = record(2, 0);
    let store = FakeStore::new(vec![completed.clone(), publishing]);
    let publisher = FakePublisher::new(PUBLISH_MIXED_GATE);
    let cancellation = CancellationToken::new();
    let mut configured = settings(2);
    configured.lease = Duration::from_millis(40);
    configured.store_timeout = Duration::from_millis(19);
    configured.drain_timeout = Duration::from_millis(5);
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), configured)
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| publisher.active.load(Ordering::SeqCst) == 2).await;
    publisher.release();
    wait_for(|| !store.lock().completes.is_empty()).await;

    assert_eq!(store.lock().completes, vec![vec![completed.claim]]);
    assert!(store.lock().renewals.is_empty());
    assert_eq!(publisher.active.load(Ordering::SeqCst), 1);

    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert_eq!(report.completed, 1);
    assert_eq!(report.aborted, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsafe_persistence_waits_for_early_renewal_then_retires_only_blocking_publisher() {
    let completed = record(0, 0);
    let publishing = record(2, 0);
    let store = FakeStore::new(vec![completed.clone(), publishing.clone()]);
    store.claim_delay_ms.store(10, Ordering::SeqCst);
    store.renew_delay_ms.store(18, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_MIXED_GATE);
    let cancellation = CancellationToken::new();
    let mut configured = settings(2);
    configured.lease = Duration::from_millis(40);
    configured.store_timeout = Duration::from_millis(19);
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), configured)
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| publisher.active.load(Ordering::SeqCst) == 2).await;
    publisher.release();
    wait_for(|| store.renew_entered.load(Ordering::SeqCst)).await;
    assert!(
        store.lock().completes.is_empty(),
        "unsafe completion was admitted before renewing its live peer"
    );
    wait_for(|| !store.lock().completes.is_empty()).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    assert_eq!(store.lock().completes, vec![vec![completed.claim]]);
    assert!(store.lock().renewals.len() <= 2);
    assert_eq!(report.completed, 1);
    assert_eq!(report.aborted, 1);
    assert!(
        store
            .lock()
            .releases
            .iter()
            .flatten()
            .any(|claim| *claim == publishing.claim)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn success_racing_protective_retirement_is_persisted_without_abort_or_release() {
    let first = record(0, 0);
    let second = record(1, 0);
    let store = FakeStore::new(vec![first.clone(), second.clone()]);
    store.claim_delay_ms.store(10, Ordering::SeqCst);
    store.renew_delay_ms.store(18, Ordering::SeqCst);
    let publisher = RacingPublisher::new();
    let cancellation = CancellationToken::new();
    let mut configured = settings(2);
    configured.lease = Duration::from_millis(40);
    configured.store_timeout = Duration::from_millis(19);
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), configured)
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| publisher.first.receiver_count() == 1 && publisher.second.receiver_count() == 1)
        .await;
    publisher.first.send_replace(true);
    wait_for(|| store.renew_entered.load(Ordering::SeqCst)).await;
    assert!(store.lock().completes.is_empty());
    publisher.second.send_replace(true);
    wait_for(|| !store.lock().completes.is_empty()).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    let completed = store
        .lock()
        .completes
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(completed.len(), 2);
    assert!(completed.contains(&first.claim));
    assert!(completed.contains(&second.claim));
    assert!(store.lock().releases.is_empty());
    assert_eq!(report.completed, 2);
    assert_eq!(report.aborted, 0);
    assert_eq!(report.released, 0);
}
