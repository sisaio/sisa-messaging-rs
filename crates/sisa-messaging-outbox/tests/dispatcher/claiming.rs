use std::sync::atomic::Ordering;
use std::time::Duration;

use sisa_messaging::FailureKind;
use sisa_messaging_outbox::{DispatcherError, OutboxDispatcher, PoisonReport};
use tokio_util::sync::CancellationToken;

use super::support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_capacity_includes_unresolved_publications_and_shutdown_drains() {
    let store = FakeStore::new((0..4).map(|index| record(index, 0)).collect());
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), settings(2))
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| publisher.active.load(Ordering::SeqCst) == 2).await;
    assert_eq!(publisher.maximum.load(Ordering::SeqCst), 2);
    assert_eq!(store.claim_calls.load(Ordering::SeqCst), 1);

    cancellation.cancel();
    publisher.release();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert_eq!(report.claimed, 2);
    assert_eq!(report.completed, 2);
    assert_eq!(publisher.maximum.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn claim_timeout_backs_off_and_shutdown_remains_bounded() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.claim_delay_ms.store(40, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_SUCCESS);
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), settings(1))
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| store.claim_calls.load(Ordering::SeqCst) >= 1).await;
    tokio::time::sleep(Duration::from_millis(15)).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert!(report.store_failures >= 1);
    assert_eq!(publisher.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn poison_reporting_does_not_block_healthy_records() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.lock().poison = PoisonReport {
        observed: 2,
        marked_dead: 1,
    };
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(
        store.clone(),
        FakePublisher::new(PUBLISH_SUCCESS),
        settings(1),
    )
    .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| !store.lock().completes.is_empty()).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert_eq!(report.poisoned, 2);
    assert_eq!(report.dead, 1);
    assert_eq!(report.completed, 1);
}

#[tokio::test]
async fn observed_poison_without_confirmed_dead_transition_is_not_counted_dead() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.lock().poison = PoisonReport {
        observed: 2,
        marked_dead: 0,
    };
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(
        store.clone(),
        FakePublisher::new(PUBLISH_SUCCESS),
        settings(1),
    )
    .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| !store.lock().completes.is_empty()).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert_eq!(report.poisoned, 2);
    assert_eq!(report.dead, 0);
    assert_eq!(report.completed, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_oversized_batch_retains_one_bounded_cleanup_chunk_then_resumes_claiming() {
    let records = (0..34).map(|index| record(index, 0)).collect::<Vec<_>>();
    let store = FakeStore::new(records.clone());
    store.claim_limit_extra.store(32, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(
        store.clone(),
        FakePublisher::new(PUBLISH_SUCCESS),
        settings(2),
    )
    .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| store.lock().releases.len() == 1).await;
    wait_for(|| !store.lock().completes.is_empty()).await;
    wait_for(|| store.claim_calls.load(Ordering::SeqCst) >= 2).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    assert_eq!(report.claimed, 2);
    assert_eq!(report.released, 2);
    assert_eq!(report.store_failures, 1);
    assert_eq!(
        store.lock().releases,
        vec![vec![records[2].claim, records[3].claim]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_active_token_is_ignored_and_never_released() {
    let active = record(1, 0);
    let store = FakeStore::new(vec![active.clone(), active]);
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), settings(2))
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| publisher.active.load(Ordering::SeqCst) == 1).await;
    publisher.release();
    wait_for(|| !store.lock().completes.is_empty()).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    assert_eq!(report.claimed, 1);
    assert_eq!(report.store_failures, 1);
    assert!(store.lock().releases.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ambiguous_duplicate_rejected_release_is_retired_once_without_a_local_retry_loop() {
    let active = record(1, 0);
    let rejected = record(2, 0);
    let rejected_claim = rejected.claim;
    let records = vec![active, rejected.clone(), rejected];
    let store = FakeStore::new(records);
    store.claim_limit_extra.store(2, Ordering::SeqCst);
    store
        .release_mode
        .store(RELEASE_TRANSIENT, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), settings(1))
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| store.lock().releases.len() == 1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(store.lock().releases, vec![vec![rejected_claim]]);
    publisher.release();
    wait_for(|| !store.lock().completes.is_empty()).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    assert_eq!(report.claimed, 1);
    assert_eq!(report.store_failures, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_finishes_rejected_cleanup_in_bounded_chunks() {
    let records = (0..3).map(|index| record(index, 0)).collect::<Vec<_>>();
    let store = FakeStore::new(records.clone());
    store.claim_limit_extra.store(2, Ordering::SeqCst);
    store.release_delay_ms.store(20, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();
    let mut configured = settings(1);
    configured.store_timeout = Duration::from_millis(30);
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher, configured)
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| store.release_entered.load(Ordering::SeqCst)).await;
    cancellation.cancel();
    let report = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap_or_else(|_| panic!("cancellation cleanup exceeded its finite bound"))
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    assert_eq!(report.claimed, 1);
    assert_eq!(report.aborted, 1);
    assert_eq!(report.released, 2);
    let releases = &store.lock().releases;
    assert_eq!(releases.len(), 2);
    assert!(releases.iter().all(|batch| batch.len() == 1));
    for record in records.into_iter().take(2) {
        assert!(
            releases
                .iter()
                .flatten()
                .any(|claim| *claim == record.claim)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn permanent_rejected_release_runs_finite_fatal_cleanup() {
    let records = (0..3).map(|index| record(index, 0)).collect::<Vec<_>>();
    let store = FakeStore::new(records.clone());
    store.claim_limit_extra.store(2, Ordering::SeqCst);
    store
        .release_mode
        .store(RELEASE_PERMANENT, Ordering::SeqCst);
    let dispatcher =
        OutboxDispatcher::new(store.clone(), FakePublisher::new(PUBLISH_GATE), settings(1))
            .unwrap_or_else(|error| panic!("settings rejected: {error}"));

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        dispatcher.run(CancellationToken::new()),
    )
    .await
    .unwrap_or_else(|_| panic!("fatal cleanup exceeded its finite bound"));
    match result {
        Err(DispatcherError::Store(error)) => assert_eq!(error.kind, FailureKind::Permanent),
        other => panic!("expected sourced permanent store error, got {other:?}"),
    }
    let releases = &store.lock().releases;
    assert_eq!(releases.len(), 2);
    assert!(releases.iter().all(|batch| batch.len() == 1));
    for record in records.into_iter().take(2) {
        assert!(
            releases
                .iter()
                .flatten()
                .any(|claim| *claim == record.claim)
        );
    }
}
