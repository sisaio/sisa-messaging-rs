use std::sync::atomic::Ordering;
use std::time::Duration;

use sisa_messaging_outbox::OutboxDispatcher;
use tokio_util::sync::CancellationToken;

use super::support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_deadline_aborts_unresolved_publish_and_releases_claim() {
    let store = FakeStore::new(vec![record(1, 0)]);
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();
    let mut configured = settings(1);
    configured.drain_timeout = Duration::from_millis(25);
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), configured)
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| publisher.active.load(Ordering::SeqCst) == 1).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert_eq!(report.aborted, 1);
    assert_eq!(report.released, 1);
    assert_eq!(publisher.active.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn shutdown_polls_due_renewal_between_bounded_store_calls() {
    let store = FakeStore::new(vec![record(0, 0), record(1, 0), record(2, 0)]);
    store
        .complete_mode
        .store(COMPLETE_TRANSIENT_ONCE, Ordering::SeqCst);
    store.complete_delay_ms.store(18, Ordering::SeqCst);
    store.fail_delay_ms.store(18, Ordering::SeqCst);
    store.release_delay_ms.store(18, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_MIXED_GATE);
    let cancellation = CancellationToken::new();
    let mut configured = settings(3);
    configured.lease = Duration::from_millis(40);
    configured.store_timeout = Duration::from_millis(19);
    configured.drain_timeout = Duration::from_millis(70);
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), configured)
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| publisher.active.load(Ordering::SeqCst) == 3).await;
    cancellation.cancel();
    publisher.release();
    task.await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    let operations = &store.lock().operations;
    let complete = operations
        .iter()
        .position(|operation| *operation == "complete")
        .unwrap_or_else(|| panic!("complete operation missing: {operations:?}"));
    let renewal = operations
        .iter()
        .position(|operation| *operation == "renew")
        .unwrap_or_else(|| panic!("renew operation missing: {operations:?}"));
    let release = operations
        .iter()
        .position(|operation| *operation == "release")
        .unwrap_or_else(|| panic!("release operation missing: {operations:?}"));
    assert!(
        complete < renewal,
        "unexpected operation order: {operations:?}"
    );
    assert!(
        renewal < release,
        "unexpected operation order: {operations:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn slow_started_renewal_cannot_bypass_the_drain_deadline() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.renew_delay_ms.store(18, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_GATE);
    let cancellation = CancellationToken::new();
    let mut configured = settings(1);
    configured.lease = Duration::from_millis(40);
    configured.store_timeout = Duration::from_millis(19);
    configured.drain_timeout = Duration::from_millis(5);
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), configured)
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| publisher.active.load(Ordering::SeqCst) == 1).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    assert_eq!(store.lock().renewals.len(), 1);
    assert_eq!(report.aborted, 1);
    assert_eq!(report.released, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_publish_racing_deadline_is_completed_before_hanging_work_is_released() {
    let completed = record(0, 0);
    let hanging = record(2, 0);
    let store = FakeStore::new(vec![completed.clone(), hanging.clone()]);
    store.renew_delay_ms.store(18, Ordering::SeqCst);
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
    cancellation.cancel();
    wait_for(|| store.renew_entered.load(Ordering::SeqCst)).await;
    publisher.release();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    assert_eq!(report.completed, 1);
    assert_eq!(report.aborted, 1);
    assert_eq!(report.released, 1);
    assert_eq!(store.lock().completes, vec![vec![completed.claim]]);
    assert_eq!(store.lock().releases, vec![vec![hanging.claim]]);
    let operations = &store.lock().operations;
    let complete = operations
        .iter()
        .position(|operation| *operation == "complete")
        .unwrap_or_else(|| panic!("complete operation missing: {operations:?}"));
    let release = operations
        .iter()
        .position(|operation| *operation == "release")
        .unwrap_or_else(|| panic!("release operation missing: {operations:?}"));
    assert!(
        complete < release,
        "resolved outcomes must persist before releases: {operations:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_deadline_shutdown_renews_unsafe_live_peer_before_persisting_completion() {
    let completed = record(0, 0);
    let publishing = record(2, 0);
    let store = FakeStore::new(vec![completed.clone(), publishing]);
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
    let mut task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    let publishers_started = tokio::time::timeout(
        Duration::from_secs(1),
        wait_for(|| publisher.active.load(Ordering::SeqCst) == 2),
    )
    .await;
    if publishers_started.is_err() {
        task.abort();
        let _ = task.await;
        panic!("publishers did not start: {:?}", store.lock().operations);
    }
    publisher.release();
    cancellation.cancel();
    let renewal_started = tokio::time::timeout(
        Duration::from_secs(1),
        wait_for(|| store.renew_entered.load(Ordering::SeqCst)),
    )
    .await;
    if renewal_started.is_err() {
        task.abort();
        let _ = task.await;
        panic!("renewal did not start: {:?}", store.lock().operations);
    }
    assert!(store.lock().completes.is_empty());
    let joined = tokio::select! {
        joined = &mut task => joined,
        () = tokio::time::sleep(Duration::from_secs(1)) => {
            task.abort();
            let _ = task.await;
            panic!("unsafe shutdown did not finish: {:?}", store.lock().operations);
        }
    };
    let report = joined
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

    let operations = store.lock().operations.clone();
    let renewal = operations
        .iter()
        .position(|operation| *operation == "renew")
        .unwrap_or_else(|| panic!("renew operation missing: {operations:?}"));
    let complete = operations
        .iter()
        .position(|operation| *operation == "complete")
        .unwrap_or_else(|| panic!("complete operation missing: {operations:?}"));
    assert!(
        renewal < complete,
        "unsafe persistence order: {operations:?}"
    );
    assert_eq!(store.lock().completes, vec![vec![completed.claim]]);
    assert_eq!(report.completed, 1);
    assert_eq!(report.aborted, 1);
}
