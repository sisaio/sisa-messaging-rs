use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use sisa_messaging_outbox::OutboxDispatcher;
use tokio_util::sync::CancellationToken;

use super::support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_during_claim_waits_for_call_then_releases_unstarted_record() {
    let store = FakeStore::new(vec![record(1, 0)]);
    store.claim_delay_ms.store(30, Ordering::SeqCst);
    let publisher = FakePublisher::new(PUBLISH_SUCCESS);
    let cancellation = CancellationToken::new();
    let mut configured = settings(1);
    configured.store_timeout = Duration::from_millis(40);
    configured.lease = Duration::from_millis(120);
    let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), configured)
        .unwrap_or_else(|error| panic!("settings rejected: {error}"));
    let run_cancel = cancellation.clone();
    let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

    wait_for(|| store.claim_entered.load(Ordering::SeqCst)).await;
    cancellation.cancel();
    let report = task
        .await
        .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
        .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));
    assert_eq!(publisher.calls.load(Ordering::SeqCst), 0);
    assert_eq!(report.released, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_does_not_drop_complete_fail_release_or_renew_store_calls() {
    async fn run_case(operation: &str) {
        let store = FakeStore::new(vec![record(1, 0)]);
        let behavior = match operation {
            "fail" => PUBLISH_TRANSIENT,
            "renew" => PUBLISH_GATE,
            _ => PUBLISH_SUCCESS,
        };
        if operation == "release" {
            store
                .complete_mode
                .store(COMPLETE_TRANSIENT_ONCE, Ordering::SeqCst);
        }

        let entered = match operation {
            "complete" => {
                store.complete_delay_ms.store(25, Ordering::SeqCst);
                Arc::clone(&store.complete_entered)
            }
            "fail" => {
                store.fail_delay_ms.store(25, Ordering::SeqCst);
                Arc::clone(&store.fail_entered)
            }
            "release" => {
                store.release_delay_ms.store(25, Ordering::SeqCst);
                Arc::clone(&store.release_entered)
            }
            "renew" => {
                store.renew_delay_ms.store(5, Ordering::SeqCst);
                Arc::clone(&store.renew_entered)
            }
            _ => unreachable!("test operation is a closed constant"),
        };

        let publisher = FakePublisher::new(behavior);
        let cancellation = CancellationToken::new();
        let mut configured = settings(1);
        configured.store_timeout = Duration::from_millis(35);
        configured.lease = Duration::from_millis(100);
        let dispatcher = OutboxDispatcher::new(store.clone(), publisher.clone(), configured)
            .unwrap_or_else(|error| panic!("settings rejected: {error}"));
        let run_cancel = cancellation.clone();
        let task = tokio::spawn(async move { dispatcher.run(run_cancel).await });

        wait_for(|| entered.load(Ordering::SeqCst)).await;
        cancellation.cancel();
        if operation == "renew" {
            publisher.release();
        }
        task.await
            .unwrap_or_else(|error| panic!("dispatcher task failed: {error}"))
            .unwrap_or_else(|error| panic!("dispatcher failed: {error}"));

        let state = store.lock();
        match operation {
            "complete" => assert_eq!(state.completes.len(), 1),
            "fail" => assert_eq!(state.failures.len(), 1),
            "release" => assert_eq!(state.releases.len(), 1),
            "renew" => assert!(!state.renewals.is_empty()),
            _ => unreachable!("test operation is a closed constant"),
        }
    }

    for operation in ["complete", "fail", "release", "renew"] {
        run_case(operation).await;
    }
}

#[test]
fn select_branches_are_readiness_only() {
    let source = include_str!("../../src/dispatcher.rs");
    let shutdown = include_str!("../../src/dispatcher/shutdown.rs");

    for selected in [source, shutdown] {
        for block in selected.split("tokio::select!").skip(1) {
            let block = block.split('}').next().unwrap_or(block);
            assert!(!block.contains("store."));
            assert!(!block.contains("publisher.publish"));
        }
    }
}
