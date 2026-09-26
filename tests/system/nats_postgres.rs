//! Typed consumer system proofs over a real PostgreSQL inbox and a real JetStream durable.
//!
//! Run the ignored tests with `NATS_URL` and either `DATABASE_URL` or the `PG*` variables set
//! against a database migrated with the repository schema. Each test uses its own inbox scope,
//! stream, and durable name.

#![forbid(unsafe_code)]

#[path = "nats_postgres/support.rs"]
mod support;

use std::time::Duration;

use sisa_messaging::{Message, MessageId, MessageType, Metadata};
use sisa_messaging_consumer::ConsumerExit;
use sisa_messaging_inbox::{
    InboxClaimOutcome, InboxRecord, InboxSettings, InboxStore, InboxUnitOfWork,
};
use sisa_messaging_postgres::PostgresInboxStore;

use support::{
    Broker, EffectHandler, OrderCreated, Receipt, Running, cleanup, effect_count, pool, receipt,
    scope, settings, wait_for_receipt,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PostgreSQL and a real JetStream server at NATS_URL"]
async fn handler_effect_commits_with_the_receipt_before_acknowledgement() {
    let pool = pool().await;
    let ack_wait = Duration::from_secs(1);
    let (broker, pull_consumer) = Broker::new(ack_wait).await;
    let scope = scope();
    let handler = EffectHandler::succeeding(&scope);

    let running = Running::spawn(
        &pool,
        pull_consumer,
        &scope,
        handler.clone(),
        settings(Duration::from_millis(200)),
    );

    let id = MessageId::new();
    broker.publish(id, "committed").await;

    // Once the broker reports the acknowledgement, the effect and completion are durably visible.
    // Ack-after-commit ordering itself is proven by the consumer crate's runtime tests and the
    // JetStream failed-commit scenario.
    broker.wait_acked(1).await;
    assert_eq!(effect_count(&pool, &scope, id).await, 1);

    assert_eq!(
        receipt(&pool, &scope, id).await,
        Some(Receipt {
            attempts: 0,
            completed: true,
            dead: false
        })
    );

    tokio::time::sleep(ack_wait * 2 + Duration::from_millis(500)).await;

    let info = broker.info().await;
    assert_eq!(info.num_ack_pending, 0);
    assert_eq!(info.delivered.consumer_sequence, 1, "no redelivery");
    assert_eq!(handler.invocations().len(), 1);
    assert_eq!(effect_count(&pool, &scope, id).await, 1);

    assert_eq!(running.stop().await.unwrap(), ConsumerExit::Cancelled);
    broker.delete().await;
    cleanup(&pool, &scope).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PostgreSQL and a real JetStream server at NATS_URL"]
async fn transient_failure_rolls_back_its_effect_before_recording_and_retries_once() {
    let pool = pool().await;
    let nak_delay = Duration::from_secs(1);
    let ack_wait = Duration::from_secs(10);
    let (broker, pull_consumer) = Broker::new(ack_wait).await;
    let scope = scope();
    let handler = EffectHandler::failing_once_after_write(&scope);

    let running = Running::spawn(
        &pool,
        pull_consumer,
        &scope,
        handler.clone(),
        settings(nak_delay),
    );

    let id = MessageId::new();
    broker.publish(id, "retried").await;

    // The retry waits at its gate, so this state is the recorded first failure.
    let failed = wait_for_receipt(&pool, &scope, id, |receipt| receipt.attempts == 1).await;

    assert_eq!(
        failed,
        Receipt {
            attempts: 1,
            completed: false,
            dead: false
        }
    );

    assert_eq!(
        effect_count(&pool, &scope, id).await,
        0,
        "effect rolled back"
    );

    handler.release_retry();
    let info = broker.wait_acked(1).await;

    assert_eq!(
        info.delivered.consumer_sequence, 2,
        "exactly one redelivery"
    );

    let invocations = handler.invocations();
    assert_eq!(invocations.len(), 2);

    let gap = invocations[1].duration_since(invocations[0]);

    assert!(
        gap >= nak_delay.mul_f32(0.9) && gap < ack_wait,
        "redelivery gap {gap:?} must follow the nak delay, not ack_wait"
    );

    assert_eq!(effect_count(&pool, &scope, id).await, 1);

    assert_eq!(
        receipt(&pool, &scope, id).await,
        Some(Receipt {
            attempts: 1,
            completed: true,
            dead: false
        })
    );

    assert_eq!(running.stop().await.unwrap(), ConsumerExit::Cancelled);
    broker.delete().await;
    cleanup(&pool, &scope).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PostgreSQL and a real JetStream server at NATS_URL"]
async fn completed_receipt_redelivery_is_acknowledged_without_invoking_the_handler() {
    let pool = pool().await;
    let ack_wait = Duration::from_secs(1);
    let (broker, pull_consumer) = Broker::new(ack_wait).await;
    let scope = scope();
    let id = MessageId::new();

    // Complete the receipt through the public store API, as an earlier delivery would have.
    let store = PostgresInboxStore::new(pool.clone(), InboxSettings::default());

    let record = InboxRecord {
        scope: scope.clone(),
        message_id: id,
        message_type: MessageType::new(OrderCreated::TYPE).unwrap(),
        version: OrderCreated::VERSION,
        metadata: Metadata::default(),
    };

    let mut transaction = store.begin().await.unwrap();

    let InboxClaimOutcome::Claimed(claimed) = store.claim(&mut transaction, &record).await.unwrap()
    else {
        panic!("a fresh receipt must be claimable");
    };

    store.complete(&mut transaction, claimed).await.unwrap();
    store.commit(transaction).await.unwrap();

    let handler = EffectHandler::succeeding(&scope);

    let running = Running::spawn(
        &pool,
        pull_consumer,
        &scope,
        handler.clone(),
        settings(Duration::from_millis(200)),
    );

    broker.publish(id, "already-completed").await;
    broker.wait_acked(1).await;

    tokio::time::sleep(ack_wait * 2 + Duration::from_millis(500)).await;

    let info = broker.info().await;
    assert_eq!(info.num_ack_pending, 0);
    assert_eq!(info.delivered.consumer_sequence, 1, "no redelivery");
    assert!(handler.invocations().is_empty());
    assert_eq!(effect_count(&pool, &scope, id).await, 0);

    assert_eq!(
        receipt(&pool, &scope, id).await,
        Some(Receipt {
            attempts: 0,
            completed: true,
            dead: false
        })
    );

    assert_eq!(running.stop().await.unwrap(), ConsumerExit::Cancelled);
    broker.delete().await;
    cleanup(&pool, &scope).await;
}
