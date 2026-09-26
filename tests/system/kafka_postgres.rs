//! Typed partitioned consumer system proofs over a real PostgreSQL inbox and a real Kafka broker.
//!
//! Run the ignored tests with `SISA_KAFKA_BOOTSTRAP_SERVERS`, a pre-provisioned single-partition
//! `SISA_KAFKA_TEST_TOPIC`, and either `DATABASE_URL` or the `PG*` variables set against a
//! database migrated with the repository schema. Each test uses its own inbox scope and consumer
//! group seeded at the topic's end.

#![forbid(unsafe_code)]

#[path = "kafka_postgres/support.rs"]
mod support;

use sisa_messaging::{Message, MessageId, MessageType, Metadata};
use sisa_messaging_consumer::{ConsumerErrorKind, ConsumerExit};
use sisa_messaging_inbox::{
    InboxClaimOutcome, InboxRecord, InboxSettings, InboxStore, InboxUnitOfWork,
};
use sisa_messaging_postgres::PostgresInboxStore;

use support::{
    EffectHandler, Fixture, OrderCreated, Receipt, Running, effect_count, receipt, run_with_cleanup,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PostgreSQL and a real Kafka broker with a single-partition test topic"]
async fn handler_effect_commits_with_the_receipt_before_the_offset_advances() {
    run_with_cleanup(move |fixture| async move {
        let Fixture {
            pool,
            broker,
            scope,
        } = fixture;

        let handler = EffectHandler::succeeding(&scope);
        let running = Running::spawn(&pool, &broker, &scope, handler.clone());

        let id = MessageId::new();
        let end = broker.publish(id, "committed").await;

        // Once the transactional offset commit covers the record, the effect and completion are
        // durably visible. Advance-after-commit ordering itself is proven by the consumer crate's
        // runtime tests.
        broker.wait_committed(end).await;
        assert_eq!(effect_count(&pool, &scope, id).await, 1);

        assert_eq!(
            receipt(&pool, &scope, id).await,
            Some(Receipt {
                attempts: 0,
                completed: true,
                dead: false
            })
        );

        assert_eq!(handler.invocations(id), 1);
        assert_eq!(running.stop().await.unwrap(), ConsumerExit::Cancelled);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PostgreSQL and a real Kafka broker with a single-partition test topic"]
async fn transient_failure_rolls_back_its_effect_and_replays_after_restart() {
    run_with_cleanup(move |fixture| async move {
        let Fixture {
            pool,
            broker,
            scope,
        } = fixture;

        let handler = EffectHandler::failing_once_after_write(&scope);
        let before = broker.committed();
        let first = Running::spawn(&pool, &broker, &scope, handler.clone());

        let id = MessageId::new();
        let end = broker.publish(id, "retried").await;

        // A partitioned record cannot be requeued: its failure is recorded after rollback, the
        // partition stays unresolved, and the run stops without advancing the offset.
        let error = first.finish().await.unwrap_err();
        assert_eq!(error.kind(), ConsumerErrorKind::PartitionUnresolved);

        assert_eq!(
            effect_count(&pool, &scope, id).await,
            0,
            "effect rolled back"
        );

        assert!(broker.committed() < end);
        assert!(broker.committed() >= before);

        assert_eq!(
            receipt(&pool, &scope, id).await,
            Some(Receipt {
                attempts: 1,
                completed: false,
                dead: false
            })
        );

        // A restarted member replays the record from the committed offset.
        let restarted = Running::spawn(&pool, &broker, &scope, handler.clone());
        broker.wait_committed(end).await;

        assert_eq!(handler.invocations(id), 2);
        assert_eq!(effect_count(&pool, &scope, id).await, 1);

        assert_eq!(
            receipt(&pool, &scope, id).await,
            Some(Receipt {
                attempts: 1,
                completed: true,
                dead: false
            })
        );

        assert_eq!(restarted.stop().await.unwrap(), ConsumerExit::Cancelled);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PostgreSQL and a real Kafka broker with a single-partition test topic"]
async fn completed_receipt_replay_advances_without_invoking_the_handler() {
    run_with_cleanup(move |fixture| async move {
        let Fixture {
            pool,
            broker,
            scope,
        } = fixture;

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

        let InboxClaimOutcome::Claimed(claimed) =
            store.claim(&mut transaction, &record).await.unwrap()
        else {
            panic!("a fresh receipt must be claimable");
        };

        store.complete(&mut transaction, claimed).await.unwrap();
        store.commit(transaction).await.unwrap();

        let handler = EffectHandler::succeeding(&scope);
        let running = Running::spawn(&pool, &broker, &scope, handler.clone());

        let end = broker.publish(id, "already-completed").await;
        broker.wait_committed(end).await;

        assert_eq!(handler.invocations(id), 0);
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
    })
    .await;
}
