//! Opt-in ownership scenarios for the Kafka partitioned source: eager rebalance, stale
//! generations, static-identity fencing, and bounded shutdown.
//!
//! Members use static identities; the eager range assignor sorts them, so `a-*` members take
//! the lowest partitions from `z-*` members deterministically.

mod support;

use std::time::{Duration, Instant};

use rdkafka::topic_partition_list::Offset;
use sisa_messaging::{
    Delivery, ErrorClassifier, FailureKind, MessageId, PartitionAdvance,
    PartitionedLogDeliverySource, PartitionedLogReceive, PartitionedLogSettlement,
};
use sisa_messaging_consumer::ConsumerExit;
use sisa_messaging_kafka::{
    KafkaClient, KafkaDeliverySource, KafkaPartition, KafkaSettlement, KafkaSettlementErrorKind,
    KafkaShutdownOutcome, KafkaSourceErrorKind,
};
use tokio_util::sync::CancellationToken;

use support::inbox::FakeInbox;
use support::{
    BROKERS_ENV, PARTITIONED_TOPIC_ENV, ScriptedHandler, Step, TEST_TIMEOUT, TOPIC_ENV,
    committed_cursor, consumer_client, consumer_settings, delivery_source, eventually,
    new_producer, partitioned_consumer, publish_order, required_env, scope, seed_group_at_end,
    unique,
};

struct Group {
    brokers: String,

    topic: String,

    group: String,

    client: KafkaClient,

    producer: rdkafka::producer::FutureProducer,
}

impl Group {
    fn new(topic_env: &str) -> Self {
        let brokers = required_env(BROKERS_ENV);
        let topic = required_env(topic_env);
        let group = unique("sisa-kafka-rebalance");
        let _ = seed_group_at_end(&brokers, &group, &topic);

        Self {
            client: consumer_client(&brokers),
            producer: new_producer(&brokers),
            brokers,
            topic,
            group,
        }
    }

    fn source(&self, instance: &str) -> KafkaDeliverySource {
        delivery_source(&self.client, &self.group, instance, &self.topic)
    }

    async fn opened(&self, instance: &str) -> KafkaDeliverySource {
        let mut source = self.source(instance);

        tokio::time::timeout(TEST_TIMEOUT, source.open())
            .await
            .unwrap_or_else(|_| panic!("Kafka source open timed out"))
            .unwrap_or_else(|error| panic!("Kafka source open failed: {error}"));

        source
    }

    fn committed(&self, partition: i32) -> Offset {
        committed_cursor(&self.brokers, &self.group, &self.topic, partition)
    }
}

enum Received {
    Delivery(KafkaSettlement),

    Lost(KafkaPartition),
}

async fn next(source: &mut KafkaDeliverySource) -> Received {
    let received = tokio::time::timeout(TEST_TIMEOUT, source.receive())
        .await
        .unwrap_or_else(|_| panic!("Kafka source produced nothing before the deadline"))
        .unwrap_or_else(|error| panic!("Kafka source failed: {error}"));

    match received {
        PartitionedLogReceive::Delivery(delivery) => Received::Delivery(delivery.into_parts().1),
        PartitionedLogReceive::OwnershipLost(partition) => Received::Lost(partition),
        _ => panic!("Kafka source closed unexpectedly"),
    }
}

async fn next_delivery(source: &mut KafkaDeliverySource) -> KafkaSettlement {
    loop {
        if let Received::Delivery(settlement) = next(source).await {
            return settlement;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker and a pre-provisioned multi-partition test topic"]
async fn rebalance_mid_flight_emits_loss_and_new_owner_replays_once() {
    let group = Group::new(PARTITIONED_TOPIC_ENV);
    let inbox = FakeInbox::new(3);
    let handler = ScriptedHandler::default();
    let message_id = MessageId::new();

    // The first attempt blocks under the old owner; the replay succeeds under the new owner.
    handler.script("moved", &[Step::Gate, Step::Succeed]);

    let old = partitioned_consumer(
        group.source("z-old"),
        &inbox,
        &handler,
        consumer_settings(4),
    );

    let old_cancel = CancellationToken::new();
    let old_run = tokio::spawn(old.run_partitioned(old_cancel.clone()));

    let (partition, offset) =
        publish_order(&group.producer, &group.topic, Some(0), message_id, "moved").await;

    eventually("the old owner is handling the record", || {
        handler.invocations("moved") == 1
    })
    .await;

    let new_source = group.source("a-new");
    let new_shutdown = new_source.shutdown_handle();
    let new = partitioned_consumer(new_source, &inbox, &handler, consumer_settings(4));
    let new_cancel = CancellationToken::new();
    let new_run = tokio::spawn(new.run_partitioned(new_cancel.clone()));

    eventually("the new owner committed the replayed record", || {
        group.committed(partition) == Offset::Offset(offset + 1)
    })
    .await;

    // The old owner's ownership loss cancelled its attempt; only the new owner's commit landed.
    assert_eq!(handler.invocations("moved"), 2);
    assert_eq!(inbox.completions(&scope(), message_id), 1);
    assert_eq!(inbox.effects(), vec!["moved".to_owned()]);
    assert!(!old_run.is_finished() && !new_run.is_finished());

    old_cancel.cancel();
    new_cancel.cancel();

    for run in [old_run, new_run] {
        let exit = tokio::time::timeout(TEST_TIMEOUT, run)
            .await
            .unwrap_or_else(|_| panic!("consumer did not stop"))
            .unwrap_or_else(|_| panic!("consumer task panicked"));

        assert!(matches!(exit, Ok(ConsumerExit::Cancelled)));
    }

    assert_eq!(inbox.live(), 0);

    let outcome = tokio::time::timeout(TEST_TIMEOUT, new_shutdown)
        .await
        .unwrap_or_else(|_| panic!("Kafka source shutdown did not finish"));

    assert_eq!(outcome, KafkaShutdownOutcome::Closed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn stale_generation_advance_is_ownership_lost() {
    let group = Group::new(TOPIC_ENV);
    let mut old = group.opened("z-old").await;

    let (partition, offset) = publish_order(
        &group.producer,
        &group.topic,
        None,
        MessageId::new(),
        "stale",
    )
    .await;

    let stale = next_delivery(&mut old).await;
    assert_eq!(stale.offset(), offset);

    let mut new = group.opened("a-new").await;

    // Eager revocation releases the partition whose record the old member still holds.
    let lost = loop {
        if let Received::Lost(lost) = next(&mut old).await {
            break lost;
        }
    };

    assert_eq!(&lost, stale.partition());

    // The old generation's snapshot is never used again: the advance is conclusively fenced
    // without broker I/O, and the cursor is unchanged.
    assert_eq!(stale.advance().await, Ok(PartitionAdvance::OwnershipLost));
    assert_eq!(group.committed(partition), Offset::Offset(offset));

    let replayed = next_delivery(&mut new).await;
    assert_eq!(replayed.offset(), offset);
    assert_eq!(replayed.advance().await, Ok(PartitionAdvance::Advanced));
    assert_eq!(group.committed(partition), Offset::Offset(offset + 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn duplicate_instance_fences_old_source_permanently() {
    let group = Group::new(TOPIC_ENV);
    let mut old = group.opened("duplicate").await;
    let _new = group.opened("duplicate").await;

    let deadline = Instant::now() + TEST_TIMEOUT;

    let error = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());

        match tokio::time::timeout(remaining, old.receive()).await {
            Ok(Err(error)) => break error,
            Ok(Ok(_)) => {}
            Err(_) => panic!("the duplicate static instance did not fence the old source"),
        }
    };

    assert_eq!(error.kind(), KafkaSourceErrorKind::InstanceFenced);
    assert_eq!(error.classify(), FailureKind::Permanent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn shutdown_completes_within_bound() {
    let group = Group::new(TOPIC_ENV);
    let shutdown_timeout = Duration::from_secs(15);
    let mut source = group.opened("closing").await;
    let shutdown = source.shutdown_handle();

    let _ = publish_order(
        &group.producer,
        &group.topic,
        None,
        MessageId::new(),
        "held",
    )
    .await;

    let held = next_delivery(&mut source).await;

    let started = Instant::now();
    drop(source);

    let outcome = tokio::time::timeout(shutdown_timeout + Duration::from_secs(5), shutdown)
        .await
        .unwrap_or_else(|_| panic!("Kafka source shutdown exceeded its bound"));

    assert_eq!(outcome, KafkaShutdownOutcome::Closed);
    assert!(started.elapsed() <= shutdown_timeout);

    // A handle that outlives its source never waits on the stopped member thread.
    let error = held
        .advance()
        .await
        .err()
        .unwrap_or_else(|| panic!("an advance after shutdown must fail"));

    assert_eq!(error.kind(), KafkaSettlementErrorKind::MemberStopped);
    assert_eq!(error.classify(), FailureKind::Transient);
}
