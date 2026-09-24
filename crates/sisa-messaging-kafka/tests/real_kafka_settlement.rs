//! Opt-in proof of synchronous commit completion after a waiter times out or is dropped.

mod support;

use std::thread;
use std::time::Instant;

use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use sisa_messaging::MessageId;
use tokio::sync::oneshot;

use support::{
    BROKERS_ENV, TEST_TIMEOUT, TOPIC_ENV, committed_offset, new_consumer, new_producer,
    publish_marker, receive_marker, required_env, subscribe, unique_group,
};

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned isolated test topic"]
async fn dropped_or_timed_out_waiter_reconciles_after_sync_worker_quiesces() {
    let brokers = required_env(BROKERS_ENV);
    let topic = required_env(TOPIC_ENV);
    let producer = new_producer(&brokers);
    let marker = MessageId::new().to_string();

    let _ = publish_marker(&producer, &topic, &marker).await;

    let group = unique_group();
    let worker_consumer = new_consumer(&brokers, &group);
    subscribe(&worker_consumer, &topic);

    let (partition, offset) = receive_marker(&worker_consumer, &topic, &marker);
    let mut offsets = TopicPartitionList::new();

    offsets
        .add_partition_offset(&topic, partition, Offset::Offset(offset + 1))
        .unwrap_or_else(|_| panic!("Kafka offset list construction failed"));

    let (completion_tx, mut completion_rx) = oneshot::channel();

    let worker = thread::spawn(move || {
        let result = worker_consumer.commit(&offsets, CommitMode::Sync);
        // Delay only delivery of the worker result, after the real broker operation returns.
        // This exercises coordinator timeout handling but is not evidence about broker-side
        // late responses; that race remains a separate broker fault-injection gate.
        thread::sleep(std::time::Duration::from_millis(150));
        let _ = completion_tx.send((worker_consumer, result));
    });

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut completion_rx)
            .await
            .is_err(),
        "the settlement waiter should time out before the delayed worker report",
    );

    let (quiescent_consumer, commit_result) = completion_rx
        .await
        .unwrap_or_else(|_| panic!("Kafka commit worker did not report after the timeout"));

    assert!(
        commit_result.is_ok(),
        "the worker must retain its synchronous commit"
    );

    worker
        .join()
        .unwrap_or_else(|_| panic!("Kafka commit worker panicked"));
    drop(quiescent_consumer);

    let authoritative_generation = new_consumer(&brokers, &group);
    subscribe(&authoritative_generation, &topic);

    let deadline = Instant::now() + TEST_TIMEOUT;

    while Instant::now() < deadline
        && authoritative_generation
            .assignment()
            .map_or(true, |set| set.count() == 0)
    {
        let _ = authoritative_generation.poll(std::time::Duration::from_millis(100));
    }

    assert!(
        authoritative_generation
            .assignment()
            .is_ok_and(|set| set.count() > 0),
        "authoritative Kafka generation did not receive an assignment",
    );
    assert_eq!(
        committed_offset(&authoritative_generation, &topic, partition),
        Offset::Offset(offset + 1),
        "the new generation must reconcile the broker's committed cursor",
    );
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned isolated test topic"]
async fn dropping_the_settlement_receiver_does_not_cancel_sync_commit_worker() {
    let brokers = required_env(BROKERS_ENV);
    let topic = required_env(TOPIC_ENV);
    let producer = new_producer(&brokers);
    let marker = MessageId::new().to_string();

    let _ = publish_marker(&producer, &topic, &marker).await;

    let group = unique_group();
    let worker_consumer = new_consumer(&brokers, &group);
    subscribe(&worker_consumer, &topic);

    let (partition, offset) = receive_marker(&worker_consumer, &topic, &marker);
    let mut offsets = TopicPartitionList::new();

    offsets
        .add_partition_offset(&topic, partition, Offset::Offset(offset + 1))
        .unwrap_or_else(|_| panic!("Kafka offset list construction failed"));

    let (completion_tx, completion_rx) = oneshot::channel();

    let worker = thread::spawn(move || {
        let result = worker_consumer.commit(&offsets, CommitMode::Sync);
        let _ = completion_tx.send((worker_consumer, result));
    });
    drop(completion_rx);

    worker
        .join()
        .unwrap_or_else(|_| panic!("Kafka commit worker panicked"));

    let authoritative_generation = new_consumer(&brokers, &group);
    subscribe(&authoritative_generation, &topic);

    let deadline = Instant::now() + TEST_TIMEOUT;

    while Instant::now() < deadline
        && authoritative_generation
            .assignment()
            .map_or(true, |set| set.count() == 0)
    {
        let _ = authoritative_generation.poll(std::time::Duration::from_millis(100));
    }

    assert_eq!(
        committed_offset(&authoritative_generation, &topic, partition),
        Offset::Offset(offset + 1),
        "the new Kafka generation, not the dropped waiter, reports the settlement outcome",
    );
}
