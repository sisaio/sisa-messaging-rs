#![allow(dead_code)]

use std::env;
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::{ClientConfig, Message};
use sisa_messaging::MessageId;
use sisa_messaging_kafka::{KafkaClient, KafkaClientSettings};

pub const BROKERS_ENV: &str = "SISA_KAFKA_BOOTSTRAP_SERVERS";
pub const TOPIC_ENV: &str = "SISA_KAFKA_TEST_TOPIC";
pub const TEST_TIMEOUT: Duration = Duration::from_secs(30);

pub fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be configured for this ignored test"))
}

pub fn new_producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("allow.auto.create.topics", "false")
        .set("message.timeout.ms", "15000")
        .create()
        .unwrap_or_else(|_| panic!("Kafka test producer construction failed"))
}

pub fn new_kafka_client(brokers: &str) -> KafkaClient {
    let config = KafkaClientSettings::new([brokers])
        .with_advanced_property("enable.idempotence", "true")
        .and_then(|config| config.with_advanced_property("allow.auto.create.topics", "false"))
        .and_then(|config| config.with_advanced_property("message.timeout.ms", "15000"))
        .unwrap_or_else(|_| panic!("Kafka test producer configuration is invalid"));

    KafkaClient::start(config).unwrap_or_else(|_| panic!("Kafka test producer construction failed"))
}

pub fn new_consumer(brokers: &str, group_id: &str) -> BaseConsumer {
    new_consumer_with_instance(brokers, group_id, None)
}

pub fn new_consumer_with_instance(
    brokers: &str,
    group_id: &str,
    instance_id: Option<&str>,
) -> BaseConsumer {
    let mut config = ClientConfig::new();

    config
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("allow.auto.create.topics", "false")
        .set("session.timeout.ms", "6000")
        .set("max.poll.interval.ms", "30000");

    if let Some(instance_id) = instance_id {
        config.set("group.instance.id", instance_id);
    }

    config
        .create()
        .unwrap_or_else(|_| panic!("Kafka test consumer construction failed"))
}

pub fn unique_group() -> String {
    format!("sisa-kafka-feasibility-{}", MessageId::new())
}

pub async fn publish_marker(producer: &FutureProducer, topic: &str, marker: &str) -> (i32, i64) {
    let record = FutureRecord::to(topic)
        .payload(marker.as_bytes())
        .key(marker.as_bytes());

    match producer.send(record, Duration::from_secs(5)).await {
        Ok(delivery) => (delivery.partition, delivery.offset),
        Err(_) => panic!("Kafka test marker was not confirmed"),
    }
}

pub fn receive_marker(consumer: &BaseConsumer, topic: &str, marker: &str) -> (i32, i64) {
    let deadline = Instant::now() + TEST_TIMEOUT;

    while Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) if message.topic() == topic => {
                if message.key() == Some(marker.as_bytes()) {
                    return (message.partition(), message.offset());
                }
            }
            Some(Err(_)) => panic!("Kafka test consumer receive failed"),
            _ => {}
        }
    }

    panic!("Kafka test marker was not received before the test deadline");
}

pub fn subscribe(consumer: &BaseConsumer, topic: &str) {
    consumer
        .subscribe(&[topic])
        .unwrap_or_else(|_| panic!("Kafka test subscription failed"));
}

pub fn commit_offset(consumer: &BaseConsumer, topic: &str, partition: i32, next_offset: i64) {
    let mut offsets = TopicPartitionList::new();

    offsets
        .add_partition_offset(topic, partition, Offset::Offset(next_offset))
        .unwrap_or_else(|_| panic!("Kafka test offset list construction failed"));

    consumer
        .commit(&offsets, CommitMode::Sync)
        .unwrap_or_else(|_| panic!("Kafka synchronous offset commit was not confirmed"));
}

pub fn committed_offset(consumer: &BaseConsumer, topic: &str, partition: i32) -> Offset {
    let mut partitions = TopicPartitionList::new();

    partitions.add_partition(topic, partition);

    let committed = consumer
        .committed_offsets(partitions, Duration::from_secs(10))
        .unwrap_or_else(|_| panic!("Kafka committed-cursor query failed"));

    committed
        .find_partition(topic, partition)
        .map(|entry| entry.offset())
        .unwrap_or(Offset::Invalid)
}
