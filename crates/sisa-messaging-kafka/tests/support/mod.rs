#![allow(dead_code)]

pub mod inbox;

use std::collections::{HashMap, VecDeque};
use std::env;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer as _};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::{ClientConfig, Message as _};
use sisa_messaging::{
    ContentType, Envelope, EnvelopeMapper, ErrorClassifier, FailureKind, Message, MessageId,
    Metadata, SerializedEnvelope, Serializer,
};
use sisa_messaging_consumer::{Consumer, ConsumerHandler, ConsumerSettings};
use sisa_messaging_inbox::InboxScope;
use sisa_messaging_kafka::{
    KafkaClient, KafkaClientSettings, KafkaConsumerSettings, KafkaDeliverySource,
    KafkaEnvelopeMapper, KafkaRecord,
};
use tokio::sync::Semaphore;

use inbox::{FakeInbox, FakeTransaction};

pub const BROKERS_ENV: &str = "SISA_KAFKA_BOOTSTRAP_SERVERS";
pub const TOPIC_ENV: &str = "SISA_KAFKA_TEST_TOPIC";
/// A pre-provisioned topic with several partitions for ordering and rebalance scenarios.
pub const PARTITIONED_TOPIC_ENV: &str = "SISA_KAFKA_TEST_PARTITIONED_TOPIC";
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

// ---------------------------------------------------------------------------------------------
// Partitioned consumer composition.

/// Test message carried as UTF-8 text so scenarios select handler behavior by label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Order {
    pub label: String,
}

impl Message for Order {
    const TYPE: &'static str = "kafka-order-created";
    const VERSION: u32 = 1;
}

#[derive(Debug)]
pub struct CodecError;

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("order codec failed")
    }
}

impl Error for CodecError {}

impl ErrorClassifier for CodecError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// Plain-text codec for [`Order`].
#[derive(Clone, Copy, Debug, Default)]
pub struct OrderCodec;

impl Serializer<Order> for OrderCodec {
    type Error = CodecError;

    fn serialize(&self, envelope: &Envelope<Order>) -> Result<SerializedEnvelope, Self::Error> {
        Ok(SerializedEnvelope {
            message_id: envelope.message_id(),
            message_type: envelope.message_type().clone(),
            message_version: envelope.message_version(),
            content_type: ContentType::new("text/plain").map_err(|_| CodecError)?,
            payload: envelope.payload().label.clone().into_bytes(),
            metadata: envelope.metadata().clone(),
            ordering_key: None,
        })
    }

    fn deserialize(&self, envelope: SerializedEnvelope) -> Result<Envelope<Order>, Self::Error> {
        if envelope.message_type.as_str() != Order::TYPE
            || envelope.message_version != Order::VERSION
        {
            return Err(CodecError);
        }

        let label = String::from_utf8(envelope.payload).map_err(|_| CodecError)?;

        Envelope::new(envelope.message_id, Order { label }, envelope.metadata)
            .map_err(|_| CodecError)
    }
}

/// One scripted handler invocation; unscripted invocations succeed.
#[derive(Clone, Copy, Debug)]
pub enum Step {
    Succeed,

    Fail(FailureKind),

    /// Sleeps, then succeeds.
    Sleep(Duration),

    /// Waits for a permit from [`ScriptedHandler::release`], then succeeds.
    Gate,
}

/// Handler failure whose rendering never includes message content.
#[derive(Debug)]
pub struct HandlerError {
    kind: FailureKind,
}

impl fmt::Display for HandlerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("order handler failed")
    }
}

impl Error for HandlerError {}

impl ErrorClassifier for HandlerError {
    fn classify(&self) -> FailureKind {
        self.kind
    }
}

#[derive(Default)]
struct HandlerState {
    scripts: Mutex<HashMap<String, VecDeque<Step>>>,

    invocations: Mutex<Vec<String>>,

    active: AtomicUsize,

    peak: AtomicUsize,
}

/// Records invocations and concurrency, then applies each label's scripted steps in order.
#[derive(Clone)]
pub struct ScriptedHandler {
    state: Arc<HandlerState>,

    gate: Arc<Semaphore>,
}

impl Default for ScriptedHandler {
    fn default() -> Self {
        Self {
            state: Arc::default(),
            gate: Arc::new(Semaphore::new(0)),
        }
    }
}

struct Active<'a>(&'a AtomicUsize);

impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl ScriptedHandler {
    pub fn script(&self, label: &str, steps: &[Step]) {
        lock(&self.state.scripts).insert(label.to_owned(), steps.iter().copied().collect());
    }

    /// Lets `count` gated invocations proceed.
    pub fn release(&self, count: usize) {
        self.gate.add_permits(count);
    }

    pub fn invocations(&self, label: &str) -> usize {
        lock(&self.state.invocations)
            .iter()
            .filter(|invoked| invoked.as_str() == label)
            .count()
    }

    /// Every invocation's label in invocation order.
    pub fn log(&self) -> Vec<String> {
        lock(&self.state.invocations).clone()
    }

    pub fn total_invocations(&self) -> usize {
        lock(&self.state.invocations).len()
    }

    pub fn peak(&self) -> usize {
        self.state.peak.load(Ordering::SeqCst)
    }

    fn next_step(&self, label: &str) -> Step {
        lock(&self.state.scripts)
            .get_mut(label)
            .and_then(VecDeque::pop_front)
            .unwrap_or(Step::Succeed)
    }
}

impl ConsumerHandler<Order, FakeTransaction> for ScriptedHandler {
    type Error = HandlerError;

    async fn handle(
        &self,
        tx: &mut FakeTransaction,
        envelope: &Envelope<Order>,
    ) -> Result<(), Self::Error> {
        let label = envelope.payload().label.clone();
        let active = self.state.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _active = Active(&self.state.active);
        self.state.peak.fetch_max(active, Ordering::SeqCst);
        lock(&self.state.invocations).push(label.clone());

        match self.next_step(&label) {
            Step::Succeed => {}
            Step::Fail(kind) => return Err(HandlerError { kind }),
            Step::Sleep(duration) => tokio::time::sleep(duration).await,
            Step::Gate => {
                if let Ok(permit) = self.gate.acquire().await {
                    permit.forget();
                }
            }
        }

        tx.record_effect(label);

        Ok(())
    }
}

/// The Kafka-typed partitioned consumer every scenario runs.
pub type KafkaConsumer = Consumer<
    Order,
    (
        KafkaDeliverySource,
        KafkaEnvelopeMapper,
        OrderCodec,
        FakeInbox,
        ScriptedHandler,
    ),
>;

pub fn scope() -> InboxScope {
    InboxScope::new("kafka-consumer-tests").unwrap_or_else(|_| panic!("test scope is valid"))
}

/// A client whose advanced properties carry over to consumer sources.
pub fn consumer_client(brokers: &str) -> KafkaClient {
    let config = KafkaClientSettings::new([brokers])
        .with_advanced_property("allow.auto.create.topics", "false")
        .and_then(|config| config.with_advanced_property("session.timeout.ms", "6000"))
        .and_then(|config| config.with_advanced_property("heartbeat.interval.ms", "500"))
        .and_then(|config| config.with_advanced_property("max.poll.interval.ms", "30000"))
        .unwrap_or_else(|_| panic!("Kafka test consumer configuration is invalid"));

    KafkaClient::start(config).unwrap_or_else(|_| panic!("Kafka test client construction failed"))
}

pub fn delivery_source(
    client: &KafkaClient,
    group: &str,
    instance: &str,
    topic: &str,
) -> KafkaDeliverySource {
    let settings = KafkaConsumerSettings::new(group, instance, [topic])
        .and_then(|settings| settings.with_shutdown_timeout(Duration::from_secs(15)))
        .unwrap_or_else(|_| panic!("Kafka test consumer settings are invalid"));

    client
        .delivery_source(settings)
        .unwrap_or_else(|_| panic!("Kafka test source construction failed"))
}

pub fn consumer_settings(max_in_flight: usize) -> ConsumerSettings {
    let mut settings = ConsumerSettings::default();

    settings.max_in_flight =
        std::num::NonZeroUsize::new(max_in_flight).unwrap_or(std::num::NonZeroUsize::MIN);

    settings.source_timeout = Duration::from_secs(30);
    settings.settlement_timeout = Duration::from_secs(30);

    settings
}

pub fn partitioned_consumer(
    source: KafkaDeliverySource,
    inbox: &FakeInbox,
    handler: &ScriptedHandler,
    settings: ConsumerSettings,
) -> KafkaConsumer {
    Consumer::new_partitioned(
        source,
        KafkaEnvelopeMapper,
        OrderCodec,
        inbox.clone(),
        scope(),
        handler.clone(),
        settings,
    )
    .unwrap_or_else(|_| panic!("Kafka test consumer settings are invalid"))
}

/// Encodes one order envelope as a Kafka record.
pub fn order_record(message_id: MessageId, label: &str) -> KafkaRecord {
    let envelope = Envelope::new(
        message_id,
        Order {
            label: label.to_owned(),
        },
        Metadata::default(),
    )
    .unwrap_or_else(|_| panic!("test envelope is valid"));

    let serialized = OrderCodec
        .serialize(&envelope)
        .unwrap_or_else(|_| panic!("test envelope serializes"));

    KafkaEnvelopeMapper
        .encode(&serialized)
        .unwrap_or_else(|_| panic!("test envelope maps to a Kafka record"))
}

fn owned_headers(record: &KafkaRecord) -> OwnedHeaders {
    record.headers.iter().fold(
        OwnedHeaders::new_with_capacity(record.headers.len()),
        |headers, header| {
            headers.insert(Header {
                key: &header.name,
                value: header.value.as_deref(),
            })
        },
    )
}

/// Publishes one order, optionally to an explicit partition; returns its partition and offset.
pub async fn publish_order(
    producer: &FutureProducer,
    topic: &str,
    partition: Option<i32>,
    message_id: MessageId,
    label: &str,
) -> (i32, i64) {
    let record = order_record(message_id, label);

    let mut future = FutureRecord::<(), [u8]>::to(topic)
        .payload(&record.payload)
        .headers(owned_headers(&record));

    if let Some(partition) = partition {
        future = future.partition(partition);
    }

    match producer.send(future, Duration::from_secs(5)).await {
        Ok(delivery) => (delivery.partition, delivery.offset),
        Err(_) => panic!("Kafka test order was not confirmed"),
    }
}

/// A transactional producer for writing committed and aborted records.
pub fn transactional_record_producer(brokers: &str) -> rdkafka::producer::BaseProducer {
    use rdkafka::producer::Producer as _;

    let producer: rdkafka::producer::BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set(
            "transactional.id",
            format!("sisa-kafka-writer-{}", MessageId::new()),
        )
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("message.timeout.ms", "15000")
        .create()
        .unwrap_or_else(|_| panic!("Kafka transactional writer construction failed"));

    producer
        .init_transactions(Duration::from_secs(15))
        .unwrap_or_else(|_| panic!("Kafka transactional writer initialization failed"));

    producer
}

/// Sends one order inside the writer's open transaction.
pub fn send_in_transaction(
    producer: &rdkafka::producer::BaseProducer,
    topic: &str,
    partition: i32,
    message_id: MessageId,
    label: &str,
) {
    let record = order_record(message_id, label);

    let future = rdkafka::producer::BaseRecord::<(), [u8]>::to(topic)
        .partition(partition)
        .payload(&record.payload)
        .headers(owned_headers(&record));

    producer
        .send(future)
        .unwrap_or_else(|_| panic!("Kafka transactional writer rejected a record"));
}

fn group_reader(brokers: &str, group: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .set("allow.auto.create.topics", "false")
        .create()
        .unwrap_or_else(|_| panic!("Kafka group reader construction failed"))
}

/// Commits every partition's high watermark for a fresh group, so it starts at the log end.
pub fn seed_group_at_end(brokers: &str, group: &str, topic: &str) -> HashMap<i32, i64> {
    let reader = group_reader(brokers, group);

    let metadata = reader
        .fetch_metadata(Some(topic), TEST_TIMEOUT)
        .unwrap_or_else(|_| panic!("Kafka topic metadata was unavailable"));

    let partitions: Vec<i32> = metadata
        .topics()
        .iter()
        .flat_map(|entry| entry.partitions().iter().map(|partition| partition.id()))
        .collect();

    let mut offsets = TopicPartitionList::new();
    let mut seeded = HashMap::new();

    for partition in partitions {
        let (_, high) = reader
            .fetch_watermarks(topic, partition, TEST_TIMEOUT)
            .unwrap_or_else(|_| panic!("Kafka watermarks were unavailable"));

        offsets
            .add_partition_offset(topic, partition, Offset::Offset(high))
            .unwrap_or_else(|_| panic!("Kafka offset list construction failed"));

        seeded.insert(partition, high);
    }

    reader
        .commit(&offsets, CommitMode::Sync)
        .unwrap_or_else(|_| panic!("Kafka seed commit was not confirmed"));

    seeded
}

/// The group's committed cursor, read stably under `read_committed`.
pub fn committed_cursor(brokers: &str, group: &str, topic: &str, partition: i32) -> Offset {
    let reader = group_reader(brokers, group);
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(topic, partition);

    let committed = reader
        .committed_offsets(partitions, TEST_TIMEOUT)
        .unwrap_or_else(|_| panic!("Kafka committed-cursor query failed"));

    committed
        .find_partition(topic, partition)
        .map_or(Offset::Invalid, |entry| entry.offset())
}

/// Polls `condition` every 50 ms until it holds or `TEST_TIMEOUT` expires.
pub async fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + TEST_TIMEOUT;

    while Instant::now() < deadline {
        if condition() {
            return;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("condition not reached before the test deadline: {what}");
}

pub fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", MessageId::new())
}
