//! Opt-in composition of the generic partitioned consumer with the Kafka source.
//!
//! Every scenario uses a fresh consumer group seeded at the topic's high watermark, a static
//! instance identity, and the test-local transactional inbox. Committed cursors are read by a
//! separate `read_committed` group reader, so each assertion observes the broker's stable
//! transactional offset commit rather than local source state.

mod support;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer as _};
use rdkafka::producer::{BaseProducer, Producer as _};
use rdkafka::topic_partition_list::Offset;
use rdkafka::{ClientConfig, error::RDKafkaErrorCode};
use sisa_messaging::{ErrorClassifier, FailureKind, MessageId};
use sisa_messaging_consumer::{ConsumerError, ConsumerErrorKind, ConsumerExit, ConsumerSettings};
use sisa_messaging_kafka::{
    KafkaClient, KafkaShutdownOutcome, KafkaSourceError, KafkaSourceErrorKind, KafkaSourceShutdown,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use support::inbox::FakeInbox;
use support::{
    BROKERS_ENV, PARTITIONED_TOPIC_ENV, ScriptedHandler, Step, TEST_TIMEOUT, TOPIC_ENV,
    committed_cursor, consumer_client, consumer_settings, delivery_source, eventually,
    new_producer, partitioned_consumer, publish_order, required_env, scope, seed_group_at_end,
    send_in_transaction, transactional_record_producer, try_committed_cursor, unique,
};

struct Fixture {
    brokers: String,

    topic: String,

    group: String,

    instance: String,

    client: KafkaClient,

    producer: rdkafka::producer::FutureProducer,

    inbox: FakeInbox,

    handler: ScriptedHandler,

    seeded: HashMap<i32, i64>,
}

struct Running {
    handle: JoinHandle<Result<ConsumerExit, ConsumerError>>,

    cancel: CancellationToken,

    shutdown: KafkaSourceShutdown,
}

impl Running {
    async fn cancel(self) -> Result<ConsumerExit, ConsumerError> {
        self.cancel.cancel();
        let exit = join(self.handle).await;
        assert_closed(self.shutdown).await;

        exit
    }
}

async fn join(
    handle: JoinHandle<Result<ConsumerExit, ConsumerError>>,
) -> Result<ConsumerExit, ConsumerError> {
    match tokio::time::timeout(TEST_TIMEOUT, handle).await {
        Ok(Ok(exit)) => exit,
        Ok(Err(_)) => panic!("consumer task panicked"),
        Err(_) => panic!("consumer did not stop before the test deadline"),
    }
}

async fn assert_closed(shutdown: KafkaSourceShutdown) {
    let outcome =
        tokio::time::timeout(TEST_TIMEOUT, std::future::IntoFuture::into_future(shutdown))
            .await
            .unwrap_or_else(|_| panic!("Kafka source shutdown did not finish"));

    assert_eq!(outcome, KafkaShutdownOutcome::Closed);
}

impl Fixture {
    fn new(topic_env: &str) -> Self {
        let brokers = required_env(BROKERS_ENV);
        let topic = required_env(topic_env);
        let group = unique("sisa-kafka-consumer");
        let seeded = seed_group_at_end(&brokers, &group, &topic);

        Self {
            client: consumer_client(&brokers),
            producer: new_producer(&brokers),
            brokers,
            topic,
            group,
            instance: "member-a".to_owned(),
            inbox: FakeInbox::new(3),
            handler: ScriptedHandler::default(),
            seeded,
        }
    }

    fn start(&self, settings: ConsumerSettings) -> Running {
        let source = delivery_source(&self.client, &self.group, &self.instance, &self.topic);
        let shutdown = source.shutdown_handle();
        let consumer = partitioned_consumer(source, &self.inbox, &self.handler, settings);
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(consumer.run_partitioned(cancel.clone()));

        Running {
            handle,
            cancel,
            shutdown,
        }
    }

    async fn publish(
        &self,
        partition: Option<i32>,
        message_id: MessageId,
        label: &str,
    ) -> (i32, i64) {
        publish_order(&self.producer, &self.topic, partition, message_id, label).await
    }

    fn committed(&self, partition: i32) -> Offset {
        committed_cursor(&self.brokers, &self.group, &self.topic, partition)
    }

    fn seeded(&self, partition: i32) -> i64 {
        self.seeded.get(&partition).copied().unwrap_or(0)
    }

    async fn await_committed(&self, partition: i32, next: i64) {
        eventually("the committed cursor reaches the expected offset", || {
            self.committed(partition) == Offset::Offset(next)
        })
        .await;
    }
}

fn source_error(error: &ConsumerError) -> Option<KafkaSourceError> {
    error
        .provider_source()
        .and_then(|source| source.downcast_ref::<KafkaSourceError>())
        .copied()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn committed_record_advances_through_transactional_offset() {
    let fixture = Fixture::new(TOPIC_ENV);
    let message_id = MessageId::new();
    let (partition, offset) = fixture.publish(None, message_id, "committed").await;

    assert_eq!(
        fixture.committed(partition),
        Offset::Offset(fixture.seeded(partition))
    );

    let running = fixture.start(consumer_settings(4));
    fixture.await_committed(partition, offset + 1).await;

    assert_eq!(fixture.inbox.completions(&scope(), message_id), 1);
    assert_eq!(fixture.inbox.effects(), vec!["committed".to_owned()]);
    assert_eq!(fixture.handler.invocations("committed"), 1);

    assert!(matches!(
        running.cancel().await,
        Ok(ConsumerExit::Cancelled)
    ));

    assert_eq!(fixture.inbox.live(), 0);
    assert_eq!(fixture.committed(partition), Offset::Offset(offset + 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn partition_order_has_no_offset_gaps_across_aborted_transactions() {
    let fixture = Fixture::new(TOPIC_ENV);
    let writer: BaseProducer = transactional_record_producer(&fixture.brokers);
    let timeout = Duration::from_secs(15);
    let labels = ["c1", "c2", "a1", "a2", "c3"];

    let commit = |writer: &BaseProducer, labels: &[&str], abort: bool| {
        writer
            .begin_transaction()
            .unwrap_or_else(|_| panic!("writer begin failed"));

        for label in labels {
            send_in_transaction(writer, &fixture.topic, 0, MessageId::new(), label);
        }

        // Aborted records must reach the log so the reader skips real aborted data; a
        // BaseProducer serves delivery reports only while flushing.
        writer
            .flush(timeout)
            .unwrap_or_else(|_| panic!("writer flush did not finish"));

        let resolved = if abort {
            writer.abort_transaction(timeout)
        } else {
            writer.commit_transaction(timeout)
        };

        resolved.unwrap_or_else(|error| {
            panic!(
                "writer transaction did not resolve: {:?}",
                error.rdkafka_error_code()
            )
        });
    };

    commit(&writer, &labels[..2], false);
    commit(&writer, &labels[2..4], true);
    commit(&writer, &labels[4..], false);
    let (partition, last) = fixture.publish(Some(0), MessageId::new(), "c4").await;

    let running = fixture.start(consumer_settings(4));
    fixture.await_committed(partition, last + 1).await;

    // Aborted records and transaction markers leave offset holes; every committed record is
    // handled once, in order, and none is skipped.
    assert_eq!(fixture.handler.log(), vec!["c1", "c2", "c3", "c4"]);
    assert_eq!(fixture.inbox.effects(), vec!["c1", "c2", "c3", "c4"]);

    assert!(matches!(
        running.cancel().await,
        Ok(ConsumerExit::Cancelled)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn failed_inbox_commit_leaves_offset_and_replays_on_restart() {
    let fixture = Fixture::new(TOPIC_ENV);
    let message_id = MessageId::new();
    let (partition, offset) = fixture.publish(None, message_id, "retry").await;
    fixture.inbox.fail_next_commits(1);

    let first = fixture.start(consumer_settings(4));

    let error = match join(first.handle).await {
        Err(error) => error,
        Ok(exit) => panic!("expected an unresolved partition, got {exit:?}"),
    };

    assert_eq!(error.kind(), ConsumerErrorKind::PartitionUnresolved);
    assert_closed(first.shutdown).await;
    assert_eq!(fixture.committed(partition), Offset::Offset(offset));
    assert_eq!(fixture.inbox.completions(&scope(), message_id), 0);

    let restarted = fixture.start(consumer_settings(4));
    fixture.await_committed(partition, offset + 1).await;

    assert_eq!(fixture.handler.invocations("retry"), 2);
    assert_eq!(fixture.inbox.completions(&scope(), message_id), 1);
    assert_eq!(fixture.inbox.effects(), vec!["retry".to_owned()]);

    assert!(matches!(
        restarted.cancel().await,
        Ok(ConsumerExit::Cancelled)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn completed_duplicate_advances_without_handler() {
    let fixture = Fixture::new(TOPIC_ENV);
    let message_id = MessageId::new();
    fixture.inbox.mark_completed(&scope(), message_id);
    let (partition, offset) = fixture.publish(None, message_id, "duplicate").await;

    let running = fixture.start(consumer_settings(4));
    fixture.await_committed(partition, offset + 1).await;

    assert_eq!(fixture.handler.total_invocations(), 0);
    assert!(fixture.inbox.effects().is_empty());

    assert!(matches!(
        running.cancel().await,
        Ok(ConsumerExit::Cancelled)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker and a pre-provisioned multi-partition test topic"]
async fn in_flight_and_lane_bounds_pause_polling_without_gaps() {
    const PER_PARTITION: usize = 10;

    let fixture = Fixture::new(PARTITIONED_TOPIC_ENV);

    let partitions: Vec<i32> = {
        let mut partitions: Vec<i32> = fixture.seeded.keys().copied().collect();
        partitions.sort_unstable();

        partitions
    };

    assert!(
        partitions.len() >= 2,
        "the partitioned topic needs several partitions"
    );

    let mut last = HashMap::new();

    for index in 0..PER_PARTITION {
        for partition in &partitions {
            let label = format!("p{partition}-{index}");

            fixture
                .handler
                .script(&label, &[Step::Sleep(Duration::from_millis(15))]);

            let (_, offset) = fixture
                .publish(Some(*partition), MessageId::new(), &label)
                .await;

            last.insert(*partition, offset);
        }
    }

    let running = fixture.start(consumer_settings(2));

    for (partition, offset) in &last {
        fixture.await_committed(*partition, offset + 1).await;
    }

    let log = fixture.handler.log();

    assert_eq!(
        log.len(),
        partitions.len() * PER_PARTITION,
        "no record is handled twice"
    );

    assert!(
        fixture.handler.peak() <= 2,
        "max_in_flight bounds concurrent handlers"
    );

    for partition in &partitions {
        let prefix = format!("p{partition}-");

        let order: Vec<usize> = log
            .iter()
            .filter_map(|label| label.strip_prefix(&prefix))
            .filter_map(|index| index.parse().ok())
            .collect();

        assert_eq!(
            order,
            (0..PER_PARTITION).collect::<Vec<_>>(),
            "partition {partition} is handled in offset order without gaps"
        );
    }

    assert!(matches!(
        running.cancel().await,
        Ok(ConsumerExit::Cancelled)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn cancellation_during_work_leaves_offset_uncommitted() {
    let fixture = Fixture::new(TOPIC_ENV);
    let message_id = MessageId::new();
    fixture.handler.script("blocked", &[Step::Gate]);
    let (partition, offset) = fixture.publish(None, message_id, "blocked").await;

    let running = fixture.start(consumer_settings(4));

    eventually("the handler is running", || {
        fixture.handler.invocations("blocked") == 1
    })
    .await;

    assert!(matches!(
        running.cancel().await,
        Ok(ConsumerExit::Cancelled)
    ));

    assert_eq!(fixture.inbox.completions(&scope(), message_id), 0);
    assert_eq!(fixture.inbox.live(), 0);
    assert_eq!(fixture.committed(partition), Offset::Offset(offset));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn advance_timeout_reconciles_and_continues_once() {
    let fixture = Fixture::new(TOPIC_ENV);
    let first_id = MessageId::new();
    fixture.handler.script("fenced", &[Step::Gate]);
    let (partition, first) = fixture.publish(None, first_id, "fenced").await;

    // A settlement bound far below the source's reconciliation time makes the consumer give up
    // on the advance while the source is still establishing its outcome.
    let mut settings = consumer_settings(4);
    settings.settlement_timeout = Duration::from_millis(250);
    let running = fixture.start(settings);

    eventually("the handler is running", || {
        fixture.handler.invocations("fenced") == 1
    })
    .await;

    // A second producer with the source's transactional identity bumps the epoch, so the
    // source's next offset commit fails as fenced and its outcome is indeterminate.
    let transactional_id = sisa_messaging_kafka::KafkaConsumerSettings::new(
        fixture.group.as_str(),
        fixture.instance.as_str(),
        [fixture.topic.as_str()],
    )
    .unwrap_or_else(|_| panic!("settings are valid"))
    .transactional_id();

    let zombie: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &fixture.brokers)
        .set("transactional.id", &transactional_id)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .create()
        .unwrap_or_else(|_| panic!("zombie producer construction failed"));

    zombie
        .init_transactions(Duration::from_secs(15))
        .unwrap_or_else(|_| panic!("zombie producer initialization failed"));

    fixture.handler.release(1);
    fixture.await_committed(partition, first + 1).await;

    let second_id = MessageId::new();
    let (_, second) = fixture.publish(None, second_id, "after").await;
    fixture.await_committed(partition, second + 1).await;

    assert_eq!(fixture.handler.invocations("fenced"), 1);
    assert_eq!(fixture.handler.invocations("after"), 1);
    assert_eq!(fixture.inbox.completions(&scope(), first_id), 1);
    assert_eq!(fixture.inbox.completions(&scope(), second_id), 1);

    assert!(
        !running.handle.is_finished(),
        "an indeterminate advance does not stop the run"
    );

    // The source re-established its outcome behind a successor epoch, which fenced the zombie:
    // the zombie's next transactional write is rejected.
    zombie
        .begin_transaction()
        .unwrap_or_else(|_| panic!("zombie begin is local"));

    send_in_transaction(
        &zombie,
        &fixture.topic,
        partition,
        MessageId::new(),
        "zombie",
    );

    let fenced = zombie.commit_transaction(Duration::from_secs(15));

    assert!(
        fenced.is_err(),
        "the successor epoch fences the zombie producer"
    );

    assert!(matches!(
        running.cancel().await,
        Ok(ConsumerExit::Cancelled)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker that permits topic creation through the admin API"]
async fn fresh_group_consumes_records_published_before_first_assignment() {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    // A dedicated topic holds only this test's records, so a group with no committed offset
    // can be observed consuming from the start of the log. Automatic topic creation stays off.
    let brokers = required_env(BROKERS_ENV);
    let topic = unique("sisa-kafka-fresh-group");

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap_or_else(|_| panic!("admin client construction failed"));

    let created = admin
        .create_topics(
            [&NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap_or_else(|_| panic!("topic creation request failed"));

    assert!(created.iter().all(Result::is_ok), "topic creation failed");

    let producer = new_producer(&brokers);
    let labels = ["early-1", "early-2", "early-3", "early-4", "early-5"];
    let mut last = 0;

    // Every record exists before any member of the brand-new, unseeded group joins.
    for label in labels {
        let (_, offset) = publish_order(&producer, &topic, Some(0), MessageId::new(), label).await;
        last = offset;
    }

    let group = unique("sisa-kafka-fresh-group");
    let inbox = FakeInbox::new(3);
    let handler = ScriptedHandler::default();
    let client = consumer_client(&brokers);
    let source = delivery_source(&client, &group, "member-a", &topic);
    let shutdown = source.shutdown_handle();
    let consumer = partitioned_consumer(source, &inbox, &handler, consumer_settings(4));
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(consumer.run_partitioned(cancel.clone()));

    eventually("the fresh group committed past its last record", || {
        try_committed_cursor(&brokers, &group, &topic, 0) == Some(Offset::Offset(last + 1))
    })
    .await;

    assert_eq!(handler.log(), labels.to_vec());
    assert_eq!(inbox.effects(), labels.to_vec());

    cancel.cancel();
    assert!(matches!(join(handle).await, Ok(ConsumerExit::Cancelled)));
    assert_closed(shutdown).await;

    let _ = admin.delete_topics(&[&topic], &AdminOptions::new()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Kafka broker"]
async fn missing_topic_fails_open_permanently() {
    let brokers = required_env(BROKERS_ENV);
    let topic = unique("sisa-kafka-missing");
    let client = consumer_client(&brokers);

    let source = delivery_source(
        &client,
        &unique("sisa-kafka-missing-group"),
        "member-a",
        &topic,
    );

    let shutdown = source.shutdown_handle();

    let consumer = partitioned_consumer(
        source,
        &FakeInbox::new(3),
        &ScriptedHandler::default(),
        consumer_settings(1),
    );

    let started = Instant::now();

    let error = match consumer.run_partitioned(CancellationToken::new()).await {
        Err(error) => error,
        Ok(exit) => panic!("expected a permanent open failure, got {exit:?}"),
    };

    assert_eq!(error.kind(), ConsumerErrorKind::SourceOpen);
    assert_eq!(error.classify(), FailureKind::Permanent);

    assert_eq!(
        source_error(&error).map(KafkaSourceError::kind),
        Some(KafkaSourceErrorKind::MissingTopic)
    );

    assert!(started.elapsed() < TEST_TIMEOUT);
    assert_closed(shutdown).await;

    // The source never creates topics.
    let reader: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("allow.auto.create.topics", "false")
        .create()
        .unwrap_or_else(|_| panic!("metadata reader construction failed"));

    let metadata = reader
        .fetch_metadata(Some(&topic), TEST_TIMEOUT)
        .unwrap_or_else(|_| panic!("metadata request failed"));

    let entry = metadata
        .topics()
        .iter()
        .find(|entry| entry.name() == topic)
        .unwrap_or_else(|| panic!("metadata omitted the requested topic"));

    assert_eq!(
        entry.error().map(RDKafkaErrorCode::from),
        Some(RDKafkaErrorCode::UnknownTopicOrPartition)
    );
}
