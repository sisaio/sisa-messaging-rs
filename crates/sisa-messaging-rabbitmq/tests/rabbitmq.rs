//! Run ignored tests against a real RabbitMQ 4 broker with `RABBITMQ_URL` set. Fault tests also
//! need `RABBITMQ_CONTAINER`, the Docker container name used for `rabbitmqctl`.
//!
//! Every test declares uniquely named topology. Fault tests that raise a broker-wide alarm or close
//! every connection take an exclusive lock while every other test holds a shared one; they run on
//! a multi-threaded runtime and call `docker exec` through `spawn_blocking` so lapin I/O keeps
//! running.

use std::{
    collections::BTreeSet,
    num::{NonZeroU16, NonZeroU32},
    process::Command,
    time::{Duration, Instant},
};

use futures_util::FutureExt;
use lapin::{
    Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{
        BasicGetOptions, ConfirmSelectOptions, ExchangeDeclareOptions, ExchangeDeleteOptions,
        QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
    },
    types::{AMQPValue, FieldTable, LongString, ShortString},
};
use sisa_messaging::{
    ContentType, Delivery, EnvelopeMapper, IndividualCapability, IndividualDeliverySource,
    IndividualSettlement, IndividualSettlementError, IndividualSourceOpenError,
    IndividualSourceRequirement, IndividualSourceRequirements, MessageId, MessageType, Metadata,
    Publisher, SerializedEnvelope,
};
use sisa_messaging_rabbitmq::{
    ExchangeName, RabbitMqDelivery, RabbitMqDeliverySource, RabbitMqError, RabbitMqMapper,
    RabbitMqPublisher, RabbitMqPublisherSettings, RabbitMqSettlement, RabbitMqSourceSettings,
    TypeRouteResolver,
};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

static BROKER: RwLock<()> = RwLock::const_new(());

const MESSAGE_TYPE: &str = "event";
const ROUTING_KEY: &str = "event.v1";
const WAIT: Duration = Duration::from_secs(5);

fn url() -> String {
    std::env::var("RABBITMQ_URL").expect("RABBITMQ_URL is required for this ignored test")
}

fn container() -> String {
    std::env::var("RABBITMQ_CONTAINER")
        .expect("RABBITMQ_CONTAINER is required for this ignored fault test")
}

/// Runs `rabbitmqctl` in the broker container and returns its trimmed standard output.
fn rabbitmqctl_blocking(args: &[String]) -> String {
    let output = Command::new("docker")
        .arg("exec")
        .arg(container())
        .arg("rabbitmqctl")
        .args(args)
        .output()
        .expect("docker must be available for fault tests");

    assert!(output.status.success(), "rabbitmqctl command failed");

    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

async fn rabbitmqctl(args: &[&str]) -> String {
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();

    tokio::task::spawn_blocking(move || rabbitmqctl_blocking(&args))
        .await
        .unwrap()
}

/// Waits until the broker reports any resource alarm (`active`) or none.
async fn await_alarms(active: bool) {
    let deadline = Instant::now() + Duration::from_secs(10);

    while (rabbitmqctl(&["-q", "eval", "rabbit_alarm:get_alarms()."]).await != "[]") != active {
        assert!(
            Instant::now() < deadline,
            "broker alarm state did not change"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn suffix() -> String {
    MessageId::new().to_string().replace('-', "")
}

fn envelope(payload: Vec<u8>) -> SerializedEnvelope {
    SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new(MESSAGE_TYPE).unwrap(),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream").unwrap(),
        payload,
        metadata: Metadata::default(),
        ordering_key: None,
    }
}

fn publisher_settings(timeout: Duration) -> RabbitMqPublisherSettings {
    RabbitMqPublisherSettings {
        publish_timeout: timeout,
        max_message_size: NonZeroU32::new(1 << 20).unwrap(),
    }
}

fn long(value: &str) -> AMQPValue {
    AMQPValue::LongString(LongString::from(value))
}

/// Shared broker access for ordinary tests, exclusive for broker-global faults.
enum Guard {
    Shared(#[allow(dead_code)] RwLockReadGuard<'static, ()>),

    Exclusive(#[allow(dead_code)] RwLockWriteGuard<'static, ()>),
}

/// Test-owned topology: one direct exchange bound to one queue, removed on completion.
struct Fixture {
    _guard: Guard,

    connection: Connection,

    admin: Channel,

    exchange: String,

    queue: String,
}

impl Fixture {
    async fn new(queue_arguments: FieldTable) -> Self {
        let guard = Guard::Shared(BROKER.read().await);

        Self::with_guard(guard, queue_arguments).await
    }

    async fn with_guard(guard: Guard, queue_arguments: FieldTable) -> Self {
        let connection = connect().await;
        let admin = connection.create_channel().await.unwrap();
        let suffix = suffix();
        let exchange = format!("sisa.test.{suffix}");
        let queue = format!("sisa.test.{suffix}");

        admin
            .exchange_declare(
                exchange.as_str().into(),
                ExchangeKind::Direct,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        let mut arguments = queue_arguments;
        arguments.insert("x-expires".into(), AMQPValue::LongInt(120_000));

        admin
            .queue_declare(
                queue.as_str().into(),
                QueueDeclareOptions::default(),
                arguments,
            )
            .await
            .unwrap();

        admin
            .queue_bind(
                queue.as_str().into(),
                exchange.as_str().into(),
                ROUTING_KEY.into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        Self {
            _guard: guard,
            connection,
            admin,
            exchange,
            queue,
        }
    }

    async fn publisher(&self) -> RabbitMqPublisher<TypeRouteResolver> {
        let channel = confirmed_channel(&self.connection).await;

        RabbitMqPublisher::new(
            channel,
            TypeRouteResolver::new(ExchangeName::new(self.exchange.clone()).unwrap()),
            publisher_settings(WAIT),
        )
        .unwrap()
    }

    async fn source(&self, prefetch: u16) -> (Channel, RabbitMqDeliverySource) {
        let channel = self.connection.create_channel().await.unwrap();

        let source = RabbitMqDeliverySource::new(
            channel.clone(),
            RabbitMqSourceSettings {
                queue: self.queue.clone(),
                prefetch: NonZeroU16::new(prefetch).unwrap(),
            },
        )
        .unwrap();

        (channel, source)
    }

    async fn opened_source(&self, prefetch: u16) -> (Channel, RabbitMqDeliverySource) {
        let (channel, mut source) = self.source(prefetch).await;

        source
            .open(IndividualSourceRequirements::new().requiring_terminal_discard())
            .await
            .unwrap();

        (channel, source)
    }

    /// Returns the ready-message and consumer counts from a passive declaration.
    async fn counts(&self) -> (u32, u32) {
        let queue = self
            .admin
            .queue_declare(
                self.queue.as_str().into(),
                QueueDeclareOptions {
                    passive: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();

        (queue.message_count(), queue.consumer_count())
    }

    /// Waits for the broker to report `ready` messages; requeue after a channel close is
    /// asynchronous inside the broker.
    async fn await_ready(&self, ready: u32) {
        let deadline = Instant::now() + WAIT;

        while self.counts().await.0 != ready {
            assert!(
                Instant::now() < deadline,
                "queue did not reach expected depth"
            );

            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Replaces a connection the broker closed, keeping the guard and topology.
    async fn reconnect(self) -> Self {
        let connection = connect().await;
        let admin = connection.create_channel().await.unwrap();

        Self {
            connection,
            admin,
            ..self
        }
    }

    async fn cleanup(self) {
        let _ = self
            .admin
            .queue_delete(self.queue.as_str().into(), QueueDeleteOptions::default())
            .await;

        let _ = self
            .admin
            .exchange_delete(
                self.exchange.as_str().into(),
                ExchangeDeleteOptions::default(),
            )
            .await;

        let _ = self.connection.close(200, "OK".into()).await;
    }
}

async fn connect() -> Connection {
    Connection::connect(&url(), ConnectionProperties::default())
        .await
        .unwrap_or_else(|_| panic!("RabbitMQ connection failed"))
}

async fn confirmed_channel(connection: &Connection) -> Channel {
    let channel = connection.create_channel().await.unwrap();

    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();

    channel
}

async fn receive(source: &mut RabbitMqDeliverySource) -> (SerializedEnvelope, RabbitMqSettlement) {
    let delivery: RabbitMqDelivery = tokio::time::timeout(WAIT, source.receive())
        .await
        .expect("delivery did not arrive")
        .unwrap()
        .expect("source closed unexpectedly");

    let (wire, settlement) = delivery.into_parts();
    let mapper = RabbitMqMapper::new(TypeRouteResolver::new(ExchangeName::default_exchange()));

    (mapper.decode(wire).unwrap(), settlement)
}

async fn assert_no_delivery(source: &mut RabbitMqDeliverySource) {
    assert!(
        tokio::time::timeout(Duration::from_millis(300), source.receive())
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn confirm_mode_required() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let resolver = TypeRouteResolver::new(ExchangeName::new(fixture.exchange.clone()).unwrap());

    let plain = fixture.connection.create_channel().await.unwrap();

    assert_eq!(
        RabbitMqPublisher::new(plain, resolver.clone(), publisher_settings(WAIT)).err(),
        Some(RabbitMqError::Settings)
    );

    let confirmed = confirmed_channel(&fixture.connection).await;

    assert_eq!(
        RabbitMqPublisher::new(confirmed, resolver, publisher_settings(Duration::ZERO)).err(),
        Some(RabbitMqError::Settings)
    );

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn oversized_payload_is_rejected_before_sending() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let channel = confirmed_channel(&fixture.connection).await;

    let publisher = RabbitMqPublisher::new(
        channel,
        TypeRouteResolver::new(ExchangeName::new(fixture.exchange.clone()).unwrap()),
        RabbitMqPublisherSettings {
            publish_timeout: WAIT,
            max_message_size: NonZeroU32::new(16).unwrap(),
        },
    )
    .unwrap();

    assert_eq!(
        publisher.publish(&envelope(vec![0; 17])).await,
        Err(RabbitMqError::PayloadTooLarge)
    );

    publisher.publish(&envelope(vec![0; 16])).await.unwrap();

    fixture.await_ready(1).await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn unroutable_mandatory_is_unroutable() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let channel = confirmed_channel(&fixture.connection).await;

    let publisher = RabbitMqPublisher::new(
        channel,
        TypeRouteResolver::new(ExchangeName::new(fixture.exchange.clone()).unwrap()),
        publisher_settings(WAIT),
    )
    .unwrap();

    let mut unbound = envelope(b"unroutable".to_vec());
    unbound.message_type = MessageType::new("unbound").unwrap();

    assert_eq!(
        publisher.publish(&unbound).await,
        Err(RabbitMqError::Unroutable)
    );

    // The channel stays usable after a returned publication.
    publisher
        .publish(&envelope(b"routed".to_vec()))
        .await
        .unwrap();

    fixture.await_ready(1).await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn overflow_reject_publish_is_rejected() {
    let mut arguments = FieldTable::default();
    arguments.insert("x-max-length".into(), AMQPValue::LongInt(0));
    arguments.insert("x-overflow".into(), long("reject-publish"));

    let fixture = Fixture::new(arguments).await;
    let publisher = fixture.publisher().await;

    assert_eq!(
        publisher.publish(&envelope(b"overflow".to_vec())).await,
        Err(RabbitMqError::Rejected)
    );

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn missing_exchange_closes_the_publisher_channel() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let channel = confirmed_channel(&fixture.connection).await;

    let publisher = RabbitMqPublisher::new(
        channel,
        TypeRouteResolver::new(ExchangeName::new(format!("{}.missing", fixture.exchange)).unwrap()),
        publisher_settings(WAIT),
    )
    .unwrap();

    assert_eq!(
        publisher.publish(&envelope(b"lost".to_vec())).await,
        Err(RabbitMqError::Publish)
    );

    assert_eq!(
        publisher.publish(&envelope(b"after".to_vec())).await,
        Err(RabbitMqError::Publish)
    );

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn concurrent_publishes_are_confirmed_individually() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let publisher = fixture.publisher().await;
    let messages: Vec<_> = (0..64).map(|i| envelope(vec![i; 32])).collect();

    let results =
        futures_util::future::join_all(messages.iter().map(|message| publisher.publish(message)))
            .await;

    assert!(results.iter().all(Result::is_ok));

    let (_channel, mut source) = fixture.opened_source(64).await;
    let mut received = BTreeSet::new();

    for _ in 0..messages.len() {
        let (decoded, settlement) = receive(&mut source).await;
        received.insert(decoded.message_id);
        settlement.ack().await.unwrap();
    }

    let expected: BTreeSet<_> = messages.iter().map(|message| message.message_id).collect();

    assert_eq!(received, expected);

    fixture.cleanup().await;
}

/// Raises the memory alarm for the whole broker and restores the previous watermark.
///
/// [`MemoryAlarm::clear`] restores it and waits until the alarm is gone; `Drop` restores it
/// without waiting if the test fails first.
struct MemoryAlarm {
    restore: Vec<String>,

    cleared: bool,
}

impl MemoryAlarm {
    async fn raise() -> Self {
        let previous = rabbitmqctl(&[
            "-q",
            "eval",
            "vm_memory_monitor:get_vm_memory_high_watermark().",
        ])
        .await;

        let mut restore = vec!["set_vm_memory_high_watermark".to_owned()];

        // A relative watermark reads back as a float, an absolute one as `{absolute,Bytes}`.
        if let Some(bytes) = previous
            .strip_prefix("{absolute,")
            .and_then(|rest| rest.strip_suffix('}'))
        {
            restore.extend(["absolute".to_owned(), bytes.to_owned()]);
        } else {
            assert!(previous.parse::<f64>().is_ok(), "unexpected watermark form");
            restore.push(previous);
        }

        let alarm = Self {
            restore,
            cleared: false,
        };

        rabbitmqctl(&["set_vm_memory_high_watermark", "0"]).await;
        await_alarms(true).await;

        alarm
    }

    async fn clear(mut self) {
        let restore = self.restore.clone();

        tokio::task::spawn_blocking(move || rabbitmqctl_blocking(&restore))
            .await
            .unwrap();

        self.cleared = true;
        await_alarms(false).await;
    }
}

impl Drop for MemoryAlarm {
    fn drop(&mut self) {
        if self.cleared {
            return;
        }

        rabbitmqctl_blocking(&self.restore);

        // Hold the exclusive guard, which drops after this, until the alarm clears. Do not panic
        // here: this may run while the test is already unwinding.
        let deadline = Instant::now() + Duration::from_secs(10);

        let query = [
            "-q".to_owned(),
            "eval".to_owned(),
            "rabbit_alarm:get_alarms().".to_owned(),
        ];

        while rabbitmqctl_blocking(&query) != "[]" && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL and RABBITMQ_CONTAINER"]
async fn blocked_broker_confirm_times_out() {
    let guard = Guard::Exclusive(BROKER.write().await);
    let fixture = Fixture::with_guard(guard, FieldTable::default()).await;

    let channel = confirmed_channel(&fixture.connection).await;
    let exchange = ExchangeName::new(fixture.exchange.clone()).unwrap();

    let short = RabbitMqPublisher::new(
        channel.clone(),
        TypeRouteResolver::new(exchange.clone()),
        publisher_settings(Duration::from_millis(500)),
    )
    .unwrap();

    let long = RabbitMqPublisher::new(
        channel,
        TypeRouteResolver::new(exchange),
        publisher_settings(Duration::from_secs(30)),
    )
    .unwrap();

    let alarm = MemoryAlarm::raise().await;

    assert_eq!(
        short.publish(&envelope(b"blocked".to_vec())).await,
        Err(RabbitMqError::Timeout)
    );

    let pending = envelope(b"pending".to_vec());

    let (pending_result, ()) = tokio::join!(long.publish(&pending), async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        rabbitmqctl(&["close_all_connections", "sisa blocked broker test"]).await;
    });

    assert_eq!(pending_result, Err(RabbitMqError::Publish));

    assert_eq!(
        long.publish(&envelope(b"closed".to_vec())).await,
        Err(RabbitMqError::Publish)
    );

    // The exclusive guard is held until the alarm is confirmed cleared.
    alarm.clear().await;

    let cleanup = connect().await;
    let admin = cleanup.create_channel().await.unwrap();

    let _ = admin
        .queue_delete(fixture.queue.as_str().into(), QueueDeleteOptions::default())
        .await;

    let _ = admin
        .exchange_delete(
            fixture.exchange.as_str().into(),
            ExchangeDeleteOptions::default(),
        )
        .await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn invalid_queue_names_are_settings_errors() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let channel = fixture.connection.create_channel().await.unwrap();

    for queue in [String::new(), "q".repeat(256), "queue\nname".to_owned()] {
        let settings = RabbitMqSourceSettings {
            queue,
            prefetch: NonZeroU16::new(1).unwrap(),
        };

        assert_eq!(
            RabbitMqDeliverySource::new(channel.clone(), settings).err(),
            Some(RabbitMqError::Settings)
        );
    }

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn unsupported_requirement_starts_no_consumer() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let (_channel, mut source) = fixture.source(4).await;

    for (requirements, expected) in [
        (
            IndividualSourceRequirements::new().requiring_delayed_retry(),
            IndividualSourceRequirement::DelayedRetry,
        ),
        (
            IndividualSourceRequirements::new().requiring_heartbeat(),
            IndividualSourceRequirement::Heartbeat,
        ),
        (
            IndividualSourceRequirements::new().requiring_ack_wait(),
            IndividualSourceRequirement::AckWait,
        ),
        (
            IndividualSourceRequirements::new().requiring_max_deliver(),
            IndividualSourceRequirement::MaxDeliver,
        ),
    ] {
        let Err(IndividualSourceOpenError::Unsupported(error)) = source.open(requirements).await
        else {
            panic!("unsupported requirement must fail open");
        };

        assert_eq!(error.requirement(), expected);
        assert_eq!(fixture.counts().await.1, 0);
    }

    assert_eq!(source.receive().await.err(), Some(RabbitMqError::Settings));

    let descriptor = source
        .open(IndividualSourceRequirements::new().requiring_terminal_discard())
        .await
        .unwrap();

    assert_eq!(descriptor.ack_wait(), None);
    assert_eq!(descriptor.max_deliver(), None);
    assert!(!descriptor.supports_delayed_retry());
    assert!(descriptor.supports_immediate_requeue());
    assert!(descriptor.supports_terminal_discard());
    assert!(!descriptor.supports_heartbeat());
    assert_eq!(fixture.counts().await.1, 1);

    let Err(IndividualSourceOpenError::Source(error)) =
        source.open(IndividualSourceRequirements::new()).await
    else {
        panic!("a second open must fail");
    };

    assert_eq!(error, RabbitMqError::Settings);

    assert!(matches!(
        source
            .open(IndividualSourceRequirements::new().requiring_heartbeat())
            .await,
        Err(IndividualSourceOpenError::Source(RabbitMqError::Settings))
    ));

    assert_eq!(fixture.counts().await.1, 1);

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn nak_delay_and_heartbeat_unsupported_leave_unacked() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let publisher = fixture.publisher().await;

    publisher
        .publish(&envelope(b"held".to_vec()))
        .await
        .unwrap();

    let (channel, mut source) = fixture.opened_source(4).await;
    let (_, mut settlement) = receive(&mut source).await;

    assert!(matches!(
        settlement.heartbeat().await,
        Err(IndividualSettlementError::Unsupported(
            IndividualCapability::Heartbeat
        ))
    ));

    assert!(matches!(
        settlement.nak(Duration::from_millis(100)).await,
        Err(IndividualSettlementError::Unsupported(
            IndividualCapability::DelayedRetry
        ))
    ));

    // Neither call settled the delivery: it is still unacknowledged, not ready.
    assert_no_delivery(&mut source).await;
    assert_eq!(fixture.counts().await.0, 0);

    channel.close(200, "OK".into()).await.unwrap();
    fixture.await_ready(1).await;

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn ack_not_redelivered() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let publisher = fixture.publisher().await;
    let message = envelope(b"acked".to_vec());
    publisher.publish(&message).await.unwrap();

    let (channel, mut source) = fixture.opened_source(4).await;
    let (decoded, settlement) = receive(&mut source).await;

    assert_eq!(decoded, message);

    settlement.ack().await.unwrap();
    channel.close(200, "OK".into()).await.unwrap();

    assert_eq!(fixture.counts().await.0, 0);

    let (_channel, mut source) = fixture.opened_source(4).await;
    assert_no_delivery(&mut source).await;

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn requeue_redelivers() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let publisher = fixture.publisher().await;
    let message = envelope(b"requeued".to_vec());
    publisher.publish(&message).await.unwrap();

    let (_channel, mut source) = fixture.opened_source(4).await;
    let (first, settlement) = receive(&mut source).await;

    settlement.nak(Duration::ZERO).await.unwrap();

    let (second, settlement) = receive(&mut source).await;

    assert_eq!(first, message);
    assert_eq!(second, message);

    settlement.ack().await.unwrap();

    fixture.cleanup().await;
}

/// Declares a fanout dead-letter exchange bound to a queue of the same name, which it returns.
async fn declare_dead_letter(admin: &Channel) -> String {
    let name = format!("sisa.test.dlx.{}", suffix());

    admin
        .exchange_declare(
            name.as_str().into(),
            ExchangeKind::Fanout,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let mut arguments = FieldTable::default();
    arguments.insert("x-expires".into(), AMQPValue::LongInt(120_000));

    admin
        .queue_declare(
            name.as_str().into(),
            QueueDeclareOptions::default(),
            arguments,
        )
        .await
        .unwrap();

    admin
        .queue_bind(
            name.as_str().into(),
            name.as_str().into(),
            ShortString::default(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    name
}

async fn remove_dead_letter(admin: &Channel, name: &str) {
    let _ = admin
        .queue_delete(name.into(), QueueDeleteOptions::default())
        .await;

    let _ = admin
        .exchange_delete(name.into(), ExchangeDeleteOptions::default())
        .await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn terminate_dead_letters() {
    let guard = Guard::Shared(BROKER.read().await);
    let connection = connect().await;
    let admin = connection.create_channel().await.unwrap();
    let dead_exchange = declare_dead_letter(&admin).await;
    let dead_queue = dead_exchange.clone();
    let mut arguments = FieldTable::default();
    arguments.insert("x-dead-letter-exchange".into(), long(&dead_exchange));

    let fixture = Fixture::with_guard(guard, arguments).await;
    let publisher = fixture.publisher().await;
    let message = envelope(b"poison".to_vec());
    publisher.publish(&message).await.unwrap();

    let (_channel, mut source) = fixture.opened_source(4).await;
    let (_, settlement) = receive(&mut source).await;

    settlement.terminate().await.unwrap();

    assert_no_delivery(&mut source).await;

    let deadline = Instant::now() + WAIT;

    let dead = loop {
        if let Some(dead) = admin
            .basic_get(dead_queue.as_str().into(), BasicGetOptions { no_ack: true })
            .await
            .unwrap()
        {
            break dead;
        }

        assert!(Instant::now() < deadline, "dead letter did not arrive");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    let headers = dead.delivery.properties.headers().clone().unwrap();

    assert!(headers.contains_key("x-death"));

    let wire = sisa_messaging_rabbitmq::RabbitMqWire {
        exchange: dead.delivery.exchange.to_string(),
        routing_key: dead.delivery.routing_key.to_string(),
        properties: dead.delivery.properties.clone(),
        payload: dead.delivery.data.clone(),
    };

    let mapper = RabbitMqMapper::new(TypeRouteResolver::new(ExchangeName::default_exchange()));

    assert_eq!(mapper.decode(wire).unwrap(), message);

    remove_dead_letter(&admin, &dead_exchange).await;

    fixture.cleanup().await;
    let _ = connection.close(200, "OK".into()).await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn prefetch_bounds_unsettled() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let publisher = fixture.publisher().await;

    for index in 0..5 {
        publisher.publish(&envelope(vec![index])).await.unwrap();
    }

    let (_channel, mut source) = fixture.opened_source(2).await;
    let (_, first) = receive(&mut source).await;
    let (_, second) = receive(&mut source).await;

    assert_no_delivery(&mut source).await;
    assert_eq!(fixture.counts().await.0, 3);

    first.ack().await.unwrap();

    let (_, third) = receive(&mut source).await;

    assert_no_delivery(&mut source).await;

    second.ack().await.unwrap();
    third.ack().await.unwrap();

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn cancelled_receive_loses_nothing() {
    const COUNT: u8 = 20;

    let fixture = Fixture::new(FieldTable::default()).await;
    let publisher = fixture.publisher().await;
    let (_channel, mut source) = fixture.opened_source(u16::from(COUNT)).await;

    for _ in 0..3 {
        assert_no_delivery(&mut source).await;
    }

    let messages: Vec<_> = (0..COUNT).map(|index| envelope(vec![index])).collect();
    let expected: Vec<_> = messages.iter().map(|message| message.message_id).collect();

    // Publish while the receive loop below polls and drops pending receives.
    let publishing = tokio::spawn(async move {
        for message in &messages {
            publisher.publish(message).await.unwrap();
        }
    });

    let mapper = RabbitMqMapper::new(TypeRouteResolver::new(ExchangeName::default_exchange()));
    let deadline = Instant::now() + WAIT;
    let mut received = Vec::new();
    let mut settlements = Vec::new();
    let mut dropped_polls = 0usize;

    while received.len() < usize::from(COUNT) {
        assert!(Instant::now() < deadline, "deliveries were lost");

        // Poll once and drop the future when no delivery is ready.
        match source.receive().now_or_never() {
            Some(result) => {
                let (wire, settlement) = result.unwrap().unwrap().into_parts();
                received.push(mapper.decode(wire).unwrap().message_id);
                settlements.push(settlement);
            }
            None => {
                dropped_polls += 1;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    publishing.await.unwrap();

    assert!(dropped_polls > 0);
    assert_eq!(received, expected);

    for settlement in settlements {
        settlement.ack().await.unwrap();
    }

    assert_no_delivery(&mut source).await;
    assert_eq!(fixture.counts().await.0, 0);

    fixture.cleanup().await;
}

/// A handle whose channel closed fails locally. The riskier case, a new channel reusing the same
/// channel id so a stale delivery tag would address a different delivery, occurs only after
/// lapin's channel-id allocator wraps around and is not exercised here.
#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn stale_settlement_after_channel_close_fails() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let publisher = fixture.publisher().await;

    publisher
        .publish(&envelope(b"stale".to_vec()))
        .await
        .unwrap();

    let (channel, mut source) = fixture.opened_source(4).await;
    let (_, settlement) = receive(&mut source).await;

    channel.close(200, "OK".into()).await.unwrap();

    assert!(matches!(
        settlement.ack().await,
        Err(IndividualSettlementError::Operation(
            RabbitMqError::Settlement
        ))
    ));

    fixture.await_ready(1).await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn queue_delete_is_source_error() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let (_channel, mut source) = fixture.opened_source(4).await;

    fixture
        .admin
        .queue_delete(fixture.queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();

    let result = tokio::time::timeout(WAIT, source.receive())
        .await
        .expect("broker cancel did not arrive");

    assert_eq!(result.err(), Some(RabbitMqError::Source));
    assert!(source.receive().await.unwrap().is_none());

    // The broker already ended the consumer, so there is nothing to cancel.
    assert_eq!(source.close().await, Ok(()));

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn close_cancels_and_allows_drain_settlement() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let publisher = fixture.publisher().await;

    publisher
        .publish(&envelope(b"draining".to_vec()))
        .await
        .unwrap();

    let (channel, mut source) = fixture.opened_source(1).await;
    let (_, settlement) = receive(&mut source).await;

    source.close().await.unwrap();

    assert!(source.receive().await.unwrap().is_none());
    assert_eq!(fixture.counts().await.1, 0);

    settlement.ack().await.unwrap();
    channel.close(200, "OK".into()).await.unwrap();

    assert_eq!(fixture.counts().await.0, 0);

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn open_after_close_is_rejected() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let (_channel, mut source) = fixture.opened_source(4).await;

    source.close().await.unwrap();

    let Err(IndividualSourceOpenError::Source(error)) =
        source.open(IndividualSourceRequirements::new()).await
    else {
        panic!("open after close must fail");
    };

    assert_eq!(error, RabbitMqError::Settings);

    // The closed state takes precedence over requirement validation.
    assert!(matches!(
        source
            .open(IndividualSourceRequirements::new().requiring_delayed_retry())
            .await,
        Err(IndividualSourceOpenError::Source(RabbitMqError::Settings))
    ));

    assert!(source.receive().await.unwrap().is_none());
    assert_eq!(fixture.counts().await.1, 0);

    let (_channel, mut unopened) = fixture.source(4).await;
    unopened.close().await.unwrap();

    assert!(matches!(
        unopened.open(IndividualSourceRequirements::new()).await,
        Err(IndividualSourceOpenError::Source(RabbitMqError::Settings))
    ));

    assert_eq!(fixture.counts().await.1, 0);

    fixture.cleanup().await;
}

/// `ack` succeeds only after the broker answered the following `basic.qos`, so the broker had
/// already processed the ack when every connection is closed from the broker side immediately
/// afterwards; the delivery must not be requeued. A fire-and-forget ack could still be in flight
/// and be lost with the connection, requeueing the delivery. This narrows but cannot
/// deterministically force that race, so it does not prove the fire-and-forget ack would fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL and RABBITMQ_CONTAINER"]
async fn acked_delivery_survives_connection_close() {
    let guard = Guard::Exclusive(BROKER.write().await);
    let fixture = Fixture::with_guard(guard, FieldTable::default()).await;
    let publisher = fixture.publisher().await;

    publisher
        .publish(&envelope(b"acked".to_vec()))
        .await
        .unwrap();

    let (_channel, mut source) = fixture.opened_source(4).await;
    let (_, settlement) = receive(&mut source).await;

    settlement.ack().await.unwrap();
    rabbitmqctl(&["close_all_connections", "sisa acked delivery test"]).await;

    let fixture = fixture.reconnect().await;

    // Requeue of unacknowledged deliveries after a close is asynchronous; give it time to show.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(fixture.counts().await, (0, 0));

    let (_channel, mut source) = fixture.opened_source(4).await;
    assert_no_delivery(&mut source).await;

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn concurrent_mixed_settlements_on_one_channel() {
    const COUNT: u8 = 12;

    let guard = Guard::Shared(BROKER.read().await);
    let connection = connect().await;
    let admin = connection.create_channel().await.unwrap();
    let dead = declare_dead_letter(&admin).await;
    let mut arguments = FieldTable::default();
    arguments.insert("x-dead-letter-exchange".into(), long(&dead));

    let fixture = Fixture::with_guard(guard, arguments).await;
    let publisher = fixture.publisher().await;

    for index in 0..COUNT {
        publisher.publish(&envelope(vec![index])).await.unwrap();
    }

    let (channel, mut source) = fixture.opened_source(u16::from(COUNT)).await;
    let mut settlements = Vec::new();

    for _ in 0..COUNT {
        let (decoded, settlement) = receive(&mut source).await;
        settlements.push((decoded.payload[0], settlement));
    }

    // Index modulo three selects ack, immediate requeue, or terminate.
    let results = futures_util::future::join_all(settlements.into_iter().map(
        |(index, settlement)| async move {
            match index % 3 {
                0 => settlement.ack().await,
                1 => settlement.nak(Duration::ZERO).await,
                _ => settlement.terminate().await,
            }
        },
    ))
    .await;

    assert!(results.iter().all(Result::is_ok));

    // Requeued deliveries may already be redelivered to this consumer; closing the channel
    // returns them to the queue.
    source.close().await.unwrap();
    channel.close(200, "OK".into()).await.unwrap();

    let per_outcome = u32::from(COUNT / 3);
    fixture.await_ready(per_outcome).await;

    let deadline = Instant::now() + WAIT;

    loop {
        let dead_queue = admin
            .queue_declare(
                dead.as_str().into(),
                QueueDeclareOptions {
                    passive: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();

        if dead_queue.message_count() == per_outcome {
            break;
        }

        assert!(Instant::now() < deadline, "dead letters did not arrive");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let mut ready = BTreeSet::new();

    while let Some(message) = admin
        .basic_get(
            fixture.queue.as_str().into(),
            BasicGetOptions { no_ack: true },
        )
        .await
        .unwrap()
    {
        ready.insert(message.delivery.data[0]);
    }

    let requeued: BTreeSet<_> = (0..COUNT).filter(|index| index % 3 == 1).collect();

    assert_eq!(ready, requeued);

    remove_dead_letter(&admin, &dead).await;
    fixture.cleanup().await;
    let _ = connection.close(200, "OK".into()).await;
}

#[tokio::test]
#[ignore = "requires a real RabbitMQ broker at RABBITMQ_URL"]
async fn close_on_closed_channel_is_source_error() {
    let fixture = Fixture::new(FieldTable::default()).await;
    let (channel, mut source) = fixture.opened_source(4).await;

    channel.close(200, "OK".into()).await.unwrap();

    assert_eq!(source.close().await, Err(RabbitMqError::Source));
    assert!(source.receive().await.unwrap().is_none());

    assert!(matches!(
        source.open(IndividualSourceRequirements::new()).await,
        Err(IndividualSourceOpenError::Source(RabbitMqError::Settings))
    ));

    // The first close released the consumer, so a second close has nothing to cancel.
    assert_eq!(source.close().await, Ok(()));

    fixture.cleanup().await;
}
