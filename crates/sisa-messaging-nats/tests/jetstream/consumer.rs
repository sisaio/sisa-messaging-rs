//! The generic consumer composed with the NATS source, mapper, and settlement.
//!
//! Every ignored scenario runs `Consumer::run` over a real JetStream durable pull consumer with a
//! unique stream and durable name, and the test-local transactional inbox.

use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_nats::jetstream::{self, consumer::PullConsumer, consumer::pull, stream};
use futures_util::StreamExt;
use sisa_messaging::{
    ContentType, Envelope, ErrorClassifier, FailureKind, Message, MessageId, Metadata, Publisher,
    SerializedEnvelope, Serializer,
};
use sisa_messaging_consumer::{
    Consumer, ConsumerConfigError, ConsumerError, ConsumerExit, ConsumerHandler, ConsumerSettings,
};
use sisa_messaging_inbox::{DeadReason, InboxScope};
use sisa_messaging_nats::{
    NatsDeliverySource, NatsMapper, NatsPublisher, NatsPublisherSettings, Subject,
    TypeSubjectResolver,
};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::SubscriberExt;

use super::inbox::{FakeInbox, FakeTransaction, Status};

/// Bound for every wait on broker or consumer progress.
pub(crate) const PROGRESS_TIMEOUT: Duration = Duration::from_secs(15);

/// Test message carried as UTF-8 text so scenarios select handler behavior by label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Order {
    pub(crate) label: String,
}

impl Message for Order {
    const TYPE: &'static str = "order-created";
    const VERSION: u32 = 1;
}

#[derive(Debug)]
pub(crate) struct CodecError;

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
pub(crate) struct OrderCodec;

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
pub(crate) enum Step {
    Succeed,

    Fail(FailureKind),

    /// Sleeps, then succeeds.
    Sleep(Duration),

    /// Waits for a permit from [`ScriptedHandler::release`], then succeeds.
    Gate,
}

/// Handler failure whose rendering never includes message content.
#[derive(Debug)]
pub(crate) struct HandlerError {
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

    invocations: Mutex<Vec<(String, Instant)>>,

    active: AtomicUsize,

    peak: AtomicUsize,
}

/// Records invocations and concurrency, then applies each label's scripted steps in order.
#[derive(Clone)]
pub(crate) struct ScriptedHandler {
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

impl ScriptedHandler {
    pub(crate) fn script(&self, label: &str, steps: &[Step]) {
        self.state
            .scripts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(label.to_owned(), steps.iter().copied().collect());
    }

    /// Lets `count` gated invocations proceed.
    pub(crate) fn release(&self, count: usize) {
        self.gate.add_permits(count);
    }

    pub(crate) fn invocations(&self, label: &str) -> Vec<Instant> {
        self.state
            .invocations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(invoked, _)| invoked == label)
            .map(|(_, at)| *at)
            .collect()
    }

    pub(crate) fn total_invocations(&self) -> usize {
        self.state
            .invocations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    pub(crate) fn active(&self) -> usize {
        self.state.active.load(Ordering::SeqCst)
    }

    pub(crate) fn peak(&self) -> usize {
        self.state.peak.load(Ordering::SeqCst)
    }

    fn next_step(&self, label: &str) -> Step {
        self.state
            .scripts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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

        self.state
            .invocations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((label.clone(), Instant::now()));

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

/// The NATS-typed consumer every scenario runs.
pub(crate) type NatsConsumer = Consumer<
    Order,
    (
        NatsDeliverySource,
        NatsMapper<TypeSubjectResolver>,
        OrderCodec,
        FakeInbox,
        ScriptedHandler,
    ),
>;

/// A unique stream, durable pull consumer, and publisher on the server at `NATS_URL`.
pub(crate) struct Broker {
    pub(crate) client: async_nats::Client,

    context: jetstream::Context,

    stream: stream::Stream,

    pub(crate) stream_name: String,

    pub(crate) durable: String,

    prefix: String,

    publisher: NatsPublisher<TypeSubjectResolver>,
}

impl Broker {
    /// Creates the stream and a durable with explicit acknowledgement and `ack_wait`.
    pub(crate) async fn new(ack_wait: Duration, max_deliver: i64) -> (Self, PullConsumer) {
        let url = std::env::var("NATS_URL").expect("NATS_URL is required for this ignored test");
        let client = async_nats::connect(url).await.unwrap();
        let context = jetstream::new(client.clone());
        let suffix = MessageId::new().to_string().replace('-', "");
        let prefix = format!("test_{suffix}");
        let stream_name = format!("TEST{suffix}");
        let durable = format!("durable{suffix}");

        let stream = context
            .create_stream(stream::Config {
                name: stream_name.clone(),
                subjects: vec![format!("{prefix}.>")],
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer = stream
            .create_consumer(pull::Config {
                durable_name: Some(durable.clone()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait,
                max_deliver,
                ..Default::default()
            })
            .await
            .unwrap();

        let publisher = NatsPublisher::new(
            context.clone(),
            resolver(&prefix),
            NatsPublisherSettings {
                publish_timeout: Duration::from_secs(5),
            },
        )
        .unwrap();

        let broker = Self {
            client,
            context,
            stream,
            stream_name,
            durable,
            prefix,
            publisher,
        };

        (broker, consumer)
    }

    /// Looks up the existing durable again, as a restarted application would.
    pub(crate) async fn lookup(&self) -> PullConsumer {
        self.stream.get_consumer(&self.durable).await.unwrap()
    }

    pub(crate) fn subject(&self) -> String {
        format!("{}.{}.v{}", self.prefix, Order::TYPE, Order::VERSION)
    }

    pub(crate) async fn publish(&self, label: &str) -> MessageId {
        let message_id = MessageId::new();
        self.publish_with_id(message_id, label).await;

        message_id
    }

    pub(crate) async fn publish_with_id(&self, message_id: MessageId, label: &str) {
        let envelope = Envelope::new(
            message_id,
            Order {
                label: label.to_owned(),
            },
            Metadata::default(),
        )
        .unwrap();

        let serialized = OrderCodec.serialize(&envelope).unwrap();
        self.publisher.publish(&serialized).await.unwrap();
    }

    /// Publishes raw headers and payload directly, bypassing the mapper.
    pub(crate) async fn publish_raw(&self, headers: async_nats::HeaderMap, payload: Vec<u8>) {
        self.context
            .publish_with_headers(self.subject(), headers, payload.into())
            .await
            .unwrap()
            .await
            .unwrap();
    }

    pub(crate) async fn info(&self) -> jetstream::consumer::Info {
        self.stream.consumer_info(&self.durable).await.unwrap()
    }

    /// Waits until the broker has acknowledged every one of the first `count` stream messages.
    pub(crate) async fn wait_acked(&self, count: u64) -> jetstream::consumer::Info {
        let deadline = Instant::now() + PROGRESS_TIMEOUT;

        loop {
            let info = self.info().await;

            if info.ack_floor.stream_sequence >= count && info.num_ack_pending == 0 {
                return info;
            }

            assert!(
                Instant::now() < deadline,
                "broker acknowledgement not observed"
            );

            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub(crate) async fn delete(self) {
        let _ = self.context.delete_stream(&self.stream_name).await;
    }
}

pub(crate) fn resolver(prefix: &str) -> TypeSubjectResolver {
    TypeSubjectResolver::new(Subject::new(prefix).unwrap())
}

pub(crate) fn scope() -> InboxScope {
    InboxScope::new(format!("orders-{}", MessageId::new())).unwrap()
}

/// Test-sized settings; each scenario overrides only the fields it exercises.
pub(crate) fn settings() -> ConsumerSettings {
    let mut settings = ConsumerSettings::default();
    settings.max_in_flight = NonZeroUsize::new(8).unwrap();
    settings.source_timeout = Duration::from_secs(5);
    settings.database_timeout = Duration::from_secs(5);
    settings.settlement_timeout = Duration::from_secs(5);
    settings.nak_delay = Duration::from_millis(200);
    settings.drain_timeout = Duration::from_secs(5);

    settings
}

/// Constructs the consumer exactly as an application does, with only provider values.
pub(crate) fn consumer(
    pull_consumer: PullConsumer,
    inbox: FakeInbox,
    scope: InboxScope,
    handler: ScriptedHandler,
    settings: ConsumerSettings,
) -> Result<NatsConsumer, ConsumerConfigError> {
    let source = NatsDeliverySource::new(pull_consumer);
    let mapper = NatsMapper::new(resolver("orders"));

    Consumer::<Order, _>::new(source, mapper, OrderCodec, inbox, scope, handler, settings)
}

/// A spawned `Consumer::run` with its cancellation token.
pub(crate) struct Running {
    cancel: CancellationToken,

    task: JoinHandle<Result<ConsumerExit, ConsumerError>>,
}

impl Running {
    pub(crate) fn spawn(consumer: NatsConsumer) -> Self {
        let cancel = CancellationToken::new();
        let task = tokio::spawn(consumer.run(cancel.child_token()));

        Self { cancel, task }
    }

    pub(crate) async fn stop(self) -> Result<ConsumerExit, ConsumerError> {
        self.cancel.cancel();

        tokio::time::timeout(PROGRESS_TIMEOUT, self.task)
            .await
            .expect("consumer did not stop")
            .expect("consumer task panicked")
    }
}

/// Polls a local condition until it holds or [`PROGRESS_TIMEOUT`] elapses.
pub(crate) async fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + PROGRESS_TIMEOUT;

    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn assert_send<T: Send>(_: &T) {}

#[test]
fn nats_typed_consumer_run_future_is_send_and_spawnable() {
    // Type-checked only: no broker is contacted.
    let _run = |consumer: NatsConsumer, cancel: CancellationToken| {
        let run = consumer.run(cancel);
        assert_send(&run);

        run
    };

    let _spawn = |consumer: NatsConsumer, cancel: CancellationToken| -> JoinHandle<_> {
        tokio::spawn(consumer.run(cancel))
    };
}

#[tokio::test]
#[ignore = "requires a real JetStream server at NATS_URL"]
async fn acknowledgement_follows_commit_and_completed_delivery_is_not_redelivered() {
    let ack_wait = Duration::from_secs(1);
    let (broker, pull_consumer) = Broker::new(ack_wait, -1).await;
    let inbox = FakeInbox::new(5);
    let handler = ScriptedHandler::default();
    let scope = scope();

    let running = Running::spawn(
        consumer(
            pull_consumer,
            inbox.clone(),
            scope.clone(),
            handler.clone(),
            settings(),
        )
        .unwrap(),
    );

    let id = broker.publish("committed").await;

    // Once the broker reports the acknowledgement, the committed completion and effect are
    // visible. Ack-after-commit ordering itself is shown by the failed-commit scenario (no
    // acknowledgement without a commit) and by the consumer crate's runtime tests.
    broker.wait_acked(1).await;
    assert_eq!(inbox.completions(&scope, id), 1);
    assert_eq!(inbox.effects(), ["committed"]);

    tokio::time::sleep(ack_wait * 2 + Duration::from_millis(500)).await;

    let info = broker.info().await;
    assert_eq!(info.num_ack_pending, 0);
    assert_eq!(info.delivered.consumer_sequence, 1);
    assert_eq!(info.delivered.stream_sequence, 1);
    assert_eq!(handler.invocations("committed").len(), 1);
    assert_eq!(inbox.completions(&scope, id), 1);
    assert_eq!(inbox.live(), 0);

    assert_eq!(running.stop().await.unwrap(), ConsumerExit::Cancelled);
    broker.delete().await;
}

#[tokio::test]
#[ignore = "requires a real JetStream server at NATS_URL"]
async fn failed_commit_is_not_acknowledged_and_redelivery_completes_once() {
    let ack_wait = Duration::from_secs(10);
    let (broker, pull_consumer) = Broker::new(ack_wait, -1).await;
    let inbox = FakeInbox::new(5);
    inbox.fail_next_commits(1);
    let handler = ScriptedHandler::default();
    let scope = scope();

    let running = Running::spawn(
        consumer(
            pull_consumer,
            inbox.clone(),
            scope.clone(),
            handler.clone(),
            settings(),
        )
        .unwrap(),
    );

    let id = broker.publish("ambiguous-commit").await;

    let info = broker.wait_acked(1).await;
    assert_eq!(inbox.completions(&scope, id), 1);

    // A failed commit is ambiguous and takes the broker-mode retry operation, a delayed
    // negative acknowledgement; redelivery before `ack_wait` rules out deadline expiry.
    assert_eq!(info.delivered.consumer_sequence, 2);
    let invocations = handler.invocations("ambiguous-commit");
    assert_eq!(invocations.len(), 2);
    let gap = invocations[1].duration_since(invocations[0]);

    assert!(
        gap < ack_wait,
        "redelivery gap {gap:?} must follow the nak delay, not ack_wait"
    );

    assert_eq!(inbox.effects(), ["ambiguous-commit"]);
    assert_eq!(inbox.committed(), 1);

    assert!(
        inbox.summaries().is_empty(),
        "commit failure is not recorded"
    );

    assert_eq!(
        inbox.status(&scope, id),
        Some(Status::Completed { attempts: 0 })
    );

    assert_eq!(running.stop().await.unwrap(), ConsumerExit::Cancelled);
    broker.delete().await;
}

#[tokio::test]
#[ignore = "requires a real JetStream server at NATS_URL"]
async fn transient_failure_is_redelivered_after_the_negative_acknowledgement_delay() {
    let nak_delay = Duration::from_secs(1);
    let (broker, pull_consumer) = Broker::new(Duration::from_secs(10), -1).await;
    let inbox = FakeInbox::new(5);
    let handler = ScriptedHandler::default();

    handler.script(
        "retry",
        &[Step::Fail(FailureKind::Transient), Step::Succeed],
    );

    let scope = scope();
    let mut settings = settings();
    settings.nak_delay = nak_delay;

    let running = Running::spawn(
        consumer(
            pull_consumer,
            inbox.clone(),
            scope.clone(),
            handler.clone(),
            settings,
        )
        .unwrap(),
    );

    let id = broker.publish("retry").await;

    let info = broker.wait_acked(1).await;
    assert_eq!(info.delivered.consumer_sequence, 2);

    let invocations = handler.invocations("retry");
    assert_eq!(invocations.len(), 2);
    let gap = invocations[1].duration_since(invocations[0]);

    assert!(
        gap >= nak_delay.mul_f32(0.9) && gap < Duration::from_secs(10),
        "redelivery gap {gap:?} must follow the nak delay, not ack_wait"
    );

    assert_eq!(
        inbox.status(&scope, id),
        Some(Status::Completed { attempts: 1 })
    );

    assert_eq!(inbox.completions(&scope, id), 1);
    assert_eq!(inbox.effects(), ["retry"]);
    assert_eq!(inbox.summaries(), ["order handler failed"]);

    assert_eq!(running.stop().await.unwrap(), ConsumerExit::Cancelled);
    broker.delete().await;
}

const SENTINEL_PAYLOAD: &str = "sentinel-payload-7f3a";
const SENTINEL_HEADER: &str = "sentinel-header-91c2";
const SENTINEL_LABEL: &str = "sentinel-label-5d0e";

#[derive(Clone)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl io::Write for SharedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(bytes);

        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for SharedWriter {
    type Writer = Self;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

/// Runs the permanent-failure and poison scenario; returns the permanent message identity and the
/// failure summaries the inbox recorded.
async fn terminate_permanent_and_poison_deliveries() -> (MessageId, Vec<String>) {
    let (broker, pull_consumer) = Broker::new(Duration::from_secs(10), -1).await;
    let inbox = FakeInbox::new(5);
    let handler = ScriptedHandler::default();
    let permanent = format!("permanent-{SENTINEL_LABEL}");
    handler.script(&permanent, &[Step::Fail(FailureKind::Permanent)]);
    let scope = scope();

    let mut terminated = broker
        .client
        .subscribe(format!(
            "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.{}.{}",
            broker.stream_name, broker.durable
        ))
        .await
        .unwrap();

    broker.client.flush().await.unwrap();

    let running = Running::spawn(
        consumer(
            pull_consumer,
            inbox.clone(),
            scope.clone(),
            handler.clone(),
            settings(),
        )
        .unwrap(),
    );

    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Sisa-Message-Id", SENTINEL_HEADER);
    headers.insert("Sisa-Custom-Secret", SENTINEL_HEADER);

    broker
        .publish_raw(headers, SENTINEL_PAYLOAD.as_bytes().to_vec())
        .await;

    let permanent_id = broker.publish(&permanent).await;

    for _ in 0..2 {
        tokio::time::timeout(PROGRESS_TIMEOUT, terminated.next())
            .await
            .expect("termination advisory not observed")
            .expect("advisory subscription closed");
    }

    let info = broker.wait_acked(2).await;
    assert_eq!(info.delivered.consumer_sequence, 2);

    assert_eq!(
        inbox.status(&scope, permanent_id),
        Some(Status::Dead {
            attempts: 1,
            reason: DeadReason::Permanent
        })
    );

    assert_eq!(
        handler.total_invocations(),
        1,
        "poison never reaches the handler"
    );

    assert_eq!(inbox.completed_total(), 0);

    let exit = running.stop().await;
    assert_eq!(exit.unwrap(), ConsumerExit::Cancelled);
    broker.delete().await;

    (permanent_id, inbox.summaries())
}

#[test]
#[ignore = "requires a real JetStream server at NATS_URL"]
fn permanent_failure_and_poison_delivery_are_terminated_without_leaking_content() {
    let output = Arc::new(Mutex::new(Vec::new()));

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .without_time()
        .with_writer(SharedWriter(Arc::clone(&output)))
        .finish()
        // The redaction contract covers the framework and provider instrumentation. The
        // async-nats SDK's own TRACE records render raw protocol frames, including headers and
        // payloads, and remain subject to the application's subscriber filter.
        .with(Targets::new().with_target("messaging", LevelFilter::TRACE));

    // The dispatcher is scoped to this thread, and the current-thread runtime polls every
    // consumer and client task here, so parallel tests never write into this capture. Callsite
    // interest is global, though: a callsite that a parallel test registers while this
    // dispatcher is being installed can be cached as uninteresting. A warm-up pass therefore
    // registers every callsite this scenario reaches, the rebuild re-evaluates them all with this
    // dispatcher present, and only the second pass is asserted. The positive controls below
    // fail rather than pass vacuously if capture is still incomplete.
    let (permanent_id, summaries) = tracing::subscriber::with_default(subscriber, || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(terminate_permanent_and_poison_deliveries());
        tracing::callsite::rebuild_interest_cache();

        output
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();

        runtime.block_on(terminate_permanent_and_poison_deliveries())
    });

    let captured = String::from_utf8(
        output
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone(),
    )
    .unwrap();

    let has_event = |fields: &[&str]| {
        captured
            .lines()
            .any(|line| fields.iter().all(|field| line.contains(field)))
    };

    // Positive controls: both terminations were captured with their stable outcome fields.
    let permanent_id = format!("message.id={permanent_id}");

    assert!(
        has_event(&[
            "delivery resolved",
            &permanent_id,
            "outcome=\"dead\"",
            "dead.reason=\"permanent\"",
            "action=\"terminate\"",
        ]),
        "permanent handler termination was not captured"
    );

    assert!(
        has_event(&[
            "delivery resolved",
            "outcome=\"malformed\"",
            "stage=\"map\"",
            "action=\"terminate\"",
        ]),
        "poison mapping termination was not captured"
    );

    assert!(
        has_event(&["delivery settled", &permanent_id, "action=\"terminate\""]),
        "permanent handler settlement was not captured"
    );

    // The only recorded failure is the handler error's safe Display, persisted by the runtime.
    assert_eq!(summaries, ["order handler failed"]);

    for sentinel in [SENTINEL_PAYLOAD, SENTINEL_HEADER, SENTINEL_LABEL] {
        assert!(
            !captured.contains(sentinel),
            "tracing output leaked content"
        );

        assert!(
            summaries.iter().all(|summary| !summary.contains(sentinel)),
            "recorded failure leaked content"
        );
    }
}

#[tokio::test]
#[ignore = "requires a real JetStream server at NATS_URL"]
async fn completed_duplicate_is_acknowledged_without_invoking_the_handler() {
    let ack_wait = Duration::from_secs(1);
    let (broker, pull_consumer) = Broker::new(ack_wait, -1).await;
    let inbox = FakeInbox::new(5);
    let handler = ScriptedHandler::default();
    let scope = scope();
    let id = MessageId::new();
    inbox.mark_completed(&scope, id);

    let running = Running::spawn(
        consumer(
            pull_consumer,
            inbox.clone(),
            scope.clone(),
            handler.clone(),
            settings(),
        )
        .unwrap(),
    );

    broker.publish_with_id(id, "duplicate").await;
    broker.wait_acked(1).await;

    tokio::time::sleep(ack_wait * 2 + Duration::from_millis(500)).await;

    let info = broker.info().await;
    assert_eq!(info.delivered.consumer_sequence, 1);
    assert_eq!(info.num_ack_pending, 0);
    assert_eq!(handler.total_invocations(), 0);
    assert_eq!(inbox.rolled_back(), 1);
    assert_eq!(inbox.completions(&scope, id), 0);
    assert!(inbox.effects().is_empty());

    assert_eq!(running.stop().await.unwrap(), ConsumerExit::Cancelled);
    broker.delete().await;
}
