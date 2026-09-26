//! Controlled source, settlement, mapper, codec, inbox, handler, and log-capture fakes.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fmt::{self, Write as _};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use sisa_messaging::{
    ContentType, Delivery, Envelope, EnvelopeMapper, ErrorClassifier, FailureKind, HeaderName,
    HeaderValue, IndividualCapability, IndividualDeliverySource, IndividualSettlement,
    IndividualSettlementError, IndividualSourceDescriptor, IndividualSourceOpenError,
    IndividualSourceRequirements, Message, MessageId, MessageType, Metadata, PartitionAdvance,
    PartitionedLogDeliverySource, PartitionedLogReceive, PartitionedLogSettlement,
    SerializedEnvelope, Serializer,
};
use sisa_messaging_consumer::{
    Consumer, ConsumerError, ConsumerExit, ConsumerHandler, ConsumerSettings, SettlementMode,
};
use sisa_messaging_inbox::{
    DeadReason, InboxClaimOutcome, InboxFailure, InboxFailureOutcome, InboxId, InboxReceipt,
    InboxRecord, InboxScope, InboxStore, InboxUnitOfWork,
};
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub const PAYLOAD_SENTINEL: &str = "SENTINEL-PAYLOAD-7f3a";
pub const HEADER_SENTINEL: &str = "SENTINEL-HEADER-91c2";
pub const PROVIDER_SENTINEL: &str = "SENTINEL-PROVIDER-5d1e";
pub const HANDLER_SENTINEL: &str = "SENTINEL-HANDLER-0b64";
pub const SCOPE_SENTINEL: &str = "SENTINEL-SCOPE-c3d7";
pub const SENTINELS: [&str; 5] = [
    PAYLOAD_SENTINEL,
    HEADER_SENTINEL,
    PROVIDER_SENTINEL,
    HANDLER_SENTINEL,
    SCOPE_SENTINEL,
];

pub const NAK_DELAY: Duration = Duration::from_secs(5);

/// The one message type every test consumer handles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Order {
    pub body: String,
}

impl Message for Order {
    const TYPE: &'static str = "test.order";
    const VERSION: u32 = 1;
}

/// One observable effect, identified by the delivery's test tag.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    Open(IndividualSourceRequirements),
    Receive,
    Begin,
    Claim(u8),
    Handle(u8),
    Complete(u8),
    Commit(u8),
    Rollback(Option<u8>),
    Fail(u8, FailureKind),
    Ack(u8),
    Nak(u8, Duration),
    Terminate(u8),
    Heartbeat(u8),
    Left(u8),
    TxDropped(Option<u8>),
}

/// A scripted result for a database, source, or settlement operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Step {
    Ok,
    Error(FailureKind),
    Unsupported,
    Hang,
    Panic,
}

/// A scripted handler behavior.
#[derive(Clone, Debug)]
pub enum HandlerStep {
    Ok,

    Fail(FailureKind),

    Panic,

    Block(Arc<Semaphore>),

    Hang,

    /// Blocks the worker thread inside one poll, then never completes.
    BlockThreadThenHang(Duration),

    /// Yields to the scheduler once, then succeeds.
    Yield,
}

/// A scripted claim behavior; `Real` uses the in-memory durable state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClaimStep {
    Real,
    InProgress,
    Db(Step),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Durable {
    Pending { failures: u32 },
    Completed,
    Dead(DeadReason),
}

#[derive(Default)]
struct Script {
    begin: VecDeque<Step>,

    rollback: VecDeque<Step>,

    claim: HashMap<u8, VecDeque<ClaimStep>>,

    handler: HashMap<u8, VecDeque<HandlerStep>>,

    complete: HashMap<u8, VecDeque<Step>>,

    commit: HashMap<u8, VecDeque<Step>>,

    fail: HashMap<u8, VecDeque<Step>>,

    settle: HashMap<u8, VecDeque<Step>>,

    fenced_advance: HashSet<u8>,
}

/// Shared observation and scripting state for every fake.
#[derive(Default)]
pub struct Probe {
    events: Mutex<Vec<Event>>,

    changed: Notify,

    tags: Mutex<HashMap<MessageId, u8>>,

    ids: Mutex<HashMap<u8, MessageId>>,

    script: Mutex<Script>,

    durable: Mutex<HashMap<MessageId, Durable>>,

    committed_writes: Mutex<Vec<String>>,

    summaries: Mutex<Vec<String>>,

    live_tx: AtomicUsize,

    max_live_tx: AtomicUsize,

    outstanding: AtomicUsize,

    max_outstanding_at_receive: AtomicUsize,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn next<T: Clone>(queue: Option<&mut VecDeque<T>>) -> Option<T> {
    queue.and_then(VecDeque::pop_front)
}

impl Probe {
    pub fn push(&self, event: Event) {
        lock(&self.events).push(event);
        self.changed.notify_waiters();
    }

    pub fn events(&self) -> Vec<Event> {
        lock(&self.events).clone()
    }

    /// Events that mention `tag`, in order.
    pub fn events_for(&self, tag: u8) -> Vec<Event> {
        self.events()
            .into_iter()
            .filter(|event| event_tag(event) == Some(tag))
            .collect()
    }

    pub fn count(&self, predicate: impl Fn(&Event) -> bool) -> usize {
        self.events()
            .iter()
            .filter(|event| predicate(event))
            .count()
    }

    pub async fn wait_until(&self, condition: impl Fn(&[Event]) -> bool) {
        loop {
            // Register before checking so a push from another worker thread is never missed.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            if condition(&lock(&self.events)) {
                return;
            }

            changed.await;
        }
    }

    pub fn id(&self, tag: u8) -> MessageId {
        *lock(&self.ids).entry(tag).or_insert_with(|| {
            let id = MessageId::new();
            lock(&self.tags).insert(id, tag);

            id
        })
    }

    pub fn tag(&self, id: MessageId) -> Option<u8> {
        lock(&self.tags).get(&id).copied()
    }

    pub fn live_tx(&self) -> usize {
        self.live_tx.load(Ordering::SeqCst)
    }

    pub fn max_live_tx(&self) -> usize {
        self.max_live_tx.load(Ordering::SeqCst)
    }

    pub fn max_outstanding_at_receive(&self) -> usize {
        self.max_outstanding_at_receive.load(Ordering::SeqCst)
    }

    pub fn committed_writes(&self) -> Vec<String> {
        lock(&self.committed_writes).clone()
    }

    pub fn summaries(&self) -> Vec<String> {
        lock(&self.summaries).clone()
    }

    pub fn mark_completed(&self, tag: u8) {
        let id = self.id(tag);
        lock(&self.durable).insert(id, Durable::Completed);
    }

    pub fn mark_dead(&self, tag: u8, reason: DeadReason) {
        let id = self.id(tag);
        lock(&self.durable).insert(id, Durable::Dead(reason));
    }

    pub fn script_begin(&self, step: Step) {
        lock(&self.script).begin.push_back(step);
    }

    pub fn script_rollback(&self, step: Step) {
        lock(&self.script).rollback.push_back(step);
    }

    pub fn script_claim(&self, tag: u8, step: ClaimStep) {
        lock(&self.script)
            .claim
            .entry(tag)
            .or_default()
            .push_back(step);
    }

    pub fn script_handler(&self, tag: u8, step: HandlerStep) {
        lock(&self.script)
            .handler
            .entry(tag)
            .or_default()
            .push_back(step);
    }

    pub fn script_complete(&self, tag: u8, step: Step) {
        lock(&self.script)
            .complete
            .entry(tag)
            .or_default()
            .push_back(step);
    }

    pub fn script_commit(&self, tag: u8, step: Step) {
        lock(&self.script)
            .commit
            .entry(tag)
            .or_default()
            .push_back(step);
    }

    pub fn script_fail(&self, tag: u8, step: Step) {
        lock(&self.script)
            .fail
            .entry(tag)
            .or_default()
            .push_back(step);
    }

    pub fn script_settle(&self, tag: u8, step: Step) {
        lock(&self.script)
            .settle
            .entry(tag)
            .or_default()
            .push_back(step);
    }

    pub fn fence_advance(&self, tag: u8) {
        lock(&self.script).fenced_advance.insert(tag);
    }

    fn tx_opened(&self) {
        let live = self.live_tx.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_live_tx.fetch_max(live, Ordering::SeqCst);
    }
}

fn event_tag(event: &Event) -> Option<u8> {
    match event {
        Event::Claim(tag)
        | Event::Handle(tag)
        | Event::Complete(tag)
        | Event::Commit(tag)
        | Event::Fail(tag, _)
        | Event::Ack(tag)
        | Event::Nak(tag, _)
        | Event::Terminate(tag)
        | Event::Heartbeat(tag)
        | Event::Left(tag) => Some(*tag),
        Event::Rollback(tag) | Event::TxDropped(tag) => *tag,
        Event::Open(_) | Event::Receive | Event::Begin => None,
    }
}

pub fn is_settlement(event: &Event) -> bool {
    matches!(
        event,
        Event::Ack(_) | Event::Nak(..) | Event::Terminate(_) | Event::Heartbeat(_)
    )
}

/// A provider error whose rendering must never escape.
#[derive(Debug)]
pub struct FakeError {
    kind: FailureKind,
}

impl fmt::Display for FakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "provider failure {PROVIDER_SENTINEL}")
    }
}

impl Error for FakeError {}

impl ErrorClassifier for FakeError {
    fn classify(&self) -> FailureKind {
        self.kind
    }
}

impl FakeError {
    pub fn kind(&self) -> FailureKind {
        self.kind
    }
}

async fn perform(step: Step) -> Result<(), FakeError> {
    match step {
        Step::Ok => Ok(()),
        Step::Error(kind) => Err(FakeError { kind }),
        Step::Unsupported => Err(FakeError {
            kind: FailureKind::Permanent,
        }),
        Step::Hang => std::future::pending().await,
        Step::Panic => panic!("scripted operation panic {PROVIDER_SENTINEL}"),
    }
}

// ---------------------------------------------------------------------------------------------
// Source and settlement.

#[derive(Debug)]
pub enum FakeWire {
    Valid {
        id: MessageId,

        message_type: &'static str,

        body: String,
    },
    Malformed,
    MapperPanic,
}

pub struct FakeSettlement {
    tag: u8,

    probe: Arc<Probe>,

    invoked: bool,

    supports_heartbeat: bool,
}

impl Drop for FakeSettlement {
    fn drop(&mut self) {
        self.probe.outstanding.fetch_sub(1, Ordering::SeqCst);

        if !self.invoked {
            self.probe.push(Event::Left(self.tag));
        }
    }
}

impl FakeSettlement {
    async fn operate(mut self, event: Event) -> Result<(), IndividualSettlementError<FakeError>> {
        self.invoked = true;
        self.probe.push(event);

        let step = next(lock(&self.probe.script).settle.get_mut(&self.tag)).unwrap_or(Step::Ok);

        match step {
            Step::Unsupported => Err(IndividualSettlementError::Unsupported(
                IndividualCapability::DelayedRetry,
            )),
            step => perform(step)
                .await
                .map_err(IndividualSettlementError::Operation),
        }
    }
}

impl IndividualSettlement for FakeSettlement {
    type Error = FakeError;

    async fn heartbeat(&mut self) -> Result<(), IndividualSettlementError<Self::Error>> {
        self.probe.push(Event::Heartbeat(self.tag));

        if !self.supports_heartbeat {
            return Err(IndividualSettlementError::Unsupported(
                IndividualCapability::Heartbeat,
            ));
        }

        let step = next(lock(&self.probe.script).settle.get_mut(&self.tag)).unwrap_or(Step::Ok);

        perform(step)
            .await
            .map_err(IndividualSettlementError::Operation)
    }

    async fn ack(self) -> Result<(), IndividualSettlementError<Self::Error>> {
        let event = Event::Ack(self.tag);

        self.operate(event).await
    }

    async fn nak(self, delay: Duration) -> Result<(), IndividualSettlementError<Self::Error>> {
        let event = Event::Nak(self.tag, delay);

        self.operate(event).await
    }

    async fn terminate(self) -> Result<(), IndividualSettlementError<Self::Error>> {
        let event = Event::Terminate(self.tag);

        self.operate(event).await
    }
}

pub struct FakeDelivery {
    wire: FakeWire,

    settlement: FakeSettlement,
}

impl Delivery for FakeDelivery {
    type Wire = FakeWire;
    type Settlement = FakeSettlement;

    fn into_parts(self) -> (Self::Wire, Self::Settlement) {
        (self.wire, self.settlement)
    }
}

enum SourceStep {
    Deliver { tag: u8, wire: FakeWire },
    OwnershipLost(u8),
    Close,
    Fail(FailureKind),
}

#[derive(Default)]
struct Queue {
    steps: Mutex<VecDeque<SourceStep>>,

    ready: Notify,
}

pub struct FakeSource {
    probe: Arc<Probe>,

    queue: Arc<Queue>,

    descriptor: IndividualSourceDescriptor,

    open: Step,

    validate: bool,
}

impl IndividualDeliverySource for FakeSource {
    type Delivery = FakeDelivery;
    type Error = FakeError;

    async fn open(
        &mut self,
        requirements: IndividualSourceRequirements,
    ) -> Result<IndividualSourceDescriptor, IndividualSourceOpenError<Self::Error>> {
        self.probe.push(Event::Open(requirements));

        perform(self.open)
            .await
            .map_err(IndividualSourceOpenError::Source)?;

        if self.validate {
            self.descriptor
                .validate(requirements)
                .map_err(IndividualSourceOpenError::Unsupported)?;
        }

        Ok(self.descriptor)
    }

    async fn receive(&mut self) -> Result<Option<Self::Delivery>, Self::Error> {
        let outstanding = self.probe.outstanding.load(Ordering::SeqCst);

        self.probe
            .max_outstanding_at_receive
            .fetch_max(outstanding, Ordering::SeqCst);

        self.probe.push(Event::Receive);

        loop {
            // Popping happens synchronously in the poll that returns, so dropping this future
            // never loses a queued step.
            let step = lock(&self.queue.steps).pop_front();

            match step {
                Some(SourceStep::Deliver { tag, wire }) => {
                    self.probe.outstanding.fetch_add(1, Ordering::SeqCst);

                    return Ok(Some(FakeDelivery {
                        wire,
                        settlement: FakeSettlement {
                            tag,
                            probe: Arc::clone(&self.probe),
                            invoked: false,
                            supports_heartbeat: self.descriptor.supports_heartbeat(),
                        },
                    }));
                }
                Some(SourceStep::Close) => return Ok(None),
                Some(SourceStep::OwnershipLost(_)) => {
                    return Err(FakeError {
                        kind: FailureKind::Permanent,
                    });
                }
                Some(SourceStep::Fail(kind)) => return Err(FakeError { kind }),
                None => self.queue.ready.notified().await,
            }
        }
    }
}

pub struct FakePartitionedSettlement {
    partition: u8,

    inner: FakeSettlement,
}

impl PartitionedLogSettlement for FakePartitionedSettlement {
    type Partition = u8;
    type Error = FakeError;

    async fn advance(self) -> Result<PartitionAdvance, Self::Error> {
        let mut inner = self.inner;
        let fenced = lock(&inner.probe.script).fenced_advance.remove(&inner.tag);

        if fenced {
            inner.invoked = true;
            inner.probe.push(Event::Ack(inner.tag));

            return Ok(PartitionAdvance::OwnershipLost);
        }

        inner
            .ack()
            .await
            .map(|()| PartitionAdvance::Advanced)
            .map_err(|error| match error {
                IndividualSettlementError::Operation(error) => error,
                _ => FakeError {
                    kind: FailureKind::Permanent,
                },
            })
    }

    fn partition(&self) -> &Self::Partition {
        &self.partition
    }
}

pub struct FakePartitionedDelivery {
    wire: FakeWire,

    settlement: FakePartitionedSettlement,
}

impl Delivery for FakePartitionedDelivery {
    type Wire = FakeWire;
    type Settlement = FakePartitionedSettlement;

    fn into_parts(self) -> (Self::Wire, Self::Settlement) {
        (self.wire, self.settlement)
    }
}

pub struct FakePartitionedSource {
    probe: Arc<Probe>,

    queue: Arc<Queue>,
}

impl PartitionedLogDeliverySource for FakePartitionedSource {
    type Partition = u8;
    type Delivery = FakePartitionedDelivery;
    type Error = FakeError;

    async fn open(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn receive(
        &mut self,
    ) -> Result<PartitionedLogReceive<Self::Delivery, Self::Partition>, Self::Error> {
        loop {
            let step = lock(&self.queue.steps).pop_front();

            match step {
                Some(SourceStep::Deliver { tag, wire }) => {
                    self.probe.outstanding.fetch_add(1, Ordering::SeqCst);

                    return Ok(PartitionedLogReceive::Delivery(FakePartitionedDelivery {
                        wire,
                        settlement: FakePartitionedSettlement {
                            partition: tag % 2,
                            inner: FakeSettlement {
                                tag,
                                probe: Arc::clone(&self.probe),
                                invoked: false,
                                supports_heartbeat: false,
                            },
                        },
                    }));
                }
                Some(SourceStep::OwnershipLost(partition)) => {
                    return Ok(PartitionedLogReceive::OwnershipLost(partition));
                }
                Some(SourceStep::Close) => return Ok(PartitionedLogReceive::Closed),
                Some(SourceStep::Fail(kind)) => return Err(FakeError { kind }),
                None => self.queue.ready.notified().await,
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Mapping and codec.

#[derive(Debug)]
pub struct MapperError;

impl fmt::Display for MapperError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "wire {PAYLOAD_SENTINEL} {HEADER_SENTINEL}")
    }
}

impl Error for MapperError {}

impl ErrorClassifier for MapperError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

pub struct FakeMapper;

impl EnvelopeMapper<FakeWire> for FakeMapper {
    type Error = MapperError;

    fn encode(&self, _envelope: &SerializedEnvelope) -> Result<FakeWire, Self::Error> {
        Err(MapperError)
    }

    fn decode(&self, wire: FakeWire) -> Result<SerializedEnvelope, Self::Error> {
        if matches!(wire, FakeWire::MapperPanic) {
            panic!("mapper panic {PAYLOAD_SENTINEL}");
        }

        let FakeWire::Valid {
            id,
            message_type,
            body,
        } = wire
        else {
            return Err(MapperError);
        };

        let mut metadata = Metadata::default();

        let name = HeaderName::new("x-sentinel").map_err(|_| MapperError)?;
        let value = HeaderValue::new(HEADER_SENTINEL).map_err(|_| MapperError)?;

        metadata
            .headers
            .insert(name, value)
            .map_err(|_| MapperError)?;

        Ok(SerializedEnvelope {
            message_id: id,
            message_type: MessageType::new(message_type).map_err(|_| MapperError)?,
            message_version: 1,
            content_type: ContentType::new("text/plain").map_err(|_| MapperError)?,
            payload: body.into_bytes(),
            metadata,
            ordering_key: None,
        })
    }
}

#[derive(Debug)]
pub struct CodecError;

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "body {PAYLOAD_SENTINEL}")
    }
}

impl Error for CodecError {}

impl ErrorClassifier for CodecError {
    fn classify(&self) -> FailureKind {
        FailureKind::Transient
    }
}

pub struct FakeCodec;

impl Serializer<Order> for FakeCodec {
    type Error = CodecError;

    fn serialize(&self, _envelope: &Envelope<Order>) -> Result<SerializedEnvelope, Self::Error> {
        Err(CodecError)
    }

    fn deserialize(&self, envelope: SerializedEnvelope) -> Result<Envelope<Order>, Self::Error> {
        if envelope.message_type.as_str() != Order::TYPE {
            return Err(CodecError);
        }

        let body = String::from_utf8(envelope.payload).map_err(|_| CodecError)?;

        if body.starts_with("undecodable") {
            return Err(CodecError);
        }

        Envelope::new(envelope.message_id, Order { body }, envelope.metadata)
            .map_err(|_| CodecError)
    }
}

// ---------------------------------------------------------------------------------------------
// Inbox and unit of work.

pub struct FakeTx {
    probe: Arc<Probe>,

    tag: Option<u8>,

    completed: Option<MessageId>,

    writes: Vec<String>,
}

impl Drop for FakeTx {
    fn drop(&mut self) {
        self.probe.live_tx.fetch_sub(1, Ordering::SeqCst);
        self.probe.push(Event::TxDropped(self.tag));
    }
}

#[derive(Debug)]
pub struct FakeReceipt {
    id: InboxId,

    failures: u32,
}

impl InboxReceipt for FakeReceipt {
    fn id(&self) -> InboxId {
        self.id
    }

    fn recorded_failures(&self) -> u32 {
        self.failures
    }
}

pub struct FakeInbox {
    probe: Arc<Probe>,

    max_attempts: NonZeroU32,
}

impl InboxUnitOfWork for FakeInbox {
    type Transaction = FakeTx;
    type Error = FakeError;

    async fn begin(&self) -> Result<Self::Transaction, Self::Error> {
        self.probe.push(Event::Begin);

        let step = lock(&self.probe.script)
            .begin
            .pop_front()
            .unwrap_or(Step::Ok);

        perform(step).await?;

        self.probe.tx_opened();

        Ok(FakeTx {
            probe: Arc::clone(&self.probe),
            tag: None,
            completed: None,
            writes: Vec::new(),
        })
    }

    async fn commit(&self, mut transaction: Self::Transaction) -> Result<(), Self::Error> {
        let tag = transaction.tag.unwrap_or(u8::MAX);
        self.probe.push(Event::Commit(tag));

        let step = next(lock(&self.probe.script).commit.get_mut(&tag)).unwrap_or(Step::Ok);
        perform(step).await?;

        if let Some(id) = transaction.completed {
            lock(&self.probe.durable).insert(id, Durable::Completed);
        }

        lock(&self.probe.committed_writes).append(&mut transaction.writes);

        Ok(())
    }

    async fn rollback(&self, transaction: Self::Transaction) -> Result<(), Self::Error> {
        self.probe.push(Event::Rollback(transaction.tag));

        let step = lock(&self.probe.script)
            .rollback
            .pop_front()
            .unwrap_or(Step::Ok);

        perform(step).await
    }
}

impl InboxStore<FakeTx> for FakeInbox {
    type Error = FakeError;
    type Receipt = FakeReceipt;

    fn max_attempts(&self) -> NonZeroU32 {
        self.max_attempts
    }

    async fn claim(
        &self,
        transaction: &mut FakeTx,
        record: &InboxRecord,
    ) -> Result<InboxClaimOutcome<Self::Receipt>, Self::Error> {
        let tag = self.probe.tag(record.message_id).unwrap_or(u8::MAX);
        transaction.tag = Some(tag);
        self.probe.push(Event::Claim(tag));

        let step = next(lock(&self.probe.script).claim.get_mut(&tag)).unwrap_or(ClaimStep::Real);

        match step {
            ClaimStep::Real => {}
            ClaimStep::InProgress => return Ok(InboxClaimOutcome::InProgressDuplicate),
            ClaimStep::Db(step) => perform(step).await?,
        }

        let state = lock(&self.probe.durable)
            .get(&record.message_id)
            .copied()
            .unwrap_or(Durable::Pending { failures: 0 });

        Ok(match state {
            Durable::Completed => InboxClaimOutcome::CompletedDuplicate,
            Durable::Dead(reason) => InboxClaimOutcome::DeadDuplicate { reason },
            Durable::Pending { failures } => InboxClaimOutcome::Claimed(FakeReceipt {
                id: InboxId::from_uuid(*record.message_id.as_uuid()),
                failures,
            }),
        })
    }

    async fn complete(
        &self,
        transaction: &mut FakeTx,
        receipt: Self::Receipt,
    ) -> Result<(), Self::Error> {
        let tag = transaction.tag.unwrap_or(u8::MAX);
        self.probe.push(Event::Complete(tag));

        let step = next(lock(&self.probe.script).complete.get_mut(&tag)).unwrap_or(Step::Ok);
        perform(step).await?;

        transaction.completed = Some(MessageId::from_uuid(receipt.id().into_uuid()));

        Ok(())
    }

    async fn fail(
        &self,
        record: &InboxRecord,
        failure: InboxFailure,
    ) -> Result<InboxFailureOutcome, Self::Error> {
        let tag = self.probe.tag(record.message_id).unwrap_or(u8::MAX);
        self.probe.push(Event::Fail(tag, failure.kind));

        let step = next(lock(&self.probe.script).fail.get_mut(&tag)).unwrap_or(Step::Ok);
        perform(step).await?;

        lock(&self.probe.summaries).push(failure.error.into_string());

        let mut durable = lock(&self.probe.durable);

        let state = durable
            .get(&record.message_id)
            .copied()
            .unwrap_or(Durable::Pending { failures: 0 });

        let (next_state, outcome) = match state {
            Durable::Completed => (Durable::Completed, InboxFailureOutcome::CompletedDuplicate),
            Durable::Dead(reason) => (
                Durable::Dead(reason),
                InboxFailureOutcome::Dead {
                    attempts: 0,
                    reason,
                },
            ),
            Durable::Pending { failures } => {
                let attempts = failures.saturating_add(1);

                if failure.kind.is_retryable() && attempts < self.max_attempts.get() {
                    (
                        Durable::Pending { failures: attempts },
                        InboxFailureOutcome::Retry { attempts },
                    )
                } else {
                    let reason = if failure.kind.is_retryable() {
                        DeadReason::Exhausted
                    } else {
                        DeadReason::Permanent
                    };

                    (
                        Durable::Dead(reason),
                        InboxFailureOutcome::Dead { attempts, reason },
                    )
                }
            }
        };

        durable.insert(record.message_id, next_state);

        Ok(outcome)
    }
}

// ---------------------------------------------------------------------------------------------
// Handler.

#[derive(Debug)]
pub struct HandlerError {
    kind: FailureKind,
}

impl fmt::Display for HandlerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "handler rejected {HANDLER_SENTINEL}")
    }
}

impl Error for HandlerError {}

impl ErrorClassifier for HandlerError {
    fn classify(&self) -> FailureKind {
        self.kind
    }
}

pub struct FakeHandler {
    probe: Arc<Probe>,
}

impl ConsumerHandler<Order, FakeTx> for FakeHandler {
    type Error = HandlerError;

    async fn handle(&self, tx: &mut FakeTx, envelope: &Envelope<Order>) -> Result<(), Self::Error> {
        let tag = self.probe.tag(envelope.message_id()).unwrap_or(u8::MAX);
        self.probe.push(Event::Handle(tag));

        let step = next(lock(&self.probe.script).handler.get_mut(&tag)).unwrap_or(HandlerStep::Ok);

        match step {
            HandlerStep::Ok => {}
            HandlerStep::Fail(kind) => return Err(HandlerError { kind }),
            HandlerStep::Panic => panic!("handler panic payload {PAYLOAD_SENTINEL}"),
            HandlerStep::Block(gate) => {
                let _permit = gate.acquire().await;
            }
            HandlerStep::Hang => std::future::pending::<()>().await,
            HandlerStep::BlockThreadThenHang(duration) => {
                std::thread::sleep(duration);
                std::future::pending::<()>().await;
            }
            HandlerStep::Yield => tokio::task::yield_now().await,
        }

        tx.writes.push(envelope.payload().body.clone());

        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Harness.

pub type TestConsumer =
    Consumer<Order, (FakeSource, FakeMapper, FakeCodec, FakeInbox, FakeHandler)>;

pub type TestPartitionedConsumer = Consumer<
    Order,
    (
        FakePartitionedSource,
        FakeMapper,
        FakeCodec,
        FakeInbox,
        FakeHandler,
    ),
>;

pub struct Harness {
    pub probe: Arc<Probe>,

    queue: Arc<Queue>,

    pub settings: ConsumerSettings,

    pub descriptor: IndividualSourceDescriptor,

    pub open: Step,

    pub validate: bool,

    pub max_attempts: NonZeroU32,
}

impl Harness {
    /// A harness whose source advertises exactly what `mode` needs.
    pub fn new(mode: SettlementMode) -> Self {
        let broker = mode == SettlementMode::Broker;
        let immediate = mode == SettlementMode::BrokerImmediateRequeue;
        let opened = descriptor(None, broker, broker || immediate);

        let opened = if immediate {
            opened.with_immediate_requeue()
        } else {
            opened
        };

        Self::with_descriptor(mode, opened)
    }

    pub fn with_descriptor(mode: SettlementMode, descriptor: IndividualSourceDescriptor) -> Self {
        let mut settings = ConsumerSettings::default();
        settings.mode = mode;

        settings.nak_delay = if mode == SettlementMode::BrokerImmediateRequeue {
            Duration::ZERO
        } else {
            NAK_DELAY
        };

        settings.max_in_flight =
            std::num::NonZeroUsize::new(4).unwrap_or(std::num::NonZeroUsize::MIN);

        Self {
            probe: Arc::new(Probe::default()),
            queue: Arc::new(Queue::default()),
            settings,
            descriptor,
            open: Step::Ok,
            validate: true,
            max_attempts: NonZeroU32::new(3).unwrap_or(NonZeroU32::MIN),
        }
    }

    fn enqueue(&self, step: SourceStep) {
        lock(&self.queue.steps).push_back(step);
        self.queue.ready.notify_one();
    }

    pub fn deliver(&self, tag: u8, body: &str) {
        self.deliver_as(tag, Order::TYPE, body);
    }

    pub fn deliver_as(&self, tag: u8, message_type: &'static str, body: &str) {
        let id = self.probe.id(tag);

        self.enqueue(SourceStep::Deliver {
            tag,
            wire: FakeWire::Valid {
                id,
                message_type,
                body: format!("{body} {PAYLOAD_SENTINEL}"),
            },
        });
    }

    pub fn deliver_malformed(&self, tag: u8) {
        self.enqueue(SourceStep::Deliver {
            tag,
            wire: FakeWire::Malformed,
        });
    }

    pub fn deliver_mapper_panic(&self, tag: u8) {
        self.enqueue(SourceStep::Deliver {
            tag,
            wire: FakeWire::MapperPanic,
        });
    }

    pub fn close(&self) {
        self.enqueue(SourceStep::Close);
    }

    pub fn fail_source(&self, kind: FailureKind) {
        self.enqueue(SourceStep::Fail(kind));
    }

    pub fn lose_partition(&self, partition: u8) {
        self.enqueue(SourceStep::OwnershipLost(partition));
    }

    /// Number of source steps not yet received.
    pub fn queued(&self) -> usize {
        lock(&self.queue.steps).len()
    }

    pub fn consumer(&self) -> Result<TestConsumer, sisa_messaging_consumer::ConsumerConfigError> {
        let source = FakeSource {
            probe: Arc::clone(&self.probe),
            queue: Arc::clone(&self.queue),
            descriptor: self.descriptor,
            open: self.open,
            validate: self.validate,
        };

        let inbox = FakeInbox {
            probe: Arc::clone(&self.probe),
            max_attempts: self.max_attempts,
        };

        let handler = FakeHandler {
            probe: Arc::clone(&self.probe),
        };

        let scope = InboxScope::new(SCOPE_SENTINEL).unwrap_or_else(|_| unreachable_scope());

        Consumer::new(
            source,
            FakeMapper,
            FakeCodec,
            inbox,
            scope,
            handler,
            self.settings.clone(),
        )
    }

    pub fn partitioned_consumer(
        &self,
    ) -> Result<TestPartitionedConsumer, sisa_messaging_consumer::ConsumerConfigError> {
        let source = FakePartitionedSource {
            probe: Arc::clone(&self.probe),
            queue: Arc::clone(&self.queue),
        };

        let inbox = FakeInbox {
            probe: Arc::clone(&self.probe),
            max_attempts: self.max_attempts,
        };

        let handler = FakeHandler {
            probe: Arc::clone(&self.probe),
        };

        let scope = InboxScope::new(SCOPE_SENTINEL).unwrap_or_else(|_| unreachable_scope());

        Consumer::new_partitioned(
            source,
            FakeMapper,
            FakeCodec,
            inbox,
            scope,
            handler,
            self.settings.clone(),
        )
    }

    /// Spawns the consumer; the result carries the live-transaction count observed the instant
    /// `run` returned.
    pub fn spawn(
        &self,
        cancel: CancellationToken,
    ) -> JoinHandle<(Result<ConsumerExit, ConsumerError>, usize)> {
        let consumer = self.consumer();
        let probe = Arc::clone(&self.probe);

        tokio::spawn(async move {
            let result = match consumer {
                Ok(consumer) => consumer.run(cancel).await,
                Err(error) => panic!("invalid test settings: {error}"),
            };

            (result, probe.live_tx())
        })
    }

    /// Runs to completion with a token that is never cancelled.
    pub async fn run(&self) -> Result<ConsumerExit, ConsumerError> {
        let (result, live) = join(self.spawn(CancellationToken::new())).await;

        assert_eq!(live, 0, "run returned while a transaction was live");

        result
    }
}

fn unreachable_scope() -> InboxScope {
    panic!("test scope must be valid")
}

pub fn descriptor(
    max_deliver: Option<u64>,
    delayed_retry: bool,
    terminal_discard: bool,
) -> IndividualSourceDescriptor {
    IndividualSourceDescriptor::new(
        None,
        max_deliver.and_then(NonZeroU64::new),
        delayed_retry,
        terminal_discard,
        false,
    )
    .unwrap_or_else(|_| panic!("valid descriptor"))
}

pub async fn join<T>(handle: JoinHandle<T>) -> T {
    match handle.await {
        Ok(value) => value,
        Err(error) => panic!("consumer task failed: {error}"),
    }
}

pub fn expect_error(result: Result<ConsumerExit, ConsumerError>) -> ConsumerError {
    match result {
        Ok(exit) => panic!("expected a consumer error, got {exit:?}"),
        Err(error) => error,
    }
}

/// Asserts neither rendering of `error` contains a sentinel.
pub fn assert_redacted(error: &ConsumerError) {
    let rendered = format!("{error} {error:?}");

    for sentinel in SENTINELS {
        assert!(
            !rendered.contains(sentinel),
            "error rendering leaked {sentinel}: {rendered}"
        );
    }

    assert!(error.source().is_none());
}

// ---------------------------------------------------------------------------------------------
// Log capture without a subscriber dependency.

#[derive(Clone, Default)]
pub struct Capture {
    lines: Arc<Mutex<Vec<String>>>,
}

/// Keeps a thread-local capture subscriber installed.
pub struct Installed {
    _guard: tracing::subscriber::DefaultGuard,

    _anchor: tracing::Dispatch,
}

impl Capture {
    /// Installs this subscriber as the thread-local default.
    ///
    /// With exactly one registered dispatcher, `tracing` recomputes callsite interest and the
    /// global level filter from the default of whichever thread registers a callsite, so a
    /// concurrently running test thread without a subscriber would disable these callsites. A
    /// second registered dispatcher makes interest account for every live dispatcher.
    pub fn install(&self) -> Installed {
        let anchor = tracing::Dispatch::new(Self::default());
        let guard = tracing::subscriber::set_default(self.clone());

        tracing::callsite::rebuild_interest_cache();

        Installed {
            _guard: guard,
            _anchor: anchor,
        }
    }

    pub fn lines(&self) -> Vec<String> {
        lock(&self.lines).clone()
    }
}

struct Fields(String);

impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        let _ = write!(self.0, "{}={value:?} ", field.name());
    }
}

impl tracing::Subscriber for Capture {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        let mut fields = Fields(format!("span {} ", span.metadata().name()));
        span.record(&mut fields);
        lock(&self.lines).push(fields.0);

        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        let mut fields = Fields(String::from("record "));
        values.record(&mut fields);
        lock(&self.lines).push(fields.0);
    }

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut fields = Fields(format!("{} ", event.metadata().target()));
        event.record(&mut fields);
        lock(&self.lines).push(fields.0);
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}
