//! The member thread: sole owner of the rdkafka consumer and transactional producer.
//!
//! The thread parks for at most [`MEMBER_PARK`] and is unparked by the consumer queue's
//! nonempty callback and by the source and its handles. Each wake drains the consumer with
//! non-blocking polls, applies captured rebalance events, commits the current generation's
//! advance requests as one transaction, and seeks then resumes released partitions. It never
//! holds the shared lock across a broker call.

use std::collections::{HashMap, HashSet};
use std::mem;
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{
    BaseConsumer, Consumer, ConsumerContext, ConsumerGroupMetadata, Rebalance,
};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::producer::{BaseProducer, DeliveryResult, Producer, ProducerContext};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::{ClientConfig, ClientContext};
use sisa_messaging::{FailureKind, PartitionAdvance};
use tokio::sync::{oneshot, watch};

use super::classify::{self, ConsumerVerdict, Stage, TxnFailure, Verdict};
use super::table::{BatchItem, Table};
use super::{KafkaPartition, KafkaShutdownOutcome, MEMBER_PARK, Reply, Shared, Waiter};
use crate::{
    KafkaHeader, KafkaRecord, KafkaSettlementError, KafkaSettlementErrorKind, KafkaSourceError,
    KafkaSourceErrorKind,
};

/// Non-blocking polls per wake before other work runs.
const POLL_BURST: usize = 512;

/// Pause between reconciliation attempts.
const RECONCILE_BACKOFF: Duration = Duration::from_millis(200);

/// Everything the member thread needs, prepared without I/O by the source constructor.
pub(super) struct Start {
    pub(super) consumer: ClientConfig,

    pub(super) producer: ClientConfig,

    pub(super) topics: Vec<Arc<str>>,

    pub(super) operation_timeout: Duration,

    pub(super) shutdown_timeout: Duration,

    pub(super) exit: watch::Sender<Option<KafkaShutdownOutcome>>,
}

/// Starts the detached member thread; the open result arrives on `opened`.
pub(super) fn spawn(
    start: Start,
    shared: Arc<Shared>,
    opened: oneshot::Sender<Result<(), KafkaSourceError>>,
) -> Result<(), KafkaSourceError> {
    let member_shared = Arc::clone(&shared);

    let handle = thread::Builder::new()
        .name("sisa-kafka-member".to_owned())
        .spawn(move || run(start, member_shared, opened))
        .map_err(|_| source_error(KafkaSourceErrorKind::Initialization, FailureKind::Transient))?;

    let _ = shared.member.set(handle.thread().clone());

    // The thread is detached: dropping the source signals it and never joins it.
    drop(handle);

    Ok(())
}

const fn source_error(kind: KafkaSourceErrorKind, failure: FailureKind) -> KafkaSourceError {
    KafkaSourceError::new(kind, failure)
}

const fn settlement_error(
    kind: KafkaSettlementErrorKind,
    failure: FailureKind,
) -> KafkaSettlementError {
    KafkaSettlementError::new(kind, failure)
}

/// One rebalance observed inside the consumer callback, applied after the poll returns.
enum RebalanceEvent {
    Assign {
        partitions: Vec<(String, i32)>,

        metadata: Option<ConsumerGroupMetadata>,
    },

    Revoke,

    Error,
}

/// Captures rebalances and discards librdkafka logs and error strings, which may name brokers.
#[derive(Default)]
struct MemberContext {
    events: Mutex<Vec<RebalanceEvent>>,
}

impl ClientContext for MemberContext {
    fn log(&self, _level: RDKafkaLogLevel, _facility: &str, _message: &str) {}

    fn error(&self, _error: KafkaError, _reason: &str) {}
}

impl ConsumerContext for MemberContext {
    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let event = match rebalance {
            // The generation and assignment are known together only here; the snapshot is
            // bound to this assignment and never refreshed at settlement.
            Rebalance::Assign(partitions) => RebalanceEvent::Assign {
                partitions: partitions
                    .elements()
                    .iter()
                    .map(|element| (element.topic().to_owned(), element.partition()))
                    .collect(),
                metadata: consumer.group_metadata(),
            },
            Rebalance::Revoke(_) => RebalanceEvent::Revoke,
            Rebalance::Error(_) => RebalanceEvent::Error,
        };

        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event);
    }
}

/// Producer context that discards librdkafka logs and error strings.
struct QuietContext;

impl ClientContext for QuietContext {
    fn log(&self, _level: RDKafkaLogLevel, _facility: &str, _message: &str) {}

    fn error(&self, _error: KafkaError, _reason: &str) {}
}

impl ProducerContext for QuietContext {
    type DeliveryOpaque = ();

    fn delivery(&self, _result: &DeliveryResult<'_>, _opaque: Self::DeliveryOpaque) {}
}

type MemberConsumer = BaseConsumer<MemberContext>;
type MemberTable = Table<KafkaPartition, KafkaRecord, Waiter>;
type TxnProducer = BaseProducer<QuietContext>;

/// Records the thread's end, including an unwinding one, for the source and shutdown handle.
struct ExitGuard {
    shared: Arc<Shared>,

    exit: watch::Sender<Option<KafkaShutdownOutcome>>,

    outcome: KafkaShutdownOutcome,
}

impl Drop for ExitGuard {
    fn drop(&mut self) {
        {
            let mut state = self.shared.lock();
            state.stopped = true;

            if state.failure.is_none() && !state.shutdown {
                state.failure = Some(source_error(
                    KafkaSourceErrorKind::MemberStopped,
                    FailureKind::Transient,
                ));
            }

            for waiter in state.table.drain_requests() {
                let _ = waiter.send(Err(settlement_error(
                    KafkaSettlementErrorKind::MemberStopped,
                    FailureKind::Transient,
                )));
            }
        }

        self.shared.ready.notify_one();
        let _ = self.exit.send(Some(self.outcome));
    }
}

fn run(start: Start, shared: Arc<Shared>, opened: oneshot::Sender<Result<(), KafkaSourceError>>) {
    let _ = shared.member.set(thread::current());

    let Start {
        consumer,
        producer,
        topics,
        operation_timeout,
        shutdown_timeout,
        exit,
    } = start;

    let mut guard = ExitGuard {
        shared: Arc::clone(&shared),
        exit,
        outcome: KafkaShutdownOutcome::Closed,
    };

    match Member::open(&consumer, producer, topics, operation_timeout, &shared) {
        Ok(member) => {
            let _ = opened.send(Ok(()));
            guard.outcome = member.run(shutdown_timeout);
        }
        Err((error, consumer)) => {
            {
                let mut state = shared.lock();
                state.failure.get_or_insert(error);
            }

            let _ = opened.send(Err(error));

            if let Some(consumer) = consumer {
                guard.outcome = close_consumer(consumer, shutdown_timeout);
            }
        }
    }
}

type OpenError = (KafkaSourceError, Option<MemberConsumer>);

struct Member {
    consumer: MemberConsumer,

    producer: Option<TxnProducer>,

    producer_config: ClientConfig,

    shared: Arc<Shared>,

    topics: Vec<Arc<str>>,

    /// The group metadata captured with the current generation's assignment.
    metadata: Option<(u64, ConsumerGroupMetadata)>,

    txn_open: bool,

    failed: bool,

    operation_timeout: Duration,
}

impl Member {
    fn open(
        consumer_config: &ClientConfig,
        producer_config: ClientConfig,
        topics: Vec<Arc<str>>,
        operation_timeout: Duration,
        shared: &Arc<Shared>,
    ) -> Result<Self, OpenError> {
        let mut consumer: MemberConsumer = consumer_config
            .create_with_context(MemberContext::default())
            .map_err(|_| {
                (
                    source_error(KafkaSourceErrorKind::Initialization, FailureKind::Permanent),
                    None,
                )
            })?;

        let member = thread::current();
        consumer.set_nonempty_callback(move || member.unpark());

        let producer = match new_producer(&producer_config, operation_timeout) {
            Ok(producer) => producer,
            Err(error) => return Err((error.open_error(), Some(consumer))),
        };

        for topic in &topics {
            if let Err(error) = verify_topic(&consumer, topic, operation_timeout) {
                return Err((error, Some(consumer)));
            }
        }

        let names: Vec<&str> = topics.iter().map(AsRef::as_ref).collect();

        if consumer.subscribe(&names).is_err() {
            return Err((
                source_error(KafkaSourceErrorKind::Subscription, FailureKind::Permanent),
                Some(consumer),
            ));
        }

        Ok(Self {
            consumer,
            producer: Some(producer),
            producer_config,
            shared: Arc::clone(shared),
            topics,
            metadata: None,
            txn_open: false,
            failed: false,
            operation_timeout,
        })
    }

    fn run(mut self, shutdown_timeout: Duration) -> KafkaShutdownOutcome {
        loop {
            if self.shared.lock().shutdown {
                break;
            }

            self.cycle();

            if self.failed {
                break;
            }

            thread::park_timeout(MEMBER_PARK);
        }

        self.shutdown(shutdown_timeout)
    }

    fn cycle(&mut self) {
        if let Some(producer) = &self.producer {
            producer.poll(Duration::ZERO);
        }

        self.poll_consumer();

        if !self.failed {
            self.commit_batch();
        }

        if !self.failed {
            self.resume_partitions();
        }

        self.notify_output();
    }

    fn poll_consumer(&mut self) {
        let mut idle = 0;

        for _ in 0..POLL_BURST {
            let polled = match self.consumer.poll(Duration::ZERO) {
                None => None,
                Some(Ok(message)) => Some(Ok(self.offer(&message))),
                Some(Err(error)) => Some(Err(error)),
            };

            // Rebalance events are applied before the next poll can deliver any record.
            self.apply_rebalance();

            if self.failed {
                return;
            }

            match polled {
                None => {
                    idle += 1;

                    if idle >= 2 {
                        return;
                    }
                }
                Some(Ok(pause)) => {
                    idle = 0;
                    self.pause(&pause);
                }
                Some(Err(error)) => {
                    idle = 0;
                    self.on_consumer_error(&error);
                }
            }

            if self.failed {
                return;
            }
        }
    }

    /// Offers one fetched record to the table; returns the partitions to pause.
    fn offer(&self, message: &BorrowedMessage<'_>) -> Vec<KafkaPartition> {
        let Some(topic) = self
            .topics
            .iter()
            .find(|topic| topic.as_ref() == message.topic())
        else {
            return Vec::new();
        };

        let key = KafkaPartition::new(Arc::clone(topic), message.partition());

        self.shared
            .lock()
            .table
            .on_record(&key, message.offset(), || record(message))
            .pause
    }

    fn on_consumer_error(&mut self, error: &KafkaError) {
        let (code, fatal) = match error {
            KafkaError::MessageConsumptionFatal(code) => (*code, true),
            KafkaError::MessageConsumption(code) => (*code, false),
            other => (
                other.rdkafka_error_code().unwrap_or(RDKafkaErrorCode::Fail),
                false,
            ),
        };

        // A fatal event reports the generic fatal code; the underlying cause is authoritative.
        // Only its code is read, never its message.
        let code = if fatal || code == RDKafkaErrorCode::Fatal {
            self.consumer
                .client()
                .fatal_error()
                .map_or(code, |(cause, _)| cause)
        } else {
            code
        };

        match classify::consumer(code, fatal) {
            ConsumerVerdict::Continue => {}
            ConsumerVerdict::InstanceFenced => {
                self.fail(KafkaSourceErrorKind::InstanceFenced, FailureKind::Permanent);
            }
            ConsumerVerdict::Authorization => {
                self.fail(KafkaSourceErrorKind::Authorization, FailureKind::Permanent);
            }
            ConsumerVerdict::Fatal => {
                self.fail(KafkaSourceErrorKind::Fatal, FailureKind::Permanent)
            }
        }
    }

    fn apply_rebalance(&mut self) {
        let events = mem::take(
            &mut *self
                .consumer
                .context()
                .events
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );

        for event in events {
            match event {
                RebalanceEvent::Assign {
                    partitions,
                    metadata,
                } => {
                    let Some(metadata) = metadata else {
                        self.fail(
                            KafkaSourceErrorKind::GroupMetadataUnavailable,
                            FailureKind::Permanent,
                        );

                        return;
                    };

                    let keys: Vec<KafkaPartition> = partitions
                        .into_iter()
                        .filter_map(|(topic, partition)| {
                            self.topics
                                .iter()
                                .find(|known| known.as_ref() == topic)
                                .map(|known| KafkaPartition::new(Arc::clone(known), partition))
                        })
                        .collect();

                    let (generation, withheld) =
                        self.shared.lock().table.assign(keys.iter().cloned());

                    self.metadata = Some((generation, metadata));
                    self.pause(&withheld);

                    // A new assignment starts every other partition fetching, whatever pause
                    // state an earlier generation left on it.
                    let fetching: Vec<KafkaPartition> = keys
                        .into_iter()
                        .filter(|key| !withheld.contains(key))
                        .collect();

                    if !fetching.is_empty()
                        && self.consumer.resume(&partition_list(&fetching)).is_err()
                    {
                        self.fail(
                            KafkaSourceErrorKind::PartitionControl,
                            FailureKind::Transient,
                        );
                    }
                }
                RebalanceEvent::Revoke => {
                    self.metadata = None;

                    {
                        let mut state = self.shared.lock();

                        for waiter in state.table.revoke().lost {
                            let _ = waiter.send(Ok(PartitionAdvance::OwnershipLost));
                        }
                    }

                    if self.txn_open {
                        self.resolve_open_transaction();
                    }

                    self.shared.ready.notify_one();
                }
                RebalanceEvent::Error => {
                    self.fail(KafkaSourceErrorKind::Rebalance, FailureKind::Permanent);

                    return;
                }
            }

            if self.failed {
                return;
            }
        }
    }

    /// Aborts a transaction left open at revocation, or fences it with a successor epoch.
    fn resolve_open_transaction(&mut self) {
        let aborted = self
            .producer
            .as_ref()
            .is_some_and(|producer| producer.abort_transaction(self.operation_timeout).is_ok());

        self.txn_open = false;

        if aborted {
            return;
        }

        self.producer = None;

        match new_producer(&self.producer_config, self.operation_timeout) {
            Ok(producer) => self.producer = Some(producer),
            Err(_) => self.fail(
                KafkaSourceErrorKind::TransactionInitialization,
                FailureKind::Transient,
            ),
        }
    }

    fn commit_batch(&mut self) {
        let batch = self
            .shared
            .lock()
            .table
            .take_batch(|waiter| !waiter.is_closed());

        if batch.items.is_empty() {
            return;
        }

        let mut offsets = TopicPartitionList::with_capacity(batch.items.len());
        let mut listed = true;

        for item in &batch.items {
            listed &= offsets
                .add_partition_offset(
                    item.key.topic(),
                    item.key.partition(),
                    Offset::Offset(item.offset + 1),
                )
                .is_ok();
        }

        let verdict = match (&self.metadata, &self.producer) {
            (Some((generation, metadata)), Some(producer))
                if *generation == batch.generation && listed =>
            {
                transact(
                    producer,
                    &mut self.txn_open,
                    &offsets,
                    metadata,
                    self.operation_timeout,
                )
            }
            // Without the generation's snapshot nothing can be sent; the generation is gone.
            (None, _) => Some(Verdict::OwnershipLost),
            _ => Some(Verdict::Reconcile),
        };

        match verdict {
            None => self.conclude(
                batch.items,
                Ok(PartitionAdvance::Advanced),
                |table, key, offset, delivered| table.advanced(key, offset, delivered),
            ),
            Some(Verdict::OwnershipLost) => self.conclude(
                batch.items,
                Ok(PartitionAdvance::OwnershipLost),
                |table, key, offset, delivered| table.fenced(key, offset, delivered),
            ),
            Some(Verdict::Permanent) => self.conclude(
                batch.items,
                Err(settlement_error(
                    KafkaSettlementErrorKind::Authorization,
                    FailureKind::Permanent,
                )),
                |table, key, offset, _| table.failed_permanently(key, offset),
            ),
            Some(Verdict::Reconcile) => self.reconcile(batch.items),
        }
    }

    /// Replies to every waiter and applies `update` under one lock, so an abandoned reply is
    /// observed either by the failed send or by the handle's abandon guard.
    fn conclude(
        &self,
        items: Vec<BatchItem<KafkaPartition, Waiter>>,
        reply: Reply,
        update: impl Fn(&mut MemberTable, &KafkaPartition, i64, bool),
    ) {
        let mut state = self.shared.lock();

        for item in items {
            let delivered = item.waiter.send(reply).is_ok();
            update(&mut state.table, &item.key, item.offset, delivered);
        }

        drop(state);
        self.shared.ready.notify_one();
    }

    /// Re-establishes the committed cursor of an indeterminate batch behind an epoch fence.
    fn reconcile(&mut self, items: Vec<BatchItem<KafkaPartition, Waiter>>) {
        {
            let mut state = self.shared.lock();

            for item in &items {
                state.table.begin_reconcile(&item.key, item.offset);
            }
        }

        // Dropping the producer and initializing a successor with the same transactional id
        // bumps the epoch: the coordinator aborts any transaction of the old epoch, and no
        // late effect of it can move the cursor afterwards.
        self.producer = None;
        self.txn_open = false;

        let mut partitions = TopicPartitionList::with_capacity(items.len());

        for item in &items {
            partitions.add_partition(item.key.topic(), item.key.partition());
        }

        let deadline = Instant::now() + self.operation_timeout.saturating_mul(3);
        let mut committed: Option<HashMap<(String, i32), Offset>> = None;
        let mut permanent = false;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());

            if remaining.is_zero() {
                break;
            }

            let step = remaining.min(self.operation_timeout);

            if self.producer.is_none() {
                match new_producer(&self.producer_config, step) {
                    Ok(producer) => self.producer = Some(producer),
                    Err(ProducerError::Init(failure))
                        if classify::is_authorization(failure.code) =>
                    {
                        permanent = true;

                        break;
                    }
                    Err(_) => {
                        thread::sleep(RECONCILE_BACKOFF.min(remaining));

                        continue;
                    }
                }
            }

            // read_committed makes this a stable offset fetch, which waits out any transaction
            // still pending for these partitions.
            if let Ok(list) = self.consumer.committed_offsets(partitions.clone(), step) {
                let mut offsets = HashMap::new();
                let mut complete = true;

                for element in list.elements() {
                    if element.error().is_ok() {
                        offsets.insert(
                            (element.topic().to_owned(), element.partition()),
                            element.offset(),
                        );
                    } else {
                        complete = false;
                    }
                }

                if complete {
                    committed = Some(offsets);

                    break;
                }
            }

            thread::sleep(
                RECONCILE_BACKOFF.min(deadline.saturating_duration_since(Instant::now())),
            );
        }

        let mut unknown = false;

        {
            let mut state = self.shared.lock();

            for item in items {
                let cursor = committed.as_ref().and_then(|offsets| {
                    offsets
                        .get(&(item.key.topic().to_owned(), item.key.partition()))
                        .copied()
                });

                let advanced = match cursor {
                    Some(Offset::Offset(next)) => Some(next > item.offset),
                    Some(Offset::Invalid) => Some(false),
                    _ => None,
                };

                match advanced {
                    Some(advanced) => {
                        // The epoch fence and an unchanged cursor prove the advance did not
                        // apply; an advanced cursor proves it did.
                        let reply = if advanced {
                            PartitionAdvance::Advanced
                        } else {
                            PartitionAdvance::OwnershipLost
                        };

                        let delivered = item.waiter.send(Ok(reply)).is_ok();

                        state
                            .table
                            .reconciled(&item.key, item.offset, advanced, delivered);
                    }
                    None => {
                        unknown = true;

                        let _ = item.waiter.send(Err(settlement_error(
                            KafkaSettlementErrorKind::Indeterminate,
                            FailureKind::Transient,
                        )));
                    }
                }
            }
        }

        self.shared.ready.notify_one();

        if permanent {
            self.fail(KafkaSourceErrorKind::Authorization, FailureKind::Permanent);
        } else if unknown {
            // The partition stays paused in the reconciling state.
            self.fail(KafkaSourceErrorKind::Reconciliation, FailureKind::Transient);
        }
    }

    fn resume_partitions(&mut self) {
        let resumable = self.shared.lock().table.resumable();

        if resumable.is_empty() {
            return;
        }

        let mut seeks = TopicPartitionList::with_capacity(resumable.len());
        let mut failed: HashSet<(String, i32)> = HashSet::new();

        for (key, next) in &resumable {
            if let Some(next) = next
                && seeks
                    .add_partition_offset(key.topic(), key.partition(), Offset::Offset(*next))
                    .is_err()
            {
                failed.insert((key.topic().to_owned(), key.partition()));
            }
        }

        // Every resume is preceded by a seek to the exact next position, so records the
        // consumer position moved past while the partition was paused are fetched again.
        if seeks.count() > 0 {
            match self
                .consumer
                .seek_partitions(seeks.clone(), self.operation_timeout)
            {
                Ok(result) => {
                    for element in result.elements() {
                        if element.error().is_err() {
                            failed.insert((element.topic().to_owned(), element.partition()));
                        }
                    }
                }
                Err(_) => {
                    for element in seeks.elements() {
                        failed.insert((element.topic().to_owned(), element.partition()));
                    }
                }
            }
        }

        let ready: Vec<KafkaPartition> = resumable
            .into_iter()
            .map(|(key, _)| key)
            .filter(|key| !failed.contains(&(key.topic().to_owned(), key.partition())))
            .collect();

        if ready.is_empty() {
            return;
        }

        // A failed seek or resume leaves the partition pending; the next wake retries it.
        if self.consumer.resume(&partition_list(&ready)).is_ok() {
            self.shared.lock().table.resumed(&ready);
        }
    }

    fn pause(&mut self, keys: &[KafkaPartition]) {
        if keys.is_empty() {
            return;
        }

        if self.consumer.pause(&partition_list(keys)).is_err() {
            self.fail(
                KafkaSourceErrorKind::PartitionControl,
                FailureKind::Transient,
            );
        }
    }

    fn notify_output(&self) {
        if self.shared.lock().table.has_output() {
            self.shared.ready.notify_one();
        }
    }

    fn fail(&mut self, kind: KafkaSourceErrorKind, failure: FailureKind) {
        self.failed = true;

        {
            let mut state = self.shared.lock();
            state.failure.get_or_insert(source_error(kind, failure));

            for waiter in state.table.drain_requests() {
                let _ = waiter.send(Err(settlement_error(
                    KafkaSettlementErrorKind::MemberStopped,
                    FailureKind::Transient,
                )));
            }
        }

        self.shared.ready.notify_one();
    }

    fn shutdown(mut self, timeout: Duration) -> KafkaShutdownOutcome {
        // Advances whose waiters are still live finish; requests with dead waiters are skipped.
        if !self.failed {
            self.commit_batch();
        }

        {
            let mut state = self.shared.lock();
            state.stopped = true;

            for waiter in state.table.drain_requests() {
                let _ = waiter.send(Err(settlement_error(
                    KafkaSettlementErrorKind::MemberStopped,
                    FailureKind::Transient,
                )));
            }
        }

        self.shared.ready.notify_one();

        if self.txn_open
            && let Some(producer) = &self.producer
        {
            let _ = producer.abort_transaction(self.operation_timeout.min(timeout));
        }

        drop(self.producer.take());

        close_consumer(self.consumer, timeout)
    }
}

/// Closes the consumer within `timeout`. rdkafka's destructor waits without a bound for an
/// unfinished close, so a consumer that does not close in time is deliberately leaked.
fn close_consumer(consumer: MemberConsumer, timeout: Duration) -> KafkaShutdownOutcome {
    let deadline = Instant::now() + timeout;

    if consumer.close_queue().is_err() {
        mem::forget(consumer);

        return KafkaShutdownOutcome::TimedOut;
    }

    while !consumer.closed() {
        let remaining = deadline.saturating_duration_since(Instant::now());

        if remaining.is_zero() {
            mem::forget(consumer);

            return KafkaShutdownOutcome::TimedOut;
        }

        let _ = consumer.poll(remaining.min(MEMBER_PARK));
    }

    drop(consumer);

    KafkaShutdownOutcome::Closed
}

/// Commits `offsets` in one transaction. Returns `None` once committed.
fn transact(
    producer: &TxnProducer,
    txn_open: &mut bool,
    offsets: &TopicPartitionList,
    metadata: &ConsumerGroupMetadata,
    timeout: Duration,
) -> Option<Verdict> {
    if let Err(error) = producer.begin_transaction() {
        return Some(resolve_failure(
            producer,
            txn_open,
            Stage::Begin,
            &error,
            timeout,
        ));
    }

    *txn_open = true;

    if let Err(error) = producer.send_offsets_to_transaction(offsets, metadata, timeout) {
        return Some(resolve_failure(
            producer,
            txn_open,
            Stage::SendOffsets,
            &error,
            timeout,
        ));
    }

    match producer.commit_transaction(timeout) {
        Ok(()) => {
            *txn_open = false;

            None
        }
        Err(error) => Some(resolve_failure(
            producer,
            txn_open,
            Stage::Commit,
            &error,
            timeout,
        )),
    }
}

fn resolve_failure(
    producer: &TxnProducer,
    txn_open: &mut bool,
    stage: Stage,
    error: &KafkaError,
    timeout: Duration,
) -> Verdict {
    let disposition = classify::transaction(stage, txn_failure(error));

    let aborted = match disposition {
        classify::Disposition::Conclude(_) => false,
        classify::Disposition::Abort { .. } => producer.abort_transaction(timeout).is_ok(),
    };

    if aborted {
        *txn_open = false;
    }

    classify::after_abort(disposition, aborted)
}

fn txn_failure(error: &KafkaError) -> TxnFailure {
    match error {
        KafkaError::Transaction(inner) => TxnFailure {
            code: inner.code(),
            fatal: inner.is_fatal(),
            abortable: inner.txn_requires_abort(),
        },
        other => TxnFailure {
            code: other.rdkafka_error_code().unwrap_or(RDKafkaErrorCode::Fail),
            fatal: false,
            abortable: false,
        },
    }
}

enum ProducerError {
    Create,

    Init(TxnFailure),
}

impl ProducerError {
    fn open_error(&self) -> KafkaSourceError {
        match self {
            Self::Create => {
                source_error(KafkaSourceErrorKind::Initialization, FailureKind::Permanent)
            }
            Self::Init(failure) if classify::is_authorization(failure.code) => {
                source_error(KafkaSourceErrorKind::Authorization, FailureKind::Permanent)
            }
            Self::Init(failure) => source_error(
                KafkaSourceErrorKind::TransactionInitialization,
                if classify::open_is_transient(failure.code, failure.fatal) {
                    FailureKind::Transient
                } else {
                    FailureKind::Permanent
                },
            ),
        }
    }
}

fn new_producer(config: &ClientConfig, timeout: Duration) -> Result<TxnProducer, ProducerError> {
    let producer: TxnProducer = config
        .create_with_context(QuietContext)
        .map_err(|_| ProducerError::Create)?;

    producer
        .init_transactions(timeout)
        .map_err(|error| ProducerError::Init(txn_failure(&error)))?;

    Ok(producer)
}

fn verify_topic(
    consumer: &MemberConsumer,
    topic: &str,
    timeout: Duration,
) -> Result<(), KafkaSourceError> {
    let metadata = consumer
        .fetch_metadata(Some(topic), timeout)
        .map_err(|error| {
            let code = error.rdkafka_error_code().unwrap_or(RDKafkaErrorCode::Fail);

            if classify::is_authorization(code) {
                source_error(KafkaSourceErrorKind::Authorization, FailureKind::Permanent)
            } else {
                source_error(KafkaSourceErrorKind::Metadata, FailureKind::Transient)
            }
        })?;

    let Some(entry) = metadata.topics().iter().find(|entry| entry.name() == topic) else {
        return Err(source_error(
            KafkaSourceErrorKind::MissingTopic,
            FailureKind::Permanent,
        ));
    };

    match entry.error().map(RDKafkaErrorCode::from) {
        None if !entry.partitions().is_empty() => Ok(()),
        None | Some(RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic) => {
            Err(source_error(
                KafkaSourceErrorKind::MissingTopic,
                FailureKind::Permanent,
            ))
        }
        Some(code) if classify::is_authorization(code) => Err(source_error(
            KafkaSourceErrorKind::Authorization,
            FailureKind::Permanent,
        )),
        Some(_) => Err(source_error(
            KafkaSourceErrorKind::Metadata,
            FailureKind::Transient,
        )),
    }
}

fn partition_list(keys: &[KafkaPartition]) -> TopicPartitionList {
    let mut list = TopicPartitionList::with_capacity(keys.len());

    for key in keys {
        list.add_partition(key.topic(), key.partition());
    }

    list
}

fn record(message: &BorrowedMessage<'_>) -> KafkaRecord {
    KafkaRecord {
        key: message.key().map(<[u8]>::to_vec),
        payload: message.payload().map_or_else(Vec::new, <[u8]>::to_vec),
        headers: message
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .map(|header| KafkaHeader {
                        name: header.key.to_owned(),
                        value: header.value.map(<[u8]>::to_vec),
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}
