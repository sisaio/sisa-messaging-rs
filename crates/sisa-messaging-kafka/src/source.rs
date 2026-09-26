//! Partitioned delivery source over one static Kafka consumer-group member.
//!
//! A dedicated member thread owns the rdkafka consumer and the transactional offset-commit
//! producer; every poll, rebalance callback, pause, seek, and transaction runs there, so no
//! blocking librdkafka call reaches the async runtime. The source and its settlement handles
//! exchange only in-memory state with that thread through [`table::Table`].

mod classify;
mod member;
mod table;

use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread::Thread;
use std::time::Duration;

use rdkafka::ClientConfig;
use sisa_messaging::{
    Delivery, FailureKind, PartitionAdvance, PartitionedLogDeliverySource, PartitionedLogReceive,
    PartitionedLogSettlement,
};
use tokio::sync::{Notify, oneshot, watch};

use crate::client::BaseConfig;
use crate::{
    KafkaClientError, KafkaClientErrorKind, KafkaConsumerSettings, KafkaRecord,
    KafkaSettlementError, KafkaSettlementErrorKind, KafkaSourceError, KafkaSourceErrorKind,
};

use table::{Admission, Popped, Table};

/// Consumer properties that identify the member; any advanced value is an override.
const CONSUMER_IDENTITY: [&str; 2] = ["group.id", "group.instance.id"];

/// Producer properties that identify the offset-commit producer.
const PRODUCER_IDENTITY: [&str; 1] = ["transactional.id"];

/// Consumer properties the fencing protocol depends on.
const CONSUMER_FORCED: [(&str, &str); 7] = [
    ("isolation.level", "read_committed"),
    ("partition.assignment.strategy", "range"),
    ("group.protocol", "classic"),
    ("enable.auto.commit", "false"),
    ("auto.commit.enable", "false"),
    ("enable.auto.offset.store", "false"),
    ("allow.auto.create.topics", "false"),
];

/// Producer properties the transactional offset commit depends on.
const PRODUCER_FORCED: [(&str, &str); 2] = [("enable.idempotence", "true"), ("acks", "all")];

/// Consumer defaults an advanced property may replace: a short broker fetch wait bounds how long
/// a resumed partition waits behind a fetch held open for other partitions.
const CONSUMER_DEFAULTS: [(&str, &str); 1] = [("fetch.wait.max.ms", "10")];

/// Producer defaults an advanced property may replace: a short initial retry backoff bounds the
/// wait while the coordinator finishes the previous transaction.
const PRODUCER_DEFAULTS: [(&str, &str); 1] = [("retry.backoff.ms", "10")];

/// An opaque topic partition owned by a Kafka source.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct KafkaPartition {
    topic: Arc<str>,

    partition: i32,
}

impl KafkaPartition {
    pub(crate) const fn new(topic: Arc<str>, partition: i32) -> Self {
        Self { topic, partition }
    }

    /// Returns the topic name.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition number.
    #[must_use]
    pub const fn partition(&self) -> i32 {
        self.partition
    }
}

/// One Kafka record with its partition settlement handle.
pub struct KafkaDelivery {
    record: KafkaRecord,

    settlement: KafkaSettlement,
}

impl fmt::Debug for KafkaDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaDelivery")
            .field("record", &"<redacted>")
            .field("settlement", &self.settlement)
            .finish()
    }
}

impl Delivery for KafkaDelivery {
    type Wire = KafkaRecord;
    type Settlement = KafkaSettlement;

    fn into_parts(self) -> (Self::Wire, Self::Settlement) {
        (self.record, self.settlement)
    }
}

/// Advances one record's partition through a generation-bound transactional offset commit.
///
/// Dropping the handle without advancing keeps its partition paused until the partition is
/// revoked or the source shuts down.
pub struct KafkaSettlement {
    partition: KafkaPartition,

    offset: i64,

    generation: u64,

    shared: Arc<Shared>,

    armed: bool,
}

impl KafkaSettlement {
    /// Returns the record offset this handle advances past.
    #[must_use]
    pub const fn offset(&self) -> i64 {
        self.offset
    }
}

impl fmt::Debug for KafkaSettlement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaSettlement")
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

impl Drop for KafkaSettlement {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let wake =
            self.shared
                .lock()
                .table
                .handle_dropped(&self.partition, self.generation, self.offset);

        if wake {
            self.shared.wake_member();
        }
    }
}

/// Queues a release if the advance future is dropped before it observes its reply.
struct AbandonGuard {
    shared: Arc<Shared>,

    partition: KafkaPartition,

    generation: u64,

    offset: i64,

    observed: bool,
}

impl Drop for AbandonGuard {
    fn drop(&mut self) {
        if self.observed {
            return;
        }

        let released = {
            let mut state = self.shared.lock();

            state
                .table
                .advance_abandoned(&self.partition, self.generation, self.offset)
        };

        if released {
            self.shared.ready.notify_one();
            self.shared.wake_member();
        }
    }
}

impl PartitionedLogSettlement for KafkaSettlement {
    type Partition = KafkaPartition;
    type Error = KafkaSettlementError;

    async fn advance(mut self) -> Result<PartitionAdvance, Self::Error> {
        self.armed = false;
        let (sender, receiver) = oneshot::channel();

        let admission = {
            let mut state = self.shared.lock();

            if state.stopped {
                return Err(KafkaSettlementError::new(
                    KafkaSettlementErrorKind::MemberStopped,
                    FailureKind::Transient,
                ));
            }

            state
                .table
                .admit(&self.partition, self.generation, self.offset, sender)
        };

        if let Admission::OwnershipLost(_) = admission {
            return Ok(PartitionAdvance::OwnershipLost);
        }

        self.shared.wake_member();

        let mut guard = AbandonGuard {
            shared: Arc::clone(&self.shared),
            partition: self.partition.clone(),
            generation: self.generation,
            offset: self.offset,
            observed: false,
        };

        let reply = receiver.await;
        guard.observed = true;

        reply.unwrap_or(Err(KafkaSettlementError::new(
            KafkaSettlementErrorKind::MemberStopped,
            FailureKind::Transient,
        )))
    }

    fn partition(&self) -> &Self::Partition {
        &self.partition
    }
}

/// How the member thread ended after its source was dropped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum KafkaShutdownOutcome {
    /// The consumer closed and its resources were released.
    Closed,

    /// The consumer did not close within the shutdown timeout and was deliberately leaked.
    TimedOut,
}

/// Resolves when a dropped source's member thread has finished.
///
/// Dropping a [`KafkaDeliverySource`] signals its member thread and never blocks. The thread
/// finishes an in-flight transaction, closes the consumer, and waits for the close within the
/// configured shutdown timeout. rdkafka's consumer destructor waits without a bound for a close
/// that did not finish, so on timeout the thread leaks the consumer instead of dropping it and
/// this handle reports [`KafkaShutdownOutcome::TimedOut`]. The leak is bounded to one consumer
/// per timed-out source.
pub struct KafkaSourceShutdown {
    exit: watch::Receiver<Option<KafkaShutdownOutcome>>,
}

impl fmt::Debug for KafkaSourceShutdown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("KafkaSourceShutdown").finish()
    }
}

impl IntoFuture for KafkaSourceShutdown {
    type Output = KafkaShutdownOutcome;
    type IntoFuture = Pin<Box<dyn Future<Output = KafkaShutdownOutcome> + Send>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            loop {
                if let Some(outcome) = *self.exit.borrow_and_update() {
                    return outcome;
                }

                if self.exit.changed().await.is_err() {
                    return self.exit.borrow().unwrap_or(KafkaShutdownOutcome::Closed);
                }
            }
        })
    }
}

/// A partitioned-log source for one static member of a Kafka consumer group.
///
/// Construct it with [`crate::KafkaClient::delivery_source`] and run it with the generic
/// partitioned consumer. Opening initializes the transactional producer, verifies that every
/// topic exists, and subscribes. Each partition has at most one outstanding record; a record's
/// offset advances only through a transactional offset commit bound to the group generation
/// that assigned the partition. Revocation releases every partition with an outstanding
/// record, and an indeterminate advance pauses its partition until the committed cursor is
/// re-established behind a producer epoch fence.
///
/// Dropping the source never blocks: it signals the member thread, which closes the consumer
/// within the shutdown timeout and otherwise leaks it; [`KafkaSourceShutdown`] reports which.
pub struct KafkaDeliverySource {
    shared: Arc<Shared>,

    start: Option<member::Start>,

    exit: watch::Receiver<Option<KafkaShutdownOutcome>>,
}

impl KafkaDeliverySource {
    pub(crate) fn new(
        base: &BaseConfig,
        settings: KafkaConsumerSettings,
    ) -> Result<Self, KafkaClientError> {
        for (name, value) in &base.advanced_properties {
            let identity = CONSUMER_IDENTITY
                .iter()
                .chain(PRODUCER_IDENTITY.iter())
                .any(|identity| identity == name);

            let conflicting =
                CONSUMER_FORCED
                    .iter()
                    .chain(PRODUCER_FORCED.iter())
                    .any(|(forced, required)| {
                        forced == name && !value.trim().eq_ignore_ascii_case(required)
                    });

            if identity || conflicting {
                return Err(KafkaClientError::new(
                    KafkaClientErrorKind::TypedPropertyOverride,
                ));
            }
        }

        let mut consumer = ClientConfig::new();
        let mut producer = ClientConfig::new();

        // Latency defaults that advanced properties may replace. Every emitted record pauses its
        // partition and resumes it with a seek, which waits out a fetch the broker is holding
        // open; each offset commit may wait for the previous transaction's markers.
        for (name, value) in CONSUMER_DEFAULTS {
            consumer.set(name, value);
        }

        for (name, value) in PRODUCER_DEFAULTS {
            producer.set(name, value);
        }

        for config in [&mut consumer, &mut producer] {
            config.set("bootstrap.servers", &base.bootstrap_servers);

            for (name, value) in &base.advanced_properties {
                config.set(name, value);
            }
        }

        consumer
            .set("group.id", &settings.group_id)
            .set("group.instance.id", &settings.group_instance_id);

        for (name, value) in CONSUMER_FORCED {
            if name != "auto.commit.enable" {
                consumer.set(name, value);
            }
        }

        producer.set("transactional.id", settings.transactional_id());

        for (name, value) in PRODUCER_FORCED {
            producer.set(name, value);
        }

        let (exit_sender, exit) = watch::channel(None);

        Ok(Self {
            shared: Arc::new(Shared::default()),
            start: Some(member::Start {
                consumer,
                producer,
                topics: settings
                    .topics
                    .iter()
                    .map(|topic| Arc::from(topic.as_str()))
                    .collect(),
                operation_timeout: settings.operation_timeout,
                shutdown_timeout: settings.shutdown_timeout,
                exit: exit_sender,
            }),
            exit,
        })
    }

    /// Returns a handle that resolves when this source's member thread finishes after the
    /// source is dropped. A source that never opened resolves as closed once dropped.
    #[must_use]
    pub fn shutdown_handle(&self) -> KafkaSourceShutdown {
        KafkaSourceShutdown {
            exit: self.exit.clone(),
        }
    }
}

impl fmt::Debug for KafkaDeliverySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaDeliverySource")
            .field("opened", &self.start.is_none())
            .finish_non_exhaustive()
    }
}

impl Drop for KafkaDeliverySource {
    fn drop(&mut self) {
        self.shared.lock().shutdown = true;
        self.shared.wake_member();
    }
}

impl PartitionedLogDeliverySource for KafkaDeliverySource {
    type Partition = KafkaPartition;
    type Delivery = KafkaDelivery;
    type Error = KafkaSourceError;

    async fn open(&mut self) -> Result<(), Self::Error> {
        let Some(start) = self.start.take() else {
            return Err(KafkaSourceError::new(
                KafkaSourceErrorKind::AlreadyOpened,
                FailureKind::Permanent,
            ));
        };

        let (opened, receiver) = oneshot::channel();
        member::spawn(start, Arc::clone(&self.shared), opened)?;

        receiver.await.unwrap_or(Err(KafkaSourceError::new(
            KafkaSourceErrorKind::MemberStopped,
            FailureKind::Transient,
        )))
    }

    async fn receive(
        &mut self,
    ) -> Result<PartitionedLogReceive<Self::Delivery, Self::Partition>, Self::Error> {
        loop {
            let notified = self.shared.ready.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();

            let (popped, wake) = {
                let mut state = self.shared.lock();

                if let Some(failure) = state.failure {
                    return Err(failure);
                }

                state.table.pop()
            };

            if wake {
                self.shared.wake_member();
            }

            match popped {
                Some(Popped::Loss(partition)) => {
                    return Ok(PartitionedLogReceive::OwnershipLost(partition));
                }
                Some(Popped::Record(lane)) => {
                    return Ok(PartitionedLogReceive::Delivery(KafkaDelivery {
                        record: lane.record,
                        settlement: KafkaSettlement {
                            partition: lane.key,
                            offset: lane.offset,
                            generation: lane.generation,
                            shared: Arc::clone(&self.shared),
                            armed: true,
                        },
                    }));
                }
                None => notified.await,
            }
        }
    }
}

/// The reply to one advance request.
type Reply = Result<PartitionAdvance, KafkaSettlementError>;

/// The member thread's handle for answering one advance request.
type Waiter = oneshot::Sender<Reply>;

struct SharedState {
    table: Table<KafkaPartition, KafkaRecord, Waiter>,

    failure: Option<KafkaSourceError>,

    /// Set by the source's drop.
    shutdown: bool,

    /// Set once the member thread no longer answers advance requests.
    stopped: bool,
}

/// In-memory state shared by the source, its handles, and the member thread. No broker or
/// network I/O happens while its lock is held.
struct Shared {
    state: Mutex<SharedState>,

    /// Wakes a pending receive when a loss, record, or failure is ready.
    ready: Notify,

    member: OnceLock<Thread>,
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            state: Mutex::new(SharedState {
                table: Table::default(),
                failure: None,
                shutdown: false,
                stopped: false,
            }),
            ready: Notify::new(),
            member: OnceLock::new(),
        }
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, SharedState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wake_member(&self) {
        if let Some(member) = self.member.get() {
            member.unpark();
        }
    }
}

/// The idle park bound of the member thread between broker polls.
const MEMBER_PARK: Duration = Duration::from_millis(100);
