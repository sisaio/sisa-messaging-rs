//! Consumer-group partition progress over a real Iggy server. Set `SISA_IGGY_SERVER_ADDRESS` to
//! enable; the credentials follow the opt-in tests' `SISA_IGGY_*` variables.
//!
//! Each iteration creates a fresh topic and consumer group, publishes its records, and opens the
//! source before timing, so provisioning, publishing, and the group lookup and join are excluded.
//!
//! - `source_receive_advance` drives the source directly, one partition, receiving and advancing
//!   each record in turn: the per-record poll-buffer, delivery, and offset-store path.
//! - `consumer_noop_*` runs the generic partitioned runtime over the source with a no-I/O inbox and
//!   a no-op handler, one active record per partition. Timing ends when the inbox has committed
//!   every record; an untimed oracle then waits for every partition's cursor to reach its last
//!   record and asserts each record committed exactly once.

#[path = "../tests/support/mod.rs"]
mod support;

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use sisa_messaging::{
    ContentType, Envelope, ErrorClassifier, FailureKind, Message, MessageId, PartitionAdvance,
    PartitionedLogSettlement, SerializedEnvelope, Serializer,
};
use sisa_messaging_consumer::{Consumer, ConsumerExit, ConsumerHandler, ConsumerSettings};
use sisa_messaging_iggy::{IggyEnvelopeMapper, IggySourceSettings};
use sisa_messaging_inbox::{
    InboxClaimOutcome, InboxFailure, InboxFailureOutcome, InboxId, InboxReceipt, InboxRecord,
    InboxScope, InboxStore, InboxUnitOfWork,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use support::{GroupTopic, identifier, next_delivery};

/// Records per timed iteration.
const MESSAGES: usize = 256;

/// Bound for one iteration; exceeding it fails the benchmark instead of reporting a sample.
const ITERATION_TIMEOUT: Duration = Duration::from_secs(60);

struct Probe;

impl Message for Probe {
    const TYPE: &'static str = "iggy.source.probe";
    const VERSION: u32 = 1;
}

#[derive(Debug)]
struct BenchError;

impl fmt::Display for BenchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("benchmark operation failed")
    }
}

impl Error for BenchError {}

impl ErrorClassifier for BenchError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// Ignores the payload so the benchmark measures the source and runtime, not decoding.
struct ProbeCodec;

impl Serializer<Probe> for ProbeCodec {
    type Error = BenchError;

    fn serialize(&self, envelope: &Envelope<Probe>) -> Result<SerializedEnvelope, Self::Error> {
        Ok(SerializedEnvelope {
            message_id: envelope.message_id(),
            message_type: envelope.message_type().clone(),
            message_version: envelope.message_version(),
            content_type: ContentType::new("application/octet-stream").map_err(|_| BenchError)?,
            payload: Vec::new(),
            metadata: envelope.metadata().clone(),
            ordering_key: None,
        })
    }

    fn deserialize(&self, envelope: SerializedEnvelope) -> Result<Envelope<Probe>, Self::Error> {
        Envelope::new(envelope.message_id, Probe, envelope.metadata).map_err(|_| BenchError)
    }
}

struct Noop;

impl ConsumerHandler<Probe, BenchTransaction> for Noop {
    type Error = BenchError;

    async fn handle(
        &self,
        _tx: &mut BenchTransaction,
        _envelope: &Envelope<Probe>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Committed completions per message id, shared by every transaction.
#[derive(Default)]
struct InboxState {
    completions: Mutex<HashMap<MessageId, u32>>,

    committed: AtomicU64,

    target: AtomicU64,

    reached: Notify,
}

/// A no-I/O inbox whose completions apply on commit.
#[derive(Clone, Default)]
struct BenchInbox(Arc<InboxState>);

struct BenchTransaction(Vec<MessageId>);

struct BenchReceipt(MessageId);

impl InboxReceipt for BenchReceipt {
    fn id(&self) -> InboxId {
        InboxId::from_uuid(self.0.into_uuid())
    }

    fn recorded_failures(&self) -> u32 {
        0
    }
}

impl InboxUnitOfWork for BenchInbox {
    type Transaction = BenchTransaction;
    type Error = BenchError;

    async fn begin(&self) -> Result<Self::Transaction, Self::Error> {
        Ok(BenchTransaction(Vec::new()))
    }

    async fn commit(&self, transaction: Self::Transaction) -> Result<(), Self::Error> {
        let mut completions = self
            .0
            .completions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        for id in transaction.0 {
            *completions.entry(id).or_default() += 1;
        }

        drop(completions);

        let committed = self.0.committed.fetch_add(1, Ordering::AcqRel) + 1;

        if committed >= self.0.target.load(Ordering::Acquire) {
            self.0.reached.notify_one();
        }

        Ok(())
    }

    async fn rollback(&self, _transaction: Self::Transaction) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl InboxStore<BenchTransaction> for BenchInbox {
    type Error = BenchError;
    type Receipt = BenchReceipt;

    fn max_attempts(&self) -> NonZeroU32 {
        NonZeroU32::MIN
    }

    async fn claim(
        &self,
        _transaction: &mut BenchTransaction,
        record: &InboxRecord,
    ) -> Result<InboxClaimOutcome<Self::Receipt>, Self::Error> {
        let completed = self
            .0
            .completions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&record.message_id);

        Ok(if completed {
            InboxClaimOutcome::CompletedDuplicate
        } else {
            InboxClaimOutcome::Claimed(BenchReceipt(record.message_id))
        })
    }

    async fn complete(
        &self,
        transaction: &mut BenchTransaction,
        receipt: Self::Receipt,
    ) -> Result<(), Self::Error> {
        transaction.0.push(receipt.0);

        Ok(())
    }

    async fn fail(
        &self,
        _record: &InboxRecord,
        _failure: InboxFailure,
    ) -> Result<InboxFailureOutcome, Self::Error> {
        Err(BenchError)
    }
}

struct Bench {
    runtime: tokio::runtime::Runtime,
}

impl Bench {
    /// Publishes `MESSAGES` records spread evenly across the fixture's partitions.
    async fn publish(fixture: &GroupTopic) {
        let per_partition = MESSAGES / fixture.partitions as usize;

        for partition in 0..fixture.partitions {
            fixture.publish(partition, per_partition).await;
        }
    }

    fn source_receive_advance(&self) -> Duration {
        self.runtime.block_on(async {
            let fixture = GroupTopic::create(1).await;
            Self::publish(&fixture).await;
            let (client, mut source) = fixture.source_with(Self::settings(&fixture)).await;

            let started = Instant::now();

            tokio::time::timeout(ITERATION_TIMEOUT, async {
                for offset in 0..MESSAGES as u64 {
                    let received = next_delivery(&mut source).await;
                    assert_eq!(received.offset, offset);

                    let advanced = received.settlement.advance().await.unwrap();
                    assert_eq!(advanced, PartitionAdvance::Advanced);
                }
            })
            .await
            .expect("records were not all advanced within the iteration timeout");

            let elapsed = started.elapsed();

            assert_eq!(fixture.stored_offset(0).await, Some(MESSAGES as u64 - 1));

            client.shutdown().await.unwrap();
            fixture.delete().await;

            elapsed
        })
    }

    fn consumer_noop(&self, partitions: u32) -> Duration {
        self.runtime.block_on(async {
            let fixture = GroupTopic::create(partitions).await;
            Self::publish(&fixture).await;
            let (client, source) = fixture.source_with(Self::settings(&fixture)).await;

            let inbox = BenchInbox::default();
            inbox.0.target.store(MESSAGES as u64, Ordering::Release);

            let mut settings = ConsumerSettings::default();
            settings.max_in_flight = NonZeroUsize::new(partitions as usize).unwrap();

            let consumer = Consumer::<Probe, _>::new_partitioned(
                source,
                IggyEnvelopeMapper,
                ProbeCodec,
                inbox.clone(),
                InboxScope::new("bench").unwrap(),
                Noop,
                settings,
            )
            .unwrap();

            let cancel = CancellationToken::new();
            let started = Instant::now();
            let task = tokio::spawn(consumer.run_partitioned(cancel.child_token()));

            tokio::time::timeout(ITERATION_TIMEOUT, async {
                while inbox.0.committed.load(Ordering::Acquire) < MESSAGES as u64 {
                    inbox.0.reached.notified().await;
                }
            })
            .await
            .expect("records were not all committed within the iteration timeout");

            let elapsed = started.elapsed();

            // Untimed oracle: every cursor reaches its last record and nothing committed twice.
            let last = (MESSAGES / partitions as usize - 1) as u64;

            for partition in 0..partitions {
                let deadline = tokio::time::Instant::now() + ITERATION_TIMEOUT;

                while fixture.stored_offset(partition).await != Some(last) {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "cursor did not reach the end"
                    );

                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }

            cancel.cancel();
            assert_eq!(task.await.unwrap().unwrap(), ConsumerExit::Cancelled);

            {
                let completions = inbox
                    .0
                    .completions
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);

                assert_eq!(completions.len(), MESSAGES);

                assert!(
                    completions.values().all(|count| *count == 1),
                    "a record committed twice"
                );
            }

            client.shutdown().await.unwrap();
            fixture.delete().await;

            elapsed
        })
    }

    /// Library defaults: a 64-record batch, 100 ms poll interval, and 1 s assignment refresh.
    fn settings(fixture: &GroupTopic) -> IggySourceSettings {
        IggySourceSettings::new(
            identifier(&fixture.stream),
            identifier(&fixture.topic),
            identifier(&fixture.group),
        )
    }
}

fn run(c: &mut Criterion, name: &str, mut iterate: impl FnMut() -> Duration) {
    let mut group = c.benchmark_group("iggy_partition_progress");
    group.throughput(Throughput::Elements(MESSAGES as u64));

    group.bench_function(name, |b| {
        b.iter_custom(|iterations| (0..iterations).map(|_| iterate()).sum());
    });

    group.finish();
}

fn partition_progress(c: &mut Criterion) {
    if std::env::var(support::SERVER_ADDRESS_ENV).is_err() {
        return;
    }

    let bench = Bench {
        runtime: tokio::runtime::Runtime::new().unwrap(),
    };

    run(c, "source_receive_advance_1_partition", || {
        bench.source_receive_advance()
    });

    for partitions in [1, 4] {
        run(c, &format!("consumer_noop_{partitions}_partitions"), || {
            bench.consumer_noop(partitions)
        });
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5));
    targets = partition_progress
}

criterion_main!(benches);
