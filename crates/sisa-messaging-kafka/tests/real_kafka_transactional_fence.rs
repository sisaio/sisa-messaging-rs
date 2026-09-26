//! Opt-in design-gate evidence for issue #32: is a KIP-447 generation-checked transactional
//! offset commit a broker-authoritative fence against partition ownership loss?
//!
//! Every test uses its own consumer group and `transactional.id`. Members are static
//! (`group.instance.id`) and use the eager range assignor, which sorts `a-new` before `z-old`,
//! so the single test partition deterministically moves to the replacement member.
//!
//! `ConsumerGroupMetadata` is captured inside the eager assign callback, which is the
//! earliest point the member can know its generation and assignment together. Capturing it at
//! settlement time would repeat the flaw of `real_kafka_rebalance.rs`.

mod support;

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use rdkafka::consumer::{
    BaseConsumer, Consumer, ConsumerContext, ConsumerGroupMetadata, Rebalance,
};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseProducer, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::{ClientConfig, ClientContext};
use sisa_messaging::MessageId;

use support::{BROKERS_ENV, TEST_TIMEOUT, TOPIC_ENV, new_producer, publish_marker, required_env};

const TXN_TIMEOUT: Duration = Duration::from_secs(15);
const STABLE_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounded read used while a transaction is expected to hold the offset unstable.
const BLOCKED_READ_TIMEOUT: Duration = Duration::from_secs(4);
const DEFAULT_POLL_INTERVAL_MS: &str = "30000";
/// Short rebalance timeout so the coordinator completes a join without an unresponsive member
/// well inside `TEST_TIMEOUT`; it must not be below `session.timeout.ms`.
const SHORT_POLL_INTERVAL_MS: &str = "7000";
const OLD_INSTANCE: &str = "z-old";
const NEW_INSTANCE: &str = "a-new";

/// Fence codes the group coordinator returns for a stale TxnOffsetCommit.
const GENERATION_FENCES: [RDKafkaErrorCode; 3] = [
    RDKafkaErrorCode::IllegalGeneration,
    RDKafkaErrorCode::UnknownMemberId,
    RDKafkaErrorCode::FencedInstanceId,
];

/// One eager assign callback: the assigned partition count and the metadata snapshot taken
/// inside the callback.
struct AssignEvent {
    partitions: usize,

    metadata: Option<ConsumerGroupMetadata>,
}

#[derive(Default)]
struct CaptureContext {
    assigns: Mutex<Vec<AssignEvent>>,
}

impl ClientContext for CaptureContext {}

impl ConsumerContext for CaptureContext {
    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(partitions) = rebalance {
            let event = AssignEvent {
                partitions: partitions.count(),
                metadata: consumer.group_metadata(),
            };

            self.assigns
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(event);
        }
    }
}

type Member = BaseConsumer<CaptureContext>;

struct Fixture {
    brokers: String,

    topic: String,

    group: String,

    transactional_id: String,

    partition: i32,

    offset: i64,
}

impl Fixture {
    async fn new() -> Self {
        let brokers = required_env(BROKERS_ENV);
        let topic = required_env(TOPIC_ENV);
        let producer = new_producer(&brokers);
        let marker = MessageId::new().to_string();
        let (partition, offset) = publish_marker(&producer, &topic, &marker).await;
        let suffix = MessageId::new();

        Self {
            brokers,
            topic,
            group: format!("sisa-kafka-txn-fence-{suffix}"),
            transactional_id: format!("sisa-kafka-txn-fence-{suffix}"),
            partition,
            offset,
        }
    }

    fn member(&self, instance_id: Option<&str>) -> Member {
        self.member_with_poll_interval(instance_id, DEFAULT_POLL_INTERVAL_MS)
    }

    /// `max.poll.interval.ms` is also the rebalance timeout the member sends in JoinGroup.
    fn member_with_poll_interval(
        &self,
        instance_id: Option<&str>,
        poll_interval_ms: &str,
    ) -> Member {
        let mut config = ClientConfig::new();

        config
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group)
            .set("partition.assignment.strategy", "range")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("isolation.level", "read_committed")
            .set("auto.offset.reset", "earliest")
            .set("allow.auto.create.topics", "false")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", poll_interval_ms);

        if let Some(instance_id) = instance_id {
            config.set("group.instance.id", instance_id);
        }

        config
            .create_with_context(CaptureContext::default())
            .unwrap_or_else(|_| panic!("Kafka transactional-fence consumer construction failed"))
    }

    fn subscribed_member(&self, instance_id: &str) -> Member {
        self.subscribe(self.member(Some(instance_id)))
    }

    fn subscribe(&self, member: Member) -> Member {
        member
            .subscribe(&[self.topic.as_str()])
            .unwrap_or_else(|_| panic!("Kafka transactional-fence subscription failed"));

        member
    }

    fn transactional_producer(&self) -> BaseProducer {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("transactional.id", &self.transactional_id)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("transaction.timeout.ms", "60000")
            .set("message.timeout.ms", "15000")
            .create()
            .unwrap_or_else(|_| panic!("Kafka transactional producer construction failed"));

        producer
            .init_transactions(TXN_TIMEOUT)
            .unwrap_or_else(|error| panic!("Kafka init_transactions failed: {}", describe(&error)));

        producer
    }

    fn next_offset(&self) -> TopicPartitionList {
        let mut offsets = TopicPartitionList::new();

        offsets
            .add_partition_offset(&self.topic, self.partition, Offset::Offset(self.offset + 1))
            .unwrap_or_else(|_| panic!("Kafka offset list construction failed"));

        offsets
    }

    /// Committed cursor read by a consumer in the group under `read_committed`, which makes
    /// librdkafka send OffsetFetch with `require_stable`.
    fn committed(&self, reader: &Member, timeout: Duration) -> Result<Offset, RDKafkaErrorCode> {
        let mut partitions = TopicPartitionList::new();

        partitions.add_partition(&self.topic, self.partition);

        let committed = reader
            .committed_offsets(partitions, timeout)
            .map_err(|error| error.rdkafka_error_code().unwrap_or(RDKafkaErrorCode::Fail))?;

        let entry = committed
            .find_partition(&self.topic, self.partition)
            .unwrap_or_else(|| {
                panic!("Kafka committed-cursor response omitted the test partition")
            });

        match entry.error() {
            Ok(()) => Ok(entry.offset()),
            Err(error) => Err(error.rdkafka_error_code().unwrap_or(RDKafkaErrorCode::Fail)),
        }
    }

    fn stable_committed(&self, reader: &Member) -> Offset {
        self.committed(reader, STABLE_READ_TIMEOUT)
            .unwrap_or_else(|code| panic!("Kafka committed-cursor query failed: {code:?}"))
    }

    fn committed_by_fresh_reader(&self) -> Offset {
        let reader = self.member(None);

        self.stable_committed(&reader)
    }
}

fn describe(error: &KafkaError) -> String {
    match error {
        KafkaError::Transaction(inner) => format!(
            "{:?} (fatal={}, abortable={}, retriable={})",
            inner.code(),
            inner.is_fatal(),
            inner.txn_requires_abort(),
            inner.is_retriable()
        ),
        other => format!("{:?}", other.rdkafka_error_code()),
    }
}

fn assign_count(member: &Member) -> usize {
    member
        .context()
        .assigns
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .len()
}

/// Removes the metadata snapshot taken in the `index`-th assign callback.
fn take_snapshot(member: &Member, index: usize) -> (usize, ConsumerGroupMetadata) {
    let mut assigns = member
        .context()
        .assigns
        .lock()
        .unwrap_or_else(PoisonError::into_inner);

    let event = assigns
        .get_mut(index)
        .unwrap_or_else(|| panic!("Kafka assign callback {index} was not observed"));

    let metadata = event
        .metadata
        .take()
        .unwrap_or_else(|| panic!("Kafka group metadata was unavailable in the assign callback"));

    (event.partitions, metadata)
}

fn last_assigned_partitions(member: &Member) -> usize {
    member
        .context()
        .assigns
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .last()
        .map_or(0, |event| event.partitions)
}

/// Polls the listed members until `done` holds or the test deadline expires.
fn poll_until(members: &[&Member], what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + TEST_TIMEOUT;

    while Instant::now() < deadline {
        for member in members {
            let _ = member.poll(Duration::from_millis(50));
        }

        if done() {
            return;
        }
    }

    panic!("Kafka did not reach the expected state before the deadline: {what}");
}

/// Joins `z-old`, which owns the single partition, and returns its assign-time snapshot.
fn join_old_owner(fixture: &Fixture) -> (Member, ConsumerGroupMetadata) {
    await_first_assignment(fixture.subscribed_member(OLD_INSTANCE))
}

fn await_first_assignment(old: Member) -> (Member, ConsumerGroupMetadata) {
    poll_until(&[&old], "z-old first assignment", || {
        assign_count(&old) >= 1
    });

    let (partitions, snapshot) = take_snapshot(&old, 0);

    assert_eq!(
        partitions, 1,
        "z-old must own the single test partition at generation N"
    );

    (old, snapshot)
}

/// Joins `a-new` and polls both members until `z-old` has rejoined at generation N+1 with an
/// empty assignment and `a-new` owns the partition.
fn move_partition_with_rejoin(fixture: &Fixture, old: &Member) -> Member {
    let new = fixture.subscribed_member(NEW_INSTANCE);

    poll_until(
        &[old, &new],
        "z-old rejoined empty and a-new owns the partition",
        || {
            assign_count(old) >= 2
                && last_assigned_partitions(old) == 0
                && assign_count(&new) >= 1
                && last_assigned_partitions(&new) == 1
        },
    );

    new
}

/// Asserts that a transactional offset commit was rejected with a generation/member fence that
/// leaves the transaction abortable, and returns the observed code.
fn assert_generation_fence(result: Result<(), KafkaError>, scenario: &str) -> RDKafkaErrorCode {
    let error = match result {
        Ok(()) => panic!("{scenario}: the broker ACCEPTED the stale transactional offset commit"),
        Err(error) => error,
    };

    let KafkaError::Transaction(inner) = &error else {
        panic!(
            "{scenario}: unexpected non-transaction error {}",
            describe(&error)
        );
    };

    assert!(
        GENERATION_FENCES.contains(&inner.code()),
        "{scenario}: expected a generation/member fence, observed {}",
        describe(&error)
    );

    assert!(
        inner.txn_requires_abort() && !inner.is_fatal(),
        "{scenario}: the fence must be abortable and non-fatal, observed {}",
        describe(&error)
    );

    eprintln!("{scenario}: rejected with {}", describe(&error));

    inner.code()
}

fn send_offsets(
    producer: &BaseProducer,
    fixture: &Fixture,
    snapshot: &ConsumerGroupMetadata,
) -> Result<(), KafkaError> {
    producer
        .begin_transaction()
        .unwrap_or_else(|error| panic!("Kafka begin_transaction failed: {}", describe(&error)));

    producer.send_offsets_to_transaction(&fixture.next_offset(), snapshot, TXN_TIMEOUT)
}

fn abort(producer: &BaseProducer) {
    producer
        .abort_transaction(TXN_TIMEOUT)
        .unwrap_or_else(|error| panic!("Kafka abort_transaction failed: {}", describe(&error)));
}

fn commit(producer: &BaseProducer) {
    producer
        .commit_transaction(TXN_TIMEOUT)
        .unwrap_or_else(|error| panic!("Kafka commit_transaction failed: {}", describe(&error)));
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn current_owner_transactional_commit_advances() {
    let fixture = Fixture::new().await;
    let producer = fixture.transactional_producer();
    let (old, snapshot) = join_old_owner(&fixture);

    send_offsets(&producer, &fixture, &snapshot).unwrap_or_else(|error| {
        panic!(
            "current owner send_offsets_to_transaction failed: {}",
            describe(&error)
        )
    });

    commit(&producer);

    assert_eq!(
        fixture.committed_by_fresh_reader(),
        Offset::Offset(fixture.offset + 1),
        "a read_committed reader must observe the current owner's transactional commit",
    );

    drop(old);
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn stale_generation_transactional_commit_is_rejected() {
    let fixture = Fixture::new().await;
    let producer = fixture.transactional_producer();

    let (old, snapshot) =
        await_first_assignment(fixture.subscribe(
            fixture.member_with_poll_interval(Some(OLD_INSTANCE), SHORT_POLL_INTERVAL_MS),
        ));

    // z-old stalls (it is never polled again), so it never rejoins. The coordinator completes
    // the join for a-new after the rebalance timeout and hands it the partition.
    let new = fixture
        .subscribe(fixture.member_with_poll_interval(Some(NEW_INSTANCE), SHORT_POLL_INTERVAL_MS));

    poll_until(
        &[&new],
        "a-new owns the partition while z-old is stalled",
        || last_assigned_partitions(&new) == 1,
    );

    assert_eq!(
        assign_count(&old),
        1,
        "the stalled z-old must not have rejoined"
    );

    let before = fixture.stable_committed(&new);

    // z-old still holds its generation-N snapshot from the assign callback. Observed: the
    // stalled static member keeps its member id, so the coordinator answers ILLEGAL_GENERATION.
    let code = assert_generation_fence(
        send_offsets(&producer, &fixture, &snapshot),
        "stale generation-N snapshot after the partition moved",
    );

    assert_eq!(code, RDKafkaErrorCode::IllegalGeneration);

    abort(&producer);

    let after = fixture.stable_committed(&new);

    assert_eq!(
        after, before,
        "the new owner's cursor must not move after a fenced commit ({code:?})"
    );

    assert_ne!(after, Offset::Offset(fixture.offset + 1));
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn rejoined_member_with_revoked_partition_is_fenced() {
    let fixture = Fixture::new().await;
    let producer = fixture.transactional_producer();
    let (old, snapshot) = join_old_owner(&fixture);
    let new = move_partition_with_rejoin(&fixture, &old);

    // The probe scenario: z-old is a live member at generation N+1 with an empty assignment,
    // but it settles work that it received at generation N using that generation's snapshot.
    let (rejoined_partitions, _fresh) = take_snapshot(&old, assign_count(&old) - 1);

    assert_eq!(
        rejoined_partitions, 0,
        "z-old must have rejoined with an empty assignment"
    );

    let before = fixture.stable_committed(&new);

    let code = assert_generation_fence(
        send_offsets(&producer, &fixture, &snapshot),
        "rejoined member committing a revoked partition with its assign-time snapshot",
    );

    assert_eq!(code, RDKafkaErrorCode::IllegalGeneration);

    abort(&producer);

    assert_eq!(
        fixture.stable_committed(&new),
        before,
        "the revoked partition's cursor must not move after a fenced commit ({code:?})",
    );
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn rejoined_member_fresh_metadata_is_accepted() {
    let fixture = Fixture::new().await;
    let producer = fixture.transactional_producer();
    let (old, _stale) = join_old_owner(&fixture);
    let new = move_partition_with_rejoin(&fixture, &old);

    let fresh = old
        .group_metadata()
        .unwrap_or_else(|| panic!("Kafka group metadata was unavailable after the rejoin"));

    // Negative control documenting the flaw: the coordinator checks member and generation,
    // not partition ownership. A settlement-time snapshot therefore carries generation N+1 and
    // is accepted for a partition z-old no longer owns.
    send_offsets(&producer, &fixture, &fresh).unwrap_or_else(|error| {
        panic!(
            "expected the fresh-metadata commit to be accepted: {}",
            describe(&error)
        )
    });

    commit(&producer);

    assert_eq!(
        fixture.stable_committed(&new),
        Offset::Offset(fixture.offset + 1),
        "expected the broker to advance the revoked partition's cursor for a settlement-time snapshot",
    );
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn pending_transaction_blocks_new_owner_until_resolved() {
    pending_transaction_scenario(PendingOutcome::Commit).await;
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn pending_aborted_transaction_leaves_cursor_unchanged() {
    pending_transaction_scenario(PendingOutcome::Abort).await;
}

#[derive(Clone, Copy, Debug)]
enum PendingOutcome {
    Commit,
    Abort,
}

async fn pending_transaction_scenario(outcome: PendingOutcome) {
    let fixture = Fixture::new().await;
    let producer = fixture.transactional_producer();
    let (old, snapshot) = join_old_owner(&fixture);
    let reader = fixture.member(None);
    let before = fixture.stable_committed(&reader);

    // TxnOffsetCommit is accepted at generation N while z-old still owns the partition; the
    // transaction is deliberately left open across the rebalance.
    send_offsets(&producer, &fixture, &snapshot).unwrap_or_else(|error| {
        panic!(
            "generation-N send_offsets_to_transaction failed: {}",
            describe(&error)
        )
    });

    let new = move_partition_with_rejoin(&fixture, &old);
    let blocked = fixture.committed(&new, BLOCKED_READ_TIMEOUT);

    eprintln!("pending {outcome:?}: new-owner read while the transaction is open -> {blocked:?}");

    // OffsetFetch with require_stable answers UNSTABLE_OFFSET_COMMIT while the transaction is
    // open; librdkafka retries it until the caller's deadline, so the read times out rather
    // than returning either the old or the pending cursor.
    assert_eq!(
        blocked,
        Err(RDKafkaErrorCode::OperationTimedOut),
        "a read_committed new owner must not obtain a cursor while the transaction is open",
    );

    let expected = match outcome {
        PendingOutcome::Commit => {
            // EndTxn carries no group generation, so the old owner can still complete a
            // transaction whose TxnOffsetCommit was accepted before it lost the partition.
            let result = producer.commit_transaction(TXN_TIMEOUT);

            eprintln!("pending Commit: post-rebalance EndTxn(commit) -> {result:?}");

            result.unwrap_or_else(|error| {
                panic!(
                    "post-rebalance commit_transaction was rejected: {}",
                    describe(&error)
                )
            });

            Offset::Offset(fixture.offset + 1)
        }
        PendingOutcome::Abort => {
            abort(&producer);

            before
        }
    };

    assert_eq!(fixture.stable_committed(&new), expected);

    assert_eq!(
        fixture.stable_committed(&new),
        expected,
        "the resolved cursor must be stable"
    );
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn ambiguous_endtxn_resolved_by_epoch_fence() {
    let fixture = Fixture::new().await;
    let (old, snapshot) = join_old_owner(&fixture);
    let reader = fixture.member(None);
    let before = fixture.stable_committed(&reader);
    let zombie = fixture.transactional_producer();

    send_offsets(&zombie, &fixture, &snapshot).unwrap_or_else(|error| {
        panic!(
            "generation-N send_offsets_to_transaction failed: {}",
            describe(&error)
        )
    });

    // The ambiguous case: the old owner never learns the EndTxn outcome. The open
    // transaction holds the cursor unstable until the transactional.id is re-initialised.
    assert_eq!(
        fixture.committed(&reader, BLOCKED_READ_TIMEOUT),
        Err(RDKafkaErrorCode::OperationTimedOut),
        "the open transaction must hold the cursor unstable",
    );

    // A fresh producer with the same transactional.id bumps the epoch, which fences the old
    // producer and makes the coordinator abort the pending transaction.
    let successor = fixture.transactional_producer();
    let first = fixture.stable_committed(&reader);
    let second = fixture.stable_committed(&reader);

    assert_eq!(
        first, before,
        "the epoch fence must abort the ambiguous transaction"
    );

    assert_eq!(
        second, first,
        "the resolved cursor must be stable across reads"
    );

    let zombie_commit = zombie.commit_transaction(TXN_TIMEOUT);

    eprintln!("ambiguous EndTxn: zombie commit after the epoch bump -> {zombie_commit:?}");

    let Err(KafkaError::Transaction(inner)) = &zombie_commit else {
        panic!("the fenced producer's commit_transaction was not rejected: {zombie_commit:?}");
    };

    assert_eq!(inner.code(), RDKafkaErrorCode::Fenced, "{}", inner.string());

    assert!(
        inner.is_fatal(),
        "a producer fence must be fatal for the zombie"
    );

    assert_eq!(fixture.stable_committed(&reader), before);
    drop((old, successor));
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn ambiguous_endtxn_dropped_producer_resolved_by_epoch_fence() {
    let fixture = Fixture::new().await;
    let (old, snapshot) = join_old_owner(&fixture);
    let reader = fixture.member(None);
    let before = fixture.stable_committed(&reader);
    let dropped = fixture.transactional_producer();

    send_offsets(&dropped, &fixture, &snapshot).unwrap_or_else(|error| {
        panic!(
            "generation-N send_offsets_to_transaction failed: {}",
            describe(&error)
        )
    });

    drop(dropped);

    // Dropping the producer neither commits nor aborts on the broker.
    assert_eq!(
        fixture.committed(&reader, BLOCKED_READ_TIMEOUT),
        Err(RDKafkaErrorCode::OperationTimedOut),
        "the open transaction must hold the cursor unstable",
    );

    let successor = fixture.transactional_producer();
    let first = fixture.stable_committed(&reader);
    let second = fixture.stable_committed(&reader);

    assert_eq!(
        first, before,
        "the epoch fence must abort the dropped producer's transaction"
    );

    assert_eq!(
        second, first,
        "the resolved cursor must be stable across reads"
    );

    drop((old, successor));
}

/// Joins `z-old` as the active owner and returns a generation -1 snapshot from a consumer in
/// the same group that has not subscribed, joined, or polled.
fn pre_join_snapshot(
    fixture: &Fixture,
    instance_id: Option<&str>,
) -> (Member, Member, ConsumerGroupMetadata) {
    let (owner, _owner_snapshot) = join_old_owner(fixture);
    let unjoined = fixture.member(instance_id);

    let snapshot = unjoined
        .group_metadata()
        .unwrap_or_else(|| panic!("Kafka group metadata was unavailable before join"));

    (owner, unjoined, snapshot)
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn pre_join_snapshot_with_static_instance_is_rejected() {
    let fixture = Fixture::new().await;
    let producer = fixture.transactional_producer();

    // Case A: a restarted process reusing the active owner's instance id before it rejoins.
    let (owner, restarted, snapshot) = pre_join_snapshot(&fixture, Some(OLD_INSTANCE));
    let before = fixture.stable_committed(&restarted);

    let restart_code = assert_generation_fence(
        send_offsets(&producer, &fixture, &snapshot),
        "pre-join snapshot reusing the owner's static instance id",
    );

    assert_eq!(restart_code, RDKafkaErrorCode::FencedInstanceId);

    abort(&producer);
    drop(restarted);

    // Case B: an unknown static instance id that has never joined the group.
    let unknown = fixture.member(Some("m-never-joined"));

    let unknown_snapshot = unknown
        .group_metadata()
        .unwrap_or_else(|| panic!("Kafka group metadata was unavailable before join"));

    let unknown_code = assert_generation_fence(
        send_offsets(&producer, &fixture, &unknown_snapshot),
        "pre-join snapshot with an unknown static instance id",
    );

    assert_eq!(unknown_code, RDKafkaErrorCode::UnknownMemberId);

    abort(&producer);

    assert_eq!(
        fixture.stable_committed(&unknown),
        before,
        "fenced pre-join commits must not move the cursor ({restart_code:?}, {unknown_code:?})",
    );

    assert_eq!(
        assign_count(&owner),
        1,
        "the active owner must keep its generation"
    );
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn pre_join_snapshot_without_instance_is_rejected_or_documented() {
    let fixture = Fixture::new().await;
    let producer = fixture.transactional_producer();
    let (owner, unjoined, snapshot) = pre_join_snapshot(&fixture, None);
    let before = fixture.stable_committed(&unjoined);
    let result = send_offsets(&producer, &fixture, &snapshot);

    eprintln!(
        "pre-join generation -1 snapshot without instance id -> {}",
        result
            .as_ref()
            .map_or_else(describe, |()| "accepted".to_owned())
    );

    // Observed on Apache Kafka 4.0.0: ACCEPTED. A snapshot with generation -1, an empty member
    // id and no group.instance.id skips the coordinator's member and generation validation for
    // transactional commits even while the group is non-empty (the pre-KIP-447 compatibility
    // path). This proves that the generation fence is opt-in by the caller: a member that
    // captures metadata before its first assignment, or any producer with such a snapshot, can
    // advance a partition actively owned by another member.
    if let Err(error) = &result {
        panic!(
            "expected the broker to accept a generation -1 snapshot without an instance id, observed {}",
            describe(error)
        );
    }

    commit(&producer);
    assert_ne!(before, Offset::Offset(fixture.offset + 1));

    assert_eq!(
        fixture.stable_committed(&unjoined),
        Offset::Offset(fixture.offset + 1),
        "expected the generation -1 commit to advance the actively owned partition",
    );

    assert_eq!(
        assign_count(&owner),
        1,
        "the active owner must keep its generation"
    );
}
