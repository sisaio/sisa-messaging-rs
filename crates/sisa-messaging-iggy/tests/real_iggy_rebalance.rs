//! Opt-in real-broker proof that a consumer-group rebalance between two replay-only sources skips
//! no record.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use sisa_messaging::{
    MessageId, PartitionedLogDeliverySource, PartitionedLogReceive, PartitionedLogSettlement,
};
use sisa_messaging_iggy::IggyDeliverySource;

use support::{GroupTopic, TEST_TIMEOUT, received};

const PARTITIONS: u32 = 4;

const PER_PARTITION: usize = 25;

const BATCH_LENGTH: u32 = 5;

/// One record a member processed.
#[derive(Clone, Copy)]
struct Seen {
    member: u8,

    partition: u32,

    offset: u64,

    message_id: MessageId,
}

#[derive(Default)]
struct Log {
    seen: Mutex<Vec<Seen>>,

    withdrawn: Mutex<Vec<(u8, u32)>>,
}

impl Log {
    fn seen(&self) -> Vec<Seen> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Receives, briefly processes, and advances records until stopped.
async fn run_member(
    member: u8,
    mut source: IggyDeliverySource,
    log: Arc<Log>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Acquire) {
        let Ok(event) = tokio::time::timeout(Duration::from_millis(200), source.receive()).await
        else {
            continue;
        };

        match event {
            Ok(PartitionedLogReceive::Delivery(delivery)) => {
                let received = received(delivery);

                log.seen
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(Seen {
                        member,
                        partition: received.partition,
                        offset: received.offset,
                        message_id: received.message_id,
                    });

                // Handler time widens the window in which a rebalance lands mid-partition.
                tokio::time::sleep(Duration::from_millis(2)).await;

                // An indeterminate advance is withdrawn and replayed by the source itself.
                let _ = tokio::time::timeout(TEST_TIMEOUT, received.settlement.advance()).await;
            }
            Ok(PartitionedLogReceive::OwnershipLost(partition)) => log
                .withdrawn
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((member, partition)),
            Ok(PartitionedLogReceive::Closed) => break,
            Ok(_) => panic!("unexpected source event"),
            Err(error) => panic!("member {member} source failed: {error}"),
        }
    }
}

async fn wait_until(condition: impl Fn() -> bool, what: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    while !condition() {
        assert!(tokio::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn a_joining_member_takes_over_partitions_without_skipping_records() {
    let fixture = GroupTopic::create(PARTITIONS).await;
    let mut published = Vec::new();

    for partition in 0..PARTITIONS {
        published.extend(fixture.publish(partition, PER_PARTITION).await);
    }

    let log = Arc::new(Log::default());
    let stop = Arc::new(AtomicBool::new(false));

    // Small batches make the first member cycle through every partition before the second joins,
    // so each partition it loses is handed over mid-stream with records already polled.
    let settings = fixture.settings().with_batch_length(BATCH_LENGTH).unwrap();

    let (client_a, source_a) = fixture.source_with(settings.clone()).await;
    let member_a = tokio::spawn(run_member(0, source_a, Arc::clone(&log), Arc::clone(&stop)));

    wait_until(
        || {
            let partitions: BTreeSet<u32> = log.seen().iter().map(|seen| seen.partition).collect();

            partitions.len() == PARTITIONS as usize
        },
        "the first member did not reach every partition",
    )
    .await;

    let (client_b, source_b) = fixture.source_with(settings).await;
    let member_b = tokio::spawn(run_member(1, source_b, Arc::clone(&log), Arc::clone(&stop)));

    for partition in 0..PARTITIONS {
        published.extend(fixture.publish(partition, PER_PARTITION).await);
    }

    let expected: BTreeSet<MessageId> = published.iter().copied().collect();

    wait_until(
        || {
            let seen: BTreeSet<MessageId> = log.seen().iter().map(|seen| seen.message_id).collect();

            expected.is_subset(&seen)
        },
        "a published record was never processed",
    )
    .await;

    // Every partition's cursor reaches its last record once both members finish.
    for partition in 0..PARTITIONS {
        let last = (2 * PER_PARTITION - 1) as u64;
        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;

        while fixture.stored_offset(partition).await != Some(last) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "partition {partition} cursor never reached its last record"
            );

            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    stop.store(true, Ordering::Release);
    member_a.await.unwrap();
    member_b.await.unwrap();

    let seen = log.seen();

    assert!(
        seen.iter().any(|seen| seen.member == 1),
        "the joining member must take over at least one partition"
    );

    let handed_over = (0..PARTITIONS).any(|partition| {
        let members: BTreeSet<u8> = seen
            .iter()
            .filter(|seen| seen.partition == partition)
            .map(|seen| seen.member)
            .collect();

        members.len() == 2
    });

    assert!(
        handed_over,
        "at least one partition must move from the first member to the second mid-stream"
    );

    // Nothing skipped: per partition, the processed offsets are exactly the published range.
    let mut offsets: BTreeMap<u32, BTreeSet<u64>> = BTreeMap::new();

    for seen in &seen {
        offsets
            .entry(seen.partition)
            .or_default()
            .insert(seen.offset);
    }

    for partition in 0..PARTITIONS {
        let expected: BTreeSet<u64> = (0..(2 * PER_PARTITION) as u64).collect();

        assert_eq!(offsets.get(&partition), Some(&expected));
    }

    // Each member delivered every partition in offset order within one ownership stint.
    for member in 0..2_u8 {
        let mut last: BTreeMap<u32, u64> = BTreeMap::new();

        for seen in seen.iter().filter(|seen| seen.member == member) {
            if let Some(previous) = last.insert(seen.partition, seen.offset) {
                let withdrew = log
                    .withdrawn
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .contains(&(member, seen.partition));

                assert!(
                    seen.offset > previous || withdrew,
                    "member {member} reordered partition {} without a withdrawal",
                    seen.partition
                );
            }
        }
    }

    client_a.shutdown().await.unwrap();
    client_b.shutdown().await.unwrap();
    fixture.delete().await;
}
