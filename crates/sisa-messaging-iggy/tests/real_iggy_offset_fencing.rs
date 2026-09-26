//! Opt-in record of the raw consumer-offset facts the replay-only delivery source relies on,
//! against `apache/iggy:0.9.0` with the `=0.11.0` SDK.
//!
//! The server fences an offset store by partition ownership at admission: a member that does not
//! own the partition is rejected with `ConsumerGroupPartitionNotOwned` (5009). It still admits a
//! store from an owner whose revocation is draining, stores are absolute (a lower store moves the
//! cursor back), and the stored offset is the last processed record, so `PollingStrategy::next`
//! resumes after it. The store request carries no membership generation (see
//! <https://github.com/sisaio/sisa-messaging-rs/issues/60>).
//!
//! These facts are why `IggySettlement::advance` never returns `OwnershipLost`: a draining owner is
//! admitted, and the SDK re-sends the same request id when it does not observe a reply, so a 5009
//! does not prove that an earlier transmission of the same store did nothing. The source stays
//! correct because a late or lower store can only cause replay.
//!
//! Group member ids reported by `get_consumer_group` are not the members' `get_me` client ids, so
//! these tests identify a partition's owner through the server's own poll fence.

mod support;

use std::time::Duration;

use iggy::prelude::{
    Client, Consumer, ConsumerGroupClient, ConsumerOffsetClient, Identifier,
    IggyClient as RawIggyClient, IggyError, MessageClient, PollingStrategy,
};

use support::{GroupTopic, identifier, new_raw_client};

/// Bound for waiting on an assignment change.
const ASSIGNMENT_TIMEOUT: Duration = Duration::from_secs(20);

struct Ids {
    stream: Identifier,

    topic: Identifier,

    consumer: Consumer,
}

impl Ids {
    fn new(fixture: &GroupTopic) -> Self {
        Self {
            stream: identifier(&fixture.stream),
            topic: identifier(&fixture.topic),
            consumer: Consumer::group(identifier(&fixture.group)),
        }
    }
}

async fn join(fixture: &GroupTopic, ids: &Ids) -> RawIggyClient {
    let member = new_raw_client().await;

    member
        .join_consumer_group(&ids.stream, &ids.topic, &identifier(&fixture.group))
        .await
        .unwrap_or_else(|error| panic!("raw member failed to join: {error}"));

    member
}

/// Polls explicitly without committing and returns the served offsets, or `None` when the
/// server fenced the poll because the member does not own the partition.
async fn poll(
    member: &RawIggyClient,
    ids: &Ids,
    partition: u32,
    strategy: PollingStrategy,
) -> Option<Vec<u64>> {
    let polled = member
        .poll_messages(
            &ids.stream,
            &ids.topic,
            Some(partition),
            &ids.consumer,
            &strategy,
            10,
            false,
        )
        .await
        .unwrap_or_else(|error| panic!("raw poll failed: {error}"));

    (polled.partition_id == partition).then(|| {
        polled
            .messages
            .iter()
            .map(|message| message.header.offset)
            .collect()
    })
}

async fn store(
    member: &RawIggyClient,
    ids: &Ids,
    partition: u32,
    offset: u64,
) -> Result<(), IggyError> {
    member
        .store_consumer_offset(
            &ids.consumer,
            &ids.stream,
            &ids.topic,
            Some(partition),
            offset,
        )
        .await
}

/// Waits until `member`'s poll of `partition` is answered (`owned`) or fenced (`!owned`).
async fn wait_ownership(member: &RawIggyClient, ids: &Ids, partition: u32, owned: bool) {
    let deadline = tokio::time::Instant::now() + ASSIGNMENT_TIMEOUT;

    while poll(member, ids, partition, PollingStrategy::next())
        .await
        .is_some()
        != owned
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "partition {partition} ownership never became {owned}"
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn a_member_that_does_not_own_the_partition_is_refused_an_offset_store() {
    let fixture = GroupTopic::create(1).await;
    fixture.publish(0, 3).await;
    let ids = Ids::new(&fixture);

    let first = join(&fixture, &ids).await;
    wait_ownership(&first, &ids, 0, true).await;

    // One partition, two members: the second member owns nothing.
    let second = join(&fixture, &ids).await;
    wait_ownership(&second, &ids, 0, false).await;

    let refused = store(&second, &ids, 0, 1).await;

    assert!(
        matches!(refused, Err(IggyError::ConsumerGroupPartitionNotOwned(..))),
        "a non-owner's store must be refused with 5009"
    );

    assert_eq!(fixture.stored_offset(0).await, None);

    Client::shutdown(&second).await.unwrap();
    Client::shutdown(&first).await.unwrap();
    fixture.delete().await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn an_owner_whose_revocation_is_draining_is_still_admitted() {
    let fixture = GroupTopic::create(2).await;
    fixture.publish(0, 5).await;
    fixture.publish(1, 5).await;
    let ids = Ids::new(&fixture);

    let first = join(&fixture, &ids).await;

    // The first member alone owns and is served both partitions, then stores nothing.
    for partition in 0..2 {
        wait_ownership(&first, &ids, partition, true).await;
    }

    let second = join(&fixture, &ids).await;

    // One partition's revocation starts: its owner's polls are fenced, but the partition does
    // not move until the owner stores what it was served.
    let deadline = tokio::time::Instant::now() + ASSIGNMENT_TIMEOUT;

    let draining = loop {
        let mut fenced = None;

        for partition in 0..2 {
            if poll(&first, &ids, partition, PollingStrategy::offset(0))
                .await
                .is_none()
            {
                fenced = Some(partition);
            }
        }

        if let Some(partition) = fenced {
            break partition;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "no revocation started"
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert!(
        poll(&second, &ids, draining, PollingStrategy::next())
            .await
            .is_none(),
        "the target member must not own a partition that is still draining"
    );

    // The draining owner's store is admitted and completes the revocation.
    store(&first, &ids, draining, 4)
        .await
        .unwrap_or_else(|error| panic!("a draining owner's store must be admitted: {error}"));

    assert_eq!(fixture.stored_offset(draining).await, Some(4));
    wait_ownership(&second, &ids, draining, true).await;

    Client::shutdown(&second).await.unwrap();
    Client::shutdown(&first).await.unwrap();
    fixture.delete().await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn stores_are_absolute_and_a_lower_store_moves_the_cursor_back() {
    let fixture = GroupTopic::create(1).await;
    fixture.publish(0, 5).await;
    let ids = Ids::new(&fixture);

    let owner = join(&fixture, &ids).await;
    wait_ownership(&owner, &ids, 0, true).await;

    store(&owner, &ids, 0, 3).await.unwrap();
    assert_eq!(fixture.stored_offset(0).await, Some(3));

    store(&owner, &ids, 0, 1).await.unwrap();
    assert_eq!(fixture.stored_offset(0).await, Some(1));

    assert_eq!(
        poll(&owner, &ids, 0, PollingStrategy::next()).await,
        Some(vec![2, 3, 4]),
        "after a lower store the group replays from the regressed cursor"
    );

    Client::shutdown(&owner).await.unwrap();
    fixture.delete().await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn the_stored_offset_is_the_last_processed_record_and_next_resumes_after_it() {
    let fixture = GroupTopic::create(1).await;
    fixture.publish(0, 5).await;
    let ids = Ids::new(&fixture);

    let owner = join(&fixture, &ids).await;
    wait_ownership(&owner, &ids, 0, true).await;

    // Nothing stored: `next` starts at the first record, and an uncommitted poll moves nothing.
    assert_eq!(
        poll(&owner, &ids, 0, PollingStrategy::next()).await,
        Some(vec![0, 1, 2, 3, 4])
    );

    assert_eq!(fixture.stored_offset(0).await, None);

    store(&owner, &ids, 0, 2).await.unwrap();

    assert_eq!(
        poll(&owner, &ids, 0, PollingStrategy::next()).await,
        Some(vec![3, 4])
    );

    Client::shutdown(&owner).await.unwrap();
    fixture.delete().await;
}
