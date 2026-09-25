//! Opt-in negative feasibility test: the consumer-offset store accepts a write from a group
//! member that provably does not own the partition.
//!
//! Finding for issue #23's deferred inbound slice, tracked by
//! <https://github.com/sisaio/sisa-messaging-rs/issues/60>: Iggy's consumer-offset store carries
//! no membership generation. A member's offset store for a partition it does not own is accepted
//! the same way as one from the owning member, with no rejection tied to group membership,
//! generation, or partition ownership. A fencing-correct implementation of the shared
//! partitioned-log delivery source profile is therefore not possible against the current server,
//! which is why this crate ships publisher-only.

mod support;

use std::time::Duration;

use iggy::prelude::{
    Consumer, ConsumerGroupClient, ConsumerOffsetClient, Identifier, SystemClient, TopicClient,
};

use support::{new_raw_client, provision_stream_and_topic, test_stream, unique_name};

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own stream and topic"]
async fn broker_accepts_an_offset_store_from_a_member_that_does_not_own_the_partition() {
    let stream = test_stream();
    let topic = unique_name("sisa-iggy-fencing-topic");
    let group_name = unique_name("sisa-iggy-fencing-group");

    let provisioning_client = new_raw_client().await;
    provision_stream_and_topic(&provisioning_client, &stream, &topic).await;

    let stream_id = Identifier::from_str_value(&stream).expect("valid test stream identifier");
    let topic_id = Identifier::from_str_value(&topic).expect("valid test topic identifier");
    let group_id = Identifier::from_str_value(&group_name).expect("valid test group identifier");

    provisioning_client
        .create_consumer_group(&stream_id, &topic_id, &group_name)
        .await
        .unwrap_or_else(|error| panic!("Iggy test consumer group creation failed: {error}"));

    let member_a = new_raw_client().await;
    let member_b = new_raw_client().await;

    member_a
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap_or_else(|error| panic!("first Iggy test member failed to join: {error}"));
    member_b
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap_or_else(|error| panic!("second Iggy test member failed to join: {error}"));

    // Give the coordinator time to settle both members' assignments before reading them back.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let member_a_client_id = member_a
        .get_me()
        .await
        .unwrap_or_else(|error| {
            panic!("first Iggy test member failed to read its own client id: {error}")
        })
        .client_id;
    let member_b_client_id = member_b
        .get_me()
        .await
        .unwrap_or_else(|error| {
            panic!("second Iggy test member failed to read its own client id: {error}")
        })
        .client_id;

    let group_details = provisioning_client
        .get_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap_or_else(|error| panic!("Iggy test consumer group lookup failed: {error}"))
        .expect("the just-created consumer group must exist");

    assert_eq!(
        group_details.members.len(),
        2,
        "expected exactly two members in the single-partition test group"
    );

    let owns_partition_zero = |client_id: u32| {
        group_details
            .members
            .iter()
            .find(|member| member.id == client_id)
            .is_some_and(|member| member.partitions.contains(&0))
    };

    let a_owns = owns_partition_zero(member_a_client_id);
    let b_owns = owns_partition_zero(member_b_client_id);
    assert_ne!(
        a_owns, b_owns,
        "expected exactly one member to own the single partition"
    );

    let (non_owner, non_owner_client_id) = if a_owns {
        (&member_b, member_b_client_id)
    } else {
        (&member_a, member_a_client_id)
    };
    assert!(
        !owns_partition_zero(non_owner_client_id),
        "the selected member must provably not own partition 0"
    );

    let consumer = Consumer::group(group_id.clone());

    let result = non_owner
        .store_consumer_offset(&consumer, &stream_id, &topic_id, Some(0), 1)
        .await;

    assert!(
        result.is_ok(),
        "expected the broker to accept an offset store from a member that does not own \
         partition 0; a rejection here would mean Iggy now fences by partition ownership and \
         https://github.com/sisaio/sisa-messaging-rs/issues/60 must be revisited",
    );

    let stored = provisioning_client
        .get_consumer_offset(&consumer, &stream_id, &topic_id, Some(0))
        .await
        .unwrap_or_else(|error| panic!("Iggy test offset read-back failed: {error}"));

    assert!(
        stored.is_some_and(|offset| offset.stored_offset == 1),
        "expected the non-owner's offset store to be visible through the offset read"
    );

    // Best-effort cleanup: this test provisions its own throwaway consumer group and topic.
    let _ = provisioning_client
        .delete_consumer_group(&stream_id, &topic_id, &group_id)
        .await;
    let _ = provisioning_client
        .delete_topic(&stream_id, &topic_id)
        .await;
}
