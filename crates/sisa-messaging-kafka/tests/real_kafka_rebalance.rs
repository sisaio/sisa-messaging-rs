//! Opt-in negative feasibility test: the broker accepts an explicit offset after revocation.

mod support;

use std::time::Instant;

use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use sisa_messaging::MessageId;

use support::{
    BROKERS_ENV, TEST_TIMEOUT, TOPIC_ENV, committed_offset, new_consumer_with_instance,
    new_producer, publish_marker, receive_marker, required_env, subscribe, unique_group,
};

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned single-partition test topic"]
async fn broker_accepts_commit_for_revoked_partition_from_current_member_generation() {
    let brokers = required_env(BROKERS_ENV);
    let topic = required_env(TOPIC_ENV);
    let producer = new_producer(&brokers);
    let marker = MessageId::new().to_string();

    let _ = publish_marker(&producer, &topic, &marker).await;

    let group = unique_group();
    // Range assignment sorts the static member identities. This makes the replacement member
    // the expected owner of the single test partition after the broker completes rebalance.
    let old_generation = new_consumer_with_instance(&brokers, &group, Some("z-old-member"));
    subscribe(&old_generation, &topic);

    let (partition, offset) = receive_marker(&old_generation, &topic, &marker);

    let new_generation = new_consumer_with_instance(&brokers, &group, Some("a-new-member"));
    subscribe(&new_generation, &topic);

    // Poll both participants to complete the coordinator's rebalance. The previous member
    // then commits an offset for a partition outside its assignment. This records whether the
    // coordinator fences by partition ownership after the member has refreshed its generation.
    let deadline = Instant::now() + TEST_TIMEOUT;
    let mut ownership_changed = false;

    while Instant::now() < deadline {
        let _ = old_generation.poll(std::time::Duration::from_millis(50));
        let _ = new_generation.poll(std::time::Duration::from_millis(50));
        let old_assignment = old_generation.assignment();
        let new_assignment = new_generation.assignment();
        if old_assignment.is_ok_and(|set| set.count() == 0)
            && new_assignment.is_ok_and(|set| set.count() > 0)
        {
            ownership_changed = true;
            break;
        }
    }

    assert!(
        ownership_changed,
        "Kafka broker did not move the partition to the new generation"
    );

    let mut revoked_offsets = TopicPartitionList::new();

    revoked_offsets
        .add_partition_offset(topic.as_str(), partition, Offset::Offset(offset + 1))
        .unwrap_or_else(|_| panic!("Kafka revoked offset list construction failed"));

    let revoked_commit_result = old_generation.commit(&revoked_offsets, CommitMode::Sync);

    assert!(
        revoked_commit_result.is_ok(),
        "expected the broker to accept an explicit offset commit from the member after it lost partition ownership",
    );
    assert_eq!(
        committed_offset(&new_generation, &topic, partition),
        Offset::Offset(offset + 1),
        "expected the broker to commit the explicit offset for the revoked partition",
    );
}
