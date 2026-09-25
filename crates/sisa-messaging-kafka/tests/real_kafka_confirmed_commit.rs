//! Opt-in proof that a confirmed synchronous commit is visible after generation replacement.

mod support;

use std::time::Instant;

use rdkafka::consumer::Consumer;
use rdkafka::topic_partition_list::Offset;
use sisa_messaging::MessageId;

use support::{
    BROKERS_ENV, TEST_TIMEOUT, TOPIC_ENV, committed_offset, new_consumer, new_producer,
    publish_marker, receive_marker, required_env, subscribe, unique_group,
};

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned isolated test topic"]
async fn confirmed_sync_commit_is_visible_to_a_new_group_generation() {
    let brokers = required_env(BROKERS_ENV);
    let topic = required_env(TOPIC_ENV);
    let producer = new_producer(&brokers);
    let marker = MessageId::new().to_string();

    let _ = publish_marker(&producer, &topic, &marker).await;

    let group = unique_group();
    let first = new_consumer(&brokers, &group);
    subscribe(&first, &topic);

    let (partition, offset) = receive_marker(&first, &topic, &marker);

    support::commit_offset(&first, &topic, partition, offset + 1);
    first.unsubscribe();

    let next_generation = new_consumer(&brokers, &group);
    subscribe(&next_generation, &topic);

    let deadline = Instant::now() + TEST_TIMEOUT;

    while Instant::now() < deadline
        && next_generation
            .assignment()
            .map_or(true, |set| set.count() == 0)
    {
        let _ = next_generation.poll(std::time::Duration::from_millis(100));
    }

    assert!(
        next_generation
            .assignment()
            .is_ok_and(|set| set.count() > 0),
        "new Kafka group generation did not receive an assignment",
    );

    assert_eq!(
        committed_offset(&next_generation, &topic, partition),
        Offset::Offset(offset + 1)
    );
}
