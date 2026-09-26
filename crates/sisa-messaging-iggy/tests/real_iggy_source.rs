//! Opt-in real-broker proofs of the replay-only delivery source: ordered progression and offset
//! semantics, replay after an unconfirmed restart, gap blocking, cancellation, a lower late store,
//! and safe error and close classification.

mod support;

use std::pin::pin;
use std::time::Duration;

use iggy::prelude::{
    Client, ConsumerGroupClient, ConsumerOffsetClient, IggyByteSize, IggyMessage, MessageClient,
    PollingStrategy, SegmentClient, TopicCreateOptions,
};
use sisa_messaging::{
    Delivery, ErrorClassifier, FailureKind, PartitionAdvance, PartitionedLogDeliverySource,
    PartitionedLogReceive, PartitionedLogSettlement,
};
use sisa_messaging_iggy::{IggyClientErrorKind, IggyDeliveryErrorKind, IggyDeliverySource};

use support::{
    identifier, new_iggy_client, new_iggy_client_as, new_raw_client, next_delivery, next_event,
    unique_name, with_group_topic, with_group_topic_options,
};

/// The smallest per-topic segment size the server accepts.
const SMALL_SEGMENT_BYTES: u64 = 1_048_576;

/// Filler records that together overflow one small segment.
const FILLER_RECORDS: usize = 24;

const FILLER_BYTES: usize = 64 * 1_024;

/// Long enough for several poll intervals and assignment refreshes of the test settings.
const QUIET: Duration = Duration::from_secs(1);

async fn advance(settlement: sisa_messaging_iggy::IggySettlement) {
    let advanced = tokio::time::timeout(support::TEST_TIMEOUT, settlement.advance())
        .await
        .unwrap_or_else(|_| panic!("advance timed out"))
        .unwrap_or_else(|error| panic!("advance failed: {error}"));

    assert_eq!(advanced, PartitionAdvance::Advanced);
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn records_arrive_in_offset_order_and_each_advance_stores_its_own_offset() {
    with_group_topic(1, |fixture| async move {
        let published = fixture.publish(0, 5).await;
        let (client, mut source) = fixture.source().await;

        for (expected_offset, expected_id) in published.iter().enumerate() {
            let received = next_delivery(&mut source).await;

            assert_eq!(received.partition, 0);
            assert_eq!(received.offset, expected_offset as u64);
            assert_eq!(received.message_id, *expected_id);

            advance(received.settlement).await;

            // Iggy stores the last consumed offset; the group resumes after it.
            assert_eq!(fixture.stored_offset(0).await, Some(expected_offset as u64));
        }

        client.shutdown().await.unwrap();

        // A restarted member resumes after the stored offset and sees only new records.
        let (restarted_client, mut restarted) = fixture.source().await;

        assert!(
            tokio::time::timeout(QUIET, restarted.receive())
                .await
                .is_err(),
            "a restarted member must not redeliver advanced records"
        );

        let later = fixture.publish(0, 1).await;
        let received = next_delivery(&mut restarted).await;

        assert_eq!(received.offset, 5);
        assert_eq!(received.message_id, later[0]);

        advance(received.settlement).await;
        restarted_client.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn a_record_never_advanced_replays_with_its_identity_after_a_restart() {
    with_group_topic(1, |fixture| async move {
        let published = fixture.publish(0, 3).await;
        let (client, mut source) = fixture.source().await;

        let first = next_delivery(&mut source).await;
        advance(first.settlement).await;

        let unconfirmed = next_delivery(&mut source).await;
        assert_eq!(unconfirmed.offset, 1);

        // The process stops before the record resolves; the settlement is never advanced.
        client.shutdown().await.unwrap();
        drop(unconfirmed);
        drop(source);

        let (restarted_client, mut restarted) = fixture.source().await;
        let replayed = next_delivery(&mut restarted).await;

        assert_eq!(replayed.offset, 1);
        assert_eq!(replayed.message_id, published[1]);

        advance(replayed.settlement).await;

        let last = next_delivery(&mut restarted).await;
        assert_eq!(last.message_id, published[2]);

        advance(last.settlement).await;
        restarted_client.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn an_unresolved_record_blocks_later_offsets_of_its_partition() {
    with_group_topic(1, |fixture| async move {
        let published = fixture.publish(0, 3).await;
        let (client, mut source) = fixture.source().await;

        let held = next_delivery(&mut source).await;
        assert_eq!(held.offset, 0);

        assert!(
            tokio::time::timeout(QUIET, source.receive()).await.is_err(),
            "no later offset may be delivered while an earlier record is unresolved"
        );

        assert_eq!(
            fixture.stored_offset(0).await,
            None,
            "nothing is stored before the record resolves"
        );

        advance(held.settlement).await;

        let next = next_delivery(&mut source).await;

        assert_eq!(next.offset, 1);
        assert_eq!(next.message_id, published[1]);

        advance(next.settlement).await;
        client.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn dropped_receive_futures_lose_no_record() {
    with_group_topic(1, |fixture| async move {
        let published = fixture.publish(0, 3).await;
        let (client, mut source) = fixture.source().await;
        let mut early = None;

        // Each attempt drops `receive` after one scheduler turn, usually while its poll is in flight.
        for _ in 0..50 {
            tokio::select! {
                biased;
                event = source.receive() => {
                    early = Some(event);

                    break;
                }
                () = tokio::task::yield_now() => {}
            }
        }

        let first = match early {
            Some(Ok(PartitionedLogReceive::Delivery(delivery))) => support::received(delivery),
            Some(_) => panic!("unexpected first source event"),
            None => next_delivery(&mut source).await,
        };

        assert_eq!(first.offset, 0);
        assert_eq!(first.message_id, published[0]);
        advance(first.settlement).await;

        for (offset, id) in published.iter().enumerate().skip(1) {
            let received = next_delivery(&mut source).await;

            assert_eq!(received.offset, offset as u64);
            assert_eq!(received.message_id, *id);

            advance(received.settlement).await;
        }

        client.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn a_dropped_advance_withdraws_the_record_and_replays_it() {
    with_group_topic(1, |fixture| async move {
        let published = fixture.publish(0, 2).await;
        let (client, mut source) = fixture.source().await;

        let first = next_delivery(&mut source).await;

        {
            let mut advancing = pin!(first.settlement.advance());

            // Polled once, so its store request may be sent, then dropped: an indeterminate advance.
            tokio::select! {
                biased;
                _ = &mut advancing => panic!("the store must not complete within one poll"),
                () = std::future::ready(()) => {}
            }
        }

        match next_event(&mut source).await {
            Ok(PartitionedLogReceive::OwnershipLost(partition)) => assert_eq!(partition, 0),
            _ => panic!("an indeterminate advance must withdraw its partition first"),
        }

        let replayed = next_delivery(&mut source).await;

        assert_eq!(replayed.offset, 0);
        assert_eq!(replayed.message_id, published[0]);

        advance(replayed.settlement).await;
        assert_eq!(fixture.stored_offset(0).await, Some(0));

        let second = next_delivery(&mut source).await;
        assert_eq!(second.message_id, published[1]);

        advance(second.settlement).await;
        client.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn a_late_lower_store_only_causes_replay() {
    with_group_topic(1, |fixture| async move {
        let published = fixture.publish(0, 5).await;
        let (client, mut source) = fixture.source().await;

        for id in &published {
            let received = next_delivery(&mut source).await;
            assert_eq!(received.message_id, *id);
            advance(received.settlement).await;
        }

        assert_eq!(fixture.stored_offset(0).await, Some(4));
        client.shutdown().await.unwrap();

        // A store for offset 1 lands after offset 4 was stored, as a delayed earlier store from a
        // previous owner would. Iggy accepts it only from the partition's owner, so a raw member that
        // alone holds the partition writes it.
        let member = new_raw_client().await;
        let stream = identifier(&fixture.stream);
        let topic = identifier(&fixture.topic);
        let group = identifier(&fixture.group);

        member
            .join_consumer_group(&stream, &topic, &group)
            .await
            .unwrap_or_else(|error| panic!("raw member failed to join: {error}"));

        let deadline = tokio::time::Instant::now() + support::TEST_TIMEOUT;

        loop {
            let stored = member
                .store_consumer_offset(
                    &iggy::prelude::Consumer::group(group.clone()),
                    &stream,
                    &topic,
                    Some(0),
                    1,
                )
                .await;

            if stored.is_ok() {
                break;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "the raw member never came to own the partition"
            );

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert_eq!(fixture.stored_offset(0).await, Some(1));
        Client::shutdown(&member).await.unwrap();

        // The cursor moved back, so the already-resolved records after offset 1 replay with their
        // own identities; nothing after the regressed cursor is skipped.
        let (replay_client, mut replaying) = fixture.source().await;

        for (offset, id) in published.iter().enumerate().skip(2) {
            let received = next_delivery(&mut replaying).await;

            assert_eq!(received.offset, offset as u64);
            assert_eq!(received.message_id, *id);

            advance(received.settlement).await;
        }

        assert_eq!(fixture.stored_offset(0).await, Some(4));
        replay_client.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn a_missing_group_fails_opening_permanently_and_is_never_created() {
    with_group_topic(1, |fixture| async move {
        let missing = unique_name("sisa-iggy-missing-group");
        let client = new_iggy_client().await;

        let settings = sisa_messaging_iggy::IggySourceSettings::new(
            identifier(&fixture.stream),
            identifier(&fixture.topic),
            identifier(&missing),
        );

        let mut source = IggyDeliverySource::new(client.clone(), settings);
        let error = source.open().await.unwrap_err();

        assert_eq!(error.kind(), IggyDeliveryErrorKind::NotFound);
        assert_eq!(error.classify(), FailureKind::Permanent);
        assert!(!format!("{error} {error:?}").contains(&missing));

        let group = fixture
            .raw
            .get_consumer_group(
                &identifier(&fixture.stream),
                &identifier(&fixture.topic),
                &identifier(&missing),
            )
            .await
            .unwrap();

        assert!(group.is_none(), "the source must never create the group");

        client.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn credential_and_permission_failures_are_permanent_and_redacted() {
    with_group_topic(1, |fixture| async move {
        let username = unique_name("sisa-u");
        let password = unique_name("sisa-iggy-secret");

        let wrong = new_iggy_client_as(&username, &password).await.unwrap_err();

        assert_eq!(wrong.kind(), IggyClientErrorKind::Authentication);
        assert_eq!(wrong.classify(), FailureKind::Permanent);
        assert!(!format!("{wrong} {wrong:?}").contains(&password));

        fixture.create_user(&username, &password).await;

        let client = new_iggy_client_as(&username, &password).await.unwrap();
        let mut source = IggyDeliverySource::new(client.clone(), fixture.settings());
        let error = source.open().await.unwrap_err();

        assert_eq!(error.kind(), IggyDeliveryErrorKind::Unauthorized);
        assert_eq!(error.classify(), FailureKind::Permanent);
        assert!(error.code().is_some());

        let rendered = format!("{error} {error:?}");

        for secret in [
            &username,
            &password,
            &fixture.stream,
            &fixture.topic,
            &fixture.group,
        ] {
            assert!(!rendered.contains(secret.as_str()));
        }

        client.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn a_group_deleted_while_running_fails_the_source_permanently() {
    with_group_topic(1, |fixture| async move {
        fixture.publish(0, 1).await;
        let (client, mut source) = fixture.source().await;

        let received = next_delivery(&mut source).await;
        advance(received.settlement).await;

        fixture
            .raw
            .delete_consumer_group(
                &identifier(&fixture.stream),
                &identifier(&fixture.topic),
                &identifier(&fixture.group),
            )
            .await
            .unwrap();

        let error = loop {
            match next_event(&mut source).await {
                Err(error) => break error,
                Ok(PartitionedLogReceive::Closed) => panic!("a deleted group is not a clean close"),
                Ok(_) => {}
            }
        };

        assert_eq!(error.kind(), IggyDeliveryErrorKind::NotFound);
        assert_eq!(error.classify(), FailureKind::Permanent);
        assert!(!format!("{error} {error:?}").contains(&fixture.group));

        client.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn shutting_down_the_client_closes_the_source_cleanly() {
    with_group_topic(1, |fixture| async move {
        let (client, mut source) = fixture.source().await;

        client.shutdown().await.unwrap();

        assert!(matches!(
            next_event(&mut source).await,
            Ok(PartitionedLogReceive::Closed)
        ));

        assert!(matches!(
            next_event(&mut source).await,
            Ok(PartitionedLogReceive::Closed)
        ));
    })
    .await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn a_replay_poll_that_skips_its_offset_fails_the_source_permanently() {
    // Small segments that flush every record let the first segment be deleted while later
    // records keep their offsets. Iggy deletes a segment only once the group's stored offset
    // covers all of it, so the gap is made below the cursor: the withdrawn record's dropped
    // store still applies, the segment holding it is deleted, and the replay poll then starts
    // past the record the source expects.
    let options = TopicCreateOptions {
        partitions_count: Some(1),
        segment_size: Some(IggyByteSize::new(SMALL_SEGMENT_BYTES)),
        messages_required_to_save: Some(1),
        ..TopicCreateOptions::default()
    };

    with_group_topic_options(options, |fixture| async move {
        // One batch that overflows the first segment seals it with every one of its records.
        let filler: Vec<IggyMessage> = (0..FILLER_RECORDS)
            .map(|_| {
                IggyMessage::builder()
                    .payload(vec![0_u8; FILLER_BYTES].into())
                    .build()
                    .unwrap_or_else(|_| panic!("filler message builds"))
            })
            .collect();

        fixture.publish_messages(0, filler).await;
        let last_sealed = FILLER_RECORDS as u64 - 1;
        fixture.publish(0, 2).await;

        // A long poll interval leaves time to delete the segment before the replay poll.
        let settings = fixture
            .settings()
            .with_batch_length(FILLER_RECORDS as u32 + 2)
            .and_then(|settings| settings.with_poll_interval(Duration::from_secs(3)))
            .unwrap();

        let (client, mut source) = fixture.source_with(settings).await;

        for expected in 0..last_sealed {
            let settlement = next_settlement(&mut source).await;
            assert_eq!(settlement.offset(), expected);
            advance(settlement).await;
        }

        let withdrawn = next_settlement(&mut source).await;
        assert_eq!(withdrawn.offset(), last_sealed);

        {
            let mut advancing = pin!(withdrawn.advance());

            tokio::select! {
                biased;
                _ = &mut advancing => panic!("the store must not complete within one poll"),
                () = std::future::ready(()) => {}
            }
        }

        assert!(matches!(
            next_event(&mut source).await,
            Ok(PartitionedLogReceive::OwnershipLost(0))
        ));

        // The dropped store was already sent; once it applies the whole segment is consumed.
        let deadline = tokio::time::Instant::now() + support::TEST_TIMEOUT;

        while fixture.stored_offset(0).await != Some(last_sealed) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the dropped store never applied"
            );

            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let stream = identifier(&fixture.stream);
        let topic = identifier(&fixture.topic);

        fixture
            .raw
            .delete_segments(&stream, &topic, 0, 1)
            .await
            .unwrap_or_else(|error| panic!("Iggy test segment deletion failed: {error}"));

        // Wait until the withdrawn record is gone from the partition.
        let probe = iggy::prelude::Consumer::new(identifier("sisa-iggy-gap-probe"));

        loop {
            let polled = fixture
                .raw
                .poll_messages(
                    &stream,
                    &topic,
                    Some(0),
                    &probe,
                    &PollingStrategy::offset(last_sealed),
                    1,
                    false,
                )
                .await
                .unwrap_or_else(|error| panic!("Iggy test probe poll failed: {error}"));

            if polled
                .messages
                .first()
                .is_some_and(|message| message.header.offset > last_sealed)
            {
                break;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "the withdrawn record's segment was never deleted"
            );

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let error = match next_event(&mut source).await {
            Err(error) => error,
            Ok(PartitionedLogReceive::Delivery(delivery)) => {
                let (_, settlement) = delivery.into_parts();

                panic!("delivered offset {} past the gap", settlement.offset())
            }
            Ok(_) => panic!("unexpected source event instead of the gap failure"),
        };

        assert_eq!(error.kind(), IggyDeliveryErrorKind::OffsetGap);
        assert_eq!(error.classify(), FailureKind::Permanent);

        // The source stays failed rather than skipping forward on the next call.
        assert!(!matches!(
            next_event(&mut source).await,
            Ok(PartitionedLogReceive::Delivery(_))
        ));

        client.shutdown().await.unwrap();
    })
    .await;
}

/// Waits for the next delivery without decoding it and returns its settlement.
async fn next_settlement(source: &mut IggyDeliverySource) -> sisa_messaging_iggy::IggySettlement {
    match next_event(source).await {
        Ok(PartitionedLogReceive::Delivery(delivery)) => delivery.into_parts().1,
        _ => panic!("expected a delivery"),
    }
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn a_header_name_that_is_not_utf8_decodes_as_an_invalid_header() {
    use sisa_messaging::EnvelopeMapper;
    use sisa_messaging_iggy::{IggyEnvelopeMapper, IggyMappingError};

    with_group_topic(1, |fixture| async move {
        let message = support::sdk_message_with(sisa_messaging::MessageId::new(), |headers| {
            let name = iggy::prelude::HeaderKey::try_from(vec![0xff_u8, 0xfe])
                .unwrap_or_else(|_| panic!("raw header name builds"));

            let value = iggy::prelude::HeaderValue::try_from("value")
                .unwrap_or_else(|_| panic!("header value builds"));

            headers.insert(name, value);
        });

        fixture.publish_messages(0, vec![message]).await;
        let (client, mut source) = fixture.source().await;

        let delivery = match next_event(&mut source).await {
            Ok(PartitionedLogReceive::Delivery(delivery)) => delivery,
            _ => panic!("the record must still be delivered"),
        };

        let (record, settlement) = delivery.into_parts();

        assert_eq!(
            IggyEnvelopeMapper.decode(record).unwrap_err(),
            IggyMappingError::InvalidHeader
        );

        drop(settlement);
        client.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires a real Iggy broker; provisions its own topic and consumer group"]
async fn header_bytes_the_sdk_cannot_parse_decode_as_an_invalid_header() {
    use sisa_messaging::EnvelopeMapper;
    use sisa_messaging_iggy::{IggyEnvelopeMapper, IggyMappingError};

    with_group_topic(1, |fixture| async move {
        let mut message = support::sdk_message(sisa_messaging::MessageId::new());

        // Header bytes that are not a valid header encoding.
        let garbage: &'static [u8] = &[0xff; 7];
        message.user_headers = Some(garbage.into());
        message.header.user_headers_length = garbage.len() as u32;

        fixture.publish_messages(0, vec![message]).await;
        let (client, mut source) = fixture.source().await;

        let delivery = match next_event(&mut source).await {
            Ok(PartitionedLogReceive::Delivery(delivery)) => delivery,
            _ => panic!("the record must still be delivered"),
        };

        let (record, settlement) = delivery.into_parts();

        assert_eq!(
            IggyEnvelopeMapper.decode(record).unwrap_err(),
            IggyMappingError::InvalidHeader
        );

        drop(settlement);
        client.shutdown().await.unwrap();
    })
    .await;
}
