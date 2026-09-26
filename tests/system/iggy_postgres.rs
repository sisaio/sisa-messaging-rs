//! Typed partitioned-consumer system proof over a real PostgreSQL inbox and a real Apache Iggy
//! consumer group.
//!
//! Run the ignored test with either `DATABASE_URL` or the `PG*` variables set against a database
//! migrated with the repository schema, and an Iggy server at `SISA_IGGY_SERVER_ADDRESS`
//! (default `127.0.0.1:8090`, credentials `SISA_IGGY_USERNAME` and `SISA_IGGY_PASSWORD`, default
//! `iggy`). Each test uses its own inbox scope, topic, and consumer group.

#![forbid(unsafe_code)]

#[path = "iggy_postgres/support.rs"]
mod support;

use std::collections::BTreeSet;

use sisa_messaging::MessageId;
use sisa_messaging_consumer::ConsumerExit;

use support::{Fixture, Member, effect_members, effects, run_with_cleanup, wait_completed};

const PARTITIONS: u32 = 4;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL and a real Iggy server at SISA_IGGY_SERVER_ADDRESS"]
async fn two_members_sharing_one_inbox_apply_each_effect_once_across_a_rebalance() {
    run_with_cleanup(PARTITIONS, |fixture| async move {
        let Fixture {
            pool,
            broker,
            scope,
        } = fixture;

        let mut published = Vec::new();

        for index in 0..40 {
            let id = MessageId::new();
            broker.publish(id, &format!("first-{index}")).await;
            published.push(id);
        }

        let first = Member::spawn("first", &broker, &pool, &scope).await;

        // The second member joins while the first holds records in flight on its partitions.
        wait_completed(&pool, &scope, 10).await;
        let second = Member::spawn("second", &broker, &pool, &scope).await;

        for index in 0..40 {
            let id = MessageId::new();
            broker.publish(id, &format!("second-{index}")).await;
            published.push(id);
        }

        wait_completed(&pool, &scope, published.len() as i64).await;

        assert_eq!(second.stop().await.unwrap(), ConsumerExit::Cancelled);
        assert_eq!(first.stop().await.unwrap(), ConsumerExit::Cancelled);

        // Exactly one effect row per published message: none skipped, none duplicated, even
        // where the rebalance or a withdrawn offset store replayed a record.
        let effects = effects(&pool, &scope).await;
        let expected: BTreeSet<MessageId> = published.iter().copied().collect();
        let observed: BTreeSet<MessageId> = effects.iter().map(|(id, _)| *id).collect();

        assert_eq!(observed, expected);
        assert!(effects.iter().all(|(_, count)| *count == 1));

        // The rebalance moved partitions: both members applied effects.
        let mut members = effect_members(&pool, &scope).await;
        members.sort();
        assert_eq!(members, ["first", "second"]);
    })
    .await;
}
