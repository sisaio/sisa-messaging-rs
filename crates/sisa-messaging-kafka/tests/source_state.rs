//! Deterministic Kafka source state: every partition transition, each error-mapping row,
//! consumer settings validation, redaction, and transactional identity derivation.

#[allow(dead_code, reason = "the member thread uses the remaining helpers")]
#[path = "../src/source/classify.rs"]
mod classify;

#[allow(dead_code, reason = "the member thread uses the remaining helpers")]
#[path = "../src/source/table.rs"]
mod table;

use rdkafka::error::RDKafkaErrorCode;
use sisa_messaging_kafka::{
    KafkaClient, KafkaClientErrorKind, KafkaClientSettings, KafkaConsumerSettings,
};

use classify::{
    ConsumerVerdict, Disposition, Stage, TxnFailure, Verdict, after_abort, consumer,
    open_is_transient, transaction,
};
use table::{Admission, LANE_CAPACITY, LANE_RESUME_AT, Popped, State, Table};

/// A reply channel stand-in: an identifier and whether its receiver is still live.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Waiter {
    id: u32,

    live: bool,
}

const LIVE: Waiter = Waiter { id: 1, live: true };
const DEAD: Waiter = Waiter { id: 2, live: false };

type TestTable = Table<u32, &'static str, Waiter>;

fn live(waiter: &Waiter) -> bool {
    waiter.live
}

/// A table owning partitions `keys` in generation 1.
fn assigned(keys: &[u32]) -> TestTable {
    let mut table = TestTable::default();
    let (generation, withheld) = table.assign(keys.iter().copied());
    assert_eq!(generation, 1);
    assert!(withheld.is_empty());

    table
}

fn pop_record(table: &mut TestTable) -> (u32, i64, u64) {
    match table.pop().0 {
        Some(Popped::Record(lane)) => (lane.key, lane.offset, lane.generation),
        Some(Popped::Loss(key)) => panic!("expected a record, got a loss for {key}"),
        None => panic!("expected a record"),
    }
}

fn pop_loss(table: &mut TestTable) -> u32 {
    match table.pop().0 {
        Some(Popped::Loss(key)) => key,
        Some(Popped::Record(lane)) => panic!("expected a loss, got record {}", lane.offset),
        None => panic!("expected a loss"),
    }
}

/// Emits, hands, and queues an advance for `offset` on partition `key`.
fn advancing(table: &mut TestTable, key: u32, offset: i64, waiter: Waiter) {
    let outcome = table.on_record(&key, offset, || "record");
    assert!(outcome.emitted);
    let (_, popped, generation) = pop_record(table);
    assert_eq!(popped, offset);

    assert!(matches!(
        table.admit(&key, generation, offset, waiter),
        Admission::Queued
    ));
}

/// Takes the single queued advance into a transaction batch.
fn sending(table: &mut TestTable, key: u32, offset: i64) {
    let batch = table.take_batch(live);
    assert_eq!(batch.items.len(), 1);
    assert_eq!(table.state(&key), Some(State::Sending { offset }));
}

// ---------------------------------------------------------------------------------------------
// Emission and bounded polling.

#[test]
fn assignment_starts_every_partition_idle_in_a_new_generation() {
    let mut table = assigned(&[0, 1]);
    assert_eq!(table.state(&0), Some(State::Idle));
    assert_eq!(table.state(&1), Some(State::Idle));
    assert_eq!(table.next_position(&0), None);

    let _ = table.revoke();
    let (generation, _) = table.assign([0]);
    assert_eq!(generation, 2);
    assert_eq!(table.generation(), 2);
}

#[test]
fn emitted_record_pauses_its_partition_and_later_fetches_are_discarded() {
    let mut table = assigned(&[0]);

    let outcome = table.on_record(&0, 10, || "first");
    assert!(outcome.emitted);
    assert_eq!(outcome.pause, vec![0]);

    assert_eq!(
        table.state(&0),
        Some(State::InFlight {
            offset: 10,
            handed: false
        })
    );

    let later = table.on_record(&0, 11, || panic!("a discarded record is never built"));
    assert!(!later.emitted);
    assert!(later.pause.is_empty());
    assert_eq!(table.lane_len(), 1);
}

#[test]
fn records_for_unowned_partitions_or_without_assignment_are_dropped() {
    let mut table = assigned(&[0]);
    assert!(!table.on_record(&9, 1, || "foreign").emitted);

    let _ = table.revoke();
    assert!(!table.on_record(&0, 1, || "revoked").emitted);
    assert!(!table.has_output());
}

#[test]
fn idle_partition_discards_fetches_behind_its_next_position_and_accepts_holes() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 10, LIVE);
    sending(&mut table, 0, 10);
    table.advanced(&0, 10, true);
    assert_eq!(table.next_position(&0), Some(11));
    table.resumed(&[0]);

    assert!(!table.on_record(&0, 10, || "stale").emitted);
    assert_eq!(table.state(&0), Some(State::Idle));

    // read_committed skips aborted records and transaction markers.
    let outcome = table.on_record(&0, 14, || "after a hole");
    assert!(outcome.emitted);

    assert_eq!(
        table.state(&0),
        Some(State::InFlight {
            offset: 14,
            handed: false
        })
    );
}

#[test]
fn full_lane_pauses_idle_partitions_until_it_drains() {
    let keys: Vec<u32> = (0..=u32::try_from(LANE_CAPACITY).unwrap_or(u32::MAX)).collect();
    let mut table = assigned(&keys);

    for key in 0..u32::try_from(LANE_CAPACITY - 1).unwrap_or(u32::MAX) {
        assert!(table.on_record(&key, 0, || "record").emitted);
    }

    assert!(!table.backpressure());

    let last = u32::try_from(LANE_CAPACITY - 1).unwrap_or(u32::MAX);
    let spare = u32::try_from(LANE_CAPACITY).unwrap_or(u32::MAX);
    let outcome = table.on_record(&last, 0, || "fills the lane");
    assert!(outcome.emitted);
    assert!(table.backpressure());
    assert_eq!(outcome.pause, vec![last, spare]);
    assert_eq!(table.state(&spare), Some(State::BackpressurePaused));

    // A fetch already in flight for the paused partition is discarded and remembered as the
    // seek target, so the resume re-fetches it.
    assert!(!table.on_record(&spare, 7, || "prefetched").emitted);
    assert!(!table.on_record(&spare, 8, || "prefetched").emitted);
    assert_eq!(table.next_position(&spare), Some(7));

    for _ in 0..(LANE_CAPACITY - LANE_RESUME_AT - 1) {
        let (_, wake) = table.pop();
        assert!(!wake);
    }

    let (_, wake) = table.pop();
    assert!(wake, "the lane drained to its resume threshold");
    assert!(!table.backpressure());
    assert_eq!(table.state(&spare), Some(State::ResumePending));
    assert_eq!(table.resumable(), vec![(spare, Some(7))]);

    table.resumed(&[spare]);
    assert_eq!(table.state(&spare), Some(State::Idle));
}

#[test]
fn partitions_released_under_backpressure_wait_for_the_lane_to_drain() {
    let keys: Vec<u32> = (0..u32::try_from(LANE_CAPACITY + 1).unwrap_or(u32::MAX)).collect();
    let mut table = assigned(&keys);
    let spare = u32::try_from(LANE_CAPACITY).unwrap_or(u32::MAX);

    // The spare partition's record is handed and its advance commits.
    advancing(&mut table, spare, 3, LIVE);
    sending(&mut table, spare, 3);

    for key in 0..spare {
        assert!(table.on_record(&key, 0, || "record").emitted);
    }

    assert!(table.backpressure());
    table.advanced(&spare, 3, true);

    // It is not resumed into a full lane; it keeps its seek target until the lane drains.
    assert!(table.resumable().is_empty());
    assert_eq!(table.state(&spare), Some(State::BackpressurePaused));

    while table.lane_len() > LANE_RESUME_AT {
        let _ = table.pop();
    }

    assert_eq!(table.resumable(), vec![(spare, Some(4))]);
}

#[test]
fn losses_are_received_before_records() {
    let mut table = assigned(&[0, 1]);
    advancing(&mut table, 0, 5, DEAD);
    assert!(table.on_record(&1, 3, || "waiting").emitted);

    let batch = table.take_batch(live);
    assert!(batch.items.is_empty());

    assert_eq!(pop_loss(&mut table), 0);
    assert_eq!(pop_record(&mut table).0, 1);
}

// ---------------------------------------------------------------------------------------------
// Advance requests and transaction outcomes.

#[test]
fn handed_record_admits_one_advance_for_its_generation() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    assert_eq!(table.state(&0), Some(State::Advancing { offset: 5 }));

    let batch = table.take_batch(live);
    assert_eq!(batch.generation, 1);
    assert_eq!(batch.items.len(), 1);
    assert_eq!(batch.items[0].offset, 5);
    assert_eq!(batch.items[0].waiter, LIVE);
    assert_eq!(table.state(&0), Some(State::Sending { offset: 5 }));
}

#[test]
fn advance_for_an_unhanded_or_mismatched_record_is_ownership_lost_without_io() {
    let mut table = assigned(&[0]);
    assert!(table.on_record(&0, 5, || "in lane").emitted);

    assert!(matches!(
        table.admit(&0, 1, 5, LIVE),
        Admission::OwnershipLost(_)
    ));

    assert!(matches!(
        table.admit(&0, 7, 5, LIVE),
        Admission::OwnershipLost(_)
    ));

    assert!(table.take_batch(live).items.is_empty());
}

#[test]
fn committed_advance_resumes_at_the_next_offset() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    sending(&mut table, 0, 5);

    table.advanced(&0, 5, true);
    assert_eq!(table.state(&0), Some(State::ResumePending));
    assert_eq!(table.resumable(), vec![(0, Some(6))]);
    assert!(!table.has_output());

    table.resumed(&[0]);
    assert_eq!(table.state(&0), Some(State::Idle));
}

#[test]
fn undelivered_committed_reply_becomes_a_release_before_the_next_offset() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    sending(&mut table, 0, 5);

    table.advanced(&0, 5, false);
    assert_eq!(table.next_position(&0), Some(6));
    table.resumed(&[0]);
    assert!(table.on_record(&0, 6, || "next").emitted);

    assert_eq!(pop_loss(&mut table), 0);
    assert_eq!(pop_record(&mut table).1, 6);
}

#[test]
fn dead_waiter_before_send_replays_the_unadvanced_offset_with_a_release() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, DEAD);

    assert!(table.take_batch(live).items.is_empty());
    assert_eq!(table.state(&0), Some(State::ResumePending));
    assert_eq!(table.resumable(), vec![(0, Some(5))]);
    assert_eq!(pop_loss(&mut table), 0);
}

#[test]
fn generation_fence_holds_the_partition_until_revocation() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    sending(&mut table, 0, 5);

    table.fenced(&0, 5, true);
    assert_eq!(table.state(&0), Some(State::Held { offset: 5 }));
    assert!(table.resumable().is_empty());
    assert!(!table.on_record(&0, 5, || "held").emitted);
}

#[test]
fn permanent_failure_holds_the_partition_without_a_release() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    sending(&mut table, 0, 5);

    table.failed_permanently(&0, 5);
    assert_eq!(table.state(&0), Some(State::Held { offset: 5 }));
    assert!(!table.has_output());
}

#[test]
fn reconciled_advance_continues_after_the_record() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    sending(&mut table, 0, 5);
    table.begin_reconcile(&0, 5);
    assert_eq!(table.state(&0), Some(State::Reconciling { offset: 5 }));
    assert!(!table.on_record(&0, 6, || "paused").emitted);

    table.reconciled(&0, 5, true, true);
    assert_eq!(table.resumable(), vec![(0, Some(6))]);
    assert!(!table.has_output());
}

#[test]
fn reconciled_unchanged_cursor_replays_the_record() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    sending(&mut table, 0, 5);
    table.begin_reconcile(&0, 5);

    table.reconciled(&0, 5, false, true);
    assert_eq!(table.resumable(), vec![(0, Some(5))]);
}

#[test]
fn reconciled_outcome_for_a_gone_waiter_is_released() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    sending(&mut table, 0, 5);
    table.begin_reconcile(&0, 5);

    table.reconciled(&0, 5, false, false);
    assert_eq!(pop_loss(&mut table), 0);
}

#[test]
fn unestablished_reconciliation_keeps_the_partition_paused() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    sending(&mut table, 0, 5);
    table.begin_reconcile(&0, 5);

    assert!(table.resumable().is_empty());
    assert_eq!(table.state(&0), Some(State::Reconciling { offset: 5 }));
}

#[test]
fn outcomes_for_a_different_offset_are_ignored() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    sending(&mut table, 0, 5);

    table.advanced(&0, 4, true);
    table.reconciled(&0, 5, true, true);
    assert_eq!(table.state(&0), Some(State::Sending { offset: 5 }));
}

#[test]
fn dropped_handle_holds_the_partition_until_revocation() {
    let mut table = assigned(&[0]);
    assert!(table.on_record(&0, 5, || "record").emitted);
    let (_, _, generation) = pop_record(&mut table);

    assert!(!table.handle_dropped(&0, generation, 5));
    assert_eq!(table.state(&0), Some(State::Held { offset: 5 }));
    assert!(table.resumable().is_empty());
}

#[test]
fn abandoned_advance_is_left_to_the_member_while_pending() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    assert!(!table.advance_abandoned(&0, 1, 5));
    sending(&mut table, 0, 5);
    assert!(!table.advance_abandoned(&0, 1, 5));
    assert!(!table.has_output());
}

#[test]
fn abandoned_advance_after_its_reply_is_released() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    sending(&mut table, 0, 5);
    table.advanced(&0, 5, true);

    assert!(table.advance_abandoned(&0, 1, 5));
    assert_eq!(pop_loss(&mut table), 0);
    assert!(!table.advance_abandoned(&0, 9, 5));
}

// ---------------------------------------------------------------------------------------------
// Revocation and reassignment.

#[test]
fn revocation_releases_handed_records_and_discards_unreceived_ones() {
    let mut table = assigned(&[0, 1, 2]);
    assert!(table.on_record(&0, 5, || "handed").emitted);
    assert_eq!(pop_record(&mut table).0, 0);
    assert!(table.on_record(&1, 3, || "unreceived").emitted);

    let revocation = table.revoke();
    assert!(revocation.lost.is_empty());

    assert_eq!(pop_loss(&mut table), 0);

    assert!(
        table.pop().0.is_none(),
        "the unreceived record is discarded without a loss"
    );

    assert!(table.is_revoked_pending(&0));
    assert!(!table.is_revoked_pending(&1));
    assert_eq!(table.state(&2), None);
}

#[test]
fn revocation_answers_queued_advances_as_ownership_lost() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);

    let revocation = table.revoke();
    assert_eq!(revocation.lost, vec![LIVE]);
    assert_eq!(pop_loss(&mut table), 0);

    assert!(
        !table.is_revoked_pending(&0),
        "the consumed handle leaves nothing to wait for"
    );
}

#[test]
fn reassigned_partition_is_withheld_until_the_old_handle_drops() {
    let mut table = assigned(&[0, 1]);
    assert!(table.on_record(&0, 5, || "old generation").emitted);
    let (_, _, old_generation) = pop_record(&mut table);

    let _ = table.revoke();
    let (generation, withheld) = table.assign([0, 1]);
    assert_eq!(generation, old_generation + 1);
    assert_eq!(withheld, vec![0]);
    assert_eq!(table.state(&0), Some(State::Withheld));
    assert_eq!(table.state(&1), Some(State::Idle));

    // Fetches for the withheld partition are discarded; the earliest is the seek target.
    assert!(!table.on_record(&0, 5, || "new generation").emitted);
    assert_eq!(table.next_position(&0), Some(5));

    assert!(table.handle_dropped(&0, old_generation, 5));
    assert_eq!(table.state(&0), Some(State::ResumePending));
    assert_eq!(table.resumable(), vec![(0, Some(5))]);
}

/// A handed record at offset 5 is revoked, the partition returns to the same source, and the old
/// handle is released before any record of the new assignment is polled. No seek target is kept:
/// librdkafka resets its consumed position when the old assignment stops fetching, and the new
/// assignment starts at the committed offset, which the old generation could not advance. A seek
/// is needed only when a polled record was discarded while withheld, as covered above.
#[test]
fn released_withheld_partition_without_discards_resumes_at_the_committed_start() {
    let mut table = assigned(&[0]);
    assert!(table.on_record(&0, 5, || "old generation").emitted);
    let (_, _, old_generation) = pop_record(&mut table);

    let _ = table.revoke();
    assert_eq!(pop_loss(&mut table), 0);
    let (_, withheld) = table.assign([0]);
    assert_eq!(withheld, vec![0]);
    assert_eq!(table.next_position(&0), None);

    // The old generation's advance is fenced locally, so the committed offset stays at or
    // below 5, and releasing its handle resumes the partition from that committed start.
    assert!(matches!(
        table.admit(&0, old_generation, 5, LIVE),
        Admission::OwnershipLost(_)
    ));

    assert_eq!(table.resumable(), vec![(0, None)]);
    table.resumed(&[0]);

    // The committed start re-fetches the revoked record, which replays in the new generation.
    assert!(table.on_record(&0, 5, || "replayed").emitted);
    let (key, offset, generation) = pop_record(&mut table);
    assert_eq!((key, offset), (0, 5));
    assert_eq!(generation, old_generation + 1);
}

#[test]
fn stale_generation_advance_is_ownership_lost_and_releases_the_withheld_partition() {
    let mut table = assigned(&[0]);
    assert!(table.on_record(&0, 5, || "old generation").emitted);
    let (_, _, old_generation) = pop_record(&mut table);

    let _ = table.revoke();
    let _ = table.assign([0]);

    assert!(matches!(
        table.admit(&0, old_generation, 5, LIVE),
        Admission::OwnershipLost(_)
    ));

    assert!(!table.is_revoked_pending(&0));
    assert_eq!(table.state(&0), Some(State::ResumePending));
}

#[test]
fn unestablished_reconciliation_withholds_later_generations() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    sending(&mut table, 0, 5);
    table.begin_reconcile(&0, 5);

    let _ = table.revoke();
    let (_, withheld) = table.assign([0]);
    assert_eq!(withheld, vec![0]);
}

#[test]
fn drained_requests_are_returned_for_failure_replies() {
    let mut table = assigned(&[0]);
    advancing(&mut table, 0, 5, LIVE);
    assert_eq!(table.drain_requests(), vec![LIVE]);
    assert!(table.take_batch(live).items.is_empty());
}

// ---------------------------------------------------------------------------------------------
// Error mapping.

const fn failure(code: RDKafkaErrorCode, fatal: bool, abortable: bool) -> TxnFailure {
    TxnFailure {
        code,
        fatal,
        abortable,
    }
}

#[test]
fn begin_failure_is_reconciled_unless_unauthorized() {
    assert_eq!(
        transaction(Stage::Begin, failure(RDKafkaErrorCode::State, false, false)),
        Disposition::Conclude(Verdict::Reconcile)
    );

    assert_eq!(
        transaction(
            Stage::Begin,
            failure(
                RDKafkaErrorCode::TransactionalIdAuthorizationFailed,
                true,
                false
            )
        ),
        Disposition::Conclude(Verdict::Permanent)
    );
}

#[test]
fn generation_fence_on_send_offsets_is_ownership_lost_after_a_clean_abort() {
    for code in [
        RDKafkaErrorCode::IllegalGeneration,
        RDKafkaErrorCode::UnknownMemberId,
        RDKafkaErrorCode::FencedInstanceId,
    ] {
        let disposition = transaction(Stage::SendOffsets, failure(code, false, true));

        assert_eq!(
            disposition,
            Disposition::Abort {
                then: Verdict::OwnershipLost,
                on_abort_failure: Verdict::Reconcile,
            }
        );

        assert_eq!(after_abort(disposition, true), Verdict::OwnershipLost);
        assert_eq!(after_abort(disposition, false), Verdict::Reconcile);
    }
}

#[test]
fn a_fatal_generation_code_is_not_a_fence_proof() {
    assert_eq!(
        transaction(
            Stage::SendOffsets,
            failure(RDKafkaErrorCode::IllegalGeneration, true, false)
        ),
        Disposition::Conclude(Verdict::Reconcile)
    );

    assert_eq!(
        transaction(
            Stage::Commit,
            failure(RDKafkaErrorCode::IllegalGeneration, false, true)
        ),
        Disposition::Abort {
            then: Verdict::Reconcile,
            on_abort_failure: Verdict::Reconcile,
        }
    );
}

#[test]
fn other_abortable_errors_reconcile_whether_or_not_the_abort_succeeds() {
    for stage in [Stage::SendOffsets, Stage::Commit] {
        let disposition = transaction(
            stage,
            failure(RDKafkaErrorCode::NotCoordinator, false, true),
        );

        assert_eq!(after_abort(disposition, true), Verdict::Reconcile);
        assert_eq!(after_abort(disposition, false), Verdict::Reconcile);
    }
}

#[test]
fn timeouts_and_producer_fences_are_indeterminate() {
    for (stage, code, fatal) in [
        (
            Stage::SendOffsets,
            RDKafkaErrorCode::OperationTimedOut,
            false,
        ),
        (Stage::Commit, RDKafkaErrorCode::OperationTimedOut, false),
        (Stage::Commit, RDKafkaErrorCode::RequestTimedOut, false),
        (Stage::SendOffsets, RDKafkaErrorCode::Fenced, true),
        (Stage::Commit, RDKafkaErrorCode::Fenced, true),
        (Stage::Commit, RDKafkaErrorCode::ProducerFenced, true),
    ] {
        assert_eq!(
            transaction(stage, failure(code, fatal, false)),
            Disposition::Conclude(Verdict::Reconcile)
        );
    }
}

#[test]
fn authorization_errors_are_permanent() {
    for code in [
        RDKafkaErrorCode::TransactionalIdAuthorizationFailed,
        RDKafkaErrorCode::GroupAuthorizationFailed,
        RDKafkaErrorCode::TopicAuthorizationFailed,
        RDKafkaErrorCode::ClusterAuthorizationFailed,
    ] {
        let abortable = transaction(Stage::SendOffsets, failure(code, false, true));
        assert_eq!(after_abort(abortable, true), Verdict::Permanent);
        assert_eq!(after_abort(abortable, false), Verdict::Permanent);

        assert_eq!(
            transaction(Stage::Commit, failure(code, true, false)),
            Disposition::Conclude(Verdict::Permanent)
        );
    }
}

#[test]
fn consumer_errors_map_fences_authorization_and_fatal_failures() {
    assert_eq!(
        consumer(RDKafkaErrorCode::FencedInstanceId, true),
        ConsumerVerdict::InstanceFenced
    );

    assert_eq!(
        consumer(RDKafkaErrorCode::GroupAuthorizationFailed, false),
        ConsumerVerdict::Authorization
    );

    assert_eq!(
        consumer(RDKafkaErrorCode::Fatal, false),
        ConsumerVerdict::Fatal
    );

    assert_eq!(
        consumer(RDKafkaErrorCode::Fail, true),
        ConsumerVerdict::Fatal
    );

    assert_eq!(
        consumer(RDKafkaErrorCode::BrokerTransportFailure, false),
        ConsumerVerdict::Continue
    );
}

#[test]
fn open_errors_are_transient_only_when_retrying_can_help() {
    assert!(open_is_transient(
        RDKafkaErrorCode::OperationTimedOut,
        false
    ));

    assert!(open_is_transient(
        RDKafkaErrorCode::CoordinatorNotAvailable,
        false
    ));

    assert!(!open_is_transient(
        RDKafkaErrorCode::OperationTimedOut,
        true
    ));

    assert!(!open_is_transient(RDKafkaErrorCode::Fenced, false));

    assert!(!open_is_transient(
        RDKafkaErrorCode::TransactionalIdAuthorizationFailed,
        false
    ));

    assert!(!open_is_transient(
        RDKafkaErrorCode::InvalidTransactionTimeout,
        false
    ));
}

// ---------------------------------------------------------------------------------------------
// Settings, redaction, and identity.

#[test]
fn consumer_settings_require_non_empty_identity_and_topics() {
    let error = |result: Result<KafkaConsumerSettings, sisa_messaging_kafka::KafkaClientError>| {
        result
            .err()
            .map(sisa_messaging_kafka::KafkaClientError::kind)
    };

    assert_eq!(
        error(KafkaConsumerSettings::new(" ", "member", ["orders"])),
        Some(KafkaClientErrorKind::EmptyGroupId)
    );

    assert_eq!(
        error(KafkaConsumerSettings::new("group", "", ["orders"])),
        Some(KafkaClientErrorKind::EmptyGroupInstanceId)
    );

    assert_eq!(
        error(KafkaConsumerSettings::new(
            "group",
            "member",
            Vec::<String>::new()
        )),
        Some(KafkaClientErrorKind::EmptyTopic)
    );

    assert_eq!(
        error(KafkaConsumerSettings::new(
            "group",
            "member",
            ["orders", " "]
        )),
        Some(KafkaClientErrorKind::EmptyTopic)
    );

    assert!(KafkaConsumerSettings::new("group", "member", ["orders"]).is_ok());
}

#[test]
fn consumer_settings_reject_zero_timeouts() {
    let kind = |result: Result<KafkaConsumerSettings, sisa_messaging_kafka::KafkaClientError>| {
        result
            .err()
            .map(sisa_messaging_kafka::KafkaClientError::kind)
    };

    assert_eq!(
        kind(settings().with_operation_timeout(std::time::Duration::ZERO)),
        Some(KafkaClientErrorKind::ZeroTimeout)
    );

    assert_eq!(
        kind(settings().with_shutdown_timeout(std::time::Duration::ZERO)),
        Some(KafkaClientErrorKind::ZeroTimeout)
    );

    assert!(
        settings()
            .with_operation_timeout(std::time::Duration::from_millis(1))
            .and_then(|settings| settings.with_shutdown_timeout(std::time::Duration::from_millis(1)))
            .is_ok()
    );
}

fn settings() -> KafkaConsumerSettings {
    KafkaConsumerSettings::new("orders-group", "member-7", ["orders-topic"])
        .unwrap_or_else(|_| panic!("settings are valid"))
}

#[test]
fn transactional_identity_is_derived_from_group_and_instance() {
    assert_eq!(settings().transactional_id(), "sisa.orders-group.member-7");
}

#[test]
fn consumer_settings_and_sources_redact_identity_and_brokers() {
    let rendered = format!("{:?}", settings());
    assert!(!rendered.contains("orders-group"));
    assert!(!rendered.contains("member-7"));
    assert!(!rendered.contains("orders-topic"));

    let client = client(&[("sasl.password", "secret-sentinel")]);
    let rendered = format!("{client:?}");
    assert!(!rendered.contains("broker-sentinel"));
    assert!(!rendered.contains("secret-sentinel"));

    let source = client
        .delivery_source(settings())
        .unwrap_or_else(|_| panic!("source configuration is valid"));

    let rendered = format!("{source:?}");
    assert!(!rendered.contains("broker-sentinel"));
    assert!(!rendered.contains("secret-sentinel"));
    assert!(!rendered.contains("orders-group"));
}

fn client(properties: &[(&str, &str)]) -> KafkaClient {
    let config = properties.iter().fold(
        KafkaClientSettings::new(["broker-sentinel:1"]),
        |config, (name, value)| {
            config
                .with_advanced_property(*name, *value)
                .unwrap_or_else(|_| panic!("test property is allowed"))
        },
    );

    KafkaClient::start(config).unwrap_or_else(|_| panic!("local producer configuration is valid"))
}

#[test]
fn identity_and_conflicting_fencing_properties_cannot_be_overridden() {
    for (name, value) in [
        ("group.id", "other"),
        ("group.instance.id", "other"),
        ("transactional.id", "other"),
        ("isolation.level", "read_uncommitted"),
        ("partition.assignment.strategy", "cooperative-sticky"),
        ("group.protocol", "consumer"),
        ("enable.auto.commit", "true"),
        ("auto.commit.enable", "true"),
        ("enable.auto.offset.store", "true"),
        ("allow.auto.create.topics", "true"),
        ("enable.idempotence", "false"),
    ] {
        let error = client(&[(name, value)])
            .delivery_source(settings())
            .err()
            .map(sisa_messaging_kafka::KafkaClientError::kind);

        assert_eq!(
            error,
            Some(KafkaClientErrorKind::TypedPropertyOverride),
            "{name} must be rejected"
        );
    }
}

#[test]
fn matching_fencing_properties_and_unrelated_properties_are_accepted() {
    let client = client(&[
        ("isolation.level", "read_committed"),
        ("enable.idempotence", "true"),
        ("allow.auto.create.topics", "false"),
        ("session.timeout.ms", "6000"),
    ]);

    assert!(client.delivery_source(settings()).is_ok());
}
