use std::future::Future;
use std::task::{Context, Poll, Waker};

use sisa_messaging::{ErrorSummary, MessageId, MessageType, Metadata};
use sisa_messaging_inbox::{
    DeadLetterBatch, InboxClaimOutcome, InboxDeadLetters, InboxFailure, InboxReceipt, InboxRecord,
    InboxScope, InboxStore, InboxUnitOfWork,
};
use uuid::Uuid;

use super::support::CompileCapabilities;

fn assert_send<T: Send>(_: T) {}

fn assert_send_future<T: Future + Send>(_: T) {}

fn run_ready<T>(future: impl Future<Output = T>) -> T {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);

    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("compile capability future unexpectedly pending"),
    }
}

#[test]
fn native_async_capabilities_are_send_and_use_static_dispatch() {
    let capabilities = CompileCapabilities;

    let record = InboxRecord {
        scope: InboxScope::new("compile").unwrap_or_else(|error| panic!("scope rejected: {error}")),
        message_id: MessageId::from_uuid(Uuid::from_u128(1)),
        message_type: MessageType::new("test.message")
            .unwrap_or_else(|error| panic!("type rejected: {error}")),
        version: 1,
        metadata: Metadata::default(),
    };

    let failure = InboxFailure {
        kind: sisa_messaging::FailureKind::Transient,
        error: ErrorSummary::from_safe_text("safe"),
    };

    let ids = [sisa_messaging_inbox::InboxId::from_uuid(Uuid::from_u128(3))];

    let batch =
        DeadLetterBatch::new(&ids).unwrap_or_else(|error| panic!("batch rejected: {error}"));

    let mut transaction = ();

    assert_send_future(capabilities.begin());
    assert_send_future(capabilities.claim(&mut transaction, &record));
    assert_send_future(capabilities.fail(&record, failure));
    assert_send_future(capabilities.commit(()));
    assert_send_future(capabilities.rollback(()));
    assert_send_future(capabilities.retry(batch));
    assert_send_future(capabilities.delete(batch));

    let outcome = run_ready(capabilities.claim(&mut transaction, &record))
        .unwrap_or_else(|error| panic!("compile claim failed: {error}"));

    let receipt = match outcome {
        InboxClaimOutcome::Claimed(receipt) => receipt,
        _ => panic!("compile claim did not return its provider receipt"),
    };

    assert_eq!(receipt.recorded_failures(), 2);
    assert_eq!(receipt.id().into_uuid(), Uuid::from_u128(1));

    run_ready(capabilities.complete(&mut transaction, receipt))
        .unwrap_or_else(|error| panic!("compile completion failed: {error}"));

    assert_send(capabilities);
}
