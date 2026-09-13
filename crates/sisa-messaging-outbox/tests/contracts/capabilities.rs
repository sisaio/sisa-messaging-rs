use std::future::Future;
use std::num::NonZeroU32;
use std::time::Duration;

use sisa_messaging_outbox::{
    ClaimRequest, DeadLetterBatch, DispatcherSettings, OutboxDeadLetters, OutboxDispatcher,
    OutboxId, OutboxStore,
};
use uuid::Uuid;

use super::support::{CompileCapabilities, CompilePublisher};

fn assert_send<T: Send>(_: T) {}

fn assert_send_future<T: Future + Send>(_: T) {}

#[test]
fn capability_futures_and_dispatcher_are_send_static_dispatch() {
    let capabilities = CompileCapabilities;
    let request = ClaimRequest {
        worker_id: "compile-worker".to_owned(),
        limit: NonZeroU32::MIN,
        lease: Duration::from_secs(30),
    };
    assert_send_future(capabilities.claim(request));
    assert_send_future(capabilities.complete(&[]));
    assert_send_future(capabilities.fail(&[]));
    assert_send_future(capabilities.release(&[]));
    assert_send_future(capabilities.extend_lease(&[], Duration::from_secs(30)));
    let dead_ids = [OutboxId::from_uuid(Uuid::from_u128(3))];
    let dead_batch = DeadLetterBatch::new(&dead_ids)
        .unwrap_or_else(|error| panic!("valid dead-letter batch rejected: {error}"));
    assert_send_future(capabilities.retry(dead_batch));
    assert_send_future(capabilities.delete(dead_batch));

    let dispatcher = OutboxDispatcher::new(
        capabilities,
        CompilePublisher,
        DispatcherSettings::default(),
    )
    .unwrap_or_else(|error| panic!("valid settings rejected: {error}"));
    assert_send(dispatcher);
}
