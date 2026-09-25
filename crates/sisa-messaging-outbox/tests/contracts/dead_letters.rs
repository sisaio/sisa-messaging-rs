use sisa_messaging::{ErrorClassifier, FailureKind};
use sisa_messaging_outbox::{
    DeadLetterBatch, DeadLetterBatchError, MAX_DEAD_LETTER_BATCH_SIZE, OutboxId,
};
use uuid::Uuid;

#[test]
fn dead_letter_batches_validate_bounds_without_changing_the_borrow() {
    let singleton = [OutboxId::from_uuid(Uuid::from_u128(1))];

    let singleton_batch = DeadLetterBatch::new(&singleton)
        .unwrap_or_else(|error| panic!("valid singleton rejected: {error}"));

    assert_eq!(singleton_batch.ids(), &singleton);
    assert!(std::ptr::eq(singleton_batch.ids(), singleton.as_slice()));

    let exact_max = vec![OutboxId::from_uuid(Uuid::from_u128(2)); MAX_DEAD_LETTER_BATCH_SIZE];

    let exact_max_batch = DeadLetterBatch::new(&exact_max)
        .unwrap_or_else(|error| panic!("valid maximum rejected: {error}"));

    assert_eq!(exact_max_batch.ids().len(), MAX_DEAD_LETTER_BATCH_SIZE);

    let too_large = vec![OutboxId::from_uuid(Uuid::from_u128(3)); MAX_DEAD_LETTER_BATCH_SIZE + 1];
    assert_eq!(DeadLetterBatch::new(&[]), Err(DeadLetterBatchError::Empty));

    assert_eq!(
        DeadLetterBatch::new(&too_large),
        Err(DeadLetterBatchError::TooLarge)
    );
}

#[test]
fn dead_letter_batches_permit_duplicates_and_validation_errors_are_safe() {
    let duplicate = OutboxId::from_uuid(Uuid::from_u128(4));
    let ids = [duplicate, duplicate];

    let batch = DeadLetterBatch::new(&ids)
        .unwrap_or_else(|error| panic!("duplicate identities rejected: {error}"));

    assert_eq!(batch.ids(), &ids);

    for (error, message) in [
        (
            DeadLetterBatchError::Empty,
            "dead-letter batch must not be empty",
        ),
        (
            DeadLetterBatchError::TooLarge,
            "dead-letter batch exceeds the maximum size",
        ),
    ] {
        assert_eq!(error.to_string(), message);
        assert_eq!(error.classify(), FailureKind::Permanent);
    }
}
