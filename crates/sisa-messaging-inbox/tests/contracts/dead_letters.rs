use sisa_messaging::{ErrorClassifier, FailureKind};
use sisa_messaging_inbox::{
    DeadLetterBatch, DeadLetterBatchError, InboxId, MAX_DEAD_LETTER_BATCH_SIZE,
};
use uuid::Uuid;

#[test]
fn dead_letter_batches_are_borrowed_bounded_and_allow_duplicate_identities() {
    let id = InboxId::from_uuid(Uuid::from_u128(1));
    let ids = [id, id];

    let batch =
        DeadLetterBatch::new(&ids).unwrap_or_else(|error| panic!("batch rejected: {error}"));

    assert_eq!(batch.ids(), &ids);
    assert!(std::ptr::eq(batch.ids(), ids.as_slice()));

    let exact_max = vec![id; MAX_DEAD_LETTER_BATCH_SIZE];
    assert!(DeadLetterBatch::new(&exact_max).is_ok());
    let too_large = vec![id; MAX_DEAD_LETTER_BATCH_SIZE + 1];
    assert_eq!(DeadLetterBatch::new(&[]), Err(DeadLetterBatchError::Empty));

    assert_eq!(
        DeadLetterBatch::new(&too_large),
        Err(DeadLetterBatchError::TooLarge)
    );

    assert_eq!(
        DeadLetterBatchError::Empty.classify(),
        FailureKind::Permanent
    );

    assert_eq!(
        DeadLetterBatchError::TooLarge.classify(),
        FailureKind::Permanent
    );
}
