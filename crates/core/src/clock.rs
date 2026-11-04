//! The one place that reads the wall clock, so tests and reviews know where time comes from.

use std::sync::{Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::{ContextV7, Uuid};

// One context for the process, so ids made in the same millisecond still sort in the order they
// were made: the context counts within a millisecond.
static CONTEXT: Mutex<ContextV7> = Mutex::new(ContextV7::new());

/// The wall clock's current time.
#[expect(clippy::disallowed_methods, reason = "this is the clock module")]
pub(crate) fn now() -> SystemTime {
    SystemTime::now()
}

/// A new UUID version 7 from the current time, greater than every one this process made before.
pub(crate) fn new_v7() -> Uuid {
    // A clock before 1970 is broken; the id then carries the epoch and its counter keeps the order.
    let since_epoch = now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let context = CONTEXT.lock().unwrap_or_else(PoisonError::into_inner);
    Uuid::new_v7(uuid::Timestamp::from_unix(
        &*context,
        since_epoch.as_secs(),
        since_epoch.subsec_nanos(),
    ))
}
