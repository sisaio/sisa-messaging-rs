use std::num::NonZeroU32;
use std::time::Duration;

use sisa_messaging_inbox::{InboxPurgeReport, InboxPurgeRequest, InboxStats};

#[test]
fn maintenance_defaults_bound_terminal_retention_without_incomplete_row_controls() {
    let request = InboxPurgeRequest::default();

    assert_eq!(
        request.completed_retention,
        Some(Duration::from_secs(7 * 24 * 60 * 60))
    );

    assert_eq!(
        request.dead_retention,
        Some(Duration::from_secs(30 * 24 * 60 * 60))
    );

    assert_eq!(
        request.batch_size,
        NonZeroU32::new(500).unwrap_or(NonZeroU32::MIN)
    );

    assert_eq!(InboxPurgeReport::default().completed_deleted, 0);
    assert_eq!(InboxPurgeReport::default().dead_deleted, 0);

    assert_eq!(
        InboxStats::default(),
        InboxStats {
            pending: 0,
            retrying: 0,
            completed: 0,
            dead: 0,
        }
    );
}
