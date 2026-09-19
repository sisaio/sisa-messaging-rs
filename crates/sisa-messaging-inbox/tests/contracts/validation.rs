use std::num::NonZeroU32;

use sisa_messaging::{ErrorClassifier, FailureKind};
use sisa_messaging_inbox::{
    InboxScope, InboxScopeError, InboxSettings, InboxSettingsError, MAX_INBOX_SCOPE_BYTES,
};

#[test]
fn inbox_scope_rejects_empty_excess_and_control_bytes_without_changing_safe_text() {
    let valid = InboxScope::new("orders-projection")
        .unwrap_or_else(|error| panic!("valid scope rejected: {error}"));
    assert_eq!(valid.as_str(), "orders-projection");
    assert_eq!(valid.into_string(), "orders-projection");

    let too_long = "x".repeat(MAX_INBOX_SCOPE_BYTES + 1);
    for (input, expected) in [
        (String::new(), InboxScopeError::Empty),
        (too_long, InboxScopeError::TooLong),
        ("bad\nscope".to_owned(), InboxScopeError::InvalidCharacter),
        (
            "bad\u{7f}scope".to_owned(),
            InboxScopeError::InvalidCharacter,
        ),
    ] {
        assert_eq!(InboxScope::new(input), Err(expected));
        assert_eq!(expected.classify(), FailureKind::Permanent);
    }
}

#[test]
fn inbox_settings_reject_database_unrepresentable_attempt_limits() {
    let maximum = NonZeroU32::new(i32::MAX as u32).unwrap_or(NonZeroU32::MIN);
    assert_eq!(
        InboxSettings::new(maximum).map(|settings| settings.max_attempts()),
        Ok(maximum)
    );

    let too_large = NonZeroU32::new(i32::MAX as u32 + 1).unwrap_or(NonZeroU32::MIN);
    assert_eq!(
        InboxSettings::new(too_large),
        Err(InboxSettingsError::MaxAttemptsNotRepresentable)
    );
    assert_eq!(
        InboxSettingsError::MaxAttemptsNotRepresentable.classify(),
        FailureKind::Permanent
    );
    assert_eq!(InboxSettings::default().max_attempts().get(), 10);
}
