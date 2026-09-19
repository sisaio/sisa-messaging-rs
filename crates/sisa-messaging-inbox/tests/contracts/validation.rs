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

    let exact_maximum = "x".repeat(MAX_INBOX_SCOPE_BYTES);
    assert_eq!(
        InboxScope::new(exact_maximum).map(|scope| scope.into_string()),
        Ok("x".repeat(128))
    );

    let exact_multibyte_maximum = "é".repeat(64);
    assert_eq!(exact_multibyte_maximum.len(), MAX_INBOX_SCOPE_BYTES);
    assert!(InboxScope::new(exact_multibyte_maximum).is_ok());

    let too_long = "x".repeat(MAX_INBOX_SCOPE_BYTES + 1);
    let too_long_multibyte = "é".repeat(65);
    assert_eq!(too_long_multibyte.len(), 130);
    for (input, expected) in [
        (String::new(), InboxScopeError::Empty),
        (too_long, InboxScopeError::TooLong),
        (too_long_multibyte, InboxScopeError::TooLong),
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

#[cfg(feature = "serde")]
#[test]
fn inbox_id_and_valid_scope_round_trip_as_transparent_json() {
    let uuid = uuid::Uuid::from_u128(1);
    let id = sisa_messaging_inbox::InboxId::from_uuid(uuid);
    let scope = InboxScope::new("orders-projection")
        .unwrap_or_else(|error| panic!("valid scope rejected: {error}"));

    let encoded_id = serde_json::to_string(&id)
        .unwrap_or_else(|error| panic!("inbox id serialization failed: {error}"));
    let encoded_uuid = serde_json::to_string(&uuid)
        .unwrap_or_else(|error| panic!("UUID serialization failed: {error}"));
    assert_eq!(encoded_id, encoded_uuid);
    assert_eq!(
        serde_json::from_str::<sisa_messaging_inbox::InboxId>(&encoded_id)
            .unwrap_or_else(|error| panic!("inbox id deserialization failed: {error}")),
        id
    );

    let encoded_scope = serde_json::to_string(&scope)
        .unwrap_or_else(|error| panic!("inbox scope serialization failed: {error}"));
    assert_eq!(encoded_scope, "\"orders-projection\"");
    assert_eq!(
        serde_json::from_str::<InboxScope>(&encoded_scope)
            .unwrap_or_else(|error| panic!("inbox scope deserialization failed: {error}")),
        scope
    );
}

#[cfg(feature = "serde")]
#[test]
fn inbox_scope_deserialization_revalidates_invalid_text() {
    for invalid in [
        String::new(),
        "x".repeat(MAX_INBOX_SCOPE_BYTES + 1),
        "bad\nscope".to_owned(),
        "bad\u{7f}scope".to_owned(),
    ] {
        let encoded = serde_json::to_string(&invalid)
            .unwrap_or_else(|error| panic!("invalid scope fixture serialization failed: {error}"));
        assert!(serde_json::from_str::<InboxScope>(&encoded).is_err());
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
