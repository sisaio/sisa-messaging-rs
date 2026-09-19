//! Inbox validation errors.

use sisa_messaging::{ErrorClassifier, FailureKind};

/// Invalid [`crate::InboxScope`] input.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum InboxScopeError {
    /// The scope was empty.
    #[error("inbox scope must not be empty")]
    Empty,

    /// The scope exceeded its bounded UTF-8 representation.
    #[error("inbox scope exceeds the maximum byte length")]
    TooLong,

    /// The scope contained an ASCII control character or DEL.
    #[error("inbox scope contains a forbidden control character")]
    InvalidCharacter,
}

impl ErrorClassifier for InboxScopeError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

/// Invalid [`crate::InboxSettings`] configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum InboxSettingsError {
    /// The provider's signed database attempt representation cannot hold the configured limit.
    #[error("inbox max attempts exceeds the provider-supported limit")]
    MaxAttemptsNotRepresentable,
}

impl ErrorClassifier for InboxSettingsError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}
