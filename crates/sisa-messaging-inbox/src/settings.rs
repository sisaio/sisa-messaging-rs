//! Inbox failure-recording settings.

use std::num::NonZeroU32;

use crate::InboxSettingsError;

/// Portable bound on recorded handler failures before a transient failure becomes terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InboxSettings {
    max_attempts: NonZeroU32,
}

impl InboxSettings {
    /// Validates an attempt limit representable by the supported provider contract.
    pub fn new(max_attempts: NonZeroU32) -> Result<Self, InboxSettingsError> {
        if max_attempts.get() > i32::MAX as u32 {
            return Err(InboxSettingsError::MaxAttemptsNotRepresentable);
        }

        Ok(Self { max_attempts })
    }

    /// Returns the validated maximum recorded failures allowed for one receipt.
    #[must_use]
    pub const fn max_attempts(&self) -> NonZeroU32 {
        self.max_attempts
    }
}

impl Default for InboxSettings {
    fn default() -> Self {
        Self {
            max_attempts: NonZeroU32::new(10).unwrap_or(NonZeroU32::MIN),
        }
    }
}
