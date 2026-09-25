//! Inbox identities and the record presented to a provider.

use std::fmt;
use std::str::FromStr;

use sisa_messaging::{MessageId, MessageType, Metadata};
use uuid::Uuid;

use crate::InboxScopeError;

/// Maximum accepted UTF-8 byte length of an inbox scope.
pub const MAX_INBOX_SCOPE_BYTES: usize = 128;

/// A validated consumer namespace used with a message identity for deduplication.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct InboxScope(String);

impl InboxScope {
    /// Validates and owns an application-selected scope.
    ///
    /// Accepts non-empty UTF-8 text of at most 128 encoded bytes. Rejects empty or excessive text,
    /// ASCII control bytes, and DEL so the value is safe for the persisted scope boundary.
    pub fn new(value: impl Into<String>) -> Result<Self, InboxScopeError> {
        let value = value.into();

        if value.is_empty() {
            return Err(InboxScopeError::Empty);
        }

        if value.len() > MAX_INBOX_SCOPE_BYTES {
            return Err(InboxScopeError::TooLong);
        }

        if value
            .as_bytes()
            .iter()
            .any(|byte| byte.is_ascii_control() || *byte == 0x7f)
        {
            return Err(InboxScopeError::InvalidCharacter);
        }

        Ok(Self(value))
    }

    /// Borrows the validated scope text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the validated scope text.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for InboxScope {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for InboxScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for InboxScope {
    type Err = InboxScopeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for InboxScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;

        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// A persistence-provider-minted durable inbox receipt identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct InboxId(Uuid);

impl InboxId {
    /// Reconstructs an identity minted by the persistence provider.
    #[must_use]
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Borrows the underlying UUID.
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Returns the underlying UUID.
    #[must_use]
    pub const fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for InboxId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for InboxId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Identity and diagnostics persisted when a delivery first reaches the inbox.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboxRecord {
    /// Deliberately selected consumer namespace for this deduplication domain.
    pub scope: InboxScope,

    /// Stable logical identity of the delivered message.
    pub message_id: MessageId,

    /// Stable message contract identity.
    pub message_type: MessageType,

    /// Stable message contract version.
    pub version: u32,

    /// Additive metadata retained for diagnostics and trace continuity.
    pub metadata: Metadata,
}
