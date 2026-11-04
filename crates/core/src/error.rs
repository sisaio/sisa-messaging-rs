//! What can be wrong with a message type or an envelope.

/// A message type, an envelope or one of its CloudEvents attributes that isn't valid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EnvelopeError {
    /// The message type breaks the rules in [`MessageType`](crate::MessageType).
    #[error("invalid message type {value:?}: {reason}")]
    InvalidMessageType {
        /// The message type as given.
        value: String,
        /// Which rule it breaks.
        reason: &'static str,
    },
    /// A CloudEvents attribute has a value the envelope can't hold, or an extension has an
    /// invalid name.
    #[error("invalid CloudEvents attribute `{name}`: {reason}")]
    InvalidAttribute {
        /// The attribute's name.
        name: String,
        /// What is wrong with it.
        reason: &'static str,
    },
    /// A required CloudEvents attribute is missing.
    #[error("missing CloudEvents attribute `{0}`")]
    MissingAttribute(&'static str),
}

impl EnvelopeError {
    pub(crate) fn invalid(name: impl Into<String>, reason: &'static str) -> Self {
        Self::InvalidAttribute {
            name: name.into(),
            reason,
        }
    }
}
