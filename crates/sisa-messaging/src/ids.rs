//! Semantic identifiers used by envelopes and metadata.

use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

macro_rules! uuid_id {
    ($(#[$meta:meta])* $name:ident, $description:literal) => {
        $(#[$meta])*
        #[doc = $description]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
        #[cfg_attr(feature = "serde", serde(transparent))]
        pub struct $name(Uuid);

        impl $name {
            /// Mints a new UUIDv7 identifier.
            #[must_use]
            #[allow(
                clippy::new_without_default,
                reason = "identity minting must remain explicit rather than happen through Default"
            )]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Reconstructs the identifier from an existing UUID.
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

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self)
            }
        }
    };
}

uuid_id!(
    MessageId,
    "The stable identity of one logical message across retries and deliveries."
);
uuid_id!(
    ConversationId,
    "The identity shared by messages in one logical conversation."
);
uuid_id!(
    RequestId,
    "The identity of the request that initiated work."
);
