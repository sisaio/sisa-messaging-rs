//! Message contracts and validated transport-independent strings.

use std::fmt;
use std::str::FromStr;

const MAX_WIRE_IDENTIFIER_BYTES: usize = 255;
const MAX_ORDERING_KEY_BYTES: usize = 512;
const MAX_METADATA_VALUE_BYTES: usize = 1_024;

/// A validation failure for a bounded messaging string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ValidationError {
    /// The value was empty.
    Empty,

    /// The UTF-8 representation exceeded the type's byte bound.
    TooLong {
        /// Maximum accepted UTF-8 byte length.
        max_bytes: usize,
    },

    /// The value contained an ASCII control byte or DEL.
    InvalidCharacter,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("value must not be empty"),
            Self::TooLong { max_bytes } => {
                write!(formatter, "value exceeds the {max_bytes}-byte limit")
            }
            Self::InvalidCharacter => {
                formatter.write_str("value contains a forbidden control character")
            }
        }
    }
}

impl std::error::Error for ValidationError {}

fn validate(value: &str, max_bytes: usize) -> Result<(), ValidationError> {
    if value.is_empty() {
        return Err(ValidationError::Empty);
    }

    if value.len() > max_bytes {
        return Err(ValidationError::TooLong { max_bytes });
    }

    if value
        .as_bytes()
        .iter()
        .any(|byte| byte.is_ascii_control() || *byte == 0x7f)
    {
        return Err(ValidationError::InvalidCharacter);
    }

    Ok(())
}

macro_rules! validated_string {
    ($name:ident, $description:literal, $max:expr) => {
        #[doc = $description]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Validates and owns a string.
            pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
                let value = value.into();

                validate(&value, $max)?;

                Ok(Self(value))
            }

            /// Borrows the validated string.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Returns the owned validated string.
            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = ValidationError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        #[cfg(feature = "serde")]
        impl serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = <String as serde::Deserialize>::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

validated_string!(
    MessageType,
    "A stable message contract name suitable for persistence and wire projection.",
    MAX_WIRE_IDENTIFIER_BYTES
);
validated_string!(
    ContentType,
    "A serializer content type suitable for persistence and wire projection.",
    MAX_WIRE_IDENTIFIER_BYTES
);
validated_string!(
    OrderingKey,
    "A business key whose equal values request serialized publication.",
    MAX_ORDERING_KEY_BYTES
);
validated_string!(
    MetadataValue,
    "A bounded transport-independent metadata string.",
    MAX_METADATA_VALUE_BYTES
);

/// A typed application message with a stable contract identity.
pub trait Message: Send + Sync + 'static {
    /// Stable message contract name.
    const TYPE: &'static str;

    /// Stable non-negative contract version.
    const VERSION: u32;

    /// Resolves the optional ordering key when an envelope is constructed.
    fn order_by(&self) -> Option<OrderingKey> {
        None
    }
}
