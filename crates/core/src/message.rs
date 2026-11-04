//! The message trait, its type and its id.

use std::fmt;
use std::str::FromStr;

use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::{EnvelopeError, clock};

// Kafka's limit on a topic name, the shortest of the destinations a message type becomes.
const MAX_MESSAGE_TYPE_LENGTH: usize = 249;

fn check_message_type(value: &str) -> Result<(), &'static str> {
    if value.len() > MAX_MESSAGE_TYPE_LENGTH {
        return Err("longer than 249 characters");
    }
    let levels: Vec<&str> = value.split('.').collect();
    if levels.len() < 2 {
        return Err("needs at least two levels, such as `order_placed.v1`");
    }
    if levels.iter().any(|level| level.is_empty()) {
        return Err("has an empty level");
    }
    let allowed = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_';
    if !value.bytes().all(|byte| allowed(byte) || byte == b'.') {
        return Err("may hold only lowercase letters, digits and `_`, in levels joined by `.`");
    }
    let version = levels.last().and_then(|level| level.strip_prefix('v'));
    if !version.is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    }) {
        return Err("must end with its version, such as `.v1`");
    }
    Ok(())
}

/// A type the bus publishes and handles.
///
/// The data goes through serde, and [`TYPE`](Message::TYPE) names the message on the wire.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use sisa_messaging_core::Message;
///
/// #[derive(Serialize, Deserialize)]
/// struct OrderPlaced {
///     id: u64,
/// }
///
/// impl Message for OrderPlaced {
///     const TYPE: &'static str = "orders.order_placed.v1";
/// }
/// ```
pub trait Message: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The CloudEvents `type`, such as `orders.order_placed.v1`, following the rules of
    /// [`MessageType`]. It is also the message's default destination, so changing it moves the
    /// message: give a new shape a new version instead.
    const TYPE: &'static str;
}

/// A checked message type: the CloudEvents `type`.
///
/// It is levels of lowercase letters, digits and `_`, joined by `.`, and its last level is the
/// version of the message's shape, `v` and digits. That makes it valid as a NATS subject, a Kafka
/// topic and an AMQP routing key, so it is the default destination. It is at most 249
/// characters, Kafka's limit.
///
/// ```
/// use sisa_messaging_core::MessageType;
///
/// assert!(MessageType::new("orders.order_placed.v1").is_ok());
/// assert!(MessageType::new("Orders.OrderPlaced").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageType(String);

impl MessageType {
    /// Checks a message type.
    ///
    /// # Errors
    ///
    /// [`EnvelopeError::InvalidMessageType`] when it breaks one of the rules above.
    pub fn new(value: impl Into<String>) -> Result<Self, EnvelopeError> {
        let value = value.into();
        match check_message_type(&value) {
            Ok(()) => Ok(Self(value)),
            Err(reason) => Err(EnvelopeError::InvalidMessageType { value, reason }),
        }
    }

    /// The message type of `Msg`, its [`Message::TYPE`].
    ///
    /// # Errors
    ///
    /// [`EnvelopeError::InvalidMessageType`] when `Msg::TYPE` breaks one of the rules above.
    pub fn of<Msg: Message>() -> Result<Self, EnvelopeError> {
        Self::new(Msg::TYPE)
    }

    /// The message type as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// Writes the message type as it goes on the wire.
impl fmt::Display for MessageType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

// Checks a message type read from text, as `new` does.
impl FromStr for MessageType {
    type Err = EnvelopeError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(text)
    }
}

/// A message's id: the CloudEvents `id`.
///
/// [`MessageId::new`] makes a UUID version 7, which starts with its millisecond, so ids sort by
/// when they were made; within a process each new id is greater than the one before. The id
/// stays the same across outbox retries and broker redeliveries, which lets the broker and the
/// inbox drop duplicates. An id read from the wire may be any UUID, since other publishers make
/// their own.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageId(Uuid);

impl MessageId {
    /// A new id, a UUID version 7 from the current time.
    #[expect(
        clippy::new_without_default,
        reason = "a default that differs on every call would surprise"
    )]
    #[must_use]
    pub fn new() -> Self {
        Self(clock::new_v7())
    }

    /// The id as a UUID.
    #[must_use]
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

// Wraps a UUID read from a database or made elsewhere.
impl From<Uuid> for MessageId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

// Unwraps the UUID, for a database column.
impl From<MessageId> for Uuid {
    fn from(id: MessageId) -> Self {
        id.0
    }
}

// Writes the hyphenated lowercase form, as it goes on the wire.
impl fmt::Display for MessageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.hyphenated().fmt(formatter)
    }
}

// Shows the id in the form logs and the wire use.
impl fmt::Debug for MessageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MessageId({self})")
    }
}

// Reads any UUID, the CloudEvents `id` of a received message.
impl FromStr for MessageId {
    type Err = EnvelopeError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(text)
            .map(Self)
            .map_err(|_| EnvelopeError::invalid("id", "not a UUID"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_levels_ending_with_a_version() {
        for value in ["order_placed.v1", "orders.order_placed.v12", "a1.b_2.c.v0"] {
            assert_eq!(
                MessageType::new(value).map(|parsed| parsed.to_string()),
                Ok(value.into())
            );
        }
    }

    #[test]
    fn rejects_a_message_type_with_the_rule_it_breaks() {
        let cases = [
            ("v1", "needs at least two levels, such as `order_placed.v1`"),
            ("orders..v1", "has an empty level"),
            (".orders.v1", "has an empty level"),
            ("orders.v1.", "has an empty level"),
            (
                "Orders.v1",
                "may hold only lowercase letters, digits and `_`, in levels joined by `.`",
            ),
            (
                "orders.*.v1",
                "may hold only lowercase letters, digits and `_`, in levels joined by `.`",
            ),
            (
                "orders-placed.v1",
                "may hold only lowercase letters, digits and `_`, in levels joined by `.`",
            ),
            (
                "orders.order_placed",
                "must end with its version, such as `.v1`",
            ),
            (
                "orders.order_placed.v",
                "must end with its version, such as `.v1`",
            ),
            (
                "orders.order_placed.v1a",
                "must end with its version, such as `.v1`",
            ),
        ];
        for (value, reason) in cases {
            assert_eq!(
                MessageType::new(value),
                Err(EnvelopeError::InvalidMessageType {
                    value: value.into(),
                    reason
                }),
            );
        }
    }

    #[test]
    fn rejects_a_message_type_longer_than_kafka_allows() {
        let longest = format!("{}.v1", "a".repeat(246));

        assert!(MessageType::new(longest.clone()).is_ok());
        assert!(matches!(
            MessageType::new(format!("a{longest}")),
            Err(EnvelopeError::InvalidMessageType {
                reason: "longer than 249 characters",
                ..
            })
        ));
    }

    #[test]
    fn takes_the_message_type_from_the_message() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct OrderPlaced;

        impl Message for OrderPlaced {
            const TYPE: &'static str = "orders.order_placed.v1";
        }

        assert_eq!(
            MessageType::of::<OrderPlaced>().unwrap().as_str(),
            "orders.order_placed.v1"
        );
    }

    #[test]
    fn makes_version_7_ids_that_increase() {
        let ids: Vec<MessageId> = (0..1_000).map(|_| MessageId::new()).collect();

        assert!(ids.iter().all(|id| id.as_uuid().get_version_num() == 7));
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn reads_any_uuid_as_an_id_and_writes_it_back() {
        let text = "0b8d6f3e-2a51-4c1e-9f4a-6c2d3e4f5a6b";

        assert_eq!(text.parse::<MessageId>().unwrap().to_string(), text);
        assert_eq!(
            "42".parse::<MessageId>(),
            Err(EnvelopeError::invalid("id", "not a UUID"))
        );
    }
}
