//! Codecs: a message to bytes and back.

use bytes::Bytes;
use serde_json::error::Category;

use crate::{ContentType, Message};

// The media type without its parameters, such as `application/json` from
// `application/json; charset=utf-8`. Media types compare without regard to case.
fn essence(content_type: &str) -> &str {
    content_type.split(';').next().unwrap_or_default().trim()
}

/// Why a message couldn't be encoded or decoded.
///
/// A decode error says where the data stopped making sense, never what the data says, since a
/// message can hold private data that must not reach logs.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CodecError {
    /// The message couldn't be turned into bytes.
    #[error("could not encode {message_type} as {content_type}: {reason}")]
    Encode {
        /// The message's [`Message::TYPE`].
        message_type: &'static str,
        /// The codec's content type.
        content_type: &'static str,
        /// What went wrong.
        reason: String,
    },
    /// The data isn't a valid message of this type.
    #[error("could not decode {message_type} from {content_type}: {reason}")]
    Decode {
        /// The message's [`Message::TYPE`].
        message_type: &'static str,
        /// The codec's content type.
        content_type: &'static str,
        /// Where and how the data is wrong, without quoting it.
        reason: String,
    },
}

/// Turns messages into bytes and back, and names the content type of those bytes.
///
/// A bus encodes what it publishes with one codec. A receiver picks the codec whose
/// [`can_decode`](Codec::can_decode) accepts the envelope's content type, so a service can move
/// to another codec while old messages drain.
pub trait Codec: Send + Sync + 'static {
    /// The media type of the bytes, such as `application/json`: the envelope's content type.
    fn content_type(&self) -> &'static str;

    /// Encodes a message.
    ///
    /// # Errors
    ///
    /// [`CodecError::Encode`] when the message can't be written in this format.
    fn encode<Msg: Message>(&self, message: &Msg) -> Result<Bytes, CodecError>;

    /// Decodes a message.
    ///
    /// # Errors
    ///
    /// [`CodecError::Decode`] when the data isn't a valid `Msg` in this format.
    fn decode<Msg: Message>(&self, data: &[u8]) -> Result<Msg, CodecError>;

    /// Whether this codec reads data of this content type. By default, when its media type is
    /// this codec's, whatever the case and parameters, so `application/json; charset=utf-8`
    /// matches `application/json`.
    fn can_decode(&self, content_type: &ContentType) -> bool {
        essence(content_type.as_str()).eq_ignore_ascii_case(self.content_type())
    }
}

/// JSON through serde, as `application/json`: the codec a bus uses unless told otherwise.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use sisa_eventing_core::{Codec, JsonCodec, Message};
///
/// #[derive(Debug, PartialEq, Serialize, Deserialize)]
/// struct OrderPlaced {
///     id: u64,
/// }
///
/// impl Message for OrderPlaced {
///     const TYPE: &'static str = "orders.order_placed.v1";
/// }
///
/// let data = JsonCodec.encode(&OrderPlaced { id: 42 })?;
/// assert_eq!(&data[..], br#"{"id":42}"#);
/// assert_eq!(JsonCodec.decode::<OrderPlaced>(&data)?, OrderPlaced { id: 42 });
/// # Ok::<(), sisa_eventing_core::CodecError>(())
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JsonCodec;

impl JsonCodec {
    const CONTENT_TYPE: &'static str = "application/json";
}

// Encodes with serde_json, and reports a decode error by its place in the data only, since
// serde_json's own message can quote the data.
impl Codec for JsonCodec {
    fn content_type(&self) -> &'static str {
        Self::CONTENT_TYPE
    }

    fn encode<Msg: Message>(&self, message: &Msg) -> Result<Bytes, CodecError> {
        serde_json::to_vec(message)
            .map(Bytes::from)
            .map_err(|error| CodecError::Encode {
                message_type: Msg::TYPE,
                content_type: Self::CONTENT_TYPE,
                reason: error.to_string(),
            })
    }

    fn decode<Msg: Message>(&self, data: &[u8]) -> Result<Msg, CodecError> {
        serde_json::from_slice(data).map_err(|error| {
            let what = match error.classify() {
                Category::Io | Category::Syntax => "invalid JSON",
                Category::Data => "JSON that doesn't match the message",
                Category::Eof => "JSON that ends early",
            };
            CodecError::Decode {
                message_type: Msg::TYPE,
                content_type: Self::CONTENT_TYPE,
                reason: format!("{what} at line {}, column {}", error.line(), error.column()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct OrderPlaced {
        id: u64,
        customer: String,
    }

    impl Message for OrderPlaced {
        const TYPE: &'static str = "orders.order_placed.v1";
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Prices(BTreeMap<Vec<u8>, u64>);

    impl Message for Prices {
        const TYPE: &'static str = "catalog.prices.v1";
    }

    fn decode_error(data: &[u8]) -> String {
        JsonCodec
            .decode::<OrderPlaced>(data)
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn round_trips_a_message() {
        let order = OrderPlaced {
            id: 42,
            customer: "Ada".into(),
        };

        let data = JsonCodec.encode(&order).unwrap();

        assert_eq!(&data[..], br#"{"id":42,"customer":"Ada"}"#);
        assert_eq!(JsonCodec.decode::<OrderPlaced>(&data), Ok(order));
    }

    #[test]
    fn says_where_invalid_json_fails() {
        assert_eq!(
            decode_error(b"{\"id\":42,\n\"customer\" x}"),
            "could not decode orders.order_placed.v1 from application/json: \
             invalid JSON at line 2, column 12",
        );
    }

    #[test]
    fn says_where_json_that_ends_early_fails() {
        assert_eq!(
            decode_error(br#"{"id":42"#),
            "could not decode orders.order_placed.v1 from application/json: \
             JSON that ends early at line 1, column 8",
        );
    }

    #[test]
    fn reports_data_that_doesnt_match_without_quoting_it() {
        let error = decode_error(br#"{"id":"secret-value","customer":"Ada"}"#);

        assert_eq!(
            error,
            "could not decode orders.order_placed.v1 from application/json: \
             JSON that doesn't match the message at line 1, column 20",
        );
    }

    #[test]
    fn reports_a_message_json_cannot_hold() {
        let prices = Prices(BTreeMap::from([(vec![1], 5)]));

        assert!(matches!(
            JsonCodec.encode(&prices),
            Err(CodecError::Encode {
                message_type: "catalog.prices.v1",
                ..
            })
        ));
    }

    #[test]
    fn decodes_its_media_type_whatever_the_case_and_parameters() {
        let accepts =
            |content_type: &str| JsonCodec.can_decode(&ContentType::new(content_type).unwrap());

        assert!(accepts("application/json"));
        assert!(accepts("Application/JSON; charset=utf-8"));
        assert!(!accepts("application/cloudevents+json"));
        assert!(!accepts("application/msgpack"));
    }
}
