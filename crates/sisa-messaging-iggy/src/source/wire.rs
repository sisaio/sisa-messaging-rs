//! Projection of polled SDK messages into the provider's owned wire record.

use iggy::prelude::IggyMessage;

use crate::{IggyHeader, IggyRecord};

/// Converts one polled message into the record the envelope mapper decodes.
///
/// Iggy does not return the messages-key on reads, so the record never carries a key.
///
/// A header name that is not UTF-8, or header bytes the SDK cannot parse (the SDK then reports
/// no headers), is never dropped or repaired here. The record instead carries one header with
/// an empty name, which the mapper rejects as
/// [`IggyMappingError::InvalidHeader`](crate::IggyMappingError::InvalidHeader), so the record
/// resolves through the consumer's malformed path rather than failing the source or decoding
/// with a misleading missing-header error.
pub(super) fn record(message: IggyMessage) -> IggyRecord {
    let parsed = message.user_headers_map();

    let headers = match parsed {
        Ok(Some(map)) => map
            .into_iter()
            .map(
                |(name, value)| match String::from_utf8(name.as_bytes().to_vec()) {
                    Ok(name) => IggyHeader {
                        name,
                        value: value.as_bytes().to_vec(),
                    },
                    Err(_) => unreadable(),
                },
            )
            .collect(),
        // The message carries no header bytes at all: the mapper reports the missing framework
        // headers.
        Ok(None) if message.user_headers.is_none() => Vec::new(),
        // Header bytes are present but the SDK could not parse them.
        Ok(None) | Err(_) => vec![unreadable()],
    };

    IggyRecord {
        id: message.header.id,
        payload: Vec::from(message.payload),
        headers,
        key: None,
    }
}

/// A header the mapper always rejects: an empty name is invalid in every mapper direction.
fn unreadable() -> IggyHeader {
    IggyHeader {
        name: String::new(),
        value: vec![0],
    }
}
