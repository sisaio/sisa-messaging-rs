use std::collections::BTreeMap;
use std::str::FromStr;

use sisa_messaging::{
    FrameworkHeader, HeaderName, MAX_CUSTOM_HEADER_BYTES, MAX_CUSTOM_HEADER_COUNT,
};

use super::{IggyHeader, IggyMappingError};

/// Iggy's `HeaderField` accepts 1 to 255 bytes for every header name and value; there is no
/// separate constant exported for this bound.
pub(super) const MAX_HEADER_FIELD_BYTES: usize = 255;

pub(super) fn push_header(
    headers: &mut Vec<IggyHeader>,
    name: FrameworkHeader,
    value: Option<impl AsRef<str>>,
) -> Result<(), IggyMappingError> {
    let Some(value) = value else {
        return Ok(());
    };

    let bytes = value.as_ref().as_bytes();

    if bytes.is_empty() || bytes.len() > MAX_HEADER_FIELD_BYTES {
        return Err(IggyMappingError::InvalidFrameworkValue);
    }

    headers.push(IggyHeader {
        name: name.name().to_owned(),
        value: bytes.to_vec(),
    });

    Ok(())
}

/// Pushes a framework header when present and within the Iggy header-value bound, silently
/// omitting it otherwise instead of failing the whole envelope.
///
/// This is reserved for headers the W3C Trace Context specification itself allows a participant
/// to drop, such as `tracestate` (a vendor's own state may legitimately exceed a transport's
/// per-header size limit). Every other framework header uses [`push_header`], which rejects an
/// oversized value instead of dropping it.
pub(super) fn push_optional_header(
    headers: &mut Vec<IggyHeader>,
    name: FrameworkHeader,
    value: Option<impl AsRef<str>>,
) {
    let Some(value) = value else {
        return;
    };

    let bytes = value.as_ref().as_bytes();

    if bytes.is_empty() || bytes.len() > MAX_HEADER_FIELD_BYTES {
        return;
    }

    headers.push(IggyHeader {
        name: name.name().to_owned(),
        value: bytes.to_vec(),
    });
}

pub(super) fn normalize_headers(
    headers: Vec<IggyHeader>,
) -> Result<BTreeMap<String, Vec<u8>>, IggyMappingError> {
    if headers.len() > FrameworkHeader::ALL.len() + MAX_CUSTOM_HEADER_COUNT {
        return Err(IggyMappingError::HeaderBoundsExceeded);
    }

    let mut total_custom_bytes = 0_usize;
    let mut normalized = BTreeMap::new();

    for header in headers {
        if header.value.is_empty() {
            return Err(IggyMappingError::InvalidHeader);
        }

        let lowercase_name = header.name.to_ascii_lowercase();

        let is_framework = FrameworkHeader::ALL
            .iter()
            .any(|known| known.name() == lowercase_name);

        let name = if is_framework {
            if header.name != lowercase_name {
                return Err(IggyMappingError::InvalidHeader);
            }

            // A framework value's shape is fully within this mapper's control and is bounded to
            // Iggy's own 255-byte header limit on encode; re-checking here keeps the two
            // directions symmetric and reports a wire-shaped defect distinctly from a malformed
            // custom header.
            if header.value.len() > MAX_HEADER_FIELD_BYTES {
                return Err(IggyMappingError::InvalidFrameworkValue);
            }

            lowercase_name
        } else {
            // A custom header value is bounded only by the shared contract (validated below via
            // the aggregate byte and count limits), not by Iggy's own per-field 255-byte cap:
            // that cap is enforced on encode, but a decoded record is not assumed to have come
            // from this mapper's own encode path.
            let validated =
                HeaderName::new(header.name).map_err(|_| IggyMappingError::InvalidHeader)?;

            total_custom_bytes = total_custom_bytes
                .checked_add(validated.as_str().len())
                .and_then(|bytes| bytes.checked_add(header.value.len()))
                .ok_or(IggyMappingError::HeaderBoundsExceeded)?;

            if total_custom_bytes > MAX_CUSTOM_HEADER_BYTES {
                return Err(IggyMappingError::HeaderBoundsExceeded);
            }

            validated.into_string()
        };

        if normalized.insert(name, header.value).is_some() {
            return Err(IggyMappingError::DuplicateHeader);
        }
    }

    Ok(normalized)
}

pub(super) fn take_optional(
    values: &mut BTreeMap<String, Vec<u8>>,
    name: FrameworkHeader,
) -> Result<Option<String>, IggyMappingError> {
    values
        .remove(name.name())
        .map(String::from_utf8)
        .transpose()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)
}

pub(super) fn take_required(
    values: &mut BTreeMap<String, Vec<u8>>,
    name: FrameworkHeader,
) -> Result<String, IggyMappingError> {
    take_optional(values, name)?.ok_or(IggyMappingError::MissingRequiredHeader)
}

pub(super) fn parse_required<T: FromStr>(
    values: &mut BTreeMap<String, Vec<u8>>,
    name: FrameworkHeader,
) -> Result<T, IggyMappingError> {
    take_required(values, name)?
        .parse()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)
}
