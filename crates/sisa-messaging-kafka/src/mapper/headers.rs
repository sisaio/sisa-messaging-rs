use std::collections::BTreeMap;
use std::str::FromStr;

use sisa_messaging::{
    FrameworkHeader, HeaderName, MAX_CUSTOM_HEADER_BYTES, MAX_CUSTOM_HEADER_COUNT,
};

use super::{KafkaHeader, KafkaMappingError};

pub(super) fn push_header(
    headers: &mut Vec<KafkaHeader>,
    name: FrameworkHeader,
    value: Option<impl AsRef<str>>,
) {
    if let Some(value) = value {
        headers.push(KafkaHeader {
            name: name.name().to_owned(),
            value: Some(value.as_ref().as_bytes().to_vec()),
        });
    }
}

pub(super) fn normalize_headers(
    headers: Vec<KafkaHeader>,
) -> Result<BTreeMap<String, Vec<u8>>, KafkaMappingError> {
    if headers.len() > FrameworkHeader::ALL.len() + MAX_CUSTOM_HEADER_COUNT {
        return Err(KafkaMappingError::HeaderBoundsExceeded);
    }

    let mut total_custom_bytes = 0_usize;
    let mut normalized = BTreeMap::new();

    for header in headers {
        let value = header.value.ok_or(KafkaMappingError::InvalidHeader)?;
        let lowercase_name = header.name.to_ascii_lowercase();

        let is_framework = FrameworkHeader::ALL
            .iter()
            .any(|known| known.name() == lowercase_name);

        let name = if is_framework {
            if header.name != lowercase_name {
                return Err(KafkaMappingError::InvalidHeader);
            }

            lowercase_name
        } else {
            let validated =
                HeaderName::new(header.name).map_err(|_| KafkaMappingError::InvalidHeader)?;

            total_custom_bytes = total_custom_bytes
                .checked_add(validated.as_str().len())
                .and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or(KafkaMappingError::HeaderBoundsExceeded)?;

            if total_custom_bytes > MAX_CUSTOM_HEADER_BYTES {
                return Err(KafkaMappingError::HeaderBoundsExceeded);
            }

            validated.into_string()
        };

        if normalized.insert(name, value).is_some() {
            return Err(KafkaMappingError::DuplicateHeader);
        }
    }

    Ok(normalized)
}

pub(super) fn take_optional(
    values: &mut BTreeMap<String, Vec<u8>>,
    name: FrameworkHeader,
) -> Result<Option<String>, KafkaMappingError> {
    values
        .remove(name.name())
        .map(String::from_utf8)
        .transpose()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)
}

pub(super) fn take_required(
    values: &mut BTreeMap<String, Vec<u8>>,
    name: FrameworkHeader,
) -> Result<String, KafkaMappingError> {
    take_optional(values, name)?.ok_or(KafkaMappingError::MissingRequiredHeader)
}

pub(super) fn parse_required<T: FromStr>(
    values: &mut BTreeMap<String, Vec<u8>>,
    name: FrameworkHeader,
) -> Result<T, KafkaMappingError> {
    take_required(values, name)?
        .parse()
        .map_err(|_| KafkaMappingError::InvalidFrameworkValue)
}
