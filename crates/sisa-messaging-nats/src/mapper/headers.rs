use crate::error::MappingError;
use async_nats::HeaderMap;
use std::str::FromStr;

pub(super) fn push_header(headers: &mut HeaderMap, name: &str, value: Option<impl AsRef<str>>) {
    if let Some(value) = value {
        headers.insert(name, value.as_ref());
    }
}

pub(super) fn one<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, MappingError> {
    let mut first = None;

    for (wire_name, values) in headers.iter() {
        let wire_name: &str = wire_name.as_ref();

        if wire_name.eq_ignore_ascii_case(name) {
            if first.is_some() || values.len() != 1 {
                return Err(MappingError::InvalidHeaders);
            }

            first = Some(values[0].as_str());
        }
    }

    Ok(first)
}

pub(super) fn required<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, MappingError> {
    one(headers, name)?.ok_or(MappingError::InvalidEnvelope)
}

pub(super) fn parsed<T: FromStr>(
    headers: &HeaderMap,
    name: &str,
) -> Result<Option<T>, MappingError> {
    one(headers, name)?
        .map(|v| v.parse().map_err(|_| MappingError::InvalidEnvelope))
        .transpose()
}
