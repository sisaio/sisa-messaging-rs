use std::time::SystemTime;

use chrono::{DateTime, Utc};
use sisa_messaging::Metadata;

use crate::PostgresError;

pub(crate) fn encode(value: &Metadata) -> Result<serde_json::Value, PostgresError> {
    serde_json::to_value(value).map_err(|_| PostgresError::InvalidData)
}

pub(crate) fn decode(value: serde_json::Value) -> Result<Metadata, PostgresError> {
    serde_json::from_value(value).map_err(|_| PostgresError::InvalidData)
}

/// Converts a caller-provided portable timestamp without allowing chrono's infallible conversion
/// to panic at the PostgreSQL boundary.
pub(crate) fn system_time_to_utc(value: SystemTime) -> Result<DateTime<Utc>, PostgresError> {
    let unix_epoch = SystemTime::UNIX_EPOCH;

    let (seconds, nanos) = match value.duration_since(unix_epoch) {
        Ok(duration) => (
            i64::try_from(duration.as_secs()).map_err(|_| PostgresError::InvalidData)?,
            duration.subsec_nanos(),
        ),
        Err(error) => {
            let duration = error.duration();

            let seconds =
                i64::try_from(duration.as_secs()).map_err(|_| PostgresError::InvalidData)?;

            if duration.subsec_nanos() == 0 {
                (-seconds, 0)
            } else {
                (-seconds - 1, 1_000_000_000 - duration.subsec_nanos())
            }
        }
    };

    DateTime::<Utc>::from_timestamp(seconds, nanos).ok_or(PostgresError::InvalidData)
}
