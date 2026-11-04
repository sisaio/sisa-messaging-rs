//! The envelope's time, written as RFC 3339 on the wire.

use std::fmt;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{EnvelopeError, clock};

const SECONDS_PER_DAY: i64 = 86_400;
const NANOS_PER_SECOND: i128 = 1_000_000_000;

// Days since 1970-01-01 to a proleptic Gregorian date, after Howard Hinnant's `civil_from_days`.
fn date_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_from_march = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_from_march + 2) / 5 + 1;
    let month = if month_from_march < 10 {
        month_from_march + 3
    } else {
        month_from_march - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    // Both fit: the month is 1 to 12 and the day 1 to 31.
    (year, month as u32, day as u32)
}

// The inverse of `date_from_days`, after Howard Hinnant's `days_from_civil`.
fn days_from_date(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let month_from_march = i64::from((month + 9) % 12);
    let day_of_year = (153 * month_from_march + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn days_in_month(year: i64, month: u32) -> u32 {
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

// The decimal number in `text[start..end]`, which must be all ASCII digits.
fn number(text: &[u8], start: usize, end: usize) -> Option<u32> {
    let digits = text.get(start..end)?;
    digits.iter().try_fold(0_u32, |total, digit| {
        digit
            .is_ascii_digit()
            .then(|| total * 10 + u32::from(digit - b'0'))
    })
}

// `YYYY-MM-DDTHH:MM:SS[.fraction](Z|+HH:MM|-HH:MM)`, RFC 3339's `date-time`.
fn parse(text: &str) -> Option<Timestamp> {
    let bytes = text.as_bytes();
    let separators = [(4, b'-'), (7, b'-'), (13, b':'), (16, b':')];
    if separators
        .iter()
        .any(|&(at, separator)| bytes.get(at) != Some(&separator))
        || !matches!(bytes.get(10), Some(b'T' | b't'))
    {
        return None;
    }
    let year = i64::from(number(bytes, 0, 4)?);
    let month = number(bytes, 5, 7)?;
    let day = number(bytes, 8, 10)?;
    let hour = number(bytes, 11, 13)?;
    let minute = number(bytes, 14, 16)?;
    let second = number(bytes, 17, 19)?;
    if !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }

    // Digits past the ninth are below a nanosecond and dropped.
    let mut rest = &bytes[19..];
    let mut nanos = 0;
    if let Some(fraction) = rest.strip_prefix(b".") {
        let length = fraction
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if length == 0 {
            return None;
        }
        for place in 0..9 {
            let digit = fraction
                .get(place)
                .filter(|_| place < length)
                .map_or(0, |byte| byte - b'0');
            nanos = nanos * 10 + u32::from(digit);
        }
        rest = &fraction[length..];
    }

    let offset_seconds = match rest {
        [b'Z' | b'z'] => 0,
        [sign @ (b'+' | b'-'), _, _, b':', _, _] => {
            let offset_hour = number(rest, 1, 3)?;
            let offset_minute = number(rest, 4, 6)?;
            if offset_hour > 23 || offset_minute > 59 {
                return None;
            }
            let offset = i64::from(offset_hour * 3_600 + offset_minute * 60);
            if *sign == b'-' { -offset } else { offset }
        }
        _ => return None,
    };

    let seconds = days_from_date(year, month, day) * SECONDS_PER_DAY
        + i64::from(hour * 3_600 + minute * 60 + second)
        - offset_seconds;
    Some(Timestamp { seconds, nanos })
}

/// A point in time, to the nanosecond: the CloudEvents `time`.
///
/// It reads and writes RFC 3339, the format CloudEvents uses. [`Display`](fmt::Display) writes UTC
/// with a `Z`, and only as many fraction digits as the time needs.
///
/// ```
/// use sisa_messaging_core::Timestamp;
///
/// let time: Timestamp = "2026-11-02T10:30:00.5+07:00".parse()?;
/// assert_eq!(time.to_string(), "2026-11-02T03:30:00.5Z");
/// # Ok::<(), sisa_messaging_core::EnvelopeError>(())
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    // Seconds since 1970-01-01T00:00:00Z, and the nanoseconds within that second.
    seconds: i64,
    nanos: u32,
}

impl Timestamp {
    /// The current time.
    #[must_use]
    pub fn now() -> Self {
        Self::from(clock::now())
    }

    /// The time this many nanoseconds after 1970-01-01T00:00:00Z, or before it when negative.
    #[must_use]
    pub fn from_unix_nanos(nanos: i128) -> Self {
        // An i128 of nanoseconds spans far more years than an i64 of seconds can hold.
        let seconds = nanos
            .div_euclid(NANOS_PER_SECOND)
            .clamp(i64::MIN.into(), i64::MAX.into());
        Self {
            seconds: seconds as i64,
            nanos: nanos.rem_euclid(NANOS_PER_SECOND) as u32,
        }
    }

    /// Nanoseconds since 1970-01-01T00:00:00Z, negative before it.
    #[must_use]
    pub fn unix_nanos(&self) -> i128 {
        i128::from(self.seconds) * NANOS_PER_SECOND + i128::from(self.nanos)
    }
}

// Converts the standard library's time, for the clock and for callers that hold one.
impl From<SystemTime> for Timestamp {
    fn from(time: SystemTime) -> Self {
        match time.duration_since(UNIX_EPOCH) {
            Ok(after) => Self::from_unix_nanos(after.as_nanos() as i128),
            Err(before) => Self::from_unix_nanos(-(before.duration().as_nanos() as i128)),
        }
    }
}

// Writes RFC 3339 in UTC, the CloudEvents `time`.
impl fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (year, month, day) = date_from_days(self.seconds.div_euclid(SECONDS_PER_DAY));
        let second_of_day = self.seconds.rem_euclid(SECONDS_PER_DAY);
        let (hour, minute, second) = (
            second_of_day / 3_600,
            second_of_day / 60 % 60,
            second_of_day % 60,
        );
        write!(
            formatter,
            "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}"
        )?;
        if self.nanos != 0 {
            let fraction = format!("{:09}", self.nanos);
            write!(formatter, ".{}", fraction.trim_end_matches('0'))?;
        }
        formatter.write_str("Z")
    }
}

// Shows the RFC 3339 form, which reads better than the raw seconds.
impl fmt::Debug for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Timestamp({self})")
    }
}

// Reads RFC 3339, with any offset; the leap second 60 isn't accepted.
impl FromStr for Timestamp {
    type Err = EnvelopeError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        parse(text).ok_or_else(|| EnvelopeError::invalid("time", "not an RFC 3339 date-time"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_utc_with_only_the_fraction_digits_it_needs() {
        let whole = Timestamp::from_unix_nanos(1_793_615_400 * NANOS_PER_SECOND);
        let fraction = Timestamp::from_unix_nanos(1_793_615_400 * NANOS_PER_SECOND + 120_000_000);

        assert_eq!(whole.to_string(), "2026-11-02T10:30:00Z");
        assert_eq!(fraction.to_string(), "2026-11-02T10:30:00.12Z");
    }

    #[test]
    fn reads_what_it_writes() {
        for nanos in [
            0,
            1,
            951_782_400_123_456_789,
            -1,
            -62_135_596_800 * NANOS_PER_SECOND,
        ] {
            let time = Timestamp::from_unix_nanos(nanos);

            assert_eq!(time.to_string().parse::<Timestamp>(), Ok(time), "{time}");
        }
    }

    #[test]
    fn reads_an_offset_as_the_same_instant_in_utc() {
        let east: Timestamp = "2026-11-02T17:30:00+07:00".parse().unwrap();
        let west: Timestamp = "2026-11-02T05:00:00-05:30".parse().unwrap();

        assert_eq!(east.to_string(), "2026-11-02T10:30:00Z");
        assert_eq!(west.to_string(), "2026-11-02T10:30:00Z");
    }

    #[test]
    fn reads_lowercase_separators_and_drops_digits_below_a_nanosecond() {
        let time: Timestamp = "2026-11-02t10:30:00.1234567891z".parse().unwrap();

        assert_eq!(time.to_string(), "2026-11-02T10:30:00.123456789Z");
    }

    #[test]
    fn rejects_text_that_isnt_rfc_3339() {
        for text in [
            "",
            "2026-11-02",
            "2026-11-02 10:30:00Z",
            "2026-11-02T10:30:00",
            "2026-13-02T10:30:00Z",
            "2026-02-29T10:30:00Z",
            "2026-11-02T24:00:00Z",
            "2026-11-02T10:30:60Z",
            "2026-11-02T10:30:00.Z",
            "2026-11-02T10:30:00+0700",
            "2026-11-02T10:30:00Zjunk",
        ] {
            assert_eq!(
                text.parse::<Timestamp>(),
                Err(EnvelopeError::invalid("time", "not an RFC 3339 date-time")),
                "{text}"
            );
        }
    }

    #[test]
    fn reads_february_29_in_a_leap_year() {
        assert!("2028-02-29T00:00:00Z".parse::<Timestamp>().is_ok());
        assert!("2000-02-29T00:00:00Z".parse::<Timestamp>().is_ok());
        assert!("2100-02-29T00:00:00Z".parse::<Timestamp>().is_err());
    }

    #[test]
    fn converts_a_system_time_before_and_after_the_epoch() {
        let second = std::time::Duration::from_secs(1);

        assert_eq!(
            Timestamp::from(UNIX_EPOCH + second).unix_nanos(),
            NANOS_PER_SECOND
        );
        assert_eq!(
            Timestamp::from(UNIX_EPOCH - second).unix_nanos(),
            -NANOS_PER_SECOND
        );
    }
}
