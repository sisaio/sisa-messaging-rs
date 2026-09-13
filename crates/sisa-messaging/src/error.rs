//! Bounded rendering for error text already classified as safe by its owner.

use std::error::Error;
use std::fmt::{self, Write};

/// Maximum UTF-8 byte length of a persisted or emitted error summary.
pub const MAX_ERROR_SUMMARY_BYTES: usize = 1_024;

/// A UTF-8-boundary-truncated error summary.
///
/// This type bounds output; it cannot discover secrets. Callers must pass only errors whose
/// `Display` implementations and source chain are already safe to persist or emit.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ErrorSummary(String);

impl ErrorSummary {
    /// Truncates caller-reviewed safe text to [`MAX_ERROR_SUMMARY_BYTES`].
    #[must_use]
    pub fn from_safe_text(text: &str) -> Self {
        Self(truncate(text))
    }

    /// Renders a caller-reviewed safe error and its sources under the byte bound.
    #[must_use]
    pub fn from_safe_error(error: &(dyn Error + 'static)) -> Self {
        let mut rendered = BoundedWriter::new();

        if write!(&mut rendered, "{error}").is_err() {
            return Self(rendered.into_string());
        }

        let mut source = error.source();

        while let Some(current) = source {
            if rendered.write_str(": ").is_err() || write!(&mut rendered, "{current}").is_err() {
                break;
            }
            source = current.source();
        }

        Self(rendered.into_string())
    }

    /// Borrows the bounded summary.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the bounded summary.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

struct BoundedWriter {
    output: String,
}

impl BoundedWriter {
    fn new() -> Self {
        Self {
            output: String::with_capacity(MAX_ERROR_SUMMARY_BYTES),
        }
    }

    fn into_string(self) -> String {
        self.output
    }
}

impl Write for BoundedWriter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let remaining = MAX_ERROR_SUMMARY_BYTES.saturating_sub(self.output.len());
        if value.len() <= remaining {
            self.output.push_str(value);
            return Ok(());
        }

        let mut boundary = remaining;
        while !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        self.output.push_str(&value[..boundary]);
        Err(fmt::Error)
    }
}

fn truncate(value: &str) -> String {
    if value.len() <= MAX_ERROR_SUMMARY_BYTES {
        return value.to_owned();
    }

    let mut boundary = MAX_ERROR_SUMMARY_BYTES;

    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }

    value[..boundary].to_owned()
}

impl AsRef<str> for ErrorSummary {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ErrorSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
