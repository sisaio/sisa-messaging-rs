//! Checks and inserts the blank-line boundaries described in the crate documentation.

mod collector;
mod lines;

use std::fmt;

use self::lines::Lines;

/// The kind of node that is missing a blank line before its leading group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    /// A struct, struct-variant, or union field.
    Field,

    /// A variant of an enum that has at least one documented variant.
    Variant,

    /// A statement inside a multi-line block.
    Statement,

    /// The tail expression of a multi-line block.
    TailExpression,
}

impl fmt::Display for Kind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Field => "field",
            Self::Variant => "variant",
            Self::Statement => "statement",
            Self::TailExpression => "tail expression",
        })
    }
}

/// One missing blank line.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Violation {
    /// The 1-based line above which the blank line belongs: the first line of the leading group.
    pub line: usize,

    /// The kind of node whose leading group starts on `line`.
    pub kind: Kind,
}

/// Returns every missing blank line in `source`, ordered by line, one per insertion line.
///
/// # Span state
///
/// Parsing with the proc-macro2 `span-locations` feature keeps span data for `source` in
/// thread-local state until [`proc_macro2::extra::invalidate_current_thread_spans`] is called.
/// A caller that processes many sources on one thread must call it between sources, as the
/// `cargo xtask` binary does; otherwise memory grows with the total input, and a very large total
/// input can exhaust the `u32` span positions. Call it only after dropping every value that holds
/// a span, such as a returned [`syn::Error`].
///
/// # Errors
///
/// Returns the parser error when `source` is not a valid Rust file.
pub fn check(source: &str) -> syn::Result<Vec<Violation>> {
    let file = syn::parse_file(source)?;
    let lines = Lines::new(source);
    let mut violations = collector::collect(&lines, &file);

    violations.sort_by_key(|violation| violation.line);
    violations.dedup_by_key(|violation| violation.line);

    Ok(violations)
}

/// Returns `source` with every missing blank line inserted.
///
/// This parses `source` through [`check`], so the same span-state obligation applies: call
/// [`proc_macro2::extra::invalidate_current_thread_spans`] between sources on one thread.
///
/// # Errors
///
/// Returns the parser error when `source` is not a valid Rust file.
pub fn fix(source: &str) -> syn::Result<String> {
    let violations = check(source)?;

    Ok(insert_blank_lines(source, &violations))
}

/// Inserts one empty line above each violation's line, never removing text.
///
/// Lines past the end of `source` are ignored. The inserted line ending matches the line above
/// the insertion point, so CRLF sources stay CRLF.
#[must_use]
pub fn insert_blank_lines(source: &str, violations: &[Violation]) -> String {
    let lines = Lines::new(source);
    let mut targets: Vec<usize> = violations.iter().map(|violation| violation.line).collect();
    targets.sort_unstable();
    targets.dedup();

    let mut fixed = String::with_capacity(source.len() + targets.len() * 2);
    let mut copied = 0;

    for line in targets {
        let Some(offset) = lines.start(line) else {
            continue;
        };

        let ending = if source[..offset].ends_with("\r\n") {
            "\r\n"
        } else {
            "\n"
        };

        fixed.push_str(&source[copied..offset]);
        fixed.push_str(ending);
        copied = offset;
    }

    fixed.push_str(&source[copied..]);

    fixed
}
