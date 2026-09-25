//! Line indexing over a source string.

use proc_macro2::LineColumn;

/// Byte offsets of line starts in a source string.
pub(super) struct Lines<'a> {
    pub(super) source: &'a str,

    starts: Vec<usize>,
}

impl<'a> Lines<'a> {
    /// Indexes the line starts of `source`.
    pub(super) fn new(source: &'a str) -> Self {
        let starts = std::iter::once(0)
            .chain(source.match_indices('\n').map(|(index, _)| index + 1))
            .filter(|&start| start < source.len())
            .collect();

        Self { source, starts }
    }

    /// Returns the byte offset where the 1-based `line` starts.
    pub(super) fn start(&self, line: usize) -> Option<usize> {
        self.starts.get(line.checked_sub(1)?).copied()
    }

    /// Converts a span location (1-based line, 0-based character column) into a byte offset.
    pub(super) fn offset(&self, location: LineColumn) -> Option<usize> {
        let start = self.start(location.line)?;
        let text = self.text(location.line)?;

        let column = text
            .char_indices()
            .nth(location.column)
            .map_or(text.len(), |(index, _)| index);

        Some(start + column)
    }

    /// Returns the text of the 1-based `line` without its line ending.
    pub(super) fn text(&self, line: usize) -> Option<&'a str> {
        let start = self.start(line)?;
        let end = self.start(line + 1).unwrap_or(self.source.len());

        Some(self.source[start..end].trim_end_matches(['\n', '\r']))
    }
}
