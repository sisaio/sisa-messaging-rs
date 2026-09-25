//! The syntax visitor that finds missing blank lines.

use proc_macro2::{Delimiter, LineColumn, TokenStream, TokenTree};
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Block, Expr, FieldsNamed, File, ItemEnum, Meta, Stmt, Variant};

use super::lines::Lines;
use super::{Kind, Violation};

/// Returns the missing blank lines in a parsed file, in visiting order.
pub(super) fn collect(lines: &Lines<'_>, file: &File) -> Vec<Violation> {
    let mut collector = Collector {
        lines,
        violations: Vec::new(),
    };

    collector.visit_file(file);

    collector.violations
}

/// The line extent of a syntax node.
#[derive(Clone, Copy)]
struct Extent {
    /// First line, including outer attributes and doc comments.
    first: usize,

    /// First line of the node itself, after its outer attributes.
    body: usize,

    /// Last line of the node.
    last: usize,
}

impl Extent {
    /// Reports whether the node, excluding outer attributes, spans more than one line.
    fn is_multi_line(self) -> bool {
        self.body != self.last
    }
}

/// Visits a file and records missing blank lines.
struct Collector<'a> {
    lines: &'a Lines<'a>,

    violations: Vec<Violation>,
}

impl Collector<'_> {
    /// Measures a node from its span.
    fn extent(&self, node: &impl Spanned) -> Extent {
        let span = node.span();
        let start = span.start();
        let end = span.end();

        Extent {
            first: start.line,
            body: self.body_line(start, end).unwrap_or(start.line),
            last: end.line,
        }
    }

    /// Returns the first line of a node after its outer attributes and doc comments.
    ///
    /// Attributes are only possible when the node text starts with `#` or a doc comment, so only
    /// then is the node's own text lexed again to skip `#[...]` pairs exactly.
    fn body_line(&self, start: LineColumn, end: LineColumn) -> Option<usize> {
        let from = self.lines.offset(start)?;
        let to = self.lines.offset(end)?;
        let text = self.lines.source.get(from..to)?;

        if !text.starts_with(['#', '/']) {
            return Some(start.line);
        }

        let tokens: Vec<TokenTree> = text.parse::<TokenStream>().ok()?.into_iter().collect();
        let mut index = 0;

        while let (Some(TokenTree::Punct(pound)), Some(TokenTree::Group(group))) =
            (tokens.get(index), tokens.get(index + 1))
        {
            if pound.as_char() != '#' || group.delimiter() != Delimiter::Bracket {
                break;
            }

            index += 2;
        }

        let relative = tokens.get(index)?.span().start().line;

        Some(start.line + relative.checked_sub(1)?)
    }

    /// Requires a blank line between `previous_last` and the leading group of the node whose
    /// first line (attributes included) is `next_first`.
    fn require(&mut self, previous_last: usize, next_first: usize, kind: Kind) {
        if next_first <= previous_last {
            return;
        }

        let mut group_first = next_first;

        while let Some(comment_first) = self.comment_above(group_first, previous_last) {
            group_first = comment_first;
        }

        let separated = (previous_last + 1..group_first).any(|line| {
            self.lines
                .text(line)
                .is_some_and(|text| text.trim().is_empty())
        });

        if !separated {
            self.violations.push(Violation {
                line: group_first,
                kind,
            });
        }
    }

    /// Returns the first line of the comment that ends directly above `line`, never reaching
    /// `floor`: a `//` line, a one-line `/* ... */`, or a block comment whose opening line starts
    /// with `/*` and whose closing line ends with `*/`.
    fn comment_above(&self, line: usize, floor: usize) -> Option<usize> {
        let above = line.checked_sub(1).filter(|&above| above > floor)?;
        let text = self.lines.text(above)?.trim();

        if text.starts_with("//") || (text.starts_with("/*") && text.ends_with("*/")) {
            return Some(above);
        }

        if !text.ends_with("*/") {
            return None;
        }

        (floor + 1..above).rev().find(|&opening| {
            self.lines
                .text(opening)
                .is_some_and(|text| text.trim().starts_with("/*"))
        })
    }

    /// Requires a blank line between every adjacent pair of nodes that do not share a line.
    fn require_between<'n, T: Spanned + 'n>(
        &mut self,
        nodes: impl IntoIterator<Item = &'n T>,
        kind: Kind,
    ) {
        let extents: Vec<Extent> = nodes.into_iter().map(|node| self.extent(node)).collect();

        for pair in extents.windows(2) {
            if let [previous, next] = pair {
                self.require(previous.last, next.first, kind);
            }
        }
    }
}

/// Reports whether a statement must be surrounded by blank lines (rules 2 and 3).
fn is_separated(statement: &Stmt, extent: Extent) -> bool {
    extent.is_multi_line()
        || matches!(
            statement,
            Stmt::Expr(
                Expr::If(_)
                    | Expr::Match(_)
                    | Expr::ForLoop(_)
                    | Expr::While(_)
                    | Expr::Loop(_)
                    | Expr::Return(_),
                _
            )
        )
}

/// Reports whether a final statement is the block's tail expression, including a macro
/// invocation without a trailing semicolon.
fn is_tail_expression(statement: &Stmt) -> bool {
    match statement {
        Stmt::Expr(_, semi) => semi.is_none(),
        Stmt::Macro(mac) => mac.semi_token.is_none(),
        Stmt::Local(_) | Stmt::Item(_) => false,
    }
}

/// Reports whether a final statement needs a blank line before it (rule 4).
fn is_tail(statement: &Stmt) -> bool {
    is_tail_expression(statement) || matches!(statement, Stmt::Expr(Expr::Return(_), _))
}

/// Reports whether an enum variant carries a doc comment.
fn is_documented(variant: &Variant) -> bool {
    variant.attrs.iter().any(|attribute| {
        attribute.path().is_ident("doc") && matches!(attribute.meta, Meta::NameValue(_))
    })
}

impl<'ast> Visit<'ast> for Collector<'_> {
    fn visit_block(&mut self, block: &'ast Block) {
        let open = block.brace_token.span.open().start().line;
        let close = block.brace_token.span.close().end().line;

        if open != close && block.stmts.len() >= 2 {
            let statements: Vec<(&Stmt, Extent)> = block
                .stmts
                .iter()
                .map(|statement| (statement, self.extent(statement)))
                .collect();

            let count = statements.len();

            for (index, pair) in statements.windows(2).enumerate() {
                if let [(previous, previous_extent), (next, next_extent)] = pair {
                    let next_is_last = index + 2 == count;
                    let tail = next_is_last && is_tail(next);

                    if tail
                        || is_separated(previous, *previous_extent)
                        || is_separated(next, *next_extent)
                    {
                        let kind = if tail && is_tail_expression(next) {
                            Kind::TailExpression
                        } else {
                            Kind::Statement
                        };

                        self.require(previous_extent.last, next_extent.first, kind);
                    }
                }
            }
        }

        visit::visit_block(self, block);
    }

    fn visit_fields_named(&mut self, fields: &'ast FieldsNamed) {
        self.require_between(&fields.named, Kind::Field);
        visit::visit_fields_named(self, fields);
    }

    fn visit_item_enum(&mut self, item: &'ast ItemEnum) {
        if item.variants.iter().any(is_documented) {
            self.require_between(&item.variants, Kind::Variant);
        }

        visit::visit_item_enum(self, item);
    }
}
