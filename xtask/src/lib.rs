//! Blank-line boundary checker and fixer for the repository's Rust sources.
//!
//! The rules are the source-shape rules in `docs/api-conventions.md` "Rust presentation":
//!
//! 1. Consecutive named fields of a struct, struct variant, or union, and consecutive variants of
//!    an enum in which at least one variant carries a doc comment, are separated by a blank line.
//!    Tuple fields are never checked, because rustfmt removes blank lines between them.
//! 2. Inside a block spanning more than one line, a statement whose own span covers more than one
//!    line has a blank line before and after it, except at the block edges.
//! 3. An expression statement whose outer expression is `if`, `match`, `for`, `while`, `loop`, or
//!    `return` has a blank line before and after it, except at the block edges.
//! 4. The block's tail expression, or a final `return` statement, has a blank line before it.
//!
//! A boundary is measured from the last line of one node to the first line of the next node's
//! leading group: its outer attributes, doc comments, and any contiguous `//` comment lines or
//! `/* ... */` block comments directly above it. A missing blank line is inserted immediately
//! above that leading group.

#![forbid(unsafe_code)]

mod blank_lines;

pub use blank_lines::{Kind, Violation, check, fix, insert_blank_lines};
