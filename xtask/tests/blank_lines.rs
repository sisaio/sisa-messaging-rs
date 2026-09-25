//! Behaviour of the blank-line checker and fixer on inline sources.

use std::io::Write as _;
use std::process::{Command, Stdio};

use sisa_messaging_xtask::{Kind, Violation, check, fix};

/// Drops the newline that follows the opening quote of an indented raw string.
fn source(text: &str) -> &str {
    text.strip_prefix('\n').unwrap_or(text)
}

/// Returns the reported `(line, kind)` pairs for a valid source.
fn reported(text: &str) -> Vec<(usize, Kind)> {
    check(source(text))
        .expect("test source must parse")
        .into_iter()
        .map(|Violation { line, kind }| (line, kind))
        .collect()
}

/// Asserts that fixing `input` yields `expected`, which is itself clean and a fixed point.
fn assert_fixes(input: &str, expected: &str) {
    let fixed = fix(source(input)).expect("test source must parse");

    assert_eq!(fixed, source(expected));
    assert_eq!(check(&fixed).expect("fixed source must parse"), Vec::new());
    assert_eq!(fix(&fixed).expect("fixed source must parse"), fixed);
}

/// Formats a source with the pinned rustfmt, as `cargo fmt` would.
fn rustfmt(text: &str) -> String {
    let mut child = Command::new("rustfmt")
        .args(["--edition", "2024", "--emit", "stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("rustfmt must be installed through rust-toolchain.toml");

    child
        .stdin
        .take()
        .expect("rustfmt stdin must be piped")
        .write_all(text.as_bytes())
        .expect("rustfmt must accept the source");

    let output = child.wait_with_output().expect("rustfmt must finish");

    assert!(
        output.status.success(),
        "rustfmt failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).expect("rustfmt output must be UTF-8")
}

#[test]
fn multi_line_lets_are_separated_from_each_other_and_the_tail() {
    let input = r#"
fn decode() -> Result<(), IggyMappingError> {
    let message_version = take_required(&mut values, FrameworkHeader::MessageVersion)?
        .parse::<u32>()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;
    let content_type = ContentType::new(take_required(&mut values, FrameworkHeader::ContentType)?)
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;
    Ok(())
}
"#;

    assert_eq!(
        reported(input),
        vec![(5, Kind::Statement), (7, Kind::TailExpression)]
    );

    assert_fixes(
        input,
        r#"
fn decode() -> Result<(), IggyMappingError> {
    let message_version = take_required(&mut values, FrameworkHeader::MessageVersion)?
        .parse::<u32>()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    let content_type = ContentType::new(take_required(&mut values, FrameworkHeader::ContentType)?)
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;

    Ok(())
}
"#,
    );
}

#[test]
fn blank_line_goes_above_a_leading_comment() {
    assert_fixes(
        r#"
fn run() {
    let first = 1;
    // Explains the call.
    // Continues the explanation.
    let second = call(
        first,
    );
    let third = 3;
}
"#,
        r#"
fn run() {
    let first = 1;

    // Explains the call.
    // Continues the explanation.
    let second = call(
        first,
    );

    let third = 3;
}
"#,
    );
}

#[test]
fn a_blank_line_between_comment_and_statement_satisfies_the_boundary() {
    let input = r#"
fn run() {
    let first = 1;
    // Trailing note about the first statement.

    let second = call(
        first,
    );
}
"#;

    assert_eq!(reported(input), Vec::new());
}

#[test]
fn tail_expression_and_final_return_need_a_blank_line() {
    assert_fixes(
        r#"
fn tail() -> u32 {
    let value = 1;
    value + 1
}

fn early() -> u32 {
    let value = 1;
    return value;
}

fn tail_macro() -> String {
    let value = 1;
    format!("{value}")
}
"#,
        r#"
fn tail() -> u32 {
    let value = 1;

    value + 1
}

fn early() -> u32 {
    let value = 1;

    return value;
}

fn tail_macro() -> String {
    let value = 1;

    format!("{value}")
}
"#,
    );
}

#[test]
fn every_control_flow_statement_is_separated() {
    let statements = [
        "if ready { go(); }",
        "match ready { _ => go() }",
        "for item in items { go(item); }",
        "while ready { go(); }",
        "loop { break; }",
        "return;",
    ];

    for statement in statements {
        let input = format!("fn run() {{\n    let a = 1;\n    {statement}\n    let b = 2;\n}}\n");

        assert_eq!(
            reported(&input),
            vec![(3, Kind::Statement), (4, Kind::Statement)],
            "{statement}"
        );
    }

    let plain = "fn run() {\n    let a = 1;\n    go();\n    let b = 2;\n}\n";

    assert_eq!(reported(plain), Vec::new());
}

#[test]
fn a_single_line_statement_with_an_attribute_is_not_multi_line() {
    let input = r#"
fn run() {
    let a = 1;
    #[allow(unused_variables)]
    let b = 2;
    let c = 3;
}
"#;

    assert_eq!(reported(input), Vec::new());
}

#[test]
fn struct_union_and_struct_variant_fields_are_separated() {
    assert_fixes(
        r#"
struct Settings {
    /// Documented field.
    name: String,
    #[allow(dead_code)]
    retries: u32,
    timeout: Duration,
}

union Bits {
    int: u32,
    float: f32,
}

enum Shape {
    Circle {
        radius: f64,
        center: Point,
    },
    Empty,
}
"#,
        r#"
struct Settings {
    /// Documented field.
    name: String,

    #[allow(dead_code)]
    retries: u32,

    timeout: Duration,
}

union Bits {
    int: u32,

    float: f32,
}

enum Shape {
    Circle {
        radius: f64,

        center: Point,
    },
    Empty,
}
"#,
    );
}

#[test]
fn one_documented_variant_separates_every_variant() {
    assert_fixes(
        r#"
enum State {
    /// Waiting to start.
    Idle,
    Running(u32),
    Done { code: i32 },
}
"#,
        r#"
enum State {
    /// Waiting to start.
    Idle,

    Running(u32),

    Done { code: i32 },
}
"#,
    );
}

#[test]
fn undocumented_single_line_variants_and_tuple_fields_are_untouched() {
    let input = r#"
enum Color {
    Red,
    Green,
    Blue,
}

struct Pair(u8, u16);

enum Message {
    Move { x: i32, y: i32 },
    Quit,
}
"#;

    assert_eq!(reported(input), Vec::new());
}

#[test]
fn wrapped_tuple_fields_are_untouched_and_rustfmt_stable() {
    let input = source(
        r#"
struct Wrapped(
    pub VeryLongTypeNameNumberOne,
    pub VeryLongTypeNameNumberTwo,
    pub VeryLongTypeNameNumberThree,
);

enum Event {
    Wrapped(
        VeryLongTypeNameNumberOne,
        VeryLongTypeNameNumberTwo,
        VeryLongTypeNameNumberThree,
    ),
    Empty,
}
"#,
    );

    assert_eq!(rustfmt(input), input, "test input must be rustfmt-clean");
    assert_eq!(check(input).expect("source must parse"), Vec::new());
    assert_eq!(fix(input).expect("source must parse"), input);
}

#[test]
fn a_final_macro_without_semicolon_is_the_tail_expression() {
    let input = r#"
fn run() {
    let a = 1;
    custom! { a }
}
"#;

    assert_eq!(reported(input), vec![(3, Kind::TailExpression)]);
}

#[test]
fn blank_line_goes_above_a_leading_block_comment() {
    assert_fixes(
        r#"
fn one_line() {
    let first = 1;
    /* One-line block comment. */
    let second = call(
        first,
    );
}

fn multi_line() {
    let first = 1;
    /*
     * Multi-line block comment.
     */
    let second = call(
        first,
    );
}

fn trailing() {
    let first = 1; /* Belongs to the first statement. */
    let second = call(
        first,
    );
}
"#,
        r#"
fn one_line() {
    let first = 1;

    /* One-line block comment. */
    let second = call(
        first,
    );
}

fn multi_line() {
    let first = 1;

    /*
     * Multi-line block comment.
     */
    let second = call(
        first,
    );
}

fn trailing() {
    let first = 1; /* Belongs to the first statement. */

    let second = call(
        first,
    );
}
"#,
    );
}

#[test]
fn consecutive_single_line_statements_are_untouched() {
    let input = r#"
fn run() {
    let a = 1;
    let b = 2;
    go(a, b);
    let c = a + b;
    drop(c);
}
"#;

    assert_eq!(reported(input), Vec::new());
}

#[test]
fn single_line_blocks_are_untouched() {
    let input = r#"
fn run() {
    let double = |value: u32| { let doubled = value * 2; doubled };
    let unused = { let a = 1; a };
}

fn short() -> u32 { let a = 1; a }
"#;

    assert_eq!(reported(input), Vec::new());
}

#[test]
fn macro_statements_are_single_units_with_untouched_bodies() {
    assert_fixes(
        r#"
fn run() {
    let a = 1;
    assert!(
        a == 1,
        "a must be one"
    );
    let b = 2;
    custom! {
        let x = 1;
        let y = call(
            x,
        );
        y
    }
}
"#,
        r#"
fn run() {
    let a = 1;

    assert!(
        a == 1,
        "a must be one"
    );

    let b = 2;

    custom! {
        let x = 1;
        let y = call(
            x,
        );
        y
    }
}
"#,
    );
}

#[test]
fn closure_bodies_match_arm_blocks_and_let_else_are_visited() {
    let input = r#"
fn run(input: Option<u32>) -> u32 {
    let adjust = |value: u32| {
        let doubled = value * 2;
        doubled + 1
    };
    let Some(value) = input else {
        return 0;
    };
    match value {
        0 => {
            let fallback = adjust(1);
            fallback
        }
        other => other,
    }
}
"#;

    assert_eq!(
        reported(input),
        vec![
            (4, Kind::TailExpression),
            (6, Kind::Statement),
            (9, Kind::TailExpression),
            (12, Kind::TailExpression),
        ]
    );
}

#[test]
fn nested_items_in_blocks_are_checked() {
    let input = r#"
impl Worker {
    fn run(&self) -> u32 {
        let inner = async {
            let value = 1;
            value
        };
        const LIMIT: u32 = 3;
        LIMIT
    }
}

trait Task {
    fn run(&self) -> u32 {
        let value = 1;
        value
    }
}
"#;

    assert_eq!(
        reported(input),
        vec![
            (5, Kind::TailExpression),
            (7, Kind::Statement),
            (8, Kind::TailExpression),
            (15, Kind::TailExpression),
        ]
    );
}

#[test]
fn fix_preserves_crlf_line_endings() {
    let input = "fn run() -> u32 {\r\n    let a = 1;\r\n    a\r\n}\r\n";

    assert_eq!(
        fix(input).expect("CRLF source must parse"),
        "fn run() -> u32 {\r\n    let a = 1;\r\n\r\n    a\r\n}\r\n"
    );
}

#[test]
fn invalid_source_is_a_parse_error() {
    assert!(check("fn run( {").is_err());
}

#[test]
fn fix_is_idempotent() {
    let input = source(
        r#"
struct Config {
    /// Name.
    name: String,
    port: u16,
}

fn run(config: &Config) -> u16 {
    let port = config.port;
    // Validates the port.
    if port == 0 {
        return 1;
    }
    let name = config
        .name
        .trim();
    port
}
"#,
    );

    let once = fix(input).expect("source must parse");
    let twice = fix(&once).expect("fixed source must parse");

    assert_ne!(once, input);
    assert_eq!(twice, once);
}

#[test]
fn rustfmt_keeps_fixed_sources_clean_and_unchanged() {
    let inputs = [
        r#"
struct Config {
    /// Name.
    name: String,
    #[allow(dead_code)]
    port: u16,
}

fn run(config: &Config) -> u16 {
    let port = config.port;
    // Validates the port.
    if port == 0 {
        return 1;
    }
    let adjust = |value: u16| {
        let doubled = value * 2;
        doubled + 1
    };
    let total = adjust(port);
    total
}
"#,
        r#"
fn decode(values: &mut Values) -> Result<(), IggyMappingError> {
    let message_version = take_required(values, FrameworkHeader::MessageVersion)?
        .parse::<u32>()
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;
    let content_type = ContentType::new(take_required(values, FrameworkHeader::ContentType)?)
        .map_err(|_| IggyMappingError::InvalidFrameworkValue)?;
    match message_version {
        1 => Ok(()),
        _ => Err(IggyMappingError::InvalidFrameworkValue),
    }
}
"#,
    ];

    for input in inputs {
        let input = source(input);

        assert_eq!(rustfmt(input), input, "test input must be rustfmt-clean");

        let fixed = fix(input).expect("source must parse");

        assert_ne!(fixed, input);

        let formatted = rustfmt(&fixed);

        assert_eq!(formatted, fixed);

        assert_eq!(
            check(&formatted).expect("formatted source must parse"),
            Vec::new()
        );
    }
}
