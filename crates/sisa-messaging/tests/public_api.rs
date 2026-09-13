use std::fs;
use std::path::Path;

#[test]
fn crate_root_reexports_match_the_public_api_inventory() {
    let lib_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");
    let source = fs::read_to_string(&lib_path).unwrap();
    let mut exports = Vec::new();
    let mut current = String::new();

    for line in source.lines() {
        let trimmed = line.trim();
        if let Some(start) = trimmed.strip_prefix("pub use ") {
            current.push_str(start);
        } else if !current.is_empty() {
            current.push_str(trimmed);
        } else {
            continue;
        }

        if current.ends_with(';') {
            current.pop();
            exports.push(
                current
                    .split_whitespace()
                    .collect::<String>()
                    .replace(",}", "}"),
            );
            current.clear();
        }
    }

    let actual = exports.join("\n");
    let expected = include_str!("fixtures/public-api.txt").trim_end();

    assert_eq!(actual, expected);
}
