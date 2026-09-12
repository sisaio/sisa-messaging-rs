use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use rustc_lexer::TokenKind;

const LIBRARY_CRATES: [&str; 6] = [
    "sisa-messaging",
    "sisa-messaging-outbox",
    "sisa-messaging-inbox",
    "sisa-messaging-consumer",
    "sisa-messaging-postgres",
    "sisa-messaging-nats",
];

#[derive(Debug, Eq, PartialEq)]
struct Dependency {
    actual_name: String,
    inherited: bool,
    runtime: bool,
}

/// Returns the root directory of the Cargo workspace under test.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("architecture test package must remain under tests/architecture")
        .to_path_buf()
}

/// Reads a UTF-8 fixture or workspace file, failing with its path on error.
fn read(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
}

/// Loads locked Cargo metadata for the workspace without dependency details.
fn cargo_metadata() -> String {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--locked", "--format-version", "1", "--no-deps"])
        .current_dir(workspace_root())
        .output()
        .unwrap_or_else(|error| panic!("failed to execute cargo metadata: {error}"));
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap_or_else(|error| panic!("cargo metadata returned invalid UTF-8: {error}"))
}

/// Advances past JSON whitespace and returns the first non-whitespace position.
fn skip_json_whitespace(bytes: &[u8], mut position: usize) -> usize {
    while bytes
        .get(position)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        position += 1;
    }
    position
}

/// Finds the byte position immediately after a JSON string, honoring escapes.
fn json_string_end(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'"') {
        return None;
    }

    let mut escaped = false;
    for (offset, byte) in bytes.get(start + 1..)?.iter().enumerate() {
        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if *byte == b'"' {
            return Some(start + offset + 2);
        }
    }
    None
}

/// Finds the end of a nested JSON array or object while ignoring quoted delimiters.
fn json_nested_value_end(bytes: &[u8], start: usize, open: u8, close: u8) -> Option<usize> {
    let mut depth = 0_u32;
    let mut position = start;
    while let Some(byte) = bytes.get(position) {
        if *byte == b'"' {
            position = json_string_end(bytes, position)?;
            continue;
        }
        if *byte == open {
            depth += 1;
        } else if *byte == close {
            depth = depth.checked_sub(1)?;
            if depth == 0 {
                return Some(position + 1);
            }
        }
        position += 1;
    }
    None
}

/// Finds the exclusive end position of the JSON value beginning at `start`.
fn json_value_end(bytes: &[u8], start: usize) -> Option<usize> {
    match bytes.get(start)? {
        b'"' => json_string_end(bytes, start),
        b'[' => json_nested_value_end(bytes, start, b'[', b']'),
        b'{' => json_nested_value_end(bytes, start, b'{', b'}'),
        _ => (start..bytes.len()).find(|position| matches!(bytes[*position], b',' | b'}' | b']')),
    }
}

/// Returns the raw value of a named top-level field in a JSON object.
fn top_level_json_field<'a>(object: &'a str, wanted_key: &str) -> Option<&'a str> {
    let bytes = object.as_bytes();
    if bytes.first() != Some(&b'{') {
        return None;
    }

    let mut position = 1;
    loop {
        position = skip_json_whitespace(bytes, position);
        if bytes.get(position) == Some(&b'}') {
            return None;
        }
        if bytes.get(position) == Some(&b',') {
            position += 1;
            continue;
        }

        let key_end = json_string_end(bytes, position)?;
        let key = object.get(position + 1..key_end - 1)?;
        position = skip_json_whitespace(bytes, key_end);
        if bytes.get(position) != Some(&b':') {
            return None;
        }
        position = skip_json_whitespace(bytes, position + 1);
        let value_end = json_value_end(bytes, position)?;
        if key == wanted_key {
            return object.get(position..value_end).map(str::trim);
        }
        position = value_end;
    }
}

/// Splits a JSON array into raw value slices.
fn json_array_values(array: &str) -> Vec<&str> {
    let bytes = array.as_bytes();
    assert_eq!(bytes.first(), Some(&b'['), "expected a JSON array");

    let mut values = Vec::new();
    let mut position = 1;
    loop {
        position = skip_json_whitespace(bytes, position);
        if bytes.get(position) == Some(&b']') {
            return values;
        }
        if bytes.get(position) == Some(&b',') {
            position += 1;
            continue;
        }
        let value_end = json_value_end(bytes, position)
            .unwrap_or_else(|| panic!("invalid JSON array value at byte {position}"));
        values.push(
            array
                .get(position..value_end)
                .expect("JSON value must end on a UTF-8 boundary"),
        );
        position = value_end;
    }
}

/// Removes the surrounding quotes from a JSON string value.
fn json_string(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .expect("expected a JSON string")
}

/// Parses dependency declarations from every supported Cargo dependency section.
fn dependency_declarations(manifest: &str) -> Vec<Dependency> {
    let mut dependencies = Vec::new();
    let mut section = "";

    for raw_line in manifest.lines() {
        let line = raw_line.split('#').next().unwrap_or_default().trim();
        if line.starts_with('[') && line.ends_with(']') {
            section = line.trim_matches(['[', ']']).trim();
            continue;
        }

        let runtime = section == "dependencies"
            || (section.starts_with("target.") && section.ends_with(".dependencies"));
        let dependency_section = runtime
            || section == "dev-dependencies"
            || section == "build-dependencies"
            || section.ends_with(".dev-dependencies")
            || section.ends_with(".build-dependencies");
        if !dependency_section || line.is_empty() {
            continue;
        }

        let Some((alias, value)) = line.split_once('=') else {
            continue;
        };
        let alias = alias.trim().trim_matches('"');
        let actual_name = inline_string_field(value, "package").unwrap_or(alias);
        dependencies.push(Dependency {
            actual_name: actual_name.to_owned(),
            inherited: inline_bool_field(value, "workspace") == Some("true"),
            runtime,
        });
    }

    dependencies
}

/// Maps workspace dependency aliases to their actual package names.
fn workspace_dependencies(manifest: &str) -> BTreeMap<String, String> {
    let mut in_workspace_dependencies = false;
    let mut dependencies = BTreeMap::new();

    for raw_line in manifest.lines() {
        let line = raw_line.split('#').next().unwrap_or_default().trim();
        if line.starts_with('[') && line.ends_with(']') {
            in_workspace_dependencies = line == "[workspace.dependencies]";
            continue;
        }
        if !in_workspace_dependencies || line.is_empty() {
            continue;
        }
        if let Some((alias, value)) = line.split_once('=') {
            let alias = alias.trim().trim_matches('"');
            let actual_name = inline_string_field(value, "package").unwrap_or(alias);
            dependencies.insert(alias.to_owned(), actual_name.to_owned());
        }
    }

    dependencies
}

/// Reports whether a manifest uses a dependency-specific table declaration.
fn uses_dependency_specific_table(manifest: &str) -> bool {
    manifest.lines().map(str::trim).any(|line| {
        let section = line.trim_matches(['[', ']']);
        section.starts_with("dependencies.")
            || section.starts_with("dev-dependencies.")
            || section.starts_with("build-dependencies.")
            || section.contains(".dependencies.")
            || section.contains(".dev-dependencies.")
            || section.contains(".build-dependencies.")
    })
}

/// Extracts a quoted string field from an inline Cargo table value.
fn inline_string_field<'a>(value: &'a str, field: &str) -> Option<&'a str> {
    let (_, tail) = value.split_once(field)?;
    let (_, tail) = tail.split_once('=')?;
    let tail = tail.trim_start();
    let quoted = tail.strip_prefix('"')?;
    let (contents, _) = quoted.split_once('"')?;
    Some(contents)
}

/// Extracts a boolean field from an inline Cargo table value.
fn inline_bool_field<'a>(value: &'a str, field: &str) -> Option<&'a str> {
    let (_, tail) = value.split_once(field)?;
    let (_, tail) = tail.split_once('=')?;
    tail.trim_start().split([',', '}']).next().map(str::trim)
}

/// Reads the manifest for a library crate in the workspace.
fn crate_manifest(crate_name: &str) -> String {
    read(
        &workspace_root()
            .join("crates")
            .join(crate_name)
            .join("Cargo.toml"),
    )
}

/// Returns a raw field value from a manifest's package section.
fn package_field<'a>(manifest: &'a str, wanted_field: &str) -> Option<&'a str> {
    let mut in_package = false;
    for raw_line in manifest.lines() {
        let line = raw_line.split('#').next().unwrap_or_default().trim();
        if line.starts_with('[') && line.ends_with(']') {
            in_package = line == "[package]";
            continue;
        }
        if !in_package {
            continue;
        }
        let Some((field, value)) = line.split_once('=') else {
            continue;
        };
        if field.trim() == wanted_field {
            return Some(value.trim());
        }
    }
    None
}

/// Recursively collects Rust source paths beneath a directory in stable order.
fn rust_sources_under(path: &Path) -> Vec<PathBuf> {
    let mut pending = vec![path.to_path_buf()];
    let mut sources = Vec::new();

    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()));
        for entry in entries {
            let entry = entry.unwrap_or_else(|error| {
                panic!("failed to inspect {}: {error}", directory.display())
            });
            let path = entry.path();
            let file_type = entry
                .file_type()
                .unwrap_or_else(|error| panic!("failed to inspect {}: {error}", path.display()));
            if file_type.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                sources.push(path);
            }
        }
    }

    sources.sort();
    sources
}

/// Builds the internal runtime dependency graph for the library crates.
fn dependency_graph() -> BTreeMap<&'static str, BTreeSet<String>> {
    let root_manifest = read(&workspace_root().join("Cargo.toml"));
    let workspace_dependencies = workspace_dependencies(&root_manifest);

    LIBRARY_CRATES
        .into_iter()
        .map(|crate_name| {
            let dependencies = dependency_declarations(&crate_manifest(crate_name))
                .into_iter()
                .filter(|dependency| dependency.runtime)
                .map(|dependency| {
                    workspace_dependencies
                        .get(&dependency.actual_name)
                        .cloned()
                        .unwrap_or(dependency.actual_name)
                })
                .filter(|dependency| dependency.starts_with("sisa-messaging"))
                .collect();
            (crate_name, dependencies)
        })
        .collect()
}

/// Converts string slices into an owned, sorted set.
fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(ToString::to_string).collect()
}

/// Verifies that workspace membership and incubation publish settings stay fixed.
#[test]
fn cargo_workspace_members_are_exactly_the_documented_packages() {
    let metadata = cargo_metadata();
    let packages = top_level_json_field(&metadata, "packages")
        .map(json_array_values)
        .expect("cargo metadata must contain a packages array");
    let expected_packages = set(&[
        "sisa-messaging",
        "sisa-messaging-outbox",
        "sisa-messaging-inbox",
        "sisa-messaging-consumer",
        "sisa-messaging-postgres",
        "sisa-messaging-nats",
        "sisa-messaging-architecture-tests",
    ]);
    let actual_packages: BTreeSet<String> = packages
        .iter()
        .map(|package| {
            top_level_json_field(package, "name")
                .map(json_string)
                .expect("each cargo metadata package must have a name")
                .to_owned()
        })
        .collect();

    assert_eq!(packages.len(), expected_packages.len());
    assert_eq!(actual_packages, expected_packages);

    for package in packages {
        let name = top_level_json_field(package, "name")
            .map(json_string)
            .expect("each cargo metadata package must have a name");
        if LIBRARY_CRATES.contains(&name) {
            assert_eq!(
                top_level_json_field(package, "publish"),
                Some("[]"),
                "{name} must remain non-publishable during incubation"
            );
        }
    }

    for crate_name in LIBRARY_CRATES {
        let manifest = crate_manifest(crate_name);
        assert_eq!(
            package_field(&manifest, "publish"),
            Some("false"),
            "{crate_name} must declare publish = false explicitly"
        );
    }
}

/// Finds a forbidden configuration-source or async-trait token pattern in Rust source.
fn forbidden_source_pattern(source: &str) -> Option<&'static str> {
    const PATTERNS: &[(&str, &[&str])] = &[
        ("std::env", &["std", ":", ":", "env"]),
        ("std::{env", &["std", ":", ":", "{", "env"]),
        ("env::var(", &["env", ":", ":", "var", "("]),
        ("env::var_os(", &["env", ":", ":", "var_os", "("]),
        ("env::vars(", &["env", ":", ":", "vars", "("]),
        ("env::vars_os(", &["env", ":", ":", "vars_os", "("]),
        ("env::args(", &["env", ":", ":", "args", "("]),
        ("env::args_os(", &["env", ":", ":", "args_os", "("]),
        ("env!(", &["env", "!", "("]),
        ("option_env!(", &["option_env", "!", "("]),
        ("include_str!(", &["include_str", "!", "("]),
        ("include_bytes!(", &["include_bytes", "!", "("]),
        ("include!(", &["include", "!", "("]),
        ("std::fs", &["std", ":", ":", "fs"]),
        ("std::{fs", &["std", ":", ":", "{", "fs"]),
        ("tokio::fs", &["tokio", ":", ":", "fs"]),
        ("tokio::{fs", &["tokio", ":", ":", "{", "fs"]),
        ("async_std::fs", &["async_std", ":", ":", "fs"]),
        ("async_std::{fs", &["async_std", ":", ":", "{", "fs"]),
        ("fs::read(", &["fs", ":", ":", "read", "("]),
        (
            "fs::read_to_string(",
            &["fs", ":", ":", "read_to_string", "("],
        ),
        ("fs::read_dir(", &["fs", ":", ":", "read_dir", "("]),
        (
            "fs::File::open(",
            &["fs", ":", ":", "File", ":", ":", "open", "("],
        ),
        ("File::open(", &["File", ":", ":", "open", "("]),
        ("OpenOptions::new(", &["OpenOptions", ":", ":", "new", "("]),
        ("#[async_trait", &["#", "[", "async_trait"]),
        ("async_trait::", &["async_trait", ":", ":"]),
    ];

    let tokens = significant_rust_tokens(source);
    PATTERNS.iter().find_map(|(label, pattern)| {
        tokens
            .windows(pattern.len())
            .any(|window| window == *pattern)
            .then_some(*label)
    })
}

/// Returns significant Rust tokens, normalizing raw identifiers and excluding trivia/literals.
fn significant_rust_tokens(source: &str) -> Vec<&str> {
    let mut offset = 0;
    rustc_lexer::tokenize(source)
        .filter_map(|token| {
            let start = offset;
            offset += token.len;
            if matches!(
                token.kind,
                TokenKind::Whitespace
                    | TokenKind::LineComment
                    | TokenKind::BlockComment { .. }
                    | TokenKind::Literal { .. }
            ) {
                return None;
            }

            let text = source
                .get(start..offset)
                .expect("lexer token must end on a UTF-8 boundary");
            if matches!(token.kind, TokenKind::RawIdent) {
                Some(
                    text.strip_prefix("r#")
                        .expect("raw identifier token must start with r#"),
                )
            } else {
                Some(text)
            }
        })
        .collect()
}

/// Reports whether a package is an OpenTelemetry SDK or exporter dependency.
fn is_forbidden_otel_dependency(name: &str) -> bool {
    let normalized = name.replace('-', "_");
    normalized == "opentelemetry_sdk"
        || normalized.starts_with("opentelemetry_exporter_")
        || matches!(
            normalized.as_str(),
            "opentelemetry_otlp"
                | "opentelemetry_jaeger"
                | "opentelemetry_prometheus"
                | "opentelemetry_stdout"
                | "opentelemetry_zipkin"
        )
}

/// Verifies the documented runtime dependency edges between library crates.
#[test]
fn documented_runtime_dependency_graph_is_exact() {
    let expected = BTreeMap::from([
        ("sisa-messaging", set(&[])),
        ("sisa-messaging-outbox", set(&["sisa-messaging"])),
        ("sisa-messaging-inbox", set(&["sisa-messaging"])),
        (
            "sisa-messaging-consumer",
            set(&["sisa-messaging", "sisa-messaging-inbox"]),
        ),
        (
            "sisa-messaging-postgres",
            set(&[
                "sisa-messaging",
                "sisa-messaging-inbox",
                "sisa-messaging-outbox",
            ]),
        ),
        ("sisa-messaging-nats", set(&["sisa-messaging"])),
    ]);

    assert_eq!(dependency_graph(), expected);
}

/// Verifies that the PostgreSQL and NATS providers remain independent.
#[test]
fn provider_packages_never_depend_on_each_other() {
    let graph = dependency_graph();
    assert!(!graph["sisa-messaging-postgres"].contains("sisa-messaging-nats"));
    assert!(!graph["sisa-messaging-nats"].contains("sisa-messaging-postgres"));
}

/// Verifies that library dependencies are inherited from the workspace manifest.
#[test]
fn library_dependencies_are_inherited_from_the_workspace() {
    let root_manifest = read(&workspace_root().join("Cargo.toml"));
    let workspace_dependencies = workspace_dependencies(&root_manifest);
    for crate_name in LIBRARY_CRATES {
        assert_eq!(
            workspace_dependencies.get(crate_name).map(String::as_str),
            Some(crate_name),
            "{crate_name} must be declared once by its canonical name in [workspace.dependencies]"
        );
    }

    for crate_name in LIBRARY_CRATES {
        let manifest = crate_manifest(crate_name);
        assert!(
            !uses_dependency_specific_table(&manifest),
            "{crate_name} must use an inline workspace dependency declaration"
        );
        for dependency in dependency_declarations(&manifest) {
            assert!(
                dependency.inherited,
                "{crate_name} must inherit dependency {} from [workspace.dependencies]",
                dependency.actual_name
            );
            assert!(
                workspace_dependencies.contains_key(&dependency.actual_name),
                "{crate_name} dependency {} is absent from [workspace.dependencies]",
                dependency.actual_name
            );
        }
    }
}

/// Verifies that library sources avoid configuration loading and async-trait.
#[test]
fn library_sources_do_not_load_configuration_or_use_async_trait() {
    for crate_name in LIBRARY_CRATES {
        let source_root = workspace_root().join("crates").join(crate_name).join("src");
        for source_path in rust_sources_under(&source_root) {
            let source = read(&source_path);
            assert!(
                forbidden_source_pattern(&source).is_none(),
                "{} contains forbidden library source pattern {:?}",
                source_path.display(),
                forbidden_source_pattern(&source)
            );
        }
    }
}

/// Verifies that library manifests exclude async-trait and OTel SDK exporters.
#[test]
fn library_dependencies_exclude_async_trait_and_otel_sdk_exporters() {
    let root_manifest = read(&workspace_root().join("Cargo.toml"));
    let workspace_dependencies = workspace_dependencies(&root_manifest);

    for crate_name in LIBRARY_CRATES {
        for dependency in dependency_declarations(&crate_manifest(crate_name)) {
            let actual_name = workspace_dependencies
                .get(&dependency.actual_name)
                .map_or(dependency.actual_name.as_str(), String::as_str);
            let normalized = actual_name.replace('_', "-");
            assert_ne!(normalized, "async-trait", "{crate_name} uses async-trait");
            assert!(
                !is_forbidden_otel_dependency(actual_name),
                "{crate_name} uses forbidden OTel SDK/exporter dependency {}",
                actual_name
            );
        }
    }
}

/// Verifies that every library enforces its crate-level unsafe-code prohibition.
#[test]
fn every_library_forbids_unsafe_code_without_bypass() {
    for crate_name in LIBRARY_CRATES {
        let source_root = workspace_root().join("crates").join(crate_name).join("src");
        let lib_source = read(&source_root.join("lib.rs"));
        assert!(
            lib_source
                .lines()
                .any(|line| line.trim() == "#![forbid(unsafe_code)]"),
            "{crate_name} must forbid unsafe code at its crate root"
        );

        for source_path in rust_sources_under(&source_root) {
            let source = read(&source_path);
            assert!(
                !source.contains("allow(unsafe_code)")
                    && !source.contains("warn(unsafe_code)")
                    && !source.contains("deny(unsafe_code)"),
                "{} attempts to weaken or replace the unsafe-code prohibition",
                source_path.display()
            );
        }
    }
}

/// Verifies that renamed forbidden dependencies retain their actual package names.
#[test]
fn dependency_parser_detects_renamed_forbidden_packages() {
    let dependencies = dependency_declarations(
        r#"
        [dependencies]
        telemetry = { workspace = true, package = "opentelemetry-otlp" }
        native-async = { workspace = true, package = "async-trait" }
        "#,
    );

    assert_eq!(dependencies[0].actual_name, "opentelemetry-otlp");
    assert!(is_forbidden_otel_dependency(&dependencies[0].actual_name));
    assert_eq!(dependencies[1].actual_name, "async-trait");
}

/// Verifies that workspace dependency aliases resolve to their package names.
#[test]
fn workspace_dependency_parser_resolves_renamed_packages() {
    let dependencies = workspace_dependencies(
        r#"
        [workspace.dependencies]
        telemetry = { version = "1", package = "opentelemetry-otlp" }
        "#,
    );

    assert_eq!(
        dependencies.get("telemetry").map(String::as_str),
        Some("opentelemetry-otlp")
    );
}

/// Verifies representative environment, argument, filesystem, and async-trait patterns.
#[test]
fn source_detector_recognizes_environment_arguments_files_and_async_trait_usage() {
    assert_eq!(
        forbidden_source_pattern("std::env::var(\"TOKEN\")"),
        Some("std::env")
    );
    assert_eq!(
        forbidden_source_pattern("use std::{ env as process_env };"),
        Some("std::{env")
    );
    assert_eq!(forbidden_source_pattern("env!(\"CONFIG\")"), Some("env!("));
    assert_eq!(
        forbidden_source_pattern("std::env::args().next()"),
        Some("std::env")
    );
    assert_eq!(
        forbidden_source_pattern("env::args().next()"),
        Some("env::args(")
    );
    assert_eq!(
        forbidden_source_pattern("env::args_os().next()"),
        Some("env::args_os(")
    );
    assert_eq!(
        forbidden_source_pattern("r#env::args_os().next()"),
        Some("env::args_os(")
    );
    assert_eq!(
        forbidden_source_pattern("std::fs::read_to_string(path)"),
        Some("std::fs")
    );
    assert_eq!(
        forbidden_source_pattern("fs::read(path)"),
        Some("fs::read(")
    );
    assert_eq!(
        forbidden_source_pattern("fs::read_to_string(path)"),
        Some("fs::read_to_string(")
    );
    assert_eq!(
        forbidden_source_pattern("fs::read_dir(path)"),
        Some("fs::read_dir(")
    );
    assert_eq!(
        forbidden_source_pattern("std /* comment */ :: env :: args()"),
        Some("std::env")
    );
    assert_eq!(
        forbidden_source_pattern("fs /* comment */ :: read_to_string(path)"),
        Some("fs::read_to_string(")
    );
    assert_eq!(
        forbidden_source_pattern("File::open(path)"),
        Some("File::open(")
    );
    assert_eq!(
        forbidden_source_pattern(r#"include_str!("settings.toml")"#),
        Some("include_str!(")
    );
    assert_eq!(
        forbidden_source_pattern(r#"include_bytes!("settings.bin")"#),
        Some("include_bytes!(")
    );
    assert_eq!(
        forbidden_source_pattern(r#"include!("generated_settings.rs")"#),
        Some("include!(")
    );
    assert_eq!(forbidden_source_pattern("vfs::read(path)"), None);
    assert_eq!(forbidden_source_pattern("ConfigFile::open(path)"), None);
    assert_eq!(
        forbidden_source_pattern("#[async_trait]"),
        Some("#[async_trait")
    );
}

/// Verifies that documentation and literals do not trigger source-pattern checks.
#[test]
fn source_detector_ignores_documentation_and_string_literals() {
    for source in [
        "/// Applications call std::env::args(), not this library.\npub struct Settings;",
        "//! Never call fs::read_to_string(path) here.",
        r#"let example = "std::fs::read_to_string(path)";"#,
        r#"#[doc = "env::args_os() is application-owned"] pub struct Settings;"#,
        r##"let example = r"#[async_trait]";"##,
    ] {
        assert_eq!(forbidden_source_pattern(source), None, "source: {source}");
    }
}
