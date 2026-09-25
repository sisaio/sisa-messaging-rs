//! Repository maintenance tasks, run through the `cargo xtask` alias.
//!
//! `cargo xtask blank-lines --check|--fix [paths...]` checks or inserts the blank-line boundaries
//! described in `docs/api-conventions.md` "Rust presentation". Without paths it covers every
//! tracked `.rs` file; a directory path covers the `.rs` files below it, skipping `target`,
//! hidden directories, and symlinks.

#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use sisa_messaging_xtask::{check, insert_blank_lines};

const USAGE: &str = "usage: cargo xtask blank-lines --check|--fix [paths...]";

/// What the `blank-lines` task does with the violations it finds.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    Check,
    Fix,
}

/// Counts the outcome of a run across files.
#[derive(Default)]
struct Summary {
    violations: usize,

    fixed_files: usize,

    errors: usize,
}

fn main() -> ExitCode {
    let Ok(arguments) = std::env::args_os()
        .skip(1)
        .map(OsString::into_string)
        .collect::<Result<Vec<String>, OsString>>()
    else {
        return usage_error();
    };

    match arguments.first().map(String::as_str) {
        Some("blank-lines") => blank_lines(&arguments[1..]),
        Some("-h" | "--help") => {
            println!("{USAGE}");

            ExitCode::SUCCESS
        }
        _ => usage_error(),
    }
}

/// Prints the usage line and returns the usage exit code.
fn usage_error() -> ExitCode {
    eprintln!("{USAGE}");

    ExitCode::from(2)
}

/// Runs the `blank-lines` task and returns 0 clean, 1 violations, 2 usage or file errors.
fn blank_lines(arguments: &[String]) -> ExitCode {
    let mut mode = None;
    let mut paths = Vec::new();

    for argument in arguments {
        match argument.as_str() {
            "--check" | "--fix" if mode.is_some() => return usage_error(),
            "--check" => mode = Some(Mode::Check),
            "--fix" => mode = Some(Mode::Fix),
            "-h" | "--help" => {
                println!("{USAGE}");

                return ExitCode::SUCCESS;
            }
            flag if flag.starts_with('-') => return usage_error(),
            path => paths.push(PathBuf::from(path)),
        }
    }

    let Some(mode) = mode else {
        return usage_error();
    };

    let mut summary = Summary::default();

    let files = if paths.is_empty() {
        match tracked_rust_files() {
            Ok(files) => files,
            Err(error) => {
                eprintln!("error: cannot list tracked Rust files: {error}");

                return ExitCode::from(2);
            }
        }
    } else {
        let mut files = Vec::new();

        for path in &paths {
            if let Err(error) = collect_rust_files(path, &mut files) {
                eprintln!("{}: error: {error}", path.display());
                summary.errors += 1;
            }
        }

        files
    };

    for file in &files {
        process(file, mode, &mut summary);

        // Span locations accumulate per thread; release them before the next file.
        proc_macro2::extra::invalidate_current_thread_spans();
    }

    if mode == Mode::Fix && summary.violations > 0 {
        eprintln!(
            "inserted {} blank line(s) in {} file(s)",
            summary.violations, summary.fixed_files
        );
    }

    if summary.errors > 0 {
        ExitCode::from(2)
    } else if mode == Mode::Check && summary.violations > 0 {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

/// Checks or fixes one file, reporting violations and errors without stopping the run.
fn process(file: &Path, mode: Mode, summary: &mut Summary) {
    let source = match fs::read_to_string(file) {
        Ok(source) => source,
        Err(error) => {
            eprintln!("{}: error: {error}", file.display());
            summary.errors += 1;

            return;
        }
    };

    let violations = match check(&source) {
        Ok(violations) => violations,
        Err(error) => {
            let location = error.span().start();

            eprintln!(
                "{}:{}: error: cannot parse: {error}",
                file.display(),
                location.line
            );

            summary.errors += 1;

            return;
        }
    };

    if violations.is_empty() {
        return;
    }

    summary.violations += violations.len();

    match mode {
        Mode::Check => {
            for violation in &violations {
                println!(
                    "{}:{}: missing blank line before {}",
                    file.display(),
                    violation.line,
                    violation.kind
                );
            }
        }
        Mode::Fix => {
            if let Err(error) = fs::write(file, insert_blank_lines(&source, &violations)) {
                eprintln!("{}: error: {error}", file.display());
                summary.errors += 1;
            } else {
                summary.fixed_files += 1;
            }
        }
    }
}

/// Lists every tracked `.rs` file, relative to the current directory, from the workspace root.
fn tracked_rust_files() -> io::Result<Vec<PathBuf>> {
    let root = workspace_root();

    let output = Command::new("git")
        .args(["ls-files", "-z", "--", "*.rs"])
        .current_dir(&root)
        .output()?;

    if !output.status.success() {
        return Err(io::Error::other(format!(
            "git ls-files failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    let listing = String::from_utf8(output.stdout)
        .map_err(|_| io::Error::other("git ls-files returned a non-UTF-8 path"))?;

    Ok(listing
        .split('\0')
        .filter(|path| !path.is_empty())
        .map(|path| relative_to_current(&root, path))
        .collect())
}

/// Returns the workspace root, the parent of this package's manifest directory.
fn workspace_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));

    manifest.parent().unwrap_or(manifest).to_path_buf()
}

/// Joins a workspace-relative path to the root, shortening it when the current directory is the
/// root so reports stay readable.
fn relative_to_current(root: &Path, path: &str) -> PathBuf {
    match std::env::current_dir() {
        Ok(current) if current == root => PathBuf::from(path),
        _ => root.join(path),
    }
}

/// Adds `path` when it is a file, or the `.rs` files below it when it is a directory.
///
/// An explicitly named path may be a symlink; symlinks found while walking a directory are
/// skipped.
fn collect_rust_files(path: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
    let metadata = fs::metadata(path)?;

    if metadata.is_file() {
        files.push(path.to_path_buf());

        return Ok(());
    }

    let mut entries: Vec<(PathBuf, fs::FileType)> = fs::read_dir(path)?
        .map(|entry| entry.and_then(|entry| Ok((entry.path(), entry.file_type()?))))
        .collect::<io::Result<_>>()?;

    entries.sort_by(|(left, _), (right, _)| left.cmp(right));

    for (entry, file_type) in entries {
        let name = entry
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");

        // `DirEntry::file_type` does not follow symlinks; skipping them rules out walk cycles.
        if file_type.is_symlink() {
            continue;
        }

        if file_type.is_dir() {
            if name != "target" && !name.starts_with('.') {
                collect_rust_files(&entry, files)?;
            }
        } else if name.ends_with(".rs") {
            files.push(entry);
        }
    }

    Ok(())
}
