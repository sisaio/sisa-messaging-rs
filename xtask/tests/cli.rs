//! Exit codes and reports of the `blank-lines` command line.

use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};

/// Returns a temporary path unique to this process and the given per-test name.
fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "sisa-messaging-xtask-{}-{name}",
        std::process::id()
    ))
}

/// A temporary Rust file removed when dropped.
struct TempSource {
    path: PathBuf,
}

impl TempSource {
    /// Writes `contents` to a file unique to this process and test.
    fn new(name: &str, contents: &str) -> Self {
        let path = temp_path(&format!("{name}.rs"));

        fs::write(&path, contents).expect("temporary source must be writable");

        Self { path }
    }

    /// Returns the file's current contents.
    fn read(&self) -> String {
        fs::read_to_string(&self.path).expect("temporary source must be readable")
    }
}

impl Drop for TempSource {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// A temporary directory removed with its contents when dropped.
struct TempDirectory {
    path: PathBuf,
}

impl TempDirectory {
    /// Creates an empty directory unique to this process and test.
    fn new(name: &str) -> Self {
        let path = temp_path(name);
        let _ = fs::remove_dir_all(&path);

        fs::create_dir_all(&path).expect("temporary directory must be creatable");

        Self { path }
    }

    /// Writes `contents` to `relative` below the directory, creating parents.
    fn write(&self, relative: &str, contents: &str) {
        let path = self.path.join(relative);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("temporary parent must be creatable");
        }

        fs::write(path, contents).expect("temporary file must be writable");
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Runs the xtask binary with the given arguments.
fn xtask<S: AsRef<OsStr>>(arguments: &[S]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_sisa-messaging-xtask"))
        .args(arguments)
        .output()
        .expect("xtask binary must run")
}

/// Returns the process exit code.
fn code(output: &Output) -> Option<i32> {
    output.status.code()
}

#[test]
fn check_reports_violations_and_fix_repairs_them() {
    let file = TempSource::new(
        "violations",
        "fn run() -> u32 {\n    let a = 1;\n    a\n}\n",
    );

    let path = file.path.to_str().expect("temporary path must be UTF-8");

    let checked = xtask(&["blank-lines", "--check", path]);

    assert_eq!(code(&checked), Some(1));

    assert_eq!(
        String::from_utf8_lossy(&checked.stdout),
        format!("{path}:3: missing blank line before tail expression\n")
    );

    let fixed = xtask(&["blank-lines", "--fix", path]);

    assert_eq!(code(&fixed), Some(0));

    assert_eq!(
        file.read(),
        "fn run() -> u32 {\n    let a = 1;\n\n    a\n}\n"
    );

    let rechecked = xtask(&["blank-lines", "--check", path]);

    assert_eq!(code(&rechecked), Some(0));
    assert!(rechecked.stdout.is_empty());
}

#[test]
fn parse_errors_exit_two_and_leave_the_file_untouched() {
    let file = TempSource::new("invalid", "fn run( {\n");
    let path = file.path.to_str().expect("temporary path must be UTF-8");

    assert_eq!(code(&xtask(&["blank-lines", "--check", path])), Some(2));
    assert_eq!(code(&xtask(&["blank-lines", "--fix", path])), Some(2));
    assert_eq!(file.read(), "fn run( {\n");
}

#[test]
fn usage_errors_exit_two() {
    assert_eq!(code(&xtask::<&str>(&[])), Some(2));
    assert_eq!(code(&xtask(&["blank-lines"])), Some(2));
    assert_eq!(code(&xtask(&["blank-lines", "--check", "--fix"])), Some(2));

    assert_eq!(
        code(&xtask(&["blank-lines", "--check", "--verbose"])),
        Some(2)
    );

    assert_eq!(code(&xtask(&["unknown", "--check"])), Some(2));
}

#[test]
fn missing_paths_exit_two() {
    let missing = temp_path("missing-file.rs");

    let output = xtask(&[
        "blank-lines",
        "--check",
        missing.to_str().expect("temporary path must be UTF-8"),
    ]);

    assert_eq!(code(&output), Some(2));
}

#[cfg(unix)]
#[test]
fn non_utf8_arguments_are_usage_errors() {
    use std::os::unix::ffi::OsStrExt as _;

    let argument = OsStr::from_bytes(b"\xff.rs");

    let output = xtask(&[OsStr::new("blank-lines"), OsStr::new("--check"), argument]);

    assert_eq!(code(&output), Some(2));
}

#[test]
fn directories_are_walked_skipping_target_hidden_and_symlinks() {
    let directory = TempDirectory::new("walk");
    let violation = "fn run() -> u32 {\n    let a = 1;\n    a\n}\n";

    directory.write("nested/inner.rs", violation);
    directory.write("nested/notes.txt", violation);
    directory.write("target/skipped.rs", violation);
    directory.write(".hidden/skipped.rs", violation);

    #[cfg(unix)]
    std::os::unix::fs::symlink(&directory.path, directory.path.join("nested/cycle"))
        .expect("symlink must be creatable");

    let root = directory
        .path
        .to_str()
        .expect("temporary path must be UTF-8");

    let output = xtask(&["blank-lines", "--check", root]);
    let inner = directory.path.join("nested/inner.rs");

    assert_eq!(code(&output), Some(1));

    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!(
            "{}:3: missing blank line before tail expression\n",
            inner.display()
        )
    );
}
