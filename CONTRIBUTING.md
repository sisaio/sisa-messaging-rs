# Contributing

Every sisaio repository has these sections in this order; what differs is the crates, the
commands and the examples.

## Workflow

1. **An issue first.** A PR closes an issue: open one with the matching form if none exists.
   Three kinds of PR come without one: Dependabot's, release-plz's, and a small `docs` or `chore`
   change that nobody needs to track.
2. **A branch** from `main`, named as [Branches](#branches) says, with one topic.
3. **The change, with its tests and docs**, in commits that follow
   [Commit messages](#commit-messages). Run [the checks](#build-and-check) before you push.
4. **A PR** that says `Closes #n` when it has an issue, fills in the template, and carries its labels. Its assignee is
   its author.
5. **Review.** CI must pass, and the PR needs one approval given after its latest push.
6. **Merge** with the rebase method, the only one allowed, so `main` stays linear.

There is no project board. An issue's labels, milestone and assignee say where it stands, as
[Issues, labels and milestones](#issues-labels-and-milestones) explains.

## Local setup

- **Rust** through rustup. The toolchain is pinned in `rust-toolchain.toml`, and rustup installs
  it on the first `cargo` command.
- **Docker**, for the tests: each one starts its own server.
- **The Git hooks**, once per clone. They check each commit message as you make it and each
  branch name as you push it, with the scripts CI runs:

  ```sh
  git config core.hooksPath .githooks
  ```

- **The tools the checks use:** [cargo-nextest](https://nexte.st),
  [cargo-deny](https://github.com/EmbarkStudios/cargo-deny),
  [cargo-semver-checks](https://github.com/obi1kenobi/cargo-semver-checks) and
  [lychee](https://github.com/lycheeverse/lychee).

## Build and check

The minimum supported Rust is the workspace's `rust-version`, 1.95. Before a PR, run what CI
runs. `--locked` makes a command fail instead of quietly rewriting `Cargo.lock`, as in CI; a
dependency change updates the lock file on purpose and commits it.

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
(set -e; for f in "" nats iggy redis postgres; do cargo check -p sisa-messaging --features "$f" --locked; done)
cargo tree -i opentelemetry --all-features --edges normal --locked > /dev/null
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked
cargo +1.95 check --workspace --all-targets --all-features --locked
cargo nextest run --workspace --all-features --locked
cargo test --doc --workspace --all-features --locked
cargo deny check
cargo semver-checks --workspace --all-features --baseline-rev origin/main   # unless labeled `breaking`
lychee --offline --include-fragments --hidden --exclude-path '(^|/)(\.git|\.claude|target)/' './**/*.md'
```

A weekly workflow also checks the workspace with every direct dependency at its lowest allowed
version: `cargo +nightly update -Z direct-minimal-versions`, then `cargo check --workspace
--all-targets --all-features`. Restore `Cargo.lock` afterwards.

CodeQL scans the workflows on every PR and the Rust code on `main` after the merge and every
week; its findings are under the repository's Security tab.

## Tests

Unit tests sit at the end of the file they test, in `#[cfg(test)] mod tests`. Tests that put
several parts together, such as a bus with its dispatcher, or anything on NATS, PostgreSQL, Iggy
or Redis, go in a file in the crate's `tests/it/` folder, which builds as one test binary, so the
files run side by side. A test that installs something process-wide, such as a meter provider,
gets a binary of its own. Each NATS, PostgreSQL, Iggy or Redis test starts its own server in
Docker through `sisa-messaging-testing`, so the suite needs Docker and nothing else. `NATS_IMAGE`
picks the NATS server, `nats:2.15-alpine` by default, `POSTGRES_IMAGE` the database,
`postgres:18-alpine` by default, `IGGY_IMAGE` the Iggy server, `apache/iggy:0.10.0` by default,
and `REDIS_IMAGE` the Redis server, `redis:8.10-alpine` by default. CI tests these newest releases
only, and runs the Redis transport's tests on Valkey (`valkey/valkey:9.1-alpine`) and Dragonfly
(`docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2`) too. The Iggy server runs on io_uring, so
its container gets the `SYS_NICE` capability and no seccomp profile; a Docker that refuses those
can't run the Iggy tests.

```sh
NATS_IMAGE=nats:2.15-alpine POSTGRES_IMAGE=postgres:18-alpine IGGY_IMAGE=apache/iggy:0.10.0 \
  REDIS_IMAGE=redis:8.10-alpine cargo nextest run --workspace --all-features
REDIS_IMAGE=valkey/valkey:9.1-alpine cargo nextest run -p sisa-messaging-redis
```

[cargo-nextest](https://nexte.st) runs each test in a process of its own, and CI uses its `ci`
profile from `.config/nextest.toml`, which runs every test even after one fails. It doesn't run
the examples in doc comments; `cargo test --doc` does. `cargo test` still runs everything
without it.

Docker Hub limits anonymous pulls. When a pull fails with `429 Too Many Requests`, run
`docker login`, or pull the image from Google's mirror under the name the tests use:

```sh
docker pull mirror.gcr.io/library/nats:2.15-alpine
docker tag mirror.gcr.io/library/nats:2.15-alpine nats:2.15-alpine
```

Coverage comes from [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov); CI measures it on
each push to main.

```sh
cargo llvm-cov --workspace --all-features --ignore-filename-regex 'crates/testing/'
```

Write tests a reader can follow: one behavior per test, named for that behavior, as few lines as
it takes. Wait for another task with `eventually` instead of sleeping, and use paused Tokio time
for timers.

## Code style

- Keep code minimal, readable, and working. Judge every change by that.
- Read [docs/architecture.md](docs/architecture.md) first; its rules section lists what reviews
  check.
- Public names are reviewed with care; rename a parameter, its getter and its field together.

`rustfmt.toml`, `clippy.toml` and the workspace's `[workspace.lints]` settle layout and lints, and
CI fails on any of them. A new package takes those lints with `[lints] workspace = true`. They
are the same in every sisaio crate repository: no unsafe code, docs and `Debug` on every public
item, whole-word names, one meaning per name, and no `unwrap`, `dbg!` or `todo!` in library code.
What they don't check:

- Imports come in groups separated by a blank line: `std`, other crates, then `crate` and
  `super`.
- Doc comments and comments are full sentences, wrapped at 100 columns like the code. A comment
  says why, or what a reader can't see from the code; it doesn't repeat the code.
- Names are whole words (`envelope`, `delivery`), and the message type parameter is `Msg`
  everywhere. Clippy's `min_ident_chars` catches single letters, `i` and `n` included.
- A name means one thing in its scope. A new binding may reuse a name for the same value
  converted (`let prefix = prefix.into()`); anything else gets a name of its own, such as
  `handle_and_ack` for the closure that wraps `handle`. Clippy's `shadow_unrelated` catches the plain
  cases.
- Library code returns errors instead of panicking: no `unwrap` outside tests, and `expect` only
  for an invariant, with a message saying why it holds. Poisoned locks are recovered with
  `into_inner`.
- A blank line separates steps in a long function; short functions need none.
- A file defines each item before the code that uses it: helpers first, then the type built from
  them, with its struct and impls together. The test module comes last.
- Each trait impl has a one-line `//` comment above it saying what it gives the caller or why it
  exists.
- Write a closure that returns a future as an async closure, `async || ...` or
  `async move || ...`, not `|| async { ... }`. Keep an `async move` block only where Rust needs
  one: the body of a public trait method that returns `impl Future + Send`, since an `async fn`
  there can't promise `Send`, and a future handed to a function such as `tokio::spawn`,
  `timeout` or `Box::pin`.
- Every public type implements `Debug`, which `missing_debug_implementations` checks. A bus, a
  transport or a persistence shows its name and settings, never a message body, a connection or a
  password in a URL.

## Public API and breaking changes

A change to any of these is breaking: it takes `!` in the commit and the `breaking` label.

- Every public Rust item that isn't `#[doc(hidden)]`. Hidden items aren't part of the supported
  API: Rust has no visibility between crates, so they are `pub` for the other crates of this
  workspace and their tests, and any crate can still reach them, but a release may change them.
- The settings' field names, which config files use, and what each default is.
- The metric names and attributes, and the span names and fields, once they are documented.
- The [data formats](docs/formats.md): the envelope's headers, the subject,
  stream and consumer names, and the outbox and inbox tables, which other languages and the
  application's own migrations depend on.
- The async-nats and sqlx types that reach the API: moving to a release of theirs that breaks
  their own API is breaking here too.

The settings structs and the error enums are `#[non_exhaustive]`, so a new setting or variant isn't
breaking. Build settings from `default()` and set fields on it.

## Docs

- API docs live in Rust doc comments: short, plain, with an example where it helps. Don't link
  Markdown files from crate docs.
- The pages in `docs/` explain how the library works, and [docs/README.md](docs/README.md) says
  which page holds what. Update the page a change affects in the same PR: the crate's
  [spec](docs/specs/README.md) when what it does changes, and [docs/formats.md](docs/formats.md)
  when a header, a name on a server or a table does.
- A choice that is hard to reverse, or that changes a rule, a data format or a dependency, gets
  an [ADR](docs/adr/README.md): open it as Proposed in the PR that needs it. An accepted ADR is
  never rewritten; a new one supersedes it.
- Draw diagrams in Mermaid, number the steps of a flow, prefer a table to a long list, and wrap
  at 100 columns.

## Issues, labels and milestones

Every issue has a type: Bug, Feature or Task. The issue forms set it, with the matching `type:`
label; set both yourself on an issue opened another way. Docs work is a Task. Pull requests have
no type; they carry a `type:` label.

Every issue and pull request gets labels from these groups, so anyone can filter the work:

| Group | Labels | Rule |
| --- | --- | --- |
| Type | `type:feature`, `type:bug`, `type:security`, `type:perf`, `type:docs`, `type:chore`, `type:deps` | One. On a PR, the one its title's type maps to: `feat`, `fix` or `revert`, `security`, `perf`, `docs`; `type:chore` for `refactor`, `test`, `build`, `ci` and `chore`. |
| Area | `area:facade`, `area:core`, `area:runtime`, `area:nats`, `area:iggy`, `area:redis`, `area:postgres`, `area:testing`, `area:ci` | Every part the change touches. An area is named like its commit scope. |
| State | `needs-info`, `blocked`, `priority:high`, `breaking`, `good first issue`, `help wanted` | When they apply. |
| Automated | `type:deps`, `release` | Set by Dependabot and release-plz on their PRs. |

- `needs-info`: the reporter owes details or a reproduction. Remove it once they answer.
- `blocked`: the work waits on something else. A comment says what.
- `priority:high`: do next. The maintainer sets it.
- `breaking`: the public API or a data format changes. The PR says how to migrate. CI's `semver`
  job fails a PR that breaks a published crate's API, against the PR's base, without it; re-run
  the job after adding it.
- Close a duplicate as "duplicate" with a link, and a declined request as "not planned" with the
  reason. Answer a question in its issue, then close it.

Where an issue stands shows on the issue itself:

| State | How it shows |
| --- | --- |
| New | Open, with no milestone |
| Accepted | Has a milestone, which only the maintainer sets |
| In progress | Has an assignee: assign yourself before starting, so two people don't take the same issue |
| Blocked | Has `blocked` |
| In review | A PR that says `Closes #n` is linked |
| Done | Closed by the merged PR |

A milestone is a release, and its due date is the target. Every issue that ends in a merged PR
needs one before the PR merges, so the milestone shows what the release ships; work the
maintainer asks for directly gets the next release's milestone when its issue is opened.
Releases are cut by hand, so an issue can be closed before it ships: while its milestone is
open, its release isn't out yet. The maintainer closes a milestone once its release is
published.

The labels and milestones are kept in [.github/labels.yml](.github/labels.yml) and
[.github/milestones.yml](.github/milestones.yml), and a workflow syncs GitHub with them after
each merge. The rulesets on `main` and the `v*` tags are in
[.github/scripts](.github/scripts/README.md), which an admin runs. Change them there in a PR: a
sync overwrites what it manages, so a change made only on GitHub doesn't last.

## Branches

- Branch from `main` as `<type>/<issue>-<topic>`, one topic per PR. `<type>` is the
  [commit type](#commit-messages) of the change and `<issue>` the number of the issue it closes,
  such as `feat/31-outbox-dispatcher`, `fix/42-nats-reconnect` or `ci/17-tag-ruleset`. The topic
  is lowercase words joined by `-`.
- A `docs/` or `chore/` branch for work with no issue may leave the number out, such as
  `docs/readme-badges`. Dependabot's and release-plz's branches keep their own names.
- Bring a branch up to date by rebasing onto `main`; don't merge `main` into it.
- The pre-push hook checks the name, and CI checks it again on the PR.

## Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/). release-plz reads
the type to pick the changelog section and the version bump.

```text
fix(postgres): release a claimed outbox row when its publish fails

What was wrong and why this fixes it, wrapped at 72 columns.
```

- The subject is `type(scope): summary`, at most 72 characters, with no period at the end. Write
  the summary in the imperative, starting lowercase: "add", "fix", "keep".
- The scope is optional. Use it when the change sits in one crate: `core`, `runtime`, `nats`,
  `iggy`, `redis`, `postgres`, `testing` or `facade`. Every repository also has `deps` for a
  dependency bump, `docs` for the pages in `docs/`, and `release`.
- Add `!` after the type or scope for a breaking change (`feat(core)!: ...`), and say how to
  migrate in the body.
- The body says what changed and why. Put `[skip ci]` there, never in the subject.
- No `Co-authored-by` trailer: a commit carries its author's own name.
- A PR title follows the same rule: with rebase merges, the title names the change in the list
  of PRs and its commits land on `main` as they are.

| Type | For | Changelog |
| --- | --- | --- |
| `feat` | a new capability for users | Added |
| `fix` | a bug fix | Fixed |
| `security` | a security fix or hardening | Security |
| `perf` | a speed or memory improvement | Changed |
| `refactor` | a code change users don't see | left out |
| `docs` | docs and doc comments only | left out |
| `test` | tests only | left out |
| `build` | dependencies, Cargo files | left out |
| `ci` | workflows | left out |
| `chore` | anything else, such as releases | left out |
| `revert` | undoing an earlier commit | Fixed |

The commit-msg hook checks each commit as you make it; `fixup!` and `squash!` commits pass
locally, for `git rebase --autosquash`, and fail in CI, so none reaches `main`. CI runs the same
hook on a PR's commits and title.

## Releases

Releases are cut by hand, so each one batches several changes. The maintainer runs the release
workflow, which opens a release PR with the version bump and the changelog section; merging it
tags the commit, drafts the GitHub release, waits for CI and the maintainer's approval, and
publishes the crates to crates.io. A run that stopped halfway is finished by re-running its
failed jobs: what is already published is skipped.

release-plz writes no changelog entry for `docs`, `ci`, `chore` and the other types the changelog
leaves out. When a release PR holds only such commits, write the `## [<version>]` section in
`CHANGELOG.md` by hand before merging it: without that heading the workflow tags nothing and
still ends green.

The steps are in the
[release skill](.agents/skills/release/SKILL.md), and what the workflow needs on GitHub in
[.github/scripts](.github/scripts/README.md#releases).

## License

Contributions are under the [MIT license](LICENSE), like the rest of the repository.
