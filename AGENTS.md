# Codex team orchestration

- Prefix every shell command with `rtk`, including every command in a chain. Read `RTK.md` before
  the first additional shell command for the complete output and fallback rules.
- Treat `docs/README.md` and the documents it indexes as normative. Do not silently change a
  documented guarantee, ownership boundary, schema invariant, or non-goal.
- The primary agent is the delivery lead and requirements steward. It creates a bounded task
  packet, writes only orchestration/documentation artifacts, and routes implementation to the
  project agents described in `docs/agent-team.md`.
- Ask `architect` only when a task needs a design decision that the normative docs do not already
  answer, or when it changes high-risk schema, transaction, locking, fencing, concurrency,
  cancellation, compatibility, or cross-crate behavior. Skip architecture review for a scoped
  implementation that the existing docs fully specify.
- Use `backend_developer` as the only Rust, runtime-SQL, and test writer for an implementation task.
  `release_engineer` exclusively owns versioned migration SQL and delivery files when assigned.
  Do not run parallel writers over overlapping files.
- Ask `reviewer` for an independent review after every material Rust, SQL, migration, test,
  manifest/dependency, CI/release, or normative-document change. Route actionable findings to the
  owning writer, then re-review all changed risks before acceptance.
- Use `release_engineer` only for GitHub CI/CD, Atlas migration mechanics, release artifacts, and
  crates.io publication preparation.
- Do not create standing BA, PO, DBA, frontend, designer, or QA agents. Follow the escalation and
  ownership rules in `docs/agent-team.md` when those concerns arise.
- Give every subagent the task packet and its narrow file scope. Expand scope only when the agent
  identifies a concrete dependency and reports it to the primary agent.
- Treat custom-agent sandbox modes as defaults. Do not spawn `architect` or `reviewer` with a live
  write-enabling override, and verify that their handoffs introduced no worktree changes.
- Keep project management lean: use the active task packet and handoffs for normal work. Do not
  create a separate backlog repository, per-subtask cards, story files, or routine ADRs. Update the
  normative docs directly when a durable decision changes; create one plan document only for a
  user-approved multi-PR effort.

# Security and performance lenses

- Never commit or expose secrets, payloads, raw header values, credentials, or connection URLs.
  Do not use `unwrap`/`expect` on database, broker, wire, or other untrusted input paths.
- Keep SQL parameterized and bounded. Do not block the Tokio runtime, spawn unbounded work, or hold
  a lock/database transaction across broker or network I/O.
- Justify every new dependency, keep features minimal, and run `cargo deny check` when dependency
  state changes. Preserve `#![forbid(unsafe_code)]` across library crates.
- For a changed hot path, name the benchmark evidence. For a changed query or index, require a
  representative PostgreSQL 18 `EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)` plan test.

# Git scope and handoff

- Never implement directly on `main`. Before edits, state the base branch, proposed
  `<type>/<short-kebab-description>` branch name, PR intent, and commit-round plan. Allowed branch
  types are `feat`, `fix`, `docs`, `refactor`, `test`, `perf`, `ci`, `build`, `chore`, `release`,
  and `hotfix`; the description is lowercase kebab-case. A `hotfix/*` branch still uses `fix`
  commit types, while release mechanics normally use `chore(release)`.
- Every new commit strictly follows Conventional Commits 1.0.0:
  `<type>[optional scope][optional !]: <description>`, optional body after one blank line, then
  optional git-trailer-style footers. Use `feat` for a feature, `fix` for a bug fix, and `!` or an
  uppercase `BREAKING CHANGE:` footer for an incompatible change. Allowed repository types are
  `build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `revert`, `style`, and `test`.
- Every commit round must state one exact compliant message, intended paths, and validation before
  staging or committing. Install the hooks from `.pre-commit-config.yaml` with `prek`; never bypass
  them with `--no-verify`. Do not create a commit unless the user asks.
- Target at most 10 changed files per commit; 20 is the hard limit. Target at most 25 changed files
  per task/PR; 90 is the hard limit, preserving margin below CodeRabbit's 100-file maximum.
- Count every added, modified, deleted, or renamed path, including tests, generated files,
  migrations, checksums, and lockfiles. Split work by capability or testable behavior before a
  limit is reached; do not use an oversized catch-all commit.
- Exceeding a target requires a documented reason and user approval before implementation. The
  20-file commit and 90-file PR hard limits are non-overridable; split the work into a multi-commit
  or multi-PR sequence before continuing.
- At every handoff report the current/proposed branch, commit round and exact message, changed-file
  count, PR total file count, checks run, and the next safe slice.
