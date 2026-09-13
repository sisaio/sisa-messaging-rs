# Codex team orchestration

- Prefix every shell command with `rtk`, including every command in a chain. Read `RTK.md` before
  the first additional shell command for the complete output and fallback rules.
- Treat `docs/README.md` and the documents it indexes as normative. Do not silently change a
  documented guarantee, ownership boundary, schema invariant, or non-goal.
- The primary agent is the delivery lead and requirements steward. It creates a bounded task
  packet from the authoritative GitHub issue, writes only orchestration/documentation artifacts,
  and routes implementation to the project agents described in `docs/agent-team.md`.
- Use only the configured GPT-5.x project roles; GPT-6 Astra is not a project role or escalation.
  High effort is reserved for the gated architect, final reviewer, or another named risk.
- Ask `architect` only when a task needs a design decision that the normative docs do not already
  answer, or when it changes high-risk schema, transaction, locking, fencing, concurrency,
  cancellation, compatibility, or cross-crate behavior. Skip architecture review for a scoped
  implementation that the existing docs fully specify.
- Use `backend_developer` as the only Rust, runtime-SQL, and test writer for an implementation task.
  `release_engineer` exclusively owns versioned migration SQL and delivery files when assigned.
  Do not run parallel writers over overlapping files.
- Ask `reviewer` for an independent review after every material Rust, SQL,
  migration, test, manifest/dependency, CI/release, normative-document,
  `.codex/config.toml`, or `.codex/agents/*.toml` change. A fresh read-only
  final reviewer checks the committed `merge-base...HEAD` diff and records
  base/head SHAs, path count, checks, and range secret scan. Both the reviewed
  merge base and reviewed HEAD must equal the current values; a change to
  either invalidates approval. Focused review cannot replace final review.
- Use `release_engineer` only for GitHub CI/CD, Atlas migration mechanics, release artifacts, and
  crates.io publication preparation.
- Do not create standing BA, PO, DBA, frontend, designer, or QA agents. Follow the escalation and
  ownership rules in `docs/agent-team.md` when those concerns arise.
- Give agents only the bounded packet defined in `docs/agent-team.md`; do not pass transcripts or
  raw logs. Reuse the owning writer for fixes and require a concrete dependency before scope grows.
- Treat custom-agent sandbox modes as defaults. Do not spawn `architect` or `reviewer` with a live
  write-enabling override, and verify that their handoffs introduced no worktree changes.
- Run broad validation once per stable head. Other roles consume that evidence and stop repeating
  checks when the required evidence is current and sufficient.
- Keep project management lean: GitHub Issues and the linked GitHub Project are the authoritative
  work tracker. Use one issue, one Codex task, one numbered branch, and normally one PR per
  repository change. For complex or multi-PR work, use one native GitHub parent issue to coordinate
  bounded sub-issues and their dependencies; do not give the parent an aggregate implementation
  task, branch, or catch-all PR. Each sub-issue requires separate owner approval and gets its own
  task, branch, and normally one PR. Approved dependency-ready sub-issues may run in parallel only
  with non-overlapping file ownership and behavioral scope. Close the parent after all required
  sub-issues and combined acceptance criteria are complete. Do not create Markdown task cards,
  story files, a second backlog, or routine ADRs. Update normative docs only when a completed change
  affects a durable decision; create one plan document only for a user-approved multi-PR effort
  that cannot be coordinated adequately through its parent and sub-issues.

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

- Read-only investigation and planning do not require an issue. Before any repository edit, ensure
  there is one open GitHub issue with the outcome, acceptance criteria, scope, non-goals, and
  validation. Give it exactly one `type:*` label, every applicable `area:*` label, and only the
  `risk:*` labels that change required evidence, using only the canonical taxonomy in
  `CONTRIBUTING.md#classify-the-issue`. Use that issue as the task record and derive every agent
  packet from it. Keep priority and normal workflow status in GitHub Project fields, not duplicate
  labels.
- Issue creation and classification are triage only. Keep a new issue in `Todo`; do not provision a
  Codex task, create its branch, or begin repository edits until the repository owner explicitly
  approves the issue. After approval, move it to `In Progress`, provision exactly one Codex task,
  and derive that task's packet from the approved issue. Approval is per issue: approval of a parent
  never implicitly approves or provisions its sub-issues.
- Never implement directly on `main`. Before edits, state the issue number and URL, base branch,
  proposed `<type>/<issue-number>-<short-kebab-description>` branch name, PR intent, and
  commit-round plan. Create the branch with `gh issue develop` so GitHub links it to the issue.
  Allowed branch types are `feat`, `fix`, `docs`, `refactor`, `test`, `perf`, `ci`, `build`,
  `chore`, `release`, and `hotfix`; the description is lowercase kebab-case. A `hotfix/*` branch
  still uses `fix` commit types, while release mechanics normally use `chore(release)`.
- Every new commit strictly follows Conventional Commits 1.0.0:
  `<type>[optional scope][optional !]: <description>`, optional body after one blank line, then
  optional git-trailer-style footers. Use `feat` for a feature, `fix` for a bug fix, and `!` or an
  uppercase `BREAKING CHANGE:` footer for an incompatible change. Allowed repository types are
  `build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `revert`, `style`, and `test`.
- Every commit round must state one exact compliant message, intended paths, and validation before
  staging or committing. Install the hooks from `.pre-commit-config.yaml` with `prek`; never bypass
  them with `--no-verify`. Reference the issue as `Refs #<issue-number>` in the commit body or
  footer. Do not create a commit unless the user asks.
- Every PR body must contain `Closes #<issue-number>` for the issue number in its branch name. The
  PR is the review record; merging it into the default branch closes the issue. Do not use a closing
  keyword in individual commits.
- Never enable auto-merge or merge a PR. After all required checks and reviews pass, report that the
  PR is ready and wait for the repository owner to review and merge it manually.
- Target at most 10 changed files per commit; 20 is the hard limit. Target at most 25 changed files
  per task/PR; 90 is the hard limit, preserving margin below CodeRabbit's 100-file maximum.
- A PR may contain at most 250 commits because GitHub's pull-request commits endpoint does not
  expose a complete list beyond that cap, so CI cannot audit every commit's changed-path limit.
  Normal small-task planning should remain far below this auditability ceiling.
- Count every added, modified, deleted, or renamed path, including tests, generated files,
  migrations, checksums, and lockfiles. Split work by capability or testable behavior before a
  limit is reached; do not use an oversized catch-all commit.
- Exceeding a target requires a documented reason and user approval before implementation. The
  20-file commit and 90-file PR hard limits are non-overridable; split the work into a multi-commit
  or multi-PR sequence before continuing.
- At every handoff report the issue, current/proposed branch, commit round and exact message,
  changed-file count, PR total file count, checks run, and the next safe slice.
- Report a PR ready only when the reviewed merge base and HEAD are current,
  required CI and range secret scan passed, and no required finding remains
  unresolved.
