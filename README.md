# sisa-messaging-rs
High-performance Rust toolkit for transactional outbox, inbox, idempotency, caching, durable messaging, and SQL-first background jobs.

> [!WARNING]
> **Pre-production status:** the workspace and delivery guardrails are being established, but the
> runtime messaging APIs are not implemented and no crates are published. Do not use this project
> in production yet.

The repository architecture is defined in [`docs/README.md`](docs/README.md). PostgreSQL schema
changes are distributed as external Atlas Community versioned migrations; see
[`docs/migrations.md`](docs/migrations.md).

## Local quality gates

Install [`prek`](https://prek.j178.dev/) 0.4.14 or later, then install every configured hook:

```text
prek install --hook-type pre-commit --hook-type commit-msg --hook-type pre-push
```

The hooks check the Git-flow branch name and staged diff, run Rust formatting and Clippy when the
workspace exists, enforce [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/)
in strict mode, and run workspace tests before push. `prek` temporarily stashes unstaged changes so
the pre-commit checks see the content that will actually be committed. Hooks are fast local
feedback; required CI checks remain authoritative because Git hooks can be bypassed locally.
