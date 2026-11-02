---
name: review-changes
description: Reviews a diff or pull request in sisa-messaging-rs, code and docs alike, against CI's checks, the design rules, the settled decisions and the docs conventions, and reports findings by severity. Use when asked to review, evaluate or check a change, a branch, a PR or a docs page, even if the word "review" isn't used.
---

# Review changes

Review what changed, not the whole crate. Report findings; don't push fixes unless asked.

## Workflow

- [ ] Get the diff: the PR's diff, or `git diff main...HEAD` on a branch.
- [ ] Run the checks in [CONTRIBUTING.md](../../../CONTRIBUTING.md#build-and-check). Note each
      failure with its first error.
- [ ] Check the diff against the
      [rules](../../../docs/01-architecture.md#rules-that-hold-the-design-together), the
      [decisions](../../../docs/01-architecture.md#decisions) and the
      [code rules](../../../AGENTS.md#code) in AGENTS.md. The ones most often broken:
  - a delivery is acked only after its handler's work is committed, never on an error path;
  - an outbox row is written on the caller's connection, never on one of the outbox's own;
  - no transaction is held across a transport call;
  - only the clock module reads the wall clock;
  - no new dependency in a library crate, and no transport or database client in core.
- [ ] Check what goes with the change: a test for each new behavior, doc comments on new public
      items, the `docs/` page it affects (above all
      [Data formats](../../../docs/01-architecture.md#data-formats)), and `breaking` on the PR if
      the public API, the wire format or a table changed.
- [ ] Check the docs in the diff: doc comments are short and plain with an example where it
      helps, and never link a Markdown file; `docs/` pages use Mermaid for diagrams and tables
      over long lists; every relative link and `#anchor` resolves.
- [ ] Check each commit subject and the PR title against
      [Commit messages](../../../CONTRIBUTING.md#commit-messages): the type matches the change
      (`feat` only for something users can do now, `!` for a breaking change).
- [ ] Look for bugs: a lock held across `.await`, a delivery acked before its commit, a message id
      made again on a retry instead of kept, an error swallowed without a log, SQL that names a
      table without its configured schema.
- [ ] Look for what only shows at scale or during an outage: a std lock or a long loop on an
      async thread, a second copy of something meant to run once (such as two dispatchers for one
      bus), a queue or buffer with no bound, a claim without `SKIP LOCKED` or without a limit, and
      how many log lines and metric points each message makes while the broker or the database is
      down.

## Gotchas

- `cargo +1.94 check` can fail where stable passes. Read the exit code, not a piped tail.
- Not bugs, by design: a message handled twice without the inbox (at least once is the promise);
  an outbox row published twice after a dispatcher crash (same message id, so the broker or the
  inbox drops it); messages out of order across dispatchers.

## Report

```markdown
**Blocking**
- `crates/postgres/src/outbox.rs:120`: what is wrong. Fix: what would fix it.

**Not blocking**
- `file:line`: what is wrong. Fix: what would fix it.

Checks: all passed, or the failing check and its first error.
```

If nothing blocks, say so in one line instead of the Blocking section.
