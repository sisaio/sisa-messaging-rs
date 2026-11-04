---
name: review-changes
description: Reviews a diff or pull request in sisa-messaging-rs, code and docs alike, against CI's checks, the design rules, the settled decisions and the docs conventions, and reports findings by severity. Use when asked to review, evaluate or check a change, a branch, a PR or a docs page, even if the word "review" isn't used.
---

# Review changes

Review what changed, not the whole crate, unless asked to review the whole repository (below).
Report findings; don't push fixes unless asked.

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
      table without its configured schema, and the patterns past reviews found (below).
- [ ] Look for what only shows at scale or during an outage: a std lock or a long loop on an
      async thread, a second copy of something meant to run once (such as two dispatchers for one
      bus), a queue or buffer with no bound, a claim without `SKIP LOCKED` or without a limit, and
      how many log lines and metric points each message makes while the broker or the database is
      down.

## Bugs past reviews found

Each was real and passed CI in sisa-caching-rs, whose async code has the same shape. Look for the
same shape anywhere in a change.

- A flag released before a final recheck, with no ordering that makes the other side's writes
  visible. Pair the swaps with `AcqRel` and recheck after releasing.
- A last-owner check (`Arc::strong_count`) made outside the lock that hands out clones.
- A time read before waiting for a lock, then compared with what was written during the wait.
- Callers that wait on a shared operation and take its success for granted. Hand them its error,
  and let them redo it if its owner was cancelled.
- A future dropped halfway (a caller's timeout, a `select!`) that leaves shared state behind: a
  claim, a held publish, an open transaction, a shutdown.
- A watermark that vouches for a run of finished items while an earlier one is still running.
- A span or log line on a helper another path also calls, or an info line per failed message.
- A test that can't fail: a wait for a value the test never set, or a sleep standing in for proof
  that something happened. Break the code the test guards and watch it fail.

## Whole-repository review

- [ ] Start from `main`'s head and note its commit. Read
      [Guarantees and limits](../../../docs/01-architecture.md#guarantees-and-limits) and the
      [rules](../../../docs/01-architecture.md#rules-that-hold-the-design-together) first, then
      each crate.
- [ ] For each invariant below, check a test still guards it: break the code and see it fail.
      An invariant the docs promise with no test is a finding. Name the test in the table once
      it exists.
- [ ] Measure before calling something slow, in alternating rounds of the old and the new code;
      for a change under 5%, compare callgrind instruction counts instead.
- [ ] Write the findings as one document: severity, evidence, and the fix grouped into proposed
      PRs, each needing its own issue.

| Invariant | Test |
| --- | --- |
| A `publish` that returns `Ok` is in the broker | not yet written |
| An outbox publish is sent after its transaction commits, or kept as `failed`; a rolled-back one never is | not yet written |
| A delivery is acked only after its handler succeeded and its transaction committed | not yet written |
| With the inbox, a handler's transaction commits at most once per endpoint and message id | not yet written |
| Data that doesn't decode goes to the error destination without retries | not yet written |
| No code deletes an outbox or inbox row; an outbox partition with pending or failed rows never expires | not yet written |
| A bus starts each worker once and stops them on shutdown or when its last clone drops | not yet written |
| The headers, the data and the tables keep their formats | not yet written |

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
