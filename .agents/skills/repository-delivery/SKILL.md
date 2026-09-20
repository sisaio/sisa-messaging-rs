---
name: repository-delivery
description: Apply this repository's issue, branch, commit, pull-request, size, and final-review workflow whenever work will mutate repository files or GitHub delivery state. Do not use for read-only investigation.
---

# Repository delivery

Read `CONTRIBUTING.md`; `docs/agent-workflow.md` section 7 for every delivery, section 4 before
delegation, and section 5 before review/coordination; plus the relevant role boundary in
`docs/agent-team.md`. Those sources are authoritative when this summary conflicts.

## Before edits

1. Ensure one open issue defines outcome, acceptance criteria, scope, non-goals, risks, and
   validation. Apply exactly one canonical `type:*` label, every applicable `area:*` label, and only
   evidence-changing `risk:*` labels. Keep priority and workflow status in GitHub Project fields.
2. Issue creation is triage. Keep it `Todo` until the repository owner explicitly approves that
   issue; then move it to `In Progress` and provision one Codex task.
3. State the issue and URL, base, proposed `<type>/<issue-number>-<short-kebab-description>` branch,
   PR intent, and commit rounds. Create the linked branch with `gh issue develop`; never edit on
   `main`.

## Commit rounds

Before staging or committing, state one exact Conventional Commits 1.0.0 message, intended paths,
and validation. Use a repository-approved commit type and add `Refs #<issue-number>` without a
closing keyword. Install and run the `prek` hooks; never use `--no-verify`. Do not commit unless the
user asks.

Target at most 10 changed paths per commit and 25 per task/PR. The non-overridable limits are 20
paths per commit, 99 per PR, and 250 commits per PR. Count every added, modified, deleted, renamed,
generated, test, migration, checksum, and lockfile path. Tracked `.sqlx/**` regenerations are exempt
from the 10/25 targets, not the 20/99 hard limits, and remain verified by CI. Obtain owner approval
before exceeding a target and split before a hard limit.

## Review and pull request

- Put `Closes #<issue-number>` in the PR body, matching the branch issue number.
- Never enable auto-merge or merge the PR; the owner merges manually.
- Use one initial review and at most one batched fix round. A no-edit review with final evidence
  satisfies the final gate; after an edit, use one fresh final review. Only a blocker/high final
  finding reopens review. Normative docs/agent configuration receive final review only; editorial
  changes receive none.
- A fresh read-only final reviewer examines the committed `merge-base...HEAD` range and records
  exact base/head SHAs, path count, checks, and a range-aligned secret scan. Any base or head change
  invalidates approval. Review diff hunks and named sections; use CI `cargo sqlx prepare --check`
  evidence instead of reading generated `.sqlx/**` metadata.
- Report readiness only when the reviewed range is current, required CI passes, and no required
  finding remains unresolved.
- At every handoff report issue, branch, current commit round and exact message, changed-path and PR
  totals, checks, and next safe slice.
- Pipe `prek run` and `git push` output through `tail -n 40`; explicit checks are the evidence.
- Await external state once: maximum-timeout `wait_agent`, `gh pr checks --watch --fail-fast`, or
  `gh run watch --exit-status`, with output piped through `tail`. Do not poll status.
- Filter `gh`/`gh api` reads with `--json`/`--jq`; do not use `--comments` or unfiltered API output.
