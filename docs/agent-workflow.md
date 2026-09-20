# Agent task and review workflow

The compact workflow source agents read for task packets, review, Git delivery, and size policy.
Rationale, role boundaries, configuration, and baselines stay in [`agent-team.md`](agent-team.md).
Portable rules G1–G9 keep their labels so they can be copied to another project unchanged.

## 4. Task packet

The primary derives a bounded packet from the authoritative GitHub issue; it never sends a
transcript, raw log, entire issue, or design document. Give paths and named sections so the agent
loads only what it needs.

```text
GitHub issue: <number, URL, type/area/risk labels, Project status and priority>
Objective and acceptance criteria:
Base and proposed <type>/<issue-number>-<short-kebab-description> branch:
PR intent, target path count, commit rounds, exact messages, and validation:
Normative documents and named sections:
Allowed read and write paths; non-goals:
Required test layers, risk evidence, and completion gates:
Known decisions and unresolved questions:
Stopping condition and expected handoff:
```

## 5. Workflow and review

One approved issue, or one bounded approved sub-issue, maps to one Codex task, one linked branch,
and normally one PR. The primary owns requirements and dispositions. Use the architect only for an
unanswered design decision or high-risk schema, transaction/locking, fencing, concurrency,
cancellation, compatibility, or cross-crate change. Its decision packet stays under 800 words.
`backend_developer` owns Rust, runtime SQL, tests, Cargo manifests, lockfiles, and adaptations they
require. `release_engineer` owns assigned CI, versioned migration SQL, `atlas.sum`, release assets,
and publication files. One writer owns the PR unless another exclusive domain is touched (G7).

The bounded sequence is one initial review and at most one batched fix round with the existing
writer (G1). If no edit is required and that fresh full-diff review has final evidence, it satisfies
the final gate; do not spawn another reviewer. After an edit, run one fresh final review. Only a
blocker/high final finding opens another fix-and-review round. A medium/low final finding whose fix
meets G3 is fixed by the owning writer without a reviewer; the primary verifies the fix hunks and
records the reviewed HEAD, final HEAD, and changed paths in the handoff and PR. Other medium/low
final findings are deferred to a linked follow-up issue. Database/migration fixes keep section 6
focused re-review (P2).

Comments, docstrings, typos, formatting, and non-normative Markdown need no agent review. Normative
docs, the agent instruction file, `.agents/**`, and their generated agent configurations receive
one fresh final review and no initial review (G2). A small fix is at most five changed paths, adds no
file, and changes no runtime code, schema, or migration. Tests, manifests, lockfiles, docs, and
config may qualify and proceed directly to fresh final review (G3).

Every reviewer is read-only and returns all findings in one batch. It reads changed hunks and named
document sections, never whole files, whole documents, or generated folders, and narrows any
command likely to exceed its output limit (G4). A named concurrency, security, or migration risk
may require broader reading; the handoff states the exception. The approximate 30-command budget
remains a stopping rule: if it or the context limit is reached, stop and report inspected and
uninspected scope plus open risk. Consume summarized broad-check evidence instead of rerunning
workspace tests, Clippy, `cargo deny`, or a full diff already evidenced.

Final review uses a fresh context containing only the issue packet, named normative sections, and
evidence. It resolves and records the exact merge base, current committed HEAD, changed-path count,
checks, and range-aligned secret scan, then reviews the complete `merge-base...HEAD` diff. A base or
HEAD change invalidates approval; focused review never replaces final review. The gate requires the
recorded range to remain current, no blocker/high, every accepted medium fixed or dispositioned,
required checks and CI evidenced, docs and behavior aligned, and no unresolved schema or public
compatibility decision.

Risk-specific depth is preserved: concurrency, cancellation, and fencing require state-transition
and adversarial-interleaving analysis; security requires redaction review and range secret scan;
test-oracle changes require determinism and isolation. The handoff separates unique, duplicate,
invalid, tooling, and unresolved production findings.

Wait for subagents once with maximum-timeout `wait_agent`. Await CI with one
`gh pr checks <n> --watch --fail-fast` or `gh run watch <id> --exit-status`, piping output through
`tail` under `set -o pipefail` so the awaited command's exit status survives; repeated status
inspection is prohibited (G8). Reading `gh`/`gh api` calls select fields
with `--json`/`--jq`; never use `--comments` or unfiltered `gh api`. Drop bot authors unless
dispositioning their findings, when only finding bodies are selected. Web search stays disabled;
unanswered design questions go to the architect gate or user (G9).

## 7. Git and review-size policy

Creating and classifying an issue is triage, not approval. Keep it in Project `Todo` until the
owner explicitly approves its content, labels, priority, scope, and acceptance criteria. Approval
is per issue, moves it to `In Progress`, and provisions exactly one task. Use `needs:decision` while
clarification is missing. Complex initiatives use a native parent with separately approved,
independently reviewable sub-issues; the parent has no catch-all task, branch, or PR.

Before editing, the primary states the issue and URL, base, proposed branch, PR intent, and commit
rounds. Create the linked branch with `gh issue develop`; never work on `main`. Branches use
`<type>/<issue-number>-<short-kebab-description>` where type is `feat`, `fix`, `docs`, `refactor`,
`test`, `perf`, `ci`, `build`, `chore`, `release`, or `hotfix`.

Commits use Conventional Commits 1.0.0 and a `Refs #<issue-number>` body/footer, never a closing
keyword. Allowed commit types are `build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`,
`revert`, `style`, and `test`; `hotfix/*` uses `fix`, and release mechanics normally use
`chore(release)`. Before each round state the exact message, intended paths and predicted count,
validation, current PR path count, and next slice. Install and run `prek`; never use `--no-verify`.
Pipe `prek run` and `git push` output through `tail -n 40` under `set -o pipefail`; explicit checks,
not hook output, are the evidence (G5). Do not stage, commit, push, tag, publish, release, or create/edit a PR unless the user
explicitly authorizes that action.

A commit targets 10 paths and has a 20-path hard limit. A task/PR targets 25 paths
and has a 99-path hard limit; a PR has at most 250 commits. All added, modified, deleted, renamed,
generated, test, migration, checksum, and lockfile paths count. Exceeding a target needs a recorded
reason and prior owner approval; hard limits cannot be overridden. Split by capability, dependency,
or independently testable behavior before a limit. Splitting into more files does not reduce scope.
CI fails closed when the commits endpoint cannot prove the per-commit audit.

For this SQLx offline repository, `.sqlx/**` regeneration is exempt from the 10/25 targets but not
the 20/99 hard limits (P1). Metadata remains tracked, root search ignores it, CI runs
`cargo sqlx prepare --check` for `sisa-messaging-postgres`, and reviewers use that result instead of
reading generated metadata (L2).

The PR body includes `Closes #<issue-number>` matching the branch. Agents never enable auto-merge
or merge. Before push or final acceptance, scan secrets over the exact final-review range; pinned CI
is independent evidence. Report issue, branch, each planned round and message, changed and PR path
counts, checks, risks, and next safe slice at handoff.

G6 keeps this file at most 8 KB. Independent fresh review, committed-range final review, flagship
high-effort architecture/review, named-risk depth, owner merge, and P2/P3 remain unchanged.
