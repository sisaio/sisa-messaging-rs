# Codex agent team

## 1. Decision

This repository uses one primary Codex agent and four project-scoped custom agents:

| Role | Model / normal effort | Writes files | Use and escalation trade-off |
|---|---|---:|---|
| Primary delivery lead | Cost-controlled GPT-5.x / low or medium | Docs only | Requirements, orchestration, and acceptance; raise effort only for a named unresolved risk |
| `architect` | `gpt-5.6-sol` / `high` | No | Flagship reasoning for gated architecture and adversarial design analysis |
| `backend_developer` | `gpt-5.6-terra` / `medium` | Yes | Cost-balanced sustained Rust/SQL implementation; high is a task-specific override for a named correctness risk |
| `reviewer` | `gpt-5.6-sol` / `high` | No | Flagship independent integration and final-approval review |
| `release_engineer` | `gpt-5.6-luna` / `medium` | Yes, narrowly | Faster delivery work; high is reserved for migration-integrity, publication, or CI-security risk |

GPT-6 Astra is not a primary or project-agent model and is not an escalation path. The model split
preserves diversity between the Terra implementation writer and Sol final reviewer. High effort is
bounded to architecture, which is spawned only behind its design/risk gate, and independent review,
which owns final approval; routine implementation and delivery remain medium. The project config
caps spawned agents at two concurrent threads, and the normal workflow remains sequential because
overlapping writers and implementation-aware final reviewers are prohibited.

[Official OpenAI model guidance](https://developers.openai.com/api/docs/guides/latest-model)
recommends explicitly tuning delegation and calibrating verification to the task. This repository
applies those harness principles with its own approved GPT-5.x model constraint and evidence gates.

## 2. Roles intentionally not created

- **BA and PO:** no standing agents. The architecture set already defines scope, guarantees,
  non-goals, user-facing capabilities, and completion gates. The primary delivery lead translates
  a request into acceptance criteria. If a request changes product semantics or compatibility,
  the primary agent asks the user instead of letting an agent invent product policy.
- **DBA:** no standing agent. Database design belongs to `architect`; runtime SQL and database tests
  belong to `backend_developer`; migration mechanics and packaging belong to `release_engineer`;
  `reviewer` provides independent database review. This preserves end-to-end ownership without an
  agent that is idle for most tasks.
- **QA:** no standing agent. The architect designs risk coverage, the developer implements every
  required test layer, and the reviewer challenges both. A separate QA handoff would fragment
  responsibility.
- **Frontend and design:** excluded because this repository publishes Rust libraries and has no UI.
- **General DevOps:** no always-on role. `release_engineer` is spawned only for repository delivery
  concerns; applications and production infrastructure are explicitly outside this library's
  ownership boundaries.

## 3. Scope and token policy

Codex custom-agent files can request read-only or workspace-write defaults, but parent-session live
permission overrides take precedence and there are no per-folder read ACLs. The scopes below are
therefore instruction-level controls for ownership and context, not security boundaries. The
primary agent supplies the exact paths in every task packet, does not spawn design/review roles
under a write-enabling override, and verifies that their handoffs introduced no worktree changes.

| Agent | Default read scope | Default write scope |
|---|---|---|
| `architect` | Relevant `docs/**`; root and crate manifests; `migrations/**` for DB work; affected public interfaces and tests | None |
| `backend_developer` | Assigned `crates/<name>/**`; named docs; related tests/examples; necessary manifests and adjacent interfaces | Only assigned implementation/test paths |
| `reviewer` | Task packet, diff, changed files, relevant docs, callers/callees, manifests, migrations, and risk-focused tests | None |
| `release_engineer` | `.github/**`, root manifests/toolchain/policy, `atlas.hcl`, `migrations/**`, release scripts, migration/release docs | Only explicitly assigned delivery paths |

Every agent must:

1. Prefix every shell command with `rtk` and batch related reads/checks.
2. Start from `rg` and targeted file ranges, not whole-repository dumps.
3. Read only the normative docs named by the task; `docs/README.md` supplies the routing order.
4. Report a concrete dependency before expanding beyond the supplied path scope.
5. Return summaries and evidence rather than raw logs.
6. Preserve unrelated user changes and never claim an unrun check passed.
7. Stop when the packet's outcome and evidence are complete; do not repeat tests, reviews, scans,
   benchmarks, or polling without a changed head, a failure to diagnose, or an unresolved finding.

Only one write-heavy agent may own overlapping files. The backend developer exclusively owns Rust,
runtime SQL, and test implementation. The release engineer exclusively owns versioned migration
SQL, `atlas.sum`, CI, and release/delivery files when assigned. The primary agent writes only team
or task documentation and orchestration artifacts. Read-only architecture or review work may run
in parallel when it is genuinely independent, but architecture must settle before implementation
and review must inspect the completed diff.

New agents inherit the smallest useful history. Architecture and final review start from a bounded
packet without the parent conversation. An implementation fix returns to the existing owning writer
through `followup_task`, which avoids reloading the same requirements and code; a fresh
`backend_developer` spawn needs a stated reason such as a lost thread or a new packet. Long-running
work compacts into a continuation packet containing only completed actions, decisions,
issue/branch/SHA identifiers, evidence, blockers, and the next goal. The project config sets
`model_auto_compact_token_limit` to 100000 so that packet is written before the context grows large.

Waiting is not polling. After spawning or following up an agent, the primary calls `wait_agent`
once with the longest allowed timeout and repeats it only when that call times out. `list_agents`,
`wait`, and shell checks are never used as status polls: every poll resends the whole primary
context, and in the measured baseline polls were a third of the largest thread's requests.

## 4. Task packet

The primary agent sends a small packet to each spawned agent:

```text
GitHub issue: <number, URL, type/area/risk labels, Project status and priority>
Objective:
Acceptance criteria:
Base branch:
Proposed branch: <type>/<issue-number>-<short-kebab-description>
PR intent and target file count:
Commit rounds: exact message, intended paths, validation for each round
Normative docs/sections:
Allowed read paths:
Allowed write paths:
Non-goals:
Required test layers and completion gates:
Known decisions and unresolved questions:
Stopping condition:
Expected handoff:
```

Derive the packet from the authoritative GitHub issue. Do not paste the entire issue or design
documents into the packet. Include the outcome, acceptance criteria, applicable risk evidence,
named document sections, allowed paths, and stopping condition. Give paths and section names so the
receiving agent loads only what it needs; never attach transcripts or raw logs.

## 5. Workflow

```text
GitHub issue, or one bounded sub-issue under a complex parent
  -> triage: complete content, labels, priority, and Todo status
  -> repository-owner approval gate for this issue only
  -> one Codex task
  -> primary: task packet and acceptance criteria derived from the issue
  -> architect: design packet, when the architecture gate applies
  -> backend_developer: implementation and tests
  -> release_engineer: delivery preparation, only when applicable
  -> reviewer: fresh isolated review of the committed complete diff, all findings in one batch
  -> owning writer via followup_task: every accepted finding in one fix round
  -> reviewer: focused finding review, skipped for a small fix
  -> reviewer: fresh final review of the complete diff
  -> primary: verify evidence and report to user
```

The architecture gate applies only when a task needs a design decision that the normative docs do
not already answer, or when it changes high-risk schema, transaction/locking, fencing, async
concurrency/cancellation, compatibility, or cross-crate behavior. A scoped implementation fully
specified by existing docs goes directly to the developer. The architect's normal design packet is
limited to 800 words and does not repeat source documents.

Independent review is required after every material Rust, runtime SQL,
versioned migration, test, manifest or dependency, CI or release,
normative-document, `.codex/config.toml`, and `.codex/agents/*.toml` change.
Only generated output with a separately reviewed source, or a change explicitly
classified as low-risk and non-behavioral by the primary agent, may skip review.
Release preparation never occurs after the final review: changes from a review
finding receive another focused reviewer pass, except for a small fix as defined below.

Each review has an investigation budget of about 30 shell commands. The reviewer consumes the
packet's summarized broad-check evidence, never reruns workspace tests, Clippy, `cargo deny`, or a
full diff dump that this evidence already covers, and runs only targeted read-only checks. The
budget is a stopping rule, not a license to skip risk: if it runs out or the reviewer's context
nears compaction, the reviewer stops and reports what was inspected, what was not, and the open
risk, and the primary decides whether a second bounded pass is needed. Concurrency, security, and
migration risks keep their mandatory extra evidence regardless of the budget.

The reviewer returns all findings in one batch. The primary dispositions them and sends every
accepted finding to the owning writer in a single `followup_task` fix round; findings are never
trickled to the writer one at a time, and no re-review starts before that round is complete. A
small fix, meaning at most 5 changed paths, no new files, and no behavior change, skips the
focused finding review and goes directly to the single fresh final review of the complete diff.
Any larger fix receives the focused finding review first, and database fixes keep the focused
re-review required by section 6.

Final review always uses a fresh read-only reviewer with only the approved issue packet, relevant
normative sections, and repository evidence. It resolves the exact base SHA with `merge-base`,
records the exact current head SHA and changed-path count, and reviews the committed
`merge-base...HEAD` diff. A material change after review makes that approval stale. A focused fix
review may close its finding, but it never substitutes for the fresh final complete-diff integration
review.

The final reviewer maps only applicable risks to extra evidence: concurrency, cancellation, and
fencing require state-transition and adversarial-interleaving analysis; security requires
redaction review and a secret scan over the reviewed range; test-oracle changes require determinism
and isolation review. Its handoff separates unique, duplicate, invalid, tooling, and unresolved
production findings.

The review gate passes only when the reviewed merge base and reviewed HEAD
equal the current values, no blocker/high finding remains, every accepted
medium finding is fixed or explicitly dispositioned, required checks and CI
have evidence, the range-aligned secret scan passed, docs and behavior agree,
and there is no unresolved schema/public compatibility decision. The reviewer
advises; the primary owns finding disposition and final acceptance.

## 6. Database and release escalation

For a database task, the primary agent asks the architect to define transactions, invariants,
locking, indexes, query-plan proof, forward migration and compatibility. The backend developer
implements Rust, runtime SQL, and tests. The release engineer exclusively authors or updates the
versioned migration SQL, checksum, replay, and packaging from that approved design. The reviewer
then inspects the complete database diff as one behavioral change. Any later fix by either writer
receives a focused re-review.

The release engineer may prepare crates, release assets, CI, or tags, but
assigned delivery work never authorizes irreversible production changes. Any
production command, including a production migration, and any publish, push,
or GitHub release action requires explicit user authorization.

## 7. Git and review-size policy

Every repository-changing task starts from one open GitHub issue and is planned as one reviewable
PR. Read-only investigation and planning can happen before an issue exists. The primary agent
defines the issue link, branch, and commit rounds before implementation:

- Creating and classifying an issue is triage only. A new issue stays in Project status `Todo`
  while the repository owner reviews its content, labels, priority, scope, and acceptance criteria.
  No Codex task, linked branch, repository edit, commit, or PR may be created for it until the owner
  explicitly approves implementation. Approval moves the issue to `In Progress` and provisions
  exactly one Codex task from the approved issue. Issues needing clarification remain in `Todo` and
  use `needs:decision` when appropriate. Approval applies only to the selected issue; approving a
  parent does not approve or provision any sub-issue.

- A complex or multi-PR initiative uses a native GitHub parent issue for the combined outcome,
  shared constraints, dependency graph, and completion roll-up. Its bounded sub-issues each define
  independently reviewable acceptance criteria, scope, non-goals, risks, validation, and labels.
  The parent does not receive an aggregate implementation task, branch, or catch-all PR. Each
  approved sub-issue receives one Codex task, one linked numbered branch, and normally one PR that
  closes only that sub-issue. Dependency-ready sub-issues may execute in parallel only when their
  file ownership and behavioral responsibilities do not overlap; shared schema, public contracts,
  or integration boundaries require serialization. Close the parent only after every required
  sub-issue and the combined acceptance criteria are complete.

- Branches use `<type>/<issue-number>-<short-kebab-description>` from the named base branch and are
  created with `gh issue develop` so GitHub records the linked branch. Allowed types are
  `feat`, `fix`, `docs`, `refactor`, `test`, `perf`, `ci`, `build`, `chore`, `release`, and
  `hotfix`. Examples: `feat/123-outbox-dispatcher`, `fix/124-claim-fencing`,
  `docs/125-consumer-guide`, and `ci/126-pr-size-gate`. The description is lowercase kebab-case.
  Work never starts directly on `main`.
- Each commit represents one testable behavior and strictly follows Conventional Commits 1.0.0:
  `<type>[optional scope][optional !]: <description>`, with body and footer sections separated by
  blank lines. `feat` means a new feature, `fix` means a bug fix, and an incompatible change uses
  `!` or an uppercase `BREAKING CHANGE:` footer. Repository commit types are `build`, `chore`, `ci`,
  `docs`, `feat`, `fix`, `perf`, `refactor`, `revert`, `style`, and `test`. A `hotfix/*` branch uses
  `fix`; release mechanics normally use `chore(release)`. Each commit body or footer references
  its issue as `Refs #<issue-number>` without closing it.
- The PR body includes `Closes #<issue-number>`, matching the branch issue number. The PR is the
  review record, and merging it into the default branch closes the issue. Closing keywords do not
  belong in individual commits.
- Automation and Codex agents never enable auto-merge or merge a PR. Once required checks and
  reviews pass, the primary reports that the PR is ready and waits for the repository owner to
  review and merge it manually.
- `prek` installs the versioned `.pre-commit-config.yaml` hooks. Before commit they validate the
  branch, staged diff, formatting, Clippy, and the commit message; before push they run the full
  workspace test suite. Hooks provide fast feedback, while CI remains the non-bypassable authority.
  Agents never use `--no-verify`. The branch hook permits Git's transient detached `HEAD` only while
  an active rebase directory exists; an ordinary detached-HEAD commit remains forbidden.
- Before push or final acceptance, run a secret scan over the same base/head commit range recorded
  by the final reviewer. The pinned CI secret-scan remains an independent non-bypassable check; a
  whole-tree or differently based scan does not replace the range-aligned evidence.
- A commit targets at most 10 changed paths and has a hard limit of 20.
- A task/PR targets at most 25 changed paths and has a non-overridable hard limit of 99. The
  99-file stop preserves CodeRabbit's strict less-than-100-file reviewability boundary.
- A PR has a 250-commit auditability ceiling because GitHub's pull-request commits endpoint does
  not return a complete list beyond that point. CI fails closed rather than silently skipping the
  per-commit path audit; normal tasks should remain nowhere near this ceiling.
- Added, modified, deleted, renamed, generated, test, migration, checksum, and lockfile paths all
  count. Companion files are not exempt.
- Work is split by capability, dependency direction, or independently testable behavior before a
  limit is reached. A later PR may depend on an earlier one, but each must remain independently
  understandable and reviewable.
- Exceeding the 10-file commit or 25-file task/PR target requires a documented reason and user
  approval before implementation. The 20-file commit and 99-file PR/task ceilings are
  non-overridable; CodeRabbit's reviewability boundary remains strictly below 100 files. An atomic
  change that cannot fit must be split into an explicit multi-commit or multi-PR sequence before
  implementation continues.
- Splitting one change into more files does not reduce its review scope. Crossing the 25-path target
  requires an explicit reviewability decision before implementation, even when each file is small.

Before each commit round, the primary agent reports:

```text
GitHub issue:
Base branch:
Working branch:
PR purpose:
Commit round N:
Exact commit message:
Intended paths and predicted count:
Validation:
Current PR path count:
Next slice:
```

Agents may propose and report Git operations, but they do not stage, commit, push, tag, publish, or
create a release unless the user explicitly requests that action. At handoff, the owning agent and
reviewer compare predicted and actual path counts and report both.

## 8. Configuration files

- `.codex/config.toml` enables the team, limits concurrency, and caps context growth with
  `model_auto_compact_token_limit`.
- `.codex/agents/architect.toml` defines the read-only architecture role.
- `.codex/agents/backend-developer.toml` defines the implementation role.
- `.codex/agents/reviewer.toml` defines the isolated read-only review role.
- `.codex/agents/release-engineer.toml` defines the on-demand delivery role.
- `.coderabbit.yaml` configures the repository-aware CodeRabbit review layer; GitHub CI owns hard
  file-count and executable quality gates.
- `AGENTS.md` contains the small routing policy loaded for every task.

Start a new Codex task after changing these files so the project instructions and custom-agent
definitions are loaded afresh. The loading check confirms the selected models and normal efforts,
the two-thread cap, bounded task history, and read-only architecture/reviewer defaults; it does not
reuse the implementation conversation as final-review context.

## 9. Lean flow and durable records

The useful workflow lessons from the earlier Reliar repository are retained without its project
management overhead:

| Lane | Expected size | Flow |
|---|---:|---|
| Patch | Usually 1 commit and at most 5 paths | Primary task packet → developer → section 5 review, ending in a fresh final review → primary |
| Standard | At most 25 paths and normally 1–3 commits | Primary → architect only if gated → developer/release engineer → section 5 review, ending in a fresh final review → primary |
| Multi-PR | More than 25 forecast paths | User-approved PR sequence; every PR follows the normal targets and hard limits |

GitHub Issues and the linked GitHub Project are the authoritative work tracker. Each repository
change uses one issue, one Codex task, one linked numbered branch, and normally one PR. Parent
issues and sub-issues coordinate multi-PR initiatives. The active Codex task packet is a transient
handoff derived from its issue; it is not a second task record.

Each actionable issue has exactly one `type:*` label, all applicable `area:*` labels, and only the
`risk:*` labels that alter review evidence. `needs:*` and `blocked` labels represent exceptional
triage conditions. [`CONTRIBUTING.md`](../CONTRIBUTING.md#classify-the-issue) is the canonical list
of valid labels; task packets use only those names. Priority and the normal `Todo` → `In Progress`
→ `Done` lifecycle live in GitHub Project fields instead of duplicative labels.

### Token-efficiency evidence

Token cost is context size multiplied by request count, so the primary metrics are requests per
thread and context per request, read from the Codex rollout logs. Record per role: spawns,
requests per thread, median and peak context per request, `wait_agent` polls, shell commands, and
compactions. Instruction-file size and reasoning-output share are secondary checks only.

Baseline from the 2026-09-19/20 rollout logs, about 388M tokens over 175 threads; reasoning output
was 0.17% and instruction files under 2k tokens per thread:

| Role | Share | Measured detail |
|---|---:|---|
| Primary orchestrator | 33% | On `gpt-5.6-sol`; largest thread 628 requests, 206 of them `wait_agent` polls at a median one-minute gap, each resending 150–217k context |
| `backend_developer` | 30% | 22 spawns |
| `reviewer` | 22% | 43 spawns; worst run 137 requests, 108 shell commands, 2 compactions, 216k context |
| Guardian auto-review | 8% | Desktop Auto approval mode |
| `architect` | 1.3% | Gated spawns only |
| `release_engineer` | 0.5% | On demand only |

The owner records the same metrics on the next full issue flow and compares them here. Two owner
actions live outside the repository: set the primary default to `gpt-5.6-terra` or `gpt-5.6-luna`
at medium effort in `~/.codex/config.toml` or the desktop picker, and reconsider the desktop Auto
review approval mode or widen its sandbox allowlist.

This repository does not maintain Markdown task cards, separate story files, another backlog, or a
routine ADR stream. The issue records intent and acceptance, the PR records review and validation,
Git history records the accepted change, and normative documents record only resulting durable
contracts and behavior.

When a decision changes a lasting guarantee, public capability, schema invariant, ownership
boundary, or non-goal, update the relevant normative document in the same PR. Create an ADR only
with explicit user approval, and only when a rare irreversible cross-cutting decision needs history
that cannot be expressed clearly in the normative document. A multi-PR effort may use one
`docs/work/<slug>.md` plan after user approval; do not create per-slice cards.

Do not copy Claude-specific slash commands, settings, or skills into Codex. Add a project skill only
when a repeated workflow needs procedural detail that cannot stay concise in `AGENTS.md` and is not
already covered by the normative docs. The completion gates and Definition of Done remain in
`docs/implementation-plan.md`; they are not duplicated into another checklist.

Cross-cutting review remains concise and risk-based:

- **Security:** no unsafe code, secrets, sensitive payload/header/error leakage, untrusted-input
  unwraps, dynamic unparameterized SQL, implicit migrations, or unjustified dependency surface.
- **Performance:** bounded concurrency/queries/batches, no blocking runtime work or transaction
  across network I/O, benchmark evidence for changed hot paths, and real PostgreSQL plan evidence
  for changed queries or indexes.
- **Supply chain:** minimal features, direct dependency ownership, pinned CI/tooling, and the
  repository's `cargo deny` and release gates.
