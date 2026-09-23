# Codex agent team

Task packets, bounded review, Git delivery, and size limits live in the compact
[`agent-workflow.md`](agent-workflow.md). This document retains the team decision, role boundaries,
scope, database/release escalation, configuration, and measured rationale.

## 1. Decision

This repository uses one primary Codex agent and four project-scoped custom agents:

| Role | Model / normal effort | Writes files | Use and escalation trade-off |
|---|---|---:|---|
| Primary delivery lead | `gpt-6-sol` / `medium` | Docs only | Requirements, orchestration, and acceptance; raise effort only for a named unresolved risk |
| `architect` | `gpt-6-sol` / `high` | No | Flagship reasoning for gated architecture and adversarial design analysis |
| `backend_developer` | `gpt-6-sol` / `medium` | Yes | Strong model with moderate reasoning for sustained Rust/SQL implementation; high is a task-specific override for a named correctness risk |
| `reviewer` | `gpt-6-sol` / `high` | No | Flagship independent integration and final-approval review |
| `release_engineer` | `gpt-6-luna` / `low` | Yes, narrowly | Fast, low-effort delivery work; raise effort only for a named migration-integrity, publication, or CI-security risk |

GPT-6 Astra is not a primary or project-agent model and is not an escalation path. Balanced uses
`gpt-6-sol` at medium effort: model capability and reasoning effort are separate settings, so this
keeps routine implementation capable without paying the high-effort cost on every request. High
effort remains bounded to architecture, which is spawned only behind its design/risk gate, and
independent review, which owns final approval. The project config caps spawned agents at two
concurrent threads, and the normal workflow remains sequential because overlapping writers and
implementation-aware final reviewers are prohibited.

[Official OpenAI model guidance](https://developers.openai.com/api/docs/guides/latest-model)
recommends explicitly tuning delegation and calibrating verification to the task. This repository
applies those harness principles with its own approved GPT-6 Sol/Luna model constraint and evidence
gates.

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
permission overrides take precedence and there are no per-folder read ACLs. In Claude Code, a
`write: false` role only loses the `Edit`, `Write`, and `NotebookEdit` tools; `Bash` stays available
for read-only inspection and is governed by the session permission rules, so a shell command could
still change files. The scopes below are therefore instruction-level controls for ownership and
context, not security boundaries, in either harness. The primary agent supplies the exact paths in
every task packet, does not spawn design/review roles under a write-enabling override, and verifies
that their handoffs introduced no worktree changes.

| Agent | Default read scope | Default write scope |
|---|---|---|
| `architect` | Relevant `docs/**`; root and crate manifests; `migrations/**` for DB work; affected public interfaces and tests | None |
| `backend_developer` | Assigned crates, tests, docs, manifests, lockfiles, and adjacent interfaces | Assigned implementation/test paths, manifests, lockfiles, and required adaptations |
| `reviewer` | Task packet, diff, changed hunks, named sections, callers/callees, manifests, migrations, and risk-focused tests | None |
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
runtime SQL, tests, Cargo manifests, lockfiles, and required code/test adaptations. The release
engineer exclusively owns versioned migration SQL, `atlas.sum`, CI, and release/delivery files when
assigned. The primary agent writes only team or task documentation and orchestration artifacts.
Read-only architecture or review work may run in parallel when genuinely independent, but
architecture must settle before implementation and review must inspect the completed diff.

New agents inherit the smallest useful history. Architecture and final review start from a bounded
packet without the parent conversation. An implementation fix returns to the existing owning writer
through `followup_task`; a fresh writer spawn needs a stated reason such as a lost thread or a new
packet. Long-running work compacts into a continuation packet containing only completed actions,
decisions, issue/branch/SHA identifiers, evidence, blockers, and the next goal. The project config
sets `model_auto_compact_token_limit` to 100000 so that packet is written before context grows large.

Waiting is not polling. Follow G8 in [`agent-workflow.md`](agent-workflow.md#5-workflow-and-review):
use one longest-timeout blocking wait for subagents or CI, and only re-check after a timeout.

## 6. Database and release escalation

For a database task, the primary agent asks the architect to define transactions, invariants,
locking, indexes, query-plan proof, forward migration, and compatibility. The backend developer
implements Rust, runtime SQL, and tests. The release engineer exclusively authors or updates the
versioned migration SQL, checksum, replay, and packaging from that approved design. The reviewer
then inspects the complete database diff as one behavioral change. Any later database or migration
fix receives focused re-review (P2).

The release engineer may prepare crates, release assets, CI, or tags, but assigned delivery work
never authorizes irreversible production changes. Any production command, including a production
migration, and any publish, push, or GitHub release action requires explicit user authorization.

## 8. Configuration files

`.agents/` is the harness-neutral source of truth for the agent team, and the `.codex/` and
`.claude/` files plus `CLAUDE.md` are generated outputs. A role or rule change edits the source and
re-runs `.agents/sync.sh all`; never edit a generated file by hand.

- `.agents/roles/<role>.md` defines one role: frontmatter `name`, `description`, `tier`
  (`flagship`, `balanced`, or `fast`), and `write` (`true` or `false`), followed by the shared
  instruction body.
- `.agents/harnesses/codex.toml` maps each tier to a Codex model and reasoning effort and holds
  Codex-only config, including the primary model, compaction limit, agent block, and plugins.
- `.agents/harnesses/claude.toml` maps tiers to generic Claude Code aliases (`opus`, `sonnet`,
  `haiku`), which resolve to the latest model in each family rather than a pinned version; it also
  adds read-only-role `disallowedTools` and holds the permission allowlist, including `Bash(rtk *)`.
- `.agents/skills/<name>/SKILL.md` holds skills shared by both harnesses.
- `.agents/sync.sh <codex|claude|all> [--check]` generates Codex and Claude role files, config,
  settings, `CLAUDE.md`, and skill links. `--check` regenerates into a temporary directory and fails
  on drift; the `prek` hook runs it.
- `.coderabbit.yaml` configures repository-aware review; GitHub CI owns hard file-count and
  executable quality gates.
- `AGENTS.md` contains the small routing policy loaded for every task; Claude loads it through the
  generated `CLAUDE.md`.

Start a new Codex or Claude Code task after changing these files so instructions, roles, and skills
load afresh. The loading check confirms models and efforts, the two-thread cap, bounded history, and
read-only architecture/reviewer defaults; it does not reuse implementation context for final review.

## 9. Lean flow and durable records

The useful workflow lessons from the earlier Reliar repository are retained without its project
management overhead:

| Lane | Expected size | Flow |
|---|---:|---|
| Patch | Usually 1 commit and at most 5 paths | One writer → one review (final if no edit) → one batched fix if needed → one fresh final review; at most two reviewer runs unless a blocker/high final finding opens G1's one extra fix-and-review round; a small-fix medium/low final finding is fixed and primary-verified without a reviewer → primary |
| Standard | At most 25 paths and normally 1–3 commits | Primary → architect only if gated → one writer, plus an exclusive-domain writer only if required → bounded [`agent-workflow.md` section 5](agent-workflow.md#5-workflow-and-review) review → primary |
| Multi-PR | More than 25 forecast paths | User-approved PR sequence; every PR follows normal targets and hard limits |

GitHub Issues and the linked GitHub Project are authoritative. Each repository change uses one
issue, one Codex task, one linked numbered branch, and normally one PR. Parent and sub-issues
coordinate multi-PR initiatives. The active packet is a transient handoff, not a second task record.

Each actionable issue has exactly one `type:*` label, all applicable `area:*` labels, and only
evidence-changing `risk:*` labels. `needs:*` and `blocked` represent exceptional triage conditions.
[`CONTRIBUTING.md`](../CONTRIBUTING.md#classify-the-issue) lists valid labels. Priority and normal
`Todo` → `In Progress` → `Done` lifecycle live only in GitHub Project fields.

### Token-efficiency evidence

Token cost is context size multiplied by request count. Record per role: spawns, requests per
thread, median and peak context per request, `wait_agent` polls, shell commands, and compactions.
Instruction-file size and reasoning-output share are secondary checks.

Baseline from the 2026-09-19/20 rollout logs, about 388M tokens over 175 threads; reasoning output
was 0.17% and instruction files under 2k tokens per thread:

- 29 of 45 reviewer spawns immediately followed another reviewer with no writer between; those
  runs consumed 48.9M of 86.1M reviewer tokens (57%), with chains of 10, 7, and 6. Two docs-only
  PRs (#33 and #35) used 16 reviewer spawns.
- Before #40, one primary made 206 `wait_agent` calls at 30–180 second timeouts. PR #47 reduced
  that to 9 calls and no timeouts, while another thread polled `gh pr checks 38` 24 times.
- Reviewer shell results added a median 1,258 tokens versus 158 for the developer. The 9% of
  results above 5k tokens contributed 47% of tool-added context; observed maxima were 7.8k for
  `git push`, 5.6k for a whole-document read, and 3.1k for unfiltered `gh pr view`.
- Agents touched `.sqlx/**` in 91 commands; 62 of PR #38's 89 paths were metadata. PR #47's
  five-path Cargo bump plus one test adaptation spawned two writers and four reviewers, consuming
  10.8M tokens across 11 threads.

| Role | Share | Measured detail |
|---|---:|---|
| Primary orchestrator | 33% | Historical baseline ran on `gpt-5.6-sol`; largest thread 628 requests, 206 `wait_agent` polls at a median one-minute gap, each resending 150–217k context |
| `backend_developer` | 30% | 22 spawns |
| `reviewer` | 22% | 45 spawns; worst run 137 requests, 108 shell commands, 2 compactions, 216k context |
| Guardian auto-review | 8% | Desktop Auto approval mode |
| `architect` | 1.3% | Gated spawns only |
| `release_engineer` | 0.5% | On demand only |

The owner records the same metrics on the next full issue flow and compares them here. That
post-merge Dependabot/feature comparison is follow-up evidence, not a gate for the policy change
that defines it. One owner action remains outside the repository: reconsider Desktop Auto review
or widen its sandbox allowlist.

This repository maintains no Markdown task cards, separate story files, second backlog, or routine
ADR stream. Issues record intent, PRs record review and validation, Git records the accepted change,
and normative documents record durable contracts and behavior. Update normative docs only for a
lasting guarantee, capability, schema invariant, ownership boundary, or non-goal. Create an ADR only
with explicit user approval for a rare irreversible decision that normative docs cannot express; an
owner-approved multi-PR effort may use one `docs/work/<slug>.md` plan.

Do not copy harness-specific commands or settings between harnesses. Add a project skill only when
a repeated workflow needs procedural detail that cannot stay concise in `AGENTS.md` and is not
already covered by normative docs. Completion gates remain in `docs/implementation-plan.md`.

Cross-cutting review remains concise and risk-based:

- **Security:** no unsafe code, secrets, sensitive payload/header/error leakage, untrusted-input
  unwraps, dynamic unparameterized SQL, implicit migrations, or unjustified dependency surface.
- **Performance:** bounded concurrency/queries/batches, no blocking runtime work or transaction
  across network I/O, benchmark evidence for changed hot paths, and real PostgreSQL plan evidence
  for changed queries or indexes.
- **Supply chain:** minimal features, direct dependency ownership, pinned CI/tooling, and the
  repository's `cargo deny` and release gates.
