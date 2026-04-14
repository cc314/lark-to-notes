# AGENTS.md — notes / lark-to-notes

> Operating manual for AI agents working in this repository.

This repo is both:

1. a live Obsidian-style notes vault, and
2. the planning and implementation home for the local **Lark-to-notes** workflow described in `plan_for_lark_to_notes_workflow.md`.

Agents must protect both the **vault's existing human-authored knowledge** and the **planned Python automation project** that will operate on it.

---

## RULE 0 — THE OVERRIDE PREROGATIVE

**The human's instructions override everything in this file.**

If the user explicitly tells you to do something, that instruction takes precedence over the defaults, workflows, and preferences below.

---

## RULE 1 — NO FILE DELETION

**Never delete a file or folder without explicit user permission.**

This includes:

- files you created yourself
- temporary-looking files inside the repo
- renames or moves that would effectively delete the original path
- cleanup of generated artifacts

If deletion seems useful, ask first.

---

## Irreversible Git & Filesystem Actions — Never Break Glass

The following are **absolutely forbidden** unless the user explicitly asks for the exact command and clearly accepts the irreversible consequences:

- `git reset --hard`
- `git clean -fd`
- `rm -rf`
- `git checkout -- <path>`
- `git restore --source ...`
- any equivalent destructive overwrite or delete command

Rules:

1. Never guess that a destructive command is safe.
2. Never use stash/revert/restore to "clean things up" around other agents' work.
3. Prefer inspection commands first: `git status`, `git diff`, `git log`, `git show`.
4. If destructive cleanup is ever requested, restate the exact impact before running it.

---

## Git Branch Policy — `main`, Never `master`

This project uses **`main`** as the canonical branch.

- All normal work happens on `main`
- Never create new work against `master`
- Never document `master` as the target branch
- Treat any `master` reference as legacy or a bug
- Do not create feature branches unless the human explicitly asks for one

If the current checkout is on another branch, do **not** switch branches on your own unless the user asks.

---

## Code Editing Discipline

### No Script-Based Code Changes

**Do not use scripts to mass-edit source files.**

That means no regex rewrite scripts, no Python/Node/Perl one-liners, and no sed-driven bulk transformations for code or structured notes.

Allowed:

- manual edits
- `apply_patch`
- tool-managed metadata updates through the tool itself, such as `br update` / `br sync`

### No File Proliferation

Revise existing files in place whenever possible.

Do **not** create variants like:

- `main_v2.py`
- `main_improved.py`
- `tasks_new.md`
- `index_rewrite.md`

New files must be genuinely new artifacts with a clear canonical purpose.

### Preserve Existing Human Work

This repo is often dirty. Assume unrelated edits are intentional unless proven otherwise.

- Never overwrite another agent's or the user's work
- Never revert files you did not author
- If another change conflicts with your task, stop and ask

---

## Multi-Agent Awareness

Multiple agents may work in this repo.

Rules:

1. **Never stash, revert, or overwrite** another agent's changes.
2. Before editing shared files, coordinate if Agent Mail is healthy.
3. Use Beads issue IDs as the work anchor when relevant.
4. If Agent Mail is unavailable, be extra conservative and avoid wide edits.

When working in shared files:

- prefer surgical patches
- read the current file first
- do not assume your view is the only active view

---

## Compiler, Type, and Validation Checks (CRITICAL)

After substantive code changes, verify you did not introduce errors.

For this project's planned Python stack, the default validation standard is:

```bash
# Lint
uv run ruff check .

# Format check (if configured)
uv run ruff format --check .

# Static typing
uv run mypy --strict src tests

# If the repo later adopts ty instead of mypy:
uv run ty check --strict

# Tests
uv run pytest
```

Additional rules:

- If you touch shell scripts, also run `shellcheck` and syntax checks if those tools already exist in the repo.
- If you touch only documentation or vault notes, code checks are not required.
- Do not invent a parallel toolchain if the repo already defines one.

---

## Testing Practice

Testing should follow the project plan, not ad hoc intuition.

Minimum expected categories once the implementation exists:

1. unit tests for normalization, fingerprinting, heuristics, and rendering primitives
2. integration tests for `lark-cli` adapters and source normalization
3. replay and idempotency tests
4. golden-file tests for raw, daily, and Current Tasks rendering
5. locking and single-writer tests
6. failure-path tests for malformed payloads, retries, and quarantine
7. redaction and sensitive-source tests

For feature work:

- test the happy path
- test the main edge cases
- test the important failure mode

For backlog and planning changes:

- validate the graph with `br` / `bv`
- keep `.beads/issues.jsonl` in sync when Beads changes are committed

---

## Development Workspace Hygiene

This repo is a live vault. Keep it tidy.

### No Scratch-File Sprawl

- Do not create throwaway markdown files in the repo for plans or notes
- Use the session `plan.md` in the session folder for working notes
- Use `/tmp` or the session artifact folder for temporary outputs

### Avoid Unnecessary UI State Churn

Do not edit `.obsidian/` files unless the task specifically requires it.

### `.beads/` Commit Rules

Commit only the durable Beads artifacts:

- `.beads/.gitignore`
- `.beads/config.yaml`
- `.beads/issues.jsonl`
- `.beads/metadata.json`

Do **not** commit:

- `.beads/beads.db`
- `.beads/*.db-wal`
- `.beads/*.db-shm`
- `.beads/.br_history/`
- `.beads/.sync.lock`
- `.beads/last-touched`

---

## Third-Party Library Usage

If you are not fully sure how to use a third-party library or tool:

1. read the local docs if they exist
2. check the latest upstream docs
3. prefer the current best practice over stale memory

Do not fake certainty about external libraries.

---

## What This Project Is

The target project is a **local Lark-to-notes workflow** that:

1. ingests selected Lark DMs, groups, documents, comments, and replies
2. normalizes them into durable internal records
3. stores append-only raw history for replay and audit
4. distills likely work items with deterministic heuristics first
5. maintains stable task identity and a visible `needs_review` lane
6. writes safe machine-owned updates into the notes vault
7. supports structured feedback and replay-driven refinement
8. remains operable, observable, and cost-aware in daily use

### Doing a Good Job Here Means

You are doing a good job when you:

- preserve provenance from every surfaced task back to source material
- protect user-authored vault content outside machine-owned blocks
- avoid duplicate notes, duplicate tasks, and unsafe rewrites
- keep replay and retry behavior idempotent
- prefer explicit, debuggable data flow over magical automation
- keep the backlog and docs aligned with the actual implementation state

---

## Current Architecture Direction

The implementation plan in `plan_for_lark_to_notes_workflow.md` is the normative project guide.

### Core V1 Shape

- Python-first implementation
- SQLite as the single local system of record
- deterministic heuristics first, selective LLM escalation second
- one serialized note writer
- plannotator + Markdown + YAML sidecar as the initial review workflow

### Planned Functional Layers

1. source access and fixture corpus
2. watched-source governance, source model, and durable schema
3. intake ledger, raw capture, and replay semantics
4. action-item generation, stable task identity, and review-lane lifecycle
5. vault-safe rendering and promotion flow
6. structured feedback and plannotator review flow
7. runtime operations, locking, and reconciliation
8. performance controls and LLM budget policy
9. explicit operator CLI surface
10. end-to-end validation harness and acceptance checks

---

## Repo Layout

### Current Repo Layout

```text
notes/
├── AGENTS.md
├── plan_for_lark_to_notes_workflow.md
├── .beads/
├── area/
├── automation/
├── daily/
├── meetings/
├── people/
├── raw/
├── templates/
├── index.md
├── projects.md
├── environment.md
├── goal.md
└── system.md
```

### Planned Code Layout

When implementation lands, prefer this structure from the plan:

```text
src/lark_to_notes/
├── cli/
├── config/
├── distill/
├── feedback/
├── intake/
├── render/
├── runtime/
├── storage/
└── tasks/

tests/
scripts/
var/
```

---

## Vault Maintenance Contract

This vault is maintained as a wiki for both human reading and agent operation.

### Canonical Locations

- Put new source material in `raw/` first.
- Use `daily/YYYY-MM-DD.md` for daily tasks, follow-ups, and short-lived context.
- Use `meetings/YYYY-MM-DD - subject.md` for actual meetings and only for discussion threads that deserve a standalone record.
- Use `people/<Name>/index.md` for stakeholder pages.
- Use `people/index.md` as the curated directory for navigating a larger people graph.
- Use `area/current tasks/index.md` as the durable open-work list for active work, project items, and follow-ups promoted out of daily notes.
- Use `projects/<Project>/index.md` for durable project pages.
- Use `area/<Topic>/index.md` for durable topic pages.

### Note Creation Rules

- Prefer updating an existing note over creating a near-duplicate.
- Keep one canonical note per entity or event and link to it from related notes.
- Keep org structure on people notes by default using fields such as `departments`, `reports_to`, and `reports`.
- Do not create a separate org/entity hierarchy unless the org unit needs durable non-person context.
- Do not create a project note unless the source clearly refers to a real project.
- Do not create a meeting note for a simple chat follow-up that fits cleanly in the daily note.
- For assets such as screenshots, PDFs, sheets, or documents, add a nearby `.md` context note in `raw/`.
- Preserve user-authored content unless the task explicitly requires restructuring it.

### Metadata Rules

- All curated notes should have `type`, `created`, `updated`, and `tags`.
- Daily and meeting notes should also carry `date`, `people`, `projects`, and `areas`.
- Raw notes should also carry `source`, `author`, and `published`.
- People notes should also carry `aliases` and `role`.
- Project notes should also carry `status` and `stakeholders`.
- Use Obsidian wikilinks inside both body content and frontmatter list properties when linking related notes.

### Maintenance Workflow

1. Capture or summarize new information in `raw/`, `daily/`, or `meetings/`.
2. Distill actionable items from messages and source material into the relevant daily note first.
3. Promote still-open work, project items, and follow-ups into `area/current tasks/index.md`.
4. Roll durable context into the relevant `people/`, `projects/`, and `area/` pages when it is more than a one-off daily task.
5. Add backlinks across the source note and the durable notes.
6. Update `updated` on substantive edits.
7. Keep `people/index.md` current as the main directory for person notes once the people graph grows beyond quick scanning in the root index.
8. Keep `index.md` current enough to serve as the vault entry point, but prefer a selective set of high-value links rather than dumping every person note there.

---

## Notes Automation Constraints

For the planned Lark-to-notes system:

1. raw source material lands in `raw/` first
2. distilled action items land in the relevant daily note first
3. still-open work is promoted into `area/current tasks/index.md`
4. broader `people/`, `projects/`, and `area/` automation stays manual or opt-in until core safety is proven
5. automatic writes happen only inside explicit machine-owned blocks
6. user-authored content outside those blocks must be preserved

This is the trust boundary. Do not cut corners here.

---

## Tooling Reference — Prepared Blurbs

These blurbs are meant to be reusable operating notes for agents.

### MCP Agent Mail — Coordination, File Reservations, and Threads

**Purpose:** coordinate multiple agents, prevent edit collisions, and keep asynchronous project communication out of the main context window.

**Use it for:**

- session bootstrap
- file reservations before editing shared files
- threaded progress updates
- handoffs between agents

**Preferred workflow:**

1. bootstrap session
2. reserve files before editing
3. send or reply in a thread keyed to the work item
4. release reservations when done

**Health check:**

```bash
command -v am
am --help
curl http://127.0.0.1:8765/api/health
curl http://127.0.0.1:8765/health/readiness
cd ~/mcp_agent_mail && uv run python -m mcp_agent_mail.cli doctor check --verbose
```

**Current local status:** healthy and reachable on this machine.

- `am` is present in `PATH` at `/Users/chenchao/.local/bin/am`
- `am` is a thin launcher that `cd`s into `~/mcp_agent_mail` and runs `uv run python -m mcp_agent_mail.cli serve-http`
- the live server is already running on `127.0.0.1:8765`
- health endpoints respond at `/api/health`, `/health/liveness`, and `/health/readiness`
- the MCP transport is mounted at `/mcp/`
- the full maintenance CLI works when invoked from the checkout with `uv run python -m mcp_agent_mail.cli ...`
- direct `python3 -m mcp_agent_mail.cli ...` from unrelated directories fails because the package is not installed into the system interpreter, which is expected for this local checkout-based setup

**Implication:** Agent Mail is available for coordination here, but use the modern endpoints and remember that `am` is the server-launch wrapper while the full admin CLI lives in `~/mcp_agent_mail`.

### Beads (`br`) — Dependency-Aware Backlog Management

**Purpose:** track work as a dependency graph in `.beads/`, including priorities, status, and explicit blockers.

**Use it for:**

- creating or updating issues
- dependency management
- closing merged or completed work
- syncing durable issue state to `.beads/issues.jsonl`

**Core rules:**

- always prefer `--json` for agent use
- always run `br sync --flush-only` after meaningful changes
- never assume `br` performs git actions

**Health check:**

```bash
br --version
br doctor
br dep cycles
```

**Current local status:** usable but degraded.

- `br` is installed (`0.1.38`)
- issue data is readable and writable
- `br ready --json` and `br dep cycles` both work on this repo
- `br doctor` currently reports a stale `blocked_issues_cache` anomaly
- SQLite integrity warnings are also present

**Implication:** use `br`, but validate graph reasoning with `bv` when in doubt.

### `bv` — Graph-Aware Triage and Priority Analysis

**Purpose:** analyze the Beads graph for critical path, bottlenecks, priority alignment, and execution order.

**Use it for:**

- triage
- topological execution planning
- bottleneck detection
- graph-health inspection

**Core rule:** **never run bare `bv`**. Use only `--robot-*` modes.

**Health check:**

```bash
bv --version
bv --robot-triage
bv --robot-insights
```

**Current local status:** healthy in robot mode on this machine.

- `bv` is installed (`v0.15.2`)
- `bv --robot-triage` returns current planning output for this repo
- `bv --robot-insights` runs successfully

**Implication:** safe to use for graph triage and backlog analysis as long as you stay in `--robot-*` mode.

### `cass` — Session Search and Prompt Archaeology

**Purpose:** search prior agent sessions for prompts, decisions, and repeated working patterns.

**Use it for:**

- "how did we do this before?"
- finding prior prompts or design decisions
- mining repeated workflows

**Recommended usage:**

```bash
cass status --json
cass index --json
cass search "keyword" --workspace /abs/path --json --fields minimal --limit 50
```

**Current local status:** healthy for lexical search, but not for semantic search.

- binary is installed (`0.2.7`)
- `cass index --json` completes successfully
- `cass status --json` is healthy after refresh and reports a fresh lexical index
- semantic model is missing

**Implication:** use `cass` normally for lexical session archaeology, and only treat semantic search as unavailable until its model is installed.

### `ubs` — Unified Static/Bug Scanning Meta-Runner

**Purpose:** run broader static scans and language-aware bug checks across the repo or staged diff.

**Use it for:**

- staged scans before commit
- quick diff scans
- cross-language static issue discovery

**Typical commands:**

```bash
ubs --staged
ubs --diff
ubs doctor
```

**Current local status:** usable with cache warnings.

- binary is present (`UBS Meta-Runner v5.0.7`)
- `ubs doctor` runs successfully on this machine
- multiple language modules and helper artifacts are not cached yet, so doctor reports warnings until they are downloaded

**Implication:** UBS is available for advisory scans, but expect first-use cache misses and incomplete language coverage until the missing modules are prefetched.

### `dcg` — Destructive Command Guard

**Purpose:** provide mechanical guardrails around destructive shell commands so agents do not casually execute irreversible operations.

**Use it for:**

- explaining why a destructive command is blocked
- dry-running whether a risky command would be blocked
- checking local guard health before relying on it
- reinforcing the repo's explicit prohibition on commands such as `git reset --hard`, `git clean -fd`, and `rm -rf`

**Preferred commands:**

```bash
dcg doctor
dcg explain "git reset --hard HEAD"
dcg test "git reset --hard HEAD"
```

**Current local status:** healthy on this machine.

- `dcg` is installed (`v0.4.0`)
- `dcg doctor` passes all checks
- hook wiring and configuration are present
- 18 pattern packs are enabled
- `dcg test "git reset --hard HEAD"` correctly flags the command as destructive

**Implication:** treat `dcg` as the default safety layer for risky shell work rather than as an optional extra.

---

## Health Recheck Commands

Before depending on these tools heavily, re-run:

```bash
command -v am
am --help
curl http://127.0.0.1:8765/api/health
curl http://127.0.0.1:8765/health/readiness
cd ~/mcp_agent_mail && uv run python -m mcp_agent_mail.cli doctor check --verbose
br doctor
br dep cycles
bv --version
bv --robot-triage
cass index --json
cass status --json
ubs doctor
dcg doctor
dcg test "git reset --hard HEAD"
```

---

## Final Standard

Agents working here should optimize for:

1. safety over speed
2. provenance over cleverness
3. canonical updates over duplicate note creation
4. replayable, inspectable systems over magic
5. disciplined coordination over solo-agent assumptions

If you are uncertain, stop, inspect more context, and choose the conservative path.
