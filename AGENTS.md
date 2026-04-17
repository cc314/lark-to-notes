# AGENTS.md — notes / lark-to-notes

> Operating manual for AI agents working in this repository.

This repo is both:

1. a live Obsidian-style notes vault, and
2. the planning and implementation home for the local **Lark-to-notes** workflow described in `plan_for_lark_to_notes_workflow.md`.

Agents must protect both the **vault's existing human-authored knowledge** and the **planned Python automation project** that will operate on it.

---

## RULE 0 - THE FUNDAMENTAL OVERRIDE PREROGATIVE

If I tell you to do something, even if it goes against what follows below, YOU MUST LISTEN TO ME. I AM IN CHARGE, NOT YOU.

---

## RULE NUMBER 1: NO FILE DELETION

**YOU ARE NEVER ALLOWED TO DELETE A FILE WITHOUT EXPRESS PERMISSION.** Even a new file that you yourself created, such as a test code file. You have a horrible track record of deleting critically important files or otherwise throwing away tons of expensive work. As a result, you have permanently lost any and all rights to determine that a file or folder should be deleted.

**YOU MUST ALWAYS ASK AND RECEIVE CLEAR, WRITTEN PERMISSION BEFORE EVER DELETING A FILE OR FOLDER OF ANY KIND.**

---

## Irreversible Git & Filesystem Actions — DO NOT EVER BREAK GLASS

1. **Absolutely forbidden commands:** `git reset --hard`, `git clean -fd`, `rm -rf`, or any command that can delete or overwrite code/data must never be run unless the user explicitly provides the exact command and states, in the same message, that they understand and want the irreversible consequences.
2. **No guessing:** If there is any uncertainty about what a command might delete or overwrite, stop immediately and ask the user for specific approval. "I think it's safe" is never acceptable.
3. **Safer alternatives first:** When cleanup or rollbacks are needed, request permission to use non-destructive options (`git status`, `git diff`, `git stash`, copying to backups) before ever considering a destructive command.
4. **Mandatory explicit plan:** Even after explicit user authorization, restate the command verbatim, list exactly what will be affected, and wait for a confirmation that your understanding is correct. Only then may you execute it—if anything remains ambiguous, refuse and escalate.
5. **Document the confirmation:** When running any approved destructive command, record (in the session notes / final response) the exact user text that authorized it, the command actually run, and the execution time. If that record is absent, the operation did not happen.

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

# Reactions CI aggregation (ruff, format, mypy, pytest, reaction E2E harness, doctor subset)
uv run python scripts/ci_reactions_gate.py
```

Additional rules:

- If you touch shell scripts, also run `shellcheck` and syntax checks if those tools already exist in the repo.
- If you touch only documentation or vault notes, code checks are not required.
- Do not invent a parallel toolchain if the repo already defines one.

---
## CODE REVIEW
After completing each bead, do a "fresh eye" self-review before moving to the next one.

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


<!-- br-agent-instructions-v1 -->

---

## Beads Workflow Integration

This project uses [beads_rust](https://github.com/Dicklesworthstone/beads_rust) (`br`/`bd`) for issue tracking. Issues are stored in `.beads/` and tracked in git.

### Essential Commands

```bash
# Health and reconciliation
br doctor
br doctor --repair
br sync --status      # Check whether DB and JSONL diverged
br sync --import-only # Import newer JSONL into DB safely

# View ready issues (open, unblocked, not deferred)
br ready              # or: bd ready

# List and search
br list --status=open # All open issues
br show <id>          # Full issue details with dependencies
br search "keyword"   # Full-text search

# Create and update
br create --title="..." --description="..." --type=task --priority=2
br update <id> --status=in_progress
br close <id> --reason="Completed"
br close <id1> <id2>  # Close multiple issues at once

# Sync with git
br sync --flush-only  # Export DB to JSONL

# Graph validation
br dep cycles
```

### Workflow Pattern

1. **Health check**: Run `br doctor` and `br sync --status`.
2. **Reconcile first**: If `br doctor` reports stale blocked cache or recoverable anomalies, run `br doctor --repair` before more writes. If JSONL is newer, run `br sync --import-only`.
3. **Start**: Run `br ready` to find actionable work.
4. **Claim**: Use `br update <id> --status=in_progress`.
5. **Work**: Implement the task.
6. **Complete**: Use `br close <id>`.
7. **Sync**: Always run `br sync --flush-only` at session end once health and sync state are clean.

### Key Concepts

- **Dependencies**: Issues can block other issues. `br ready` shows only open, unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers 0-4, not words)
- **Types**: task, bug, feature, epic, chore, docs, question
- **Blocking**: `br dep add <issue> <depends-on>` to add dependencies
- **Graph hygiene**: Do not add a **`blocks`** edge from a **parent epic** to a **downstream child** you intend to finish *after* earlier siblings (e.g. epic → last milestone). `br` propagates a parent’s open blockers to descendants, which freezes `br ready` (child N cannot start while the parent is “blocked by” child N+k). Prefer **child → child** `blocks` chains for sequencing; close the epic when the subtree is done.

### Session Protocol

**Before ending any session, run this checklist:**

```bash
git status              # Check what changed
git add <files>         # Stage code changes
br sync --flush-only    # Export beads changes to JSONL
git commit -m "..."     # Commit everything
git push                # Push to remote
```

### Best Practices

- Check `br ready` at session start to find available work
- Update status as you work (in_progress → closed)
- Create new issues with `br create` when you discover tasks
- Use descriptive titles and set appropriate priority/type
- Prefer normal DB-backed `br` usage for day-to-day work; use `--no-db` only for deliberate JSONL-only edits or recovery investigation
- After any `--no-db` mutation, reconcile back into the DB with `br sync --import-only` before returning to ordinary DB-backed work
- Treat `blocked_issues_cache is marked stale and needs rebuild` as a real stop-and-repair warning, not background noise
- After `br` upgrades, repair attempts, or DB rebuilds, validate behavior in a disposable `/tmp` workspace before trusting changed assumptions
- Always sync before ending session

### Malformed SQLite DB (`database disk image is malformed`, bad indexes)

This workspace has seen **SQLite page/index corruption** in `.beads/beads.db` (for example `idx_issues_list_active_order` out of order or “row N missing from index”) while **`issues.jsonl` stayed valid**. JSONL remains the **recovery source of truth**; the DB is rebuildable from it.

**Prevention (reduces risk; not a guarantee):**

- Obey **`br` Operational Guardrails** below—especially **no parallel `br` writes**, no huge chained mutating batches, and do not poke `beads.db` with ad-hoc `sqlite3` while `br` (or another agent) is using the same workspace.
- After **`br doctor --repair`**, **`br sync --import-only`**, or a **`br` upgrade**, run **`br doctor`** before doing more writes.
- Treat **`WARN db.sidecars`** (WAL/SHM files next to `beads.db`) as a signal to **avoid concurrent DB users** and to **re-check** `br doctor` if anything still looks off.

**When you see it (typical `br doctor` lines):**

- `ERROR sqlite.integrity_check` / `ERROR sqlite3.integrity_check` mentioning **malformed** disk image or a specific **index** name.
- `br doctor --repair` may report **“Repair complete: imported …”** but **post-repair verification still fails** on the **same index**—that means the DB was rebuilt from JSONL but an index btree is still bad.

**Fix (non-destructive first; run from repo root):**

1. **`br doctor`** and **`br sync --status`** — confirm JSONL parses and counts match expectations.
2. **`br doctor --repair`** — re-import from `issues.jsonl` into the DB.
3. If integrity errors **name an index** (often `idx_issues_list_active_order`), rebuild **only that index** from table data, then verify:

   ```bash
   sqlite3 .beads/beads.db "REINDEX idx_issues_list_active_order; PRAGMA integrity_check;"
   ```

   Use the **exact index name** from the `br doctor` / `sqlite3` error if it differs in a future `br` version.
4. **`br doctor`** again until **`OK sqlite.integrity_check`** and **`OK sqlite3.integrity_check`**.

If **`REINDEX`** fails or **`PRAGMA integrity_check`** still does not return a single `ok`, stop mutating beads, treat **`issues.jsonl`** as authoritative, and escalate to a human (any step that removes or replaces `beads.db` / WAL/SHM files is **filesystem deletion**—requires **explicit permission** under Rule Number 1).

### `br` Operational Guardrails

Treat `br` as a stateful local tool, not as a perfectly transactional graph API.

- Do not run parallel `br` write operations.
- Do not chain large batches of `br create`, `br update`, or `br dep add`; prefer small sequential writes.
- Do not mix DB-backed mode and `--no-db` mode in the same editing session unless you intentionally want JSONL-only work.
- Do not assume a failed `br` command rolled back cleanly; treat writes as partially successful until verified.
- Do not ignore `database is busy`, stale DB or JSONL warnings, or odd nested-ID lookup failures.
- Do not ignore `db.recoverable_anomalies: blocked_issues_cache is marked stale and needs rebuild`; repair before more mutation-heavy work.
- Do not keep mutating after `br show` works but `br update` or `br close` says the same issue is missing; assume DB inconsistency and investigate.
- Do not treat a closed upstream GitHub issue as proof the whole failure family is solved; verify locally in `/tmp` before changing workflow assumptions.
- Do not force `br sync --flush-only` if it warns that export would lose issues; inspect and reconcile first.
- Do not put shell-sensitive backticks or complex interpolation directly into `br create` or `br update` arguments; use careful quoting.
- Do not trust long dependency-edit chains blindly; verify the touched issues after each small batch.
- Re-run `br dep cycles` frequently while reshaping the graph.
- If DB-backed output disagrees with `--no-db`, treat `issues.jsonl` as the recovery truth and stop further writes until you reconcile.

### Safer `br` Write Pattern

1. Run `br doctor`.
2. Run `br sync --status`.
3. If `br doctor` reports stale blocked cache or other recoverable anomalies, run `br doctor --repair` before making more edits.
4. If JSONL is newer, run `br sync --import-only` before making more edits.
5. Create parent issues first, then child issues, then dependency edges.
6. Make small sequential writes and verify after each batch with `br show`, `br dep list`, or `br list --json`.
7. Run `br dep cycles` before and after dependency-heavy edits.
8. If anything feels inconsistent, stop and verify with `br show <id>`, `br show <id> --no-db`, and, when needed, `sqlite3 .beads/beads.db "PRAGMA integrity_check;"`. If the error names a bad **index**, use **Malformed SQLite DB** (`REINDEX` that index).
9. Only run `br sync --flush-only` once the graph is stable and sync state is healthy.

### `br` Recovery And Upgrade Discipline

When `br` starts behaving strangely, prefer a short explicit recovery loop over more experimentation:

1. Stop further writes.
2. Run `br doctor` and `br sync --status`.
3. If you see stale blocked cache or recoverable anomalies, run `br doctor --repair`.
4. If JSONL is newer, run `br sync --import-only`.
5. If **`br doctor` still reports SQLite integrity / malformed index errors** after repair, follow **Malformed SQLite DB** above (`REINDEX` the named index, then re-run `br doctor`).
6. If DB-backed commands still disagree with `--no-db`, treat the DB as suspect and keep JSONL as the recovery source of truth.
7. After a repair or `br` upgrade, validate the claimed fix in `/tmp` before trusting the live repo workspace.

Minimal `/tmp` smoke tests worth re-running after upgrades:

- dotted-ID `show` and `update`
- one `--no-db` mutation followed by `br sync --import-only`
- `br doctor` after ordinary writes to see whether blocked-cache warnings still appear

<!-- end-br-agent-instructions -->

<!-- bv-agent-instructions-v2 -->

### Using bv as an AI sidecar

bv is a graph-aware triage engine for Beads projects (.beads/beads.jsonl). Instead of parsing JSONL or hallucinating graph traversal, use robot flags for deterministic, dependency-aware outputs with precomputed metrics (PageRank, betweenness, critical path, cycles, HITS, eigenvector, k-core).

**Scope boundary:** bv handles *what to work on* (triage, priority, planning). `br` handles creating, modifying, and closing beads.

**CRITICAL: Use ONLY --robot-* flags. Bare bv launches an interactive TUI that blocks your session.**

#### The Workflow: Start With Triage

**`bv --robot-triage` is your single entry point.** It returns everything you need in one call:
- `quick_ref`: at-a-glance counts + top 3 picks
- `recommendations`: ranked actionable items with scores, reasons, unblock info
- `quick_wins`: low-effort high-impact items
- `blockers_to_clear`: items that unblock the most downstream work
- `project_health`: status/type/priority distributions, graph metrics
- `commands`: copy-paste shell commands for next steps

```bash
bv --robot-triage        # THE MEGA-COMMAND: start here
bv --robot-next          # Minimal: just the single top pick + claim command

# Token-optimized output (TOON) for lower LLM context usage:
bv --robot-triage --format toon
```

#### Other bv Commands

| Command | Returns |
|---------|---------|
| `--robot-plan` | Parallel execution tracks with unblocks lists |
| `--robot-priority` | Priority misalignment detection with confidence |
| `--robot-insights` | Full metrics: PageRank, betweenness, HITS, eigenvector, critical path, cycles, k-core |
| `--robot-alerts` | Stale issues, blocking cascades, priority mismatches |
| `--robot-suggest` | Hygiene: duplicates, missing deps, label suggestions, cycle breaks |
| `--robot-diff --diff-since <ref>` | Changes since ref: new/closed/modified issues |
| `--robot-graph [--graph-format=json\|dot\|mermaid]` | Dependency graph export |

#### Scoping & Filtering

```bash
bv --robot-plan --label backend              # Scope to label's subgraph
bv --robot-insights --as-of HEAD~30          # Historical point-in-time
bv --recipe actionable --robot-plan          # Pre-filter: ready to work (no blockers)
bv --recipe high-impact --robot-triage       # Pre-filter: top PageRank scores
```

### br Commands for Issue Management

```bash
br ready              # Show issues ready to work (no blockers)
br list --status=open # All open issues
br show <id>          # Full issue details with dependencies
br create --title="..." --type=task --priority=2
br update <id> --status=in_progress
br close <id> --reason="Completed"
br close <id1> <id2>  # Close multiple issues at once
br sync --flush-only  # Export DB to JSONL
```

### Workflow Pattern

1. **Triage**: Run `bv --robot-triage` to find the highest-impact actionable work
2. **Claim**: Use `br update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: Use `br close <id>`
5. **Sync**: Always run `br sync --flush-only` at session end

### Key Concepts

- **Dependencies**: Issues can block other issues. `br ready` shows only unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers 0-4, not words)
- **Types**: task, bug, feature, epic, chore, docs, question
- **Blocking**: `br dep add <issue> <depends-on>` to add dependencies

### Session Protocol

```bash
git status              # Check what changed
git add <files>         # Stage code changes
br sync --flush-only    # Export beads changes to JSONL
git commit -m "..."     # Commit everything
git push                # Push to remote
```

<!-- end-bv-agent-instructions -->


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

## Final Standard

Agents working here should optimize for:

1. safety over speed
2. provenance over cleverness
3. canonical updates over duplicate note creation
4. replayable, inspectable systems over magic
5. disciplined coordination over solo-agent assumptions

If you are uncertain, stop, inspect more context, and choose the conservative path.






---


