# Lark-to-Notes

![Python](https://img.shields.io/badge/python-3.12%2B-blue)
![License](https://img.shields.io/badge/license-proprietary-lightgrey)
![Status](https://img.shields.io/badge/status-local%20prototype-orange)

Turn captured Lark conversations into a local SQLite-backed task pipeline for an Obsidian-style notes vault.

This repository is both:

- a Python implementation of a local `lark-to-notes` operator CLI
- the planning home for the larger workflow described in [`plan_for_lark_to_notes_workflow.md`](plan_for_lark_to_notes_workflow.md)

Quick setup:

```bash
uv sync --dev
uv run lark-to-notes --help
```

Current reality, in one sentence: the offline replay -> classify -> render workflow is implemented and tested; the repo also ships an in-tree mixed poll/event chat-intake ledger, live polling via `lark-cli`, and a stdin `sync-events` path for `im.message.receive_v1` NDJSON from `lark-cli event +subscribe`, all writing into the same canonical SQLite store.

```text
JSONL capture / fixture corpus
            |
            v
      SQLite local store
  (sources, checkpoints, raw,
   tasks, feedback, budget)
            |
            v
 heuristics-first distillation
   + optional LLM routing hooks
            |
            v
   vault-safe Markdown rendering
 (machine-owned blocks only)
```

## TL;DR
### The Problem

Important work arrives in Lark chats, docs, comments, and follow-ups, but it is easy for that context to stay trapped in conversations or be copied into notes inconsistently.

### The Solution

`lark-to-notes` gives that workflow a local system of record:

- ingest raw message-like records into SQLite
- classify them with deterministic English/Chinese/mixed-language heuristics
- keep stable task identity via fingerprints
- render results into an Obsidian-style vault without clobbering nearby human text
- import structured feedback back into the task registry

### Why Use It?

| Capability | What it does now | Why it matters |
|---|---|---|
| Local-first storage | Uses SQLite as the system of record | Replay, audit, and debugging stay cheap and inspectable |
| Idempotent replay | Replays JSONL with `INSERT OR IGNORE` semantics | Safe reruns do not duplicate raw records |
| Multilingual heuristics | Matches English, Chinese, and mixed-language task cues | Good first-pass recall without mandatory LLM calls |
| Stable task identity | Hashes normalized text + source + week bucket | Repeated asks update the same task instead of exploding duplicates |
| Vault-safe rendering | Writes only machine-owned blocks with `ltn:` markers | Human-authored Markdown outside those blocks is preserved |
| Feedback loop | Imports YAML sidecars into `feedback_events` and task overrides | Human review changes later behavior without manual DB edits |
| Budget accounting | Tracks fallbacks, cache hits, token fields, and latency hooks | LLM usage can stay observable and optional |

### Implementation Status

| Area | Status | Notes |
|---|---|---|
| Offline replay/classify/render loop | Implemented | Verified locally with the checked-in fixture corpus and `uv run pytest` |
| Raw notes and Current Tasks rendering | Implemented | Writes `raw/` notes and `area/current tasks/index.md` |
| Daily-note rendering | Partially wired | Renderer exists, but the current CLI render path does not hydrate `event_date`, so daily-note output is not yet end-to-end in the offline flow |
| Feedback import | Implemented | YAML sidecars update tasks and persist feedback events |
| LLM routing | Interface ready | Budgeting, cache, and fallback logic exist, but no provider implementation ships in-tree |
| Mixed chat intake ledger | Implemented in-tree | Poll and `im.message.receive_v1` events converge on one canonical SQLite intake row with coalescing; pipe `lark-cli event +subscribe` NDJSON into `sync-events` |
| Live sync/backfill | In-repo (`lark-cli`) | `sync-once`, `sync-daemon`, and `backfill` load the worker-style JSON config and run `lark_to_notes.live.chat_live.ChatLiveAdapter` into the canonical SQLite store (requires `lark-cli` and Lark auth) |
| IM emoji reactions | Planned (beads `lw-pzj.*`) | Event types, schema, vault blocks, and distillation rules are specified in [`plan_for_lark_to_notes_workflow.md`](plan_for_lark_to_notes_workflow.md) (see **Message reaction ingestion** and the traceability matrix). `sync-events` today accepts only `im.message.receive_v1`; reaction envelopes are not ingested yet. |

### IM reactions — privacy, retention, and semantics (operator defaults)

These bullets summarize normative **policy** for the reaction epic; they intentionally stay short. Full requirements and code anchors are in the plan’s **Message reaction ingestion** section and traceability matrix.

1. **Sensitive fields:** Treat operator identity fields inside reaction payloads (for example `open_id`, `user_id`, `union_id` when present) as **sensitive PII** for storage, logs, and vault text. Prefer **aggregates** (counts, emoji histograms) in rendered `raw/` notes unless an explicit, versioned configuration widens disclosure.
2. **Retention / deletes:** A platform “reaction removed” event is stored as its own **append-only** row; the ledger does not pretend earlier adds never happened. Effective “who has reacted now?” state is always **derived** from ordered add/remove history or an equivalent deterministic summary.
3. **Emoji meaning:** Heuristics stay **conservative**; politeness or ambiguous reacts must not silently promote work. Anything that can surface a **task without supporting message text** remains **opt-in**, default off, and must record **`policy_version`** when enabled.
4. **Explicit non-goals (near term):** No cross-tenant reaction analytics, no automatic assignment of humans from emoji alone, no mutating Lark reactions from this tool, and no emoji sentiment models unless a dedicated future issue expands scope.

### IM reactions — Lark scopes and capability matrix (operator checklist)

Scopes and product behavior **vary by tenant** (especially whether a bot may sit in the target DM/group). Treat this table as the **default checklist** for Feishu/Lark IM reaction capture; adjust with your tenant admin when doctor or `lark-cli` reports `Permission denied`.

| Goal | Typical event or call | Scopes / prerequisites | If missing |
| --- | --- | --- | --- |
| Receive chat message events | `im.message.receive_v1` on `lark-cli event +subscribe` | IM message read scopes as required by your app type (often **`im:message`**, **`im:message.group_at_msg`**, or tenant-specific bundles) | `sync-events` ingests nothing useful; doctor should surface “no permission” vs “no traffic”. |
| Receive reaction create/delete | `im.message.reaction.created_v1`, `im.message.reaction.deleted_v1` (same NDJSON pipe) | **`im:message.reactions:read`** (name may differ slightly by platform version) plus the same chat membership constraints as messages | **Capability blocker:** do not assume an empty reaction table means “no reacts”; verify scope and bot placement. |
| Historical reaction lists | REST / `lark-cli` message or reaction list APIs (backfill) | Same reaction read scopes; often **pagination** and stricter rate caps | Partial backfill is expected; operator docs must describe deferral (see plan §5). |
| Bot-in-chat | All of the above | Bot user must be allowed in the chat; some orgs forbid bots in DMs | Documented **non-code** blocker; no amount of local SQLite work fixes missing membership. |

**Machine-checkable direction:** keep the left column stable in docs; wire **doctor / preflight** (`lw-pzj.9.1`, `lw-pzj.14.3`) to emit explicit JSON keys for “missing scope”, “bot not in chat”, and “API not exposed for tenant” rather than collapsing to zero counts.

## Quick Example

This is the most reliable self-contained demo path in the current repo. It uses the checked-in fixture corpus rather than requiring live Lark credentials.

```bash
uv sync --dev
mkdir -p demo-vault

uv run lark-to-notes replay \
  --db var/demo.db \
  --raw-dir tests/fixtures/lark-worker \
  --json

uv run lark-to-notes reclassify \
  --db var/demo.db \
  --limit 25 \
  --json

uv run lark-to-notes render \
  --db var/demo.db \
  --vault-root demo-vault \
  --limit 25 \
  --json

uv run lark-to-notes doctor \
  --db var/demo.db \
  --fixture-corpus tests/fixtures/lark-worker/fixture-corpus \
  --json

uv run lark-to-notes budget status \
  --db var/demo.db \
  --json
```

What to expect:

- `replay` ingests the checked-in JSONL corpus into SQLite
- `reclassify` creates task records from raw messages
- `render` writes raw provenance notes plus `demo-vault/area/current tasks/index.md`
- `doctor` confirms the fixture corpus matches replayable raw data
- `budget status` shows whether any real LLM calls happened

Important: in the current CLI path, `render` does not yet produce daily-note output because `event_date` is not reconstructed from SQLite when building `RenderItem`s.

## Design Philosophy

### 1. Replayability over magic

The core loop is built around a durable SQLite store and idempotent replay from raw JSONL. If the output looks wrong, you should be able to inspect state and rerun stages without guesswork.

### 2. Heuristics first, LLM second

The classifier intentionally starts with explicit regex-based task cues and only escalates through a routing layer when confidence is low or content is long. The system remains usable in heuristics-only mode.

### 3. Vault safety is non-negotiable

Rendered Markdown is wrapped in machine-owned blocks such as:

```markdown
<!-- ltn:begin id="ltn-ct-abcdef1234567890" -->
- [ ] Review pricing update
<!-- ltn:end id="ltn-ct-abcdef1234567890" -->
```

That lets rerenders replace only the intended region while leaving surrounding human notes intact.

### 4. Stable task identity beats repeated surfacing

Tasks are keyed by a deterministic fingerprint of normalized content, source, and time bucket. The goal is conservative deduplication, not a fresh task row for every repeated ask.

### 5. Operator visibility matters

The CLI exposes `doctor`, `reconcile`, runtime history, feedback events, and LLM budget status because this project is meant to be operated locally, not treated as a black box.

## Comparison

| Approach | Strengths | Weaknesses | Best fit |
|---|---|---|---|
| Manual copy/paste from Lark into notes | Full human judgment | High effort, inconsistent provenance, easy to miss follow-ups | Low volume, one-off capture |
| One-shot LLM summarization | Fast for ad hoc review | Weak replay story, expensive at scale, no stable task identity | Disposable summaries |
| Export scripts with no stateful DB | Easy to start | Hard to reconcile retries, edits, and feedback | Throwaway prototypes |
| `lark-to-notes` | Replayable local store, safe rendering, feedback loop, in-tree live chat polling via `lark-cli`, budget hooks | Still a prototype; document-side live parity and some plan-level contracts are not finished | Operator-driven local workflow development |

## Installation

### Recommended: repo-managed dev environment

```bash
uv sync --dev
```

This gives you the CLI plus linting, typing, and test dependencies.

### Runtime-only environment

```bash
uv sync
```

Use this if you only want to run the CLI and do not need the dev tools.

### Install as a standalone CLI tool

```bash
uv tool install --from . lark-to-notes
```

After that, the command should be available as:

```bash
lark-to-notes --help
```

Requirements:

- Python `3.12+`
- `uv`
- a writable SQLite path and vault path
- for live commands (`sync-once`, `sync-daemon`, `backfill`, live `reconcile`): `lark-cli` installed and authenticated for your Lark tenant

## Quick Start

1. Install dependencies.

```bash
uv sync --dev
```

2. Create a minimal config file if you do not want the defaults.

```toml
# lark-to-notes.toml
db_path = "var/lark-to-notes.db"
vault_root = "demo-vault"
```

3. Replay the checked-in raw fixture logs into SQLite.

```bash
uv run lark-to-notes replay \
  --db var/lark-to-notes.db \
  --raw-dir tests/fixtures/lark-worker
```

4. Distill raw messages into task records.

```bash
uv run lark-to-notes reclassify \
  --db var/lark-to-notes.db \
  --limit 100
```

5. Render the current task set into a vault directory.

```bash
uv run lark-to-notes render \
  --db var/lark-to-notes.db \
  --vault-root demo-vault \
  --limit 100
```

6. Check fixture health and replay consistency.

```bash
uv run lark-to-notes doctor \
  --db var/lark-to-notes.db \
  --fixture-corpus tests/fixtures/lark-worker/fixture-corpus
```

7. Run the automated tests.

```bash
uv run pytest
```

## Command Reference

| Command | Purpose | Example |
|---|---|---|
| `sources list` | Show watched sources from SQLite | `uv run lark-to-notes sources list --db var/lark-to-notes.db --json` |
| `sources validate` | Validate watched source rows | `uv run lark-to-notes sources validate --db var/lark-to-notes.db --json` |
| `replay` | Ingest raw JSONL logs into SQLite | `uv run lark-to-notes replay --db var/lark-to-notes.db --raw-dir tests/fixtures/lark-worker --json` |
| `reclassify` | Re-run heuristics and upsert tasks | `uv run lark-to-notes reclassify --db var/lark-to-notes.db --limit 50 --json` |
| `render` | Render tasks into vault notes | `uv run lark-to-notes render --db var/lark-to-notes.db --vault-root demo-vault --limit 50 --json` |
| `doctor` | Validate schema + fixture corpus + runtime health | `uv run lark-to-notes doctor --db var/lark-to-notes.db --fixture-corpus tests/fixtures/lark-worker/fixture-corpus --json` |
| `feedback import` | Apply YAML feedback into SQLite and task overrides | `uv run lark-to-notes feedback import feedback.yaml --db var/lark-to-notes.db --json` |
| `feedback draft` | Emit a YAML stub for review-lane tasks (edit actions, then `feedback import`) | `uv run lark-to-notes feedback draft --db var/lark-to-notes.db --out review.yaml --json` |
| `reconcile` | Compare stored checkpoints against source state | `uv run lark-to-notes reconcile --db var/lark-to-notes.db --json` |
| `budget status` | Inspect recorded usage, fallbacks, and quality metrics | `uv run lark-to-notes budget status --db var/lark-to-notes.db --json` |
| `sync-once` | Poll enabled live sources once | `uv run lark-to-notes sync-once --config /path/to/worker.json --db var/lark-to-notes.db --json` |
| `sync-daemon` | Run repeated live polling cycles | `uv run lark-to-notes sync-daemon --config /path/to/worker.json --db var/lark-to-notes.db --max-cycles 2 --json` |
| `backfill` | Re-ingest live history via `lark-cli` polling | `uv run lark-to-notes backfill --config /path/to/worker.json --db var/lark-to-notes.db --days 14 --json` |
| `sync-events` | Ingest `im.message.receive_v1` NDJSON lines from stdin | Run `lark-cli event +subscribe …`, then pipe its stdout into `uv run lark-to-notes sync-events --db var/lark-to-notes.db --source-id dm:ou_xxx --json` |

Notes on the live commands:

- they are part of the CLI surface today
- `sync-once`, `sync-daemon`, `backfill`, and live `reconcile` read a JSON file with `vault_root`, `state_db`, `poll_interval_seconds`, `poll_lookback_days`, and `sources[]` (`lark_to_notes.live.worker_config`). **`--db` is the canonical SQLite store** (checkpoints, watched sources, intake ledgers, runtime runs). The `state_db` path in JSON is a **compatibility field** only; it is not merged as a second source of truth into `--db`.
- `sync-once`, `sync-daemon`, and `backfill` run entirely in-tree via `ChatLiveAdapter` (`lark-cli` transport into the same pipeline as replay)
- `sync-events` reads NDJSON from stdin; you normally pair it with `lark-cli event +subscribe` in a shell pipeline. By default it also **drains** ready rows from `chat_intake_ledger` into `raw_messages` under the same runtime lock as other writers: `{parent-of---db}/lark-to-notes.runtime.lock` (for example `var/lark-to-notes.runtime.lock` next to `var/lark-to-notes.db`). Pass `--no-drain` to only append ledger observations.
- `sync-once`, `sync-daemon`, and `backfill` require a working `lark-cli` install and credentials with access to the configured sources; `sync-events` only needs stdin (often fed by `lark-cli` in another process)
- operator smoke / CI: `uv run python scripts/verify_live_adapter.py` exercises `doctor` plus `sync-events` with structured stderr; add `--artifacts-dir` to retain `verify_live_steps.jsonl` for dashboards or failure triage (see `tests/test_integration_logging.py`)

#### Live adapter validation matrix

| Concern | Where it is covered |
| --- | --- |
| Single-writer / runtime lock (poll, drain, cross-process) | `tests/test_live_sync_controls.py`, `tests/test_runtime.py` (`TestRuntimeLock`, `TestRuntimeExecutor`) |
| Mixed poll + event intake ledger | `tests/test_intake.py`, `tests/test_live_chat_events.py`, `tests/test_live_chat_ingest.py` |
| Live CLI wiring (`sync-once`, `sync-events`, `doctor` JSON) | `tests/test_cli.py`, `tests/test_e2e_workflow.py` |
| Budget / heuristics routing | `tests/test_budget.py`, `tests/test_distill.py` |
| Offline operator smoke (structured stderr steps + optional NDJSON artifact) | `uv run python scripts/verify_live_adapter.py` and `… --artifacts-dir /path/to/dir` (see `tests/test_integration_logging.py`) |
| Full pipeline demo (no Lark credentials) | `uv run python scripts/integration_run.py` |

### Background live operation (`sync-daemon`, reconcile, launchd)

- **Ownership:** `sync-daemon` is a thin loop around the same in-repo stack as `sync-once` (`ChatLiveAdapter`, canonical `--db`, runtime lock under `{vault_root}/var/lark-to-notes.runtime.lock`). There is no separate Python “worker” process contract inside this repo—supervisors should invoke the published CLI.
- **macOS:** see `scripts/macos/launchd/com.lark-to-notes.sync-daemon.example.plist` for a LaunchAgent template (`ProgramArguments` runs `uv run lark-to-notes sync-daemon …` from a fixed working directory). Edit placeholders before `launchctl load`.
- **Reconcile:** `reconcile --config …` loads the same JSON shape for `vault_root`, `state_db`, and `sources[]`. Live truth for gap detection comes from `ChatLiveAdapter.collect_live_source_states` (`lark-cli` peek). Repair uses the same in-repo poll path as `sync-once`. Checkpoints and watched sources are written only into `--db`; the JSON `state_db` path is not merged as a second source of truth.

## Configuration

Project config is loaded from the first existing path in this order:

1. `$LARK_TO_NOTES_CONFIG`
2. `~/.config/lark-to-notes/config.toml`
3. `./lark-to-notes.toml`

Minimal config:

```toml
db_path = "var/lark-to-notes.db"
vault_root = "demo-vault"
```

Relevant runtime defaults:

| Setting | Default |
|---|---|
| `db_path` | `var/lark-to-notes.db` |
| `vault_root` | current working directory |
| raw replay dir | `<vault_root>/raw/lark-worker` |
| fixture corpus for `doctor` | `<vault_root>/raw/lark-worker/fixture-corpus` |

### Background execution (supervised / launchd)

Live mode is intentionally **just the canonical CLI** plus your supervisor. There is no second in-process “worker daemon” inside this repository.

- **Polling loop:** run `uv run lark-to-notes sync-daemon …` under `launchd`, `systemd`, or any other supervisor. Use absolute paths for `--config`, `--db`, and the `uv` binary. For macOS, see `contrib/sync-daemon.launchd.example.plist` (copy, edit `CHANGE_ME_*`, install under `~/Library/LaunchAgents/`, then `launchctl load`).
- **Event stream:** run `lark-cli event +subscribe …` and pipe its stdout into `uv run lark-to-notes sync-events …` (see `contrib/sync-events-pipeline.example.sh`). A LaunchAgent should execute that shell wrapper, not try to embed a shell pipeline in the plist.
- **Reconcile / repair:** `reconcile --config …` already probes live cursors via `ChatLiveAdapter.collect_live_source_states` and triggers `_worker_poll_once` (in-repo poll) as repair — there is no separate worker-state mirror on the canonical path.

### Feedback artifact format

Structured review feedback is a YAML sidecar:

```yaml
version: 1
tasks:
  550e8400-e29b-41d4-a716-446655440000:
    action: wrong_class
    task_class: task
    comment: This is a concrete task, not just a review item.
source_items:
  msg-456:
    action: missed_task
    title: Follow up with procurement
    task_class: task
    comment: This was missed on the first pass.
```

Task-targeted actions:

- `confirm`
- `dismiss`
- `merge`
- `snooze`
- `wrong_class`

Source-item actions:

- `missed_task`

Notes:

- `missed_task` is only valid under `source_items`, not `tasks`.
- `missed_task` requires `title` and `task_class`; if `promotion_rec` is omitted, it defaults from `task_class`.
- Task-targeted actions persist operator intent in `manual_override_state`; `confirm` can also pin fields like `title`, `summary`, `due_at`, `task_class`, and `promotion_rec` when the operator wants replay-stable overrides instead of a bare reopen.

## Architecture

### End-to-end flow

```text
fixture JSONL or worker output
            |
            v
   intake.replay / intake.ledger
            |
            +--> chat_intake_ledger (mixed poll/event chat sightings)
            |
            v
      raw_messages (SQLite)
            |
            v
 distill.heuristics + distill.routing
            |
            v
       tasks + task_evidence
            |
            v
        render.NoteWriter
            |
            +--> raw/*.md
            +--> daily/*.md        (supported by renderer, not fully wired in CLI flow)
            \--> area/current tasks/index.md

feedback YAML
     |
     v
feedback_events + task overrides

budget store
     |
     v
llm_usage_records + content_cache
```

### Key modules

| Module | Role |
|---|---|
| `src/lark_to_notes/cli` | Operator CLI and command wiring |
| `src/lark_to_notes/config` | Watched source and checkpoint models |
| `src/lark_to_notes/intake` | Raw message model, ledger, replay |
| `src/lark_to_notes/storage` | SQLite connection and schema migrations |
| `src/lark_to_notes/distill` | Heuristics, routing, classifier result models |
| `src/lark_to_notes/tasks` | Fingerprinting, task registry, evidence |
| `src/lark_to_notes/render` | Machine-owned block rendering and note writer |
| `src/lark_to_notes/feedback` | YAML artifacts, event persistence, task overrides |
| `src/lark_to_notes/runtime` | Locking, retries, run history, reconciliation |
| `src/lark_to_notes/budget` | Budget policy, cache, usage rollups |
| `src/lark_to_notes/testing` | Fixture corpus loaders and source-access utilities |

### SQLite tables that matter most

| Table | Purpose |
|---|---|
| `watched_sources` | Govern which sources exist locally |
| `checkpoints` | Store per-source cursors |
| `chat_intake_ledger` | Canonical mixed poll/event chat observations before raw capture |
| `raw_messages` | Durable raw intake ledger |
| `intake_runs` | Audit replay/intake sessions |
| `tasks` | Stable task registry |
| `task_evidence` | Additional evidence linked to tasks |
| `runtime_runs` | Runtime history for sync/reconcile work |
| `dead_letters` | Quarantined failures |
| `feedback_events` | Imported structured feedback |
| `llm_usage_records` | Budget and fallback accounting |
| `content_cache` | Cached classification results |

## Repository Layout

```text
.
├── AGENTS.md
├── plan_for_lark_to_notes_workflow.md
├── pyproject.toml
├── scripts/
│   └── integration_run.py
├── src/
│   └── lark_to_notes/
├── tests/
│   ├── fixtures/
│   └── test_*.py
├── var/
└── README.md
```

Two especially important files:

- [`plan_for_lark_to_notes_workflow.md`](plan_for_lark_to_notes_workflow.md): normative requirements and architecture direction
- `tests/fixtures/lark-worker/fixture-corpus`: checked-in fixture corpus for `doctor` and replay validation

## Testing and Verification

Recommended checks:

```bash
uv run pytest
uv run lark-to-notes doctor \
  --db var/lark-to-notes.db \
  --fixture-corpus tests/fixtures/lark-worker/fixture-corpus \
  --json
```

What those checks cover:

- unit coverage for heuristics, fingerprints, rendering, runtime, feedback, and CLI behavior
- fixture-backed replay validation
- health reporting for schema and runtime tables

The repo also contains `scripts/integration_run.py`, but in this checkout it currently expects a fixture corpus path under `raw/lark-worker/fixture-corpus` that does not exist here by default.

## Troubleshooting

### `doctor` fails with `manifest.json` not found

The default fixture-corpus path assumes a vault-style layout under `raw/lark-worker/fixture-corpus`. In this repo, the checked-in corpus lives under `tests/fixtures/lark-worker/fixture-corpus`.

Use:

```bash
uv run lark-to-notes doctor \
  --db var/lark-to-notes.db \
  --fixture-corpus tests/fixtures/lark-worker/fixture-corpus
```

### `scripts/integration_run.py` fails in Step 7

That script currently points at `raw/lark-worker/fixture-corpus`, not the checked-in test corpus location. Use `uv run pytest` and the explicit `doctor --fixture-corpus ...` command as the reliable verification path for this checkout.

### `sync-once`, `sync-daemon`, or `backfill` fail at load time

Check that `--config` points to valid JSON with `vault_root`, `state_db`, and `sources`. Malformed JSON raises a clear error from the in-repo config loader. If `lark-cli` is missing or misconfigured, failures surface as runtime errors from the live adapter.

### `sync-once` reports auth or scope problems

The CLI already prints the right operator hint:

```bash
lark-cli auth login --as user
```

You also need watched sources that are visible to the configured Lark app.

### Rendered raw notes start with `unknown-` and no daily note appears

That is a current implementation gap, not operator error. The render layer supports daily notes, but the CLI render path does not currently rehydrate `event_date` from stored raw messages.

### `budget status` shows records even though no real LLM was called

That is expected. The budget layer records fallbacks and cache hits too. The number that matters for real provider usage is `net_llm_call_count`.

## Limitations

- The turnkey path without Lark credentials is offline replay from JSONL; live polling needs `lark-cli`, auth, and sources your app can access.
- No LLM provider implementation ships in-tree, even though routing and budget hooks exist.
- The current CLI render path does not yet create daily-note output end to end.
- Live commands depend on `lark-cli` and real Lark access; the offline `replay` path remains the turnkey way to exercise the pipeline without credentials.
- The implementation is intentionally narrower than the full plan in [`plan_for_lark_to_notes_workflow.md`](plan_for_lark_to_notes_workflow.md), especially around rich document/comment revision modeling and broader vault promotion flows.

## FAQ

### Is this repository a notes vault or a Python project?

Both in intent, but this checkout is primarily the Python implementation plus its planning documents and tests. The original plan assumes a larger Obsidian-style vault context.

### Does `lark-to-notes` talk to Lark directly right now?

Not in the self-contained offline flow. The core package replays JSONL and manages local state. The live commands call `lark-cli` through the in-repo live adapter.

### Do I need LLM access to use it?

No. The default, reliable path today is heuristics-only. The routing layer can record fallbacks without making any provider call.

### What does the CLI render into the vault today?

In the current offline path, raw provenance notes and Current Tasks output are the dependable surfaces. Daily-note rendering exists in the codebase but is not yet fully wired into the CLI flow.

### Where should I start if I want to understand the intended future system?

Read [`plan_for_lark_to_notes_workflow.md`](plan_for_lark_to_notes_workflow.md). It is the normative requirements and architecture document.

### Can tasks be completed automatically?

Not in the current design. The plan explicitly keeps task completion as a manual operator action for the initial version.

### How do I inspect whether feedback changed task state?

Use `feedback import` to load the YAML artifact, then inspect the `tasks` row and `feedback_events`. The tests in `tests/test_feedback.py` show the expected behavior concretely.

## About Contributions

*About Contributions:* Please don't take this the wrong way, but I do not accept outside contributions for any of my projects. I simply don't have the mental bandwidth to review anything, and it's my name on the thing, so I'm responsible for any problems it causes; thus, the risk-reward is highly asymmetric from my perspective. I'd also have to worry about other "stakeholders," which seems unwise for tools I mostly make for myself for free. Feel free to submit issues, and even PRs if you want to illustrate a proposed fix, but know I won't merge them directly. Instead, I'll have Claude or Codex review submissions via `gh` and independently decide whether and how to address them. Bug reports in particular are welcome. Sorry if this offends, but I want to avoid wasted time and hurt feelings. I understand this isn't in sync with the prevailing open-source ethos that seeks community contributions, but it's the only way I can move at this velocity and keep my sanity.

## License

Proprietary.
