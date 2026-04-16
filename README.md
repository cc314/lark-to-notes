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

Current reality, in one sentence: the offline replay -> classify -> render workflow is implemented and tested, while live sync commands are scaffolded but depend on an external worker module that is not present in this checkout.

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
| Live sync/backfill | External dependency | `sync-once`, `sync-daemon`, and `backfill` expect `automation.lark_worker` on the Python path |

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
| `lark-to-notes` | Replayable local store, safe rendering, feedback loop, budget hooks | Still a prototype; live sync is not self-contained in this repo | Operator-driven local workflow development |

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
- optional: a separate live worker environment if you want `sync-once`, `sync-daemon`, or `backfill`

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
| `reconcile` | Compare stored checkpoints against source state | `uv run lark-to-notes reconcile --db var/lark-to-notes.db --json` |
| `budget status` | Inspect recorded usage, fallbacks, and quality metrics | `uv run lark-to-notes budget status --db var/lark-to-notes.db --json` |
| `sync-once` | Poll enabled live sources once | `uv run lark-to-notes sync-once --config /path/to/worker.json --db var/lark-to-notes.db --json` |
| `sync-daemon` | Run repeated live polling cycles | `uv run lark-to-notes sync-daemon --config /path/to/worker.json --db var/lark-to-notes.db --max-cycles 2 --json` |
| `backfill` | Re-ingest live history through the worker service | `uv run lark-to-notes backfill --config /path/to/worker.json --db var/lark-to-notes.db --days 14 --json` |

Notes on the live commands:

- they are part of the CLI surface today
- they dynamically import `automation.lark_worker.*`
- that module is not present in this checkout, so those commands are integration points, not turnkey commands, unless you provide that dependency separately

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

### Feedback artifact format

Structured review feedback is a YAML sidecar:

```yaml
version: 1
tasks:
  550e8400-e29b-41d4-a716-446655440000:
    action: wrong_class
    task_class: task
    comment: This is a concrete task, not just a review item.
source_items: {}
```

Supported task actions:

- `confirm`
- `dismiss`
- `merge`
- `snooze`
- `wrong_class`
- `missed_task`

## Architecture

### End-to-end flow

```text
fixture JSONL or worker output
            |
            v
   intake.replay / intake.ledger
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

### `sync-once`, `sync-daemon`, or `backfill` fail with import errors

Those commands dynamically import `automation.lark_worker`. The module is not present in this repository tree, so live sync is not self-contained here. Use the offline `replay` workflow unless you have the worker package available separately.

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

- The self-contained, reliable path today is offline replay from JSONL, not live Lark polling.
- No LLM provider implementation ships in-tree, even though routing and budget hooks exist.
- The current CLI render path does not yet create daily-note output end to end.
- Live worker commands assume `automation.lark_worker` exists elsewhere.
- The implementation is intentionally narrower than the full plan in [`plan_for_lark_to_notes_workflow.md`](plan_for_lark_to_notes_workflow.md), especially around rich document/comment revision modeling and broader vault promotion flows.

## FAQ

### Is this repository a notes vault or a Python project?

Both in intent, but this checkout is primarily the Python implementation plus its planning documents and tests. The original plan assumes a larger Obsidian-style vault context.

### Does `lark-to-notes` talk to Lark directly right now?

Not in the self-contained offline flow. The core package replays JSONL and manages local state. The live commands delegate to a separate worker integration.

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
