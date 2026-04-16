# Lark-to-Notes Workflow Plan

This document is the current requirements and implementation-guidance file for the local Lark-to-notes workflow. It captures the scope, defaults, architecture, rationale, risks, and phased execution guidance, and it should stay aligned with implementation decisions as the project moves from planning into execution.

## Problem

Important work and context arrive through Lark in multiple forms:

- direct messages
- group chats
- documents
- document comments

That content may be in English, Chinese, or mixed-language form.

Without a structured workflow, useful context remains trapped in Lark or is copied manually into notes with inconsistent quality and incomplete follow-through. The project should provide a reliable path from Lark into the notes vault so that raw context can become actionable items and durable knowledge.

The workflow must do more than ingest content. It must also be trustworthy under retries, source edits, mixed polling plus event-driven updates, human edits inside the vault, and uneven source volume. A system that captures many items but creates duplicates, loses provenance, or overwrites user notes will quickly lose trust.

## Goals

### Key Objectives

1. Reduce manual effort in turning Lark content into useful notes.
2. Capture important work items from DMs, groups, documents, and comments.
3. Maintain a durable open-task view instead of leaving action items scattered across conversations.
4. Accumulate durable knowledge in the notes vault over time.
5. Improve workflow quality continuously through human feedback on the generated outputs.
6. Make automation trustworthy through provenance, safe note updates, and idempotent sync behavior.

### Expected Benefits

1. Faster context capture.
2. Fewer neglected work items.
3. Better continuity between communication tools and durable notes.
4. Lower repeated triage effort.
5. Controlled LLM usage and lower long-term model cost.
6. Clear traceability from each surfaced task back to its source.
7. Safe reruns, recovery, and reprocessing without duplicate note churn.

### Operating Preferences

1. The system should prefer some false positives over false negatives because neglecting work items is costlier than later cleanup.
2. The system should automatically update machine-owned note sections by default.
3. The system must not overwrite user-authored content outside machine-owned sections.
4. For the initial version, task completion can remain a manual user action.
5. Polling every five minutes is acceptable for non-urgent sources.
6. Event-driven updates should usually propagate within a couple of seconds where technically available.
7. Watched sources should be explicitly configured and pausable.
8. Deterministic processing should handle most inputs. LLM use should be reserved for uncertainty, summarization, or multilingual disambiguation that meaningfully improves output quality.

### Implementation Defaults

The following defaults should be treated as implementation decisions for v1 unless later evidence forces a change:

1. watched sources default to explicit allowlists rather than broad discovery
2. SQLite is the single local system of record for pipeline state
3. one active provider policy is used at a time; v1 does not use multi-model blending
4. task completion remains manual
5. one serialized note writer owns all vault mutations
6. plannotator remains the first review UI, with machine-parseable feedback stored in a YAML sidecar by default
7. deterministic heuristics remain the default classifier, with selective LLM escalation only for uncertain or long-context cases
8. the existing `automation/lark_worker/` MVP is a non-normative reference only; the actual implementation may differ materially if it better satisfies this document

## Environment

### Access and Tooling

1. Access to Lark content is through `lark-cli`.
2. `lark-cli` uses Lark app credentials to call the underlying Lark APIs.
3. Source access, permissions, and visibility are therefore constrained by the configured Lark app and its credentials.
4. Retrieval paths and payload fidelity may differ by source class and should be validated during implementation.

### Source Surfaces

1. In-scope sources are:
   - direct messages
   - group chats
   - Lark documents
   - comments on Lark documents
   - comment replies on watched documents when accessible
2. Documents, document comments, and comment replies should be treated as the same high-level source class for this project, but they should remain distinct record types internally.
3. The user may identify relevant documents by link or by name.
4. An initial watched-document example is:
   - `https://gotocompany.sg.larksuite.com/docx/D5IXd2EQLoZ3yvxY1WBlJi71gmf`
5. Watched sources should be modeled as explicit allowlists with optional ignore rules, pause/resume controls, and configurable backfill windows.

### Watched-Document Selection and Resolution Contract

Document selection should be explicit, deterministic, and operator-visible.

Allowed selection modes:

1. `explicit_link`: user provides a full document link.
2. `explicit_id`: user provides a stable document token or ID directly.
3. `name_search`: user provides a document name or query that is resolved through controlled search.

Resolution rules:

1. `explicit_link` must be normalized by extracting the canonical doc token from the link and validating access through `lark-cli`.
2. `explicit_id` must validate that the token exists, is accessible, and maps to a supported document type before activation.
3. `name_search` must call `lark-cli docs +search` and resolve to concrete candidate document tokens before any watched-source write.
4. Search and link resolution should always materialize to the same canonical watched-source identity keyed by stable document token, not by mutable display name.

Ambiguity and drift handling:

1. Zero search matches should create an explicit unresolved result with operator-facing reason, not a silent no-op.
2. Multiple matches should remain pending and require explicit operator choice; automatic best-guess selection is out of scope for v1.
3. If a previously selected document is renamed, the watched source should remain bound to the same stable token while display metadata updates.
4. If access is later lost or token resolution fails, the source should move to a blocked or paused state with a visible blocker reason rather than being silently dropped.

Canonical watched-source materialization:

1. Resolved document sources should be persisted as explicit allowlist entries with:
   - stable `source_id` keyed by document token
   - `selection_mode` and original selection reference
   - resolved display metadata and last-resolution timestamp
   - resolution status such as `active`, `pending_resolution`, or `blocked`
2. Downstream document, comment, and reply ingestion should only run for sources in an active resolved state.
3. Operator-facing status commands should distinguish configured-but-unresolved document selections from active watched documents.

### Content Characteristics

1. Source content may be English, Chinese, or mixed-language.
2. Task-bearing signals may be expressed by configurable phrasing and patterns such as:
   - `?`
   - `need`
   - `需要`
   - `帮忙`
   - `看下`
   - `看看`
3. These patterns should remain configurable rather than hard-coded as the final truth.
4. Useful task signals may also come from assignee cues, due-date cues, question forms, urgency markers, or repeated follow-up language.
5. Documents, comments, and replies are mutable. They may be edited or deleted after first capture and should therefore be treated as revision-bearing records rather than static text blobs.

### Vault Contract

1. New source material should land in `raw/` first.
2. Distilled action items should land in the relevant `daily/YYYY-MM-DD.md` first.
3. Still-open work should be promoted into `area/current tasks/index.md`.
4. Durable context should roll into `people/`, `projects/`, and `area/` notes only when it is more than a one-off item.
5. Source notes and curated notes should cross-link with Obsidian wikilinks.
6. Automation should prefer updating existing canonical notes over creating near-duplicates.
7. Automation must preserve user-authored content and only update machine-owned blocks.
8. SQLite raw capture remains the durable system of record for replay, reconciliation, and reclassification.
9. `raw/` contains vault-visible raw notes or context artifacts derived from that raw store; "raw first" refers to vault rendering order, not to bypassing the database-backed raw capture layer.

### Live Raw-First Rendering Contract (Chat and Document)

Live sync must materialize the same raw-first vault contract as replay: every new or revised upstream item that is accepted into the pipeline appears first as durable SQLite raw history, then as a vault-visible `raw/` artifact when rendering runs, never the other way around.

Relationship to the canonical store:

1. Immutable raw rows in SQLite remain authoritative for replay, audit, and reclassification; `raw/` Markdown is a derived, machine-owned projection of that history plus normalized metadata.
2. Rendering must not mint parallel "shadow" raw content that only exists in the vault; if a note path is created, there must be a binding from note path back to `raw_record_id` or `ingest_key` in the local model.
3. Skipping vault projection (for example under caps) is allowed only when stage state records explicit deferral; the operator must still be able to see deferred backlog via CLI or health output, not silent absence.

Placement and naming:

1. Default path pattern is `raw/lark/<source_type>/<stable_stream_or_doc_token>/<source_item_key>.md` or an equivalent single-file layout documented in implementation, but every layout must guarantee one canonical note path per logical `source_item_key` so rerenders update in place.
2. Chat-derived artifacts should prefer thread- or stream-scoped filenames that remain stable across message edits; document-derived artifacts should key on document token plus anchor-scoped comment or reply identity when IDs are not globally unique.
3. Large document bodies may use a summary or excerpt note in `raw/` plus pointer to stored payload in SQLite rather than duplicating megabytes in Markdown, but the excerpt note still counts as the vault-visible raw artifact and must carry full provenance links.

Surface-specific expectations:

1. **Chat (DM and group):** raw notes carry message-level identity, canonical deep link, author, timestamps, intake path, and a short normalized excerpt; reply threads should nest or cross-link by stable parent keys without creating a second file per transient render pass.
2. **Document, comment, reply:** raw notes distinguish `record_type` in frontmatter, include document token, comment or reply ID, revision when available, and lifecycle state; edits create superseding vault projections aligned with new raw rows, not in-place edits that erase prior captured text without a tombstone or supersession trail in the store.

Metadata and backlinks:

1. Raw artifacts use the vault metadata contract (`type`, `created`, `updated`, `tags`, `source`, `author`, `published`) plus machine fields such as `ingest_key`, `source_stream_id`, and `canonical_link` where helpful for operators.
2. Each raw note should wikilink to any already-known related daily stub or person page only when confidence is sufficient; mandatory backlinks go to upstream Lark URLs and to sibling raw context for the same thread or document.
3. Daily and downstream notes that consume a raw item should backlink to the canonical raw path so navigation stays two-way after promotion.

Dedup and rerender:

1. Idempotent rerender replaces machine-owned body or section of the same path for the same `ingest_key`; it must not create `raw/...copy.md` variants on retry.
2. When normalized content is unchanged and policy version unchanged, content-hash skip may suppress vault rewrite but must not remove existing paths or break bindings.

### User Interface Environment

1. The current task view is a Markdown file rendered in Obsidian.
2. The initial feedback surface should use plannotator on Markdown artifacts.
3. Markdown and Obsidian remain the current operating UI for the notes vault.
4. A separate frontend can be considered later if plannotator-based feedback becomes too limiting.

### LLM Environment

1. The current model provider is Copilot through an enterprise account.
2. Other providers may be used in the future.
3. Provider limits and pricing affect how aggressively LLM calls can be used.
4. The actual Copilot request-count and pricing-related constraints should be confirmed.
5. The design should tolerate a heuristics-only mode when budgets or provider access are unavailable.

## Recommended Tech Stack

### V1 Recommendation

1. Start with a Python-only implementation for the first version.
2. Use Python for the runtime, CLI, sync workers, storage access, note rendering, feedback processing, and reconciliation logic.
3. Do not introduce Rust in the first version.
4. Do not build a dedicated frontend in the first version unless Markdown and plannotator prove clearly insufficient.

### Why Python First

1. The hardest parts of the system are sync correctness, idempotency, replay, revision handling, task heuristics, and safe note writes rather than CPU-bound computation.
2. End-to-end latency is more likely to be dominated by `lark-cli`, Lark API access, LLM calls, and disk I/O than by local compute.
3. Python is a strong fit for local automation, text-heavy workflows, Markdown generation, LLM orchestration, and SQLite-backed pipelines.
4. Python will allow faster iteration on heuristics, feedback loops, and note-rendering behavior while the workflow is still evolving.

### Core Stack

1. Language: Python 3.12 or newer.
2. Environment and dependency management: `uv`.
3. Storage: SQLite as the local durable store for the intake ledger, per-source cursors, raw capture metadata, task registry, feedback signals, and run history.
4. CLI: `typer`.
5. Schema and config validation: `pydantic`.
6. Persistence layer: a thin SQLite repository layer or `sqlalchemy` if the model grows enough to justify it.
7. HTTP and process integration: `httpx` plus Python subprocess support for `lark-cli`.
8. Retry handling: `tenacity`.
9. Logging: structured logging through `structlog` or disciplined standard-library logging.
10. Testing: `pytest`.
11. Linting and formatting: `ruff`.
12. Type checking: `mypy`.
13. Markdown and frontmatter handling: focused Python libraries such as `python-frontmatter` and a Markdown parser only where structured edits are needed.

### Suggested Repository Structure

1. `pyproject.toml` for dependencies, tooling, and project metadata managed through `uv`.
2. `src/lark_to_notes/` as the main Python package.
3. `src/lark_to_notes/config/` for settings models, source governance rules, and provider configuration.
4. `src/lark_to_notes/intake/` for `lark-cli` integration, polling, event intake, cursor handling, and the intake ledger.
5. `src/lark_to_notes/storage/` for SQLite schema management, repositories, and persistence helpers.
6. `src/lark_to_notes/distill/` for heuristics, optional LLM routing, classification, and task generation.
7. `src/lark_to_notes/tasks/` for task identity, fingerprinting, merge rules, lifecycle state, and promotion logic.
8. `src/lark_to_notes/render/` for raw-note creation, daily-note updates, Current Tasks updates, and machine-owned block rendering.
9. `src/lark_to_notes/feedback/` for parsing structured triage actions, feedback artifacts, and tuning inputs.
10. `src/lark_to_notes/runtime/` for workers, retry policy, reconciliation, scheduling, locking, and health checks.
11. `src/lark_to_notes/cli/` for `typer` entrypoints such as sync, replay, reconcile, and backfill commands.
12. `tests/` for unit, integration, and replay/idempotency tests.
13. `scripts/` only for lightweight developer utilities that do not belong in the main package.
14. `var/` or another local-only ignored directory for SQLite files, run logs, caches, and temporary artifacts produced at runtime.

### Architecture Guidance

1. Keep the Python runtime as the source of truth for intake, replay, task generation, note writing, and reconciliation.
2. Use SQLite-backed state instead of adding external infrastructure such as Redis or a separate queue for the first version.
3. Prefer a clean module boundary between ingestion, normalization, raw capture, distillation, task registry, feedback ingestion, and vault rendering.
4. Design the runtime so that future UI layers can call into it through a stable local API, CLI contract, or shared database schema.

### Future Evolution

1. Add a TypeScript frontend later only if the workflow outgrows Markdown, Obsidian, and plannotator.
2. Introduce Rust only after profiling shows a real hotspot such as large-document diffing, indexing, or high-volume replay.
3. If a later dedicated UI is built, prefer a Python runtime with a TypeScript frontend rather than splitting core workflow logic across multiple languages too early.

## Project

### Scope

The project is to build a local workflow that:

1. ingests selected Lark DMs, groups, documents, and document comments
2. normalizes captured items into stable internal records
3. stores raw source material durably with revision history
4. distills raw context into candidate action items and note content
5. writes notes automatically into the vault according to the vault contract
6. maintains a durable Current Tasks view
7. supports structured feedback, replay, and reprocessing
8. provides runtime, observability, reconciliation, and cost controls for daily use

### Out of Scope

1. Replacing Lark as the system of record for communication.
2. Building an outbound reply or action layer for now.
3. Requiring an LLM call for every input item.
4. Treating manual note editing as obsolete.
5. Automatically deciding that tasks are complete in the initial version.
6. Building a custom frontend before Markdown and plannotator prove insufficient.

### V1 Execution Slice

The first implementation should ship a coherent slice rather than try to complete every future-facing idea at once.

1. V1 should cover watched-source governance, raw capture, replay, heuristics-first distillation, a visible `needs-review` lane, safe daily-note plus Current Tasks rendering, and structured feedback import.
2. Automatic durable-note updates beyond `raw/`, daily notes, and Current Tasks should start as opt-in or later-phase work after note safety and task identity are stable.
3. A single local runtime with SQLite and a serialized note writer is sufficient for v1; queues, extra processes, or more elaborate concurrency models are implementation details rather than baseline requirements.
4. The initial implementation should favor a minimal end-to-end loop over broad surface-area completeness.
5. The current `automation/lark_worker/` MVP may inform naming or operational lessons, but it is not the target architecture.

### Migration Boundary and Rollout Gates

The repo needs one durable decision record for how the in-tree implementation relates to the older external worker. The default boundary should be:

1. Port in-tree:
   - source governance and watched-source state
   - canonical intake ledger, raw capture, replay, and reconciliation logic
   - classifier, task, render, feedback, runtime, and operator CLI behavior that define the trusted local workflow
2. Adapt or redesign in-tree:
   - transport-specific Lark access paths where the MVP proved feasibility but not the right long-term shape
   - event versus polling coordination, batching, checkpointing, and repair semantics
   - operator-facing status, diagnostics, and scope messaging
3. Replace rather than port verbatim:
   - any worker behavior that bypasses the canonical SQLite ledger, hides provenance, or assumes side-effect ordering that conflicts with this plan
   - any automation that writes outside the machine-owned trust boundary without explicit safety controls
4. Keep reference-only:
   - naming ideas, fixture examples, auth lessons, and retrieval experiments from `automation/lark_worker/`
   - source-surface feasibility hints that still need confirmation in the in-repo architecture

Honest rollout gates should be:

1. Gate A: self-contained offline baseline
   - The repo can replay checked-in raw fixtures, classify, render, import feedback, and reconcile local state without any external worker package.
   - This gate is the minimum claim for a reliable self-contained checkout.
2. Gate B: chat-first self-contained live slice
   - The repo can watch governed chat sources directly in-tree, persist canonical intake state, and run through the same task and render pipeline without `automation/lark_worker`.
   - This gate is enough to claim self-contained live support for the validated chat slice only, not full source-class parity.
3. Gate C: broader source-class parity
   - Documents, comments, replies, capability diagnostics, and operator-facing blockers are implemented honestly enough that the remaining gaps are narrow rather than structural.
   - Only this gate justifies language that implies broad live-source coverage beyond chat-first scope.

Operator-facing messaging should follow the same boundary:

1. Do not describe the repo as self-contained for live sync while `sync-once`, `sync-daemon`, or `backfill` still require `automation/lark_worker`.
2. Do describe the current repo as self-contained for offline replay, storage, rendering, feedback import, and the design work that defines the in-repo target architecture.
3. When a source class is not yet in the current gate, report it as unsupported or blocked rather than implying silent partial support.

### Main Workstreams

1. source governance and intake
2. raw storage, checkpoints, and replay
3. lifecycle modeling for mutable sources
4. classification and action-item generation
5. task identity and deduplication
6. note rendering and vault-safe writes
7. feedback collection and quality improvement
8. runtime, reconciliation, and operations
9. performance, batching, and cost control

### Main Milestones

1. requirements and governance baseline approved
2. source model and ingestion contract approved
3. raw-capture and replay model approved
4. task-identity and generation model approved
5. note-rendering and vault-safety model approved
6. runtime, observability, and reconciliation model approved
7. feedback and cost-control model approved
8. execution roadmap approved

## End-to-End Workflows

### 1. Add or Update a Watched Source

1. The user selects a DM, group, or document by direct link, ID, or approved search rule.
2. For document sources, the system resolves the selection into a canonical document token and records the selection mode plus original reference.
3. Ambiguous or unresolved document selections remain pending with an explicit operator-visible reason instead of silently selecting a best guess.
4. The source is stored in versioned governance config with backfill bounds, pause state, and optional ignore or redaction rules.
5. The system validates access through `lark-cli` before the source becomes active.

### 2. Initial Backfill

1. The system fetches historical records for the selected source.
2. Each source item is normalized into the shared ingestion contract.
3. Immutable raw records are stored before classification or rendering.
4. Derived notes and task candidates are generated from raw state rather than directly from the upstream payload.

### 3. Incremental Sync

1. Polling and event-driven updates both enter the same intake ledger.
2. Per-source cursors advance only after the batch's raw capture, ledger update, and durable handoff into downstream stage state are committed.
3. Content-hash skip prevents redundant downstream work when the effective content did not change.
4. Poll and event sightings of the same canonical source item must converge on one ledger state rather than becoming parallel downstream queues.

### 4. Task Review and Promotion

1. New candidate tasks first appear in the relevant daily note.
2. Still-open or durable work is promoted into `area/current tasks/index.md`.
3. Lower-confidence items remain visible in a review lane and are not auto-promoted into Current Tasks until confirmed or reclassified with sufficient confidence.
4. Every surfaced task includes provenance and a stable task identity.

### 5. Feedback and Correction

1. The user reviews generated outputs through Markdown artifacts in Obsidian and plannotator.
2. Structured actions such as confirm, dismiss, merge, snooze, wrong-class, and missed-task are captured in a machine-readable feedback artifact.
3. Manual feedback updates later heuristic tuning, routing thresholds, and source governance.

### 6. Replay and Reclassification

1. The user can rerun classification, rendering, and repair flows from stored raw records plus ledger state.
2. Replay does not mutate raw history and should rebuild downstream artifacts from the earliest requested or invalidated stage.
3. Reclassification reuses stored raw records, reruns later policy-sensitive stages, and updates machine-owned note blocks and task state idempotently.
4. Reconcile compares raw history, ledger state, derived artifacts, and checkpoints to repair drift without inventing new canonical identities or silently discarding provenance.

## System

### Required Functions

The system shall:

1. ingest selected DMs, groups, documents, and document comments from Lark through `lark-cli`
2. normalize captured items into a stable internal representation with source-specific external IDs
3. preserve immutable raw records for replay, debugging, and audit
4. maintain per-source cursors and a shared intake ledger that merges polling and event-driven updates into one logical stream
5. ensure end-to-end idempotency so replay, retries, or mixed intake paths do not create duplicate tasks or note edits
6. classify captured items into at least:
   - context
   - follow-up
   - task
   - needs-review
7. generate action items using configurable patterns, heuristics, and selective LLM assistance
8. support multilingual input including English, Chinese, and mixed-language text
9. attach provenance to each surfaced task or note update
10. update notes automatically within machine-owned blocks only
11. follow the raw-first, daily-first, and promotion workflow defined by the vault contract
12. maintain a durable Current Tasks view and a promotion path into durable knowledge notes
13. collect structured human feedback plus optional free-text comments
14. measure and monitor LLM usage, quality, sync latency, and runtime health
15. provide backfill, replay, reclassification, and reconciliation commands

### Source Governance

1. Every watched source should be explicitly allowlisted by chat, document, or search rule.
2. The system should support per-source ignore rules, pause/resume, and configurable backfill windows.
3. Sensitive sources should support opt-out or redaction rules before durable note rendering.
4. Per-run and per-source caps should prevent accidental flood ingestion.
5. Governance configuration should be versioned so later behavior changes can be traced back to the governing rules that produced them.

### Search-Rule Watched Sources and Result Stability

Search-rule watched sources are **first-class governed sources**, not ad hoc discovery shortcuts. Each rule is versioned configuration that resolves to **concrete source identities** before ingestion runs.

Identity and persistence:

1. A search rule stores a stable `source_id` for the rule itself plus the **last materialized match set** (document tokens, chat IDs, or other allowed surface IDs) and a **materialization timestamp**.
2. Resolution runs through the same **allowlist-first** posture as other sources: the rule defines an explicit predicate; the system never expands into arbitrary tenant-wide scraping.

Drift and ambiguity:

1. **Zero matches** or **ambiguous multi-match** states follow the same operator-visible semantics as document `name_search`: unresolved or pending choice, never silent best-guess ingestion.
2. When a new poll or manual **re-resolve** changes the match set, the runtime records **diff metadata** (added sources, removed sources, unchanged) in governance history and surfaces **result drift** in status output so operators can see that the effective watch set changed.
3. Removed IDs stop receiving new intake but **retain historical raw and ledger rows** under normal retention policy; re-added IDs resume without minting alternate identities.

Replay and caps:

1. Search-rule expansion obeys the same **per-run and per-source caps** as explicit sources; cap hits split work across runs with explicit deferral rather than truncating match lists silently.
2. Replay scoped to a search rule replays against **stored resolution snapshots** plus upstream re-fetch only when the operator requests a fresh resolve.

### Governance and Policy Versioning for Live Explainability

Live and replay must answer **which rules produced this row** without guessing from timestamps alone.

Version identifiers:

1. **Watched-source governance** carries a monotonic or content-addressed **`governance_version`** (or equivalent) that bumps when allowlists, caps, ignore rules, redaction, pause state, or search-rule predicates change materially.
2. **Classifier, distillation, rendering, and budget policies** share a **`policy_version`** (or a small set of named versions such as `classifier@3`, `render@2`) stored in configuration artifacts under version control when possible.

Attachment rules:

1. **Intake ledger rows**, **immutable raw records**, **task registry rows**, **note bindings**, and **run history** all persist the **`policy_version` and `governance_version`** (or a combined effective tuple) that last successfully completed each stage for that entity.
2. **Derived Markdown** is not the primary version carrier, but machine-owned headers or sidecar metadata may echo the versions for human inspection.

Replay and reclassification:

1. When **`governance_version`** changes, only stages whose semantics depend on governance must invalidate (for example eligibility, caps, redaction); pure transport facts may not.
2. When **`policy_version`** changes, downstream stages that consume those rules must invalidate per the existing replay contract even if normalized text is unchanged.
3. **Replay** must never silently rewrite history under a new version; it recomputes forward while preserving immutable raw rows and explicit supersession.

Operator visibility:

1. **Doctor, inspect, and reconcile** output should be able to show **effective versions** for the last run and for a sampled task or ledger row so operators can correlate surprising output with a config change.

### Ingestion Caps and Flood-Protection Contract

1. The system should enforce at least two bounded intake limits:
   - a global per-run cap across all active sources
   - a per-source cap within the same run
2. Cap checks should run before expensive downstream stages and should never rely on implicit truncation by upstream pagination alone.
3. Hitting a cap should not silently drop work; it should create explicit deferred backlog state tied to source, run, and cursor position.
4. Deferred backlog should remain replay-safe: the next run must continue from durable cursor/ledger state without minting alternate source identities.
5. Backfill runs may use different cap values than incremental runs, but both must produce explicit operator-visible cap-hit diagnostics.
6. Retry behavior should respect the same cap policy so retry storms cannot bypass intake limits.
7. Coalescing and batching may reduce downstream load after intake, but they are not a substitute for explicit intake caps at governance boundaries.
8. When caps are exceeded repeatedly for a source, runtime health output should surface that source as lagging or throttled rather than merely showing generic queue growth.
9. Cap policy changes must be versioned and reflected in run diagnostics so operators can explain why intake volume changed between runs.
10. Operator-facing status output should include at least:
    - cap values in effect
    - per-source cap-hit counts
    - deferred-item estimates or next-drain signals
    - whether truncation occurred in the last run

### Metadata to Preserve

For traceability and replay, the system shall preserve at least:

1. author
2. create time
3. update time
4. source
5. channel or stream
6. source class and record type
7. stable external ID such as message ID, comment ID, reply ID, doc ID, or revision ID
8. parent ID, thread ID, or document anchor when applicable
9. canonical Lark deep link
10. participant or watcher context when useful
11. raw payload reference
12. normalized text excerpt
13. content hash
14. lifecycle state such as `active`, `edited`, `deleted`, or `superseded`
15. capture time and intake path such as `poll` or `event`
16. processing policy version or rules version

### Ingestion Contract

1. Each observed source item should first map to a stable `source_item_key` derived from source stream, record type, and stable external ID; when an external ID is only unique within a parent anchor, thread, or document-local namespace, that parent anchor must be part of the canonical identity.
2. Each material upstream change should then map to a `revision_key` derived from the `source_item_key` plus a revision, edit, delete, or supersession marker; if upstream does not expose a revision ID, the fallback should be deterministic and based on content hash plus trustworthy upstream update metadata.
3. The canonical `ingest_key` used for immutable raw capture should identify one revision-bearing record, not one observation event.
4. Polling and event-driven sources should feed a shared intake ledger instead of independent pipelines, and repeated sightings of the same `ingest_key` should update one ledger row rather than create duplicate raw or derived artifacts.
5. The intake ledger should record at least first-seen time, last-seen time, latest revision seen, lifecycle state, first and last intake path, observation counters, retry or error state, and downstream stage completion.
6. Downstream stage completion should be explicit rather than implicit. The durable state should be able to answer whether raw capture, distillation, task upsert, and note rendering have already run for a given canonical revision and under which policy or rules version.
7. Lifecycle transitions such as `active` -> `edited`, `active` -> `deleted`, or `edited` -> `superseded` must not mint a new `source_item_key`; they should produce a new `revision_key` or lifecycle-bearing revision for the same canonical source item unless the upstream system has actually created a distinct new item.
8. Cursors should be tracked per source stream rather than globally and may contain both a monotonic ordering cursor and an opaque upstream paging token when the source surface requires both.
9. Cursor advancement should happen only after the raw capture and stage-handoff state for the batch are durable enough that replay can resume without re-fetch-dependent guesswork.
10. The system should assume at-least-once delivery from upstream and make downstream stages idempotent.
11. Incremental sync should process only new or changed records since the stored cursor or revision, while content-hash skip should suppress downstream reruns only when canonical content and lifecycle state are unchanged.
12. Replay mode should be able to rebuild derived artifacts from raw capture plus stored stage state without mutating raw history or minting new canonical identities.
13. Reclassification mode should allow rules or model-policy changes to rerun on stored raw records by updating later-stage state idempotently instead of reinserting raw rows or double-writing notes.

### Dedup and Stage-Completion Contract

1. An exact `ingest_key` match means "same canonical revision seen again" and should update observation metadata, counters, and coalescing state only; it must not append a second raw row or enqueue a second independent downstream job.
2. A stable `source_item_key` with a new `revision_key` means "same logical item, new revision" and should append a new immutable raw record, advance current-item state, and invalidate only the downstream stages whose inputs actually changed.
3. Content-hash skip should apply only after raw capture and ledger state are durable. It is a rule for suppressing redundant downstream work, not for skipping raw-history preservation.
4. Downstream stages may be treated as already satisfied only when the canonical content hash, lifecycle state, and relevant policy version for that stage are unchanged for the revision being processed.
5. A policy or rules-version change should invalidate the affected later stages even when the raw payload and normalized text are unchanged.
6. A lifecycle transition such as delete, undelete, or supersession should invalidate any later stage whose output depends on visibility or current-state semantics, even if the human-visible text is unchanged.
7. Poll and event observations that disagree only in arrival path, timing, or duplicated payload delivery should converge on one ledger row and one stage-state lineage rather than racing to produce duplicate task or render side effects.
8. Coalescing may delay downstream processing for bursty edits or reply storms, but the final coalesced batch must still preserve revision provenance and must not erase the fact that multiple observations occurred.
9. Stage completion should be tracked per stage boundary so recovery can rerun only the first incomplete or invalidated stage instead of restarting the whole pipeline by default.
10. The plan should prefer "resume from the earliest affected stage" over "rerun everything" whenever provenance, policy version, and stage-state data are sufficient to do so safely.

### Replay, Reclassification, and Reconcile Contract

1. Replay should read immutable raw history plus ledger and checkpoint state, then deterministically rebuild derived stages without minting new raw rows for already-captured revisions.
2. Replay may be scoped by source, time window, source item, or revision range, but every scoped replay must still preserve the same canonical identity and stage-order rules as normal live intake.
3. Reclassification should start from stored raw records and rerun only the stages whose behavior depends on rules, prompts, thresholds, or model-policy changes; it should not rewrite raw history or reset unrelated successful stages.
4. Reconcile should detect drift such as missing current-state materializations, missing rendered blocks, stale checkpoints, or stage-state gaps, then repair them by resuming from the earliest missing or invalid stage rather than fabricating alternate identities.
5. Reconcile must be able to explain every repair in terms of existing provenance: which raw revision was authoritative, which stage was missing or stale, and which policy version justified the rerun.
6. None of replay, reclassification, or reconcile should advance source-stream checkpoints past unseen upstream data; their job is to repair or recompute local derived state against already-captured history unless the operator explicitly runs a new sync or backfill.
7. A failed replay, reclassification, or reconcile run must leave enough durable stage-state and runtime diagnostics behind that the next run can resume safely instead of guessing which downstream effects already happened.
8. If drift cannot be resolved deterministically, the system should quarantine the affected item or stage and surface it for review rather than silently mutating notes or tasks under uncertainty.

### Task-Generation Policy

1. The default behavior should penalize false negatives more than false positives.
2. First-pass detection should rely mainly on deterministic heuristics plus configuration.
3. Confidence bands should separate `high-confidence task`, `candidate follow-up`, and `needs-review`.
4. Lower-confidence items should remain visible in a review lane instead of silently disappearing.
5. LLM assistance should be used only for uncertain classification, summarization of large context, or multilingual disambiguation.
6. Generated tasks should include a stable task fingerprint and a short explanation of why the task was surfaced.
7. Duplicate or repeated asks from the same thread or document should update existing task records rather than create new ones when the fingerprint matches strongly.
8. Manual user updates can remain the primary way to mark tasks as completed in the early version.

### Current Tasks Behavior

1. `area/current tasks/index.md` should remain the durable Markdown-first open-work view.
2. Distilled work should land in the relevant `daily/YYYY-MM-DD.md` first, then be promoted into Current Tasks when still open or clearly durable.
3. Items in `needs-review` should remain in the daily-note review lane or feedback artifact rather than appearing in Current Tasks by default.
4. Current Tasks should remain durable until manually updated by the user or later lifecycle logic is introduced.
5. Each open task should retain backlinks to its daily note entry and canonical source context.
6. Duplicate tasks should merge by stable task ID or fingerprint rather than appear as repeated bullets.

### Note Rendering Contract

1. Raw source notes belong in `raw/` first.
2. Daily notes are the first curated capture surface for short-lived action items and follow-ups.
3. `area/current tasks/index.md` is the durable open-work list, not a transient scratch file.
4. Automatic durable-note updates into `people/`, `projects/`, and `area/` notes should begin only after `raw/`, daily, and Current Tasks rendering is stable; early v1 may keep those promotions manual or opt-in.
5. One canonical note should exist per entity or event where practical. Automation should prefer updating existing notes over creating near-duplicates.
6. Machine-owned sections should be explicitly delimited and updated by stable IDs.
7. User-authored narrative outside machine-owned sections must be preserved.
8. Generated notes should add backlinks and wikilinks to related source, people, project, and area notes when confidence is sufficient.

### Live Durable-Note Boundary (People, Projects, and Area)

Live sync must treat broader vault curation as out of bounds for automatic writers until the core machine-owned surfaces (`raw/`, `daily/`, `area/current tasks/index.md`) are proven stable under replay, caps, and rerender stress.

Default posture:

1. **No automatic writes** in v1 to `people/*/index.md`, `projects/*/index.md`, or `area/<topic>/index.md` except where an explicit future opt-in flag names the target path and section contract; implied or heuristic promotion into those trees is forbidden.
2. **Link-only assistance** is allowed: generated raw or daily content may wikilink to existing people, project, or area pages when the link target already exists and the classifier confidence meets a documented threshold; creating new durable entity pages from live sync remains off unless opt-in.
3. **Current Tasks is not a substitute** for people or project pages: durable org and project context still rolls up through human curation or later opt-in automation, not through silent duplication of task bullets into stakeholder notes.

Operator messaging:

1. CLI, doctor, and migration docs should state clearly that live mode owns `raw/`, daily managed sections, and Current Tasks machine blocks only.
2. When a heuristic might suggest a durable-page update (for example detecting a project name), the system should surface that as a review hint or `needs_review` note in daily or raw context, not perform the write.
3. Opt-in durable automation, when introduced, must use the same machine-owned block pattern, stable IDs, and binding metadata as core surfaces so it cannot expand scope without a configuration change.

### Live Daily-Note Insertion Contract

Live-created tasks and review candidates always land in **`daily/YYYY-MM-DD.md`** before any Current Tasks write. The date is derived from a documented, deterministic rule (for example **source event timestamp in the operator-local timezone**, or **capture time** when the upstream event has no trustworthy clock) recorded on the task and in run diagnostics so replay picks the same path.

Managed sections:

1. **Open tasks and high-confidence candidates** live in a machine-owned section such as `daily:<YYYY-MM-DD>:auto-open-tasks`, using per-task block IDs as in the machine-owned block pattern. Only these sections are eligible for automatic list-item replacement.
2. **`needs_review` and low-confidence candidates** live in a separate section such as `daily:<YYYY-MM-DD>:review-lane` so reviewers can scan uncertainty without mixing it into default open-work lists. That section is still machine-owned and rerender-safe.
3. Sections must be **created idempotently**: if the daily file does not exist, the writer creates it with required frontmatter for a daily note; if it exists, the writer appends or refreshes only the managed sections.

Incremental behavior:

1. A **repeated sighting** or **revision** of the same `task_id` updates the existing block inside the daily section (same stable block ID), attaching newer excerpts or links as evidence rather than duplicating bullets.
2. A **lifecycle change** (for example dismissed in feedback, completed manually) stops automatic updates to that block; later raw evidence may still append to raw notes but does not resurrect the daily machine block unless policy explicitly reopens it.
3. **User-authored body text** outside `<!-- lark-to-notes:section ... -->` markers is never modified by the renderer; if the daily note has no machine section yet, the writer inserts sections without reordering unrelated headings the user placed above or below.

### Live Current Tasks Promotion Contract

Promotion from daily to **`area/current tasks/index.md`** is **deliberate and gated**, not an automatic mirror of every daily bullet.

Promotion triggers (any one may apply; all require stable `task_id` and non-`needs_review` disposition unless overridden by explicit operator action):

1. Classifier **promotion recommendation** of `current-tasks` **and** task state not in `needs_review`, **or**
2. **Structured feedback** `confirm` / equivalent that marks the item as durable open work, **or**
3. **Explicit operator CLI** promotion for a listed `task_id`.

Non-promotion:

1. Items whose latest disposition is **`needs_review`**, **snoozed**, or **dismissed** must not appear in Current Tasks unless a later confirm or override clears that state.
2. **Daily-only** recommendations never promote automatically.

Merge and shape:

1. Current Tasks uses **one machine-owned section** (for example `current-tasks:auto`) with **per-task block IDs** mirroring the daily pattern. If the same `task_id` is promoted from multiple days, the renderer keeps **one** open entry and merges provenance links.
2. **Fingerprints** that strongly match an existing promoted `task_id` update that block instead of adding a second bullet.

Backlinks and persistence:

1. Each Current Tasks block retains a **wikilink or path reference** to the **canonical raw note** and the **daily section** where the item was last surfaced, plus the Lark deep link stored in metadata.
2. Promoted items remain in Current Tasks until **manual completion**, **merge**, **dismiss** via feedback import, or **supersession** by a later task record; the system does not auto-remove items solely because the daily section was archived or rotated—reconciliation may refresh wording but respects explicit lifecycle state.

### Machine-Owned Block Pattern

Generated note sections should use an explicit managed section per note plus stable per-item block IDs inside that section so rerenders can replace only the intended content. A suitable v1 pattern is:

```markdown
## Auto Open Tasks
<!-- lark-to-notes:section begin id=daily:2026-04-13:auto-open-tasks -->
<!-- lark-to-notes:block begin id=task:task_123 -->
- [ ] Review pricing update
  - task_id: `task_123`
  - source: [Lark message](...)
<!-- lark-to-notes:block end id=task:task_123 -->
<!-- lark-to-notes:section end id=daily:2026-04-13:auto-open-tasks -->
```

This pattern keeps updates predictable, allows stable item-level replacement inside a bounded section, and protects nearby user-authored content.

### Live Rerender Safety for Machine-Owned Blocks

Live rerenders are higher risk than batch replay because humans may edit the same file between arrivals. The writer must treat **IDs, not cursor positions**, as authoritative.

Replacement rules:

1. The renderer **parses only paired** `<!-- lark-to-notes:block begin id=... -->` / `... end ...` markers with **known prefixes** (`task:`, `section` ids owned by this runtime). Unrecognized or malformed pairs are **left untouched** and reported through doctor or reconcile quarantine rather than repaired heuristically in place.
2. Updates **replace inner Markdown** between begin/end for a **single declared ID** atomically in one file write. Partial writes, duplicated begin markers without matching ends, or ambiguous nesting must **fail the render stage** for that note without truncating the file.
3. When upstream evidence **removes** a task, the default behavior is to **remove the managed block pair** for that `task_id` while leaving all other blocks and all non-managed text unchanged; optional **tombstone one-liners** inside the same section are a policy choice but must not resurrect deleted IDs silently.

User-authored boundaries:

1. Text **outside** all managed `section` spans is **never** auto-edited, including headings the user inserted inside a file that also contains machine sections.
2. If the user **reorders** machine blocks within a section, the next rerender should still locate blocks **by ID** and may normalize ordering back to canonical sort **only** when an explicit `normalize_block_order` policy is enabled; default is **preserve user order** inside the section to reduce surprise.

Concurrency:

1. The **single-writer lock** applies across live and replay paths. A rerender reads the latest file snapshot, applies deterministic transforms, and writes once; conflicts with simultaneous human saves are detected via hash or mtime precondition and surfaced as **blocked render** rather than blind overwrite.

### LLM Usage Requirements

The system shall monitor at least:

1. call count
2. token count
3. call duration
4. average and p95 call duration
5. budget consumption per run and per day
6. cache hit rate for reusable model results
7. fallback count when LLM use is skipped or blocked
8. extreme cases

The system should use this information together with provider pricing and limits to reduce unnecessary LLM cost over time and to degrade gracefully into heuristics-only processing when limits are hit.

## Architecture Proposal

This section proposes one design that satisfies the requirements above.

```mermaid
flowchart LR
watchlist[Watchlist]
polling[PollingFetch]
events[EventStream]
ledger[IntakeLedger]
rawStore[RawCapture]
distill[Distillation]
taskRegistry[TaskRegistry]
vault[VaultRender]
feedback[FeedbackSignals]

watchlist --> polling
watchlist --> events
polling --> ledger
events --> ledger
ledger --> rawStore
rawStore --> distill
distill --> taskRegistry
taskRegistry --> vault
vault --> feedback
feedback --> distill
```

### 1. Source Governance Layer

Use explicit source governance before ingesting anything.

1. Configuration should define allowlists, ignore rules, backfill windows, pause/resume state, and redaction policies.
2. New sources should enter the workflow only through explicit configuration rather than broad implicit discovery.
3. Governance changes should be versioned so shifts in capture behavior can be tied back to a specific configuration change.
4. Per-source volume caps and backfill boundaries should prevent accidental high-volume imports.

### 2. Source Intake Layer

Use a hybrid intake model:

1. polling for sources where periodic fetch is acceptable or necessary
2. event-driven intake where lower latency is useful and technically available

The design target is:

1. roughly five-minute latency for polling-driven updates
2. a couple of seconds for event-driven updates

Implementation rules:

1. All intake paths should enqueue into a shared intake ledger keyed by stable external ID and revision marker.
2. Source-specific cursors should track progress independently for each DM, group, document, or comment stream.
3. A short coalescing window such as 30 to 120 seconds should collapse bursts of replies or rapid document edits into a single downstream batch when quality would not suffer.
4. Intake should run with a single active writer per source stream to avoid conflicting checkpoint updates.

For document coverage, the planned `lark-cli` access paths are:

1. `lark-cli docs +search` to find documents by name when needed
2. `lark-cli docs +fetch` to fetch document content
3. `lark-cli drive file.comments` to retrieve document comments
4. `lark-cli drive file.comment.replys` to retrieve comment replies when needed
5. message retrieval paths for DMs and groups should be confirmed and mapped into the same intake contract

### 3. Unified Source Model

Model the following source classes:

1. DM
2. group chat
3. document source

The document source class contains both:

1. document content
2. document comments

This keeps the model simpler while still allowing metadata to distinguish individual record types internally.

Normalized records should include at least:

1. `source_type`
2. `record_type`
3. `source_stream_id`
4. `source_item_id`
5. `parent_item_id`
6. `revision_id`
7. `lifecycle_state`
8. `canonical_link`
9. `timestamps`
10. `author`
11. `participants`
12. `content_hash`
13. `raw_payload_pointer`
14. `source_item_key`
15. `revision_key`
16. `ingest_key`
17. `intake_path`
18. `observed_at`
19. `stage_state`

`stage_state` should be a durable description of which downstream stages have run for the current canonical revision and whether they succeeded, failed, or still need replay. This must stay separate from immutable raw history so partial failures do not force raw mutation.

`source_item_key` should stay stable across lifecycle transitions for the same logical item, while `revision_key` captures the specific edited, deleted, or superseded version being processed. For reply-like or anchor-scoped records, `parent_item_id` is part of that canonical identity whenever the upstream ID is not globally unique on its own.

### 4. Raw Capture Layer

Every captured item should first be stored as raw source material with stable metadata before higher-level interpretation.

Principles:

1. raw history should be append-only
2. each unique ingest key should map to one immutable raw record
3. a separate current-state materialization may track the latest active revision per source item
4. content-hash checks should skip downstream work when a refetch produces no effective change
5. edits should create superseding raw revisions rather than mutate prior raw history
6. deletes should create tombstone records rather than silently disappear
7. malformed or unparseable payloads should be quarantined for inspection instead of blocking the full pipeline
8. repeated poll or event observations of the same canonical revision should update ledger metadata rather than create a second immutable raw record
9. stage-state persistence must be sufficient to resume distillation, task upsert, or rendering after a crash without requiring ambiguous refetches

Purposes:

1. replay
2. debugging
3. reclassification after rules change
4. source-to-note traceability
5. safe recovery after crashes or partial failures

### 5. Distillation Layer

The distillation layer should convert raw source material into note-ready items and task candidates using configurable signals.

Signals may include:

1. configurable task-like phrasing
2. source type
3. author and participant role
4. directionality
5. language cues
6. urgency cues
7. assignee cues
8. due-date cues
9. surrounding local context
10. thread context
11. document section context
12. prior feedback patterns

Processing policy:

1. deterministic heuristics should run first
2. raw records inside the coalescing window should be distilled as a batch when useful
3. LLM assistance should be reserved for uncertainty, long-context summarization, or multilingual disambiguation
4. each candidate should emit classification, confidence, fingerprint, provenance, a short reason code, and a promotion recommendation such as `daily-only`, `review`, or `current-tasks`

### 6. Task Registry and Lifecycle

The system should maintain stable task identity rather than treating each capture as a new task.

1. `task_id` or task fingerprint should be derived from normalized ask text, source anchor, assignee cues, and a bounded time window.
2. A strong fingerprint match in the same active horizon should update an existing task instead of creating a new one.
3. Repeated evidence should attach additional source links, excerpts, or confidence to the existing task.
4. Task states may include:
   - `open`
   - `needs_review`
   - `snoozed`
   - `dismissed`
   - `completed`
   - `merged`
   - `superseded`
5. Manual completion should remain the default behavior in the early version.
6. Cross-source duplicate linking should start conservative. The first version should prefer linking related evidence over aggressive automatic cross-source merge.

### 7. Feedback Layer

The system should incorporate human feedback into quality improvement rather than depending only on static heuristics.

Feedback actions should include at least:

1. confirm useful
2. dismiss as noise
3. wrong class
4. missed task
5. snooze
6. merge duplicate
7. optional free-text comment

This feedback should guide later tuning of patterns, thresholds, source governance, and model-calling policy.

### 8. Feedback UI Layer

The project should provide a plannotator-based feedback surface for reviewing open tasks, related source context, and structured feedback actions.

Initial UI capabilities should include:

1. show open tasks
2. show related source context
3. show stable task ID, classification, confidence, and canonical source link
4. allow structured triage actions before free-text comments
5. make it easy to point out likely classification errors
6. make it easy to point out likely missed classifications

Artifact rules:

1. feedback artifacts should be machine-parseable through consistent inline conventions or sidecar metadata
2. feedback artifacts may be kept separate from primary durable notes when that reduces merge risk
3. the initial plan still assumes plannotator is the primary feedback interface
4. the v1 default should be a YAML sidecar keyed by stable task IDs and source IDs so humans can inspect and edit it safely without a custom frontend

If a future dedicated frontend is built, it should borrow useful ideas from the plannotator workflow.

### Live-Created Candidates and the Plannotator Feedback Loop

Live intake must not bypass the same structured review and feedback import path used for replay-generated work. The only differences should be provenance labels and timing, not artifact shape or downstream semantics.

What enters review:

1. Any task or candidate emitted with `needs_review`, low confidence, or a classifier reason that implies human triage should appear in the same Markdown review surfaces as replay outputs: primarily the relevant `daily/` lane and any linked `raw/` context the renderer attached.
2. High-confidence promotions to `daily-only` or `current-tasks` may skip plannotator in the moment, but the operator can still open structured feedback against those stable task IDs later; the sidecar format must accept feedback for any rendered stable ID.
3. Live-created uncertain items should never be "CLI-only" or "DB-only" triage in v1: if they are worth surfacing, they must be visible in vault Markdown (or explicitly marked deferred with a backlog reason), then reviewable through plannotator on the same artifacts.

Structured triage and round-trip:

1. Structured actions (confirm, dismiss, wrong class, missed task, snooze, merge, optional comment) apply identically whether the underlying evidence arrived from live workers or from replay.
2. `lark-to-notes feedback import` treats live and replay origins the same: it keys on stable task and source identifiers in the YAML sidecar, not on run mode.
3. `lark-to-notes feedback draft --out <path>` lists review-lane task IDs in a YAML sidecar with placeholder actions so the operator can start from the live registry, edit directives in plannotator or any editor, then import; the draft is intentionally invalid until each `action` is replaced.
4. Import updates the task registry and any bound note metadata, then schedules or completes reconciliation the same way for both paths.

Effect on tuning and policy:

1. Feedback signals from live-created candidates feed the same tuning inputs (pattern weights, thresholds, routing, cap policy hints) as replay-derived feedback; provenance should record `origin=live|replay` for analytics, not for different merge rules.
2. Policy or governance revisions prompted by repeated live-only failure modes (for example systematic wrong-class on a source) should still be expressed as durable config or watched-source changes, not ad hoc one-off overrides hidden outside the feedback pipeline.

Operator expectations:

1. Reviewing a live-created candidate should feel the same as reviewing a replay item: same sidecar shape, same commands, same stable IDs in Markdown.
2. When live sync is ahead of vault render, the operator may see backlog in health or doctor output; that is a delivery lag signal, not a separate feedback channel.
3. Operators should expect higher churn in the `needs_review` lane under noisy live sources; caps and flood protection reduce volume but do not change how reviewed items are corrected through plannotator.

### 9. Note Rendering Layer

The system should write automatically into the notes vault.

Rendering order:

1. raw source note or context record in `raw/`
2. relevant daily note
3. Current Tasks promotion or update
4. durable people, project, or area note updates when warranted

Safety rules:

1. machine-owned blocks only
2. stable IDs and anchors for generated sections
3. one-writer runtime for note updates
4. no overwrite of user-authored sections
5. backlink creation between source notes and curated notes
6. preference for updating existing canonical notes over creating near-duplicates

### 10. Runtime and Operations Layer

The system should provide both:

1. one-shot commands for initialization, replay, backfill, sync, reclassification, and reconcile
2. a continuous background runtime for normal operation

Operational behavior should include:

1. a shared queue for intake and distillation
2. worker concurrency limits and backpressure
3. retries with exponential backoff for transient failures
4. a quarantine or dead-letter path for malformed data and permanent failures
5. a runtime lock to prevent concurrent writers
6. health metrics for lag, queue depth, error rate, duplicate rate, and backlog age
7. periodic reconciliation that compares stored cursors against source state and refetches gaps
8. auth-expiry and credential-recovery guidance

### Live Serialized Note Writer and Locking Contract

Live mode amplifies concurrency pressure because **poll**, **event**, **replay**, **reclassify**, and **reconcile** can all attempt note mutations in the same wall-clock window. The contract is still **exactly one serialized writer** for vault files.

Queue and lock model:

1. All vault mutations (raw, daily, Current Tasks, and any future opt-in durable pages) pass through a **single writer queue** with a durable **runtime lock** (for example SQLite advisory lock, filesystem lock file under `var/`, or equivalent) held for the duration of **read–merge–write** for each target path.
2. **Intake and ledger writers** may proceed in parallel only where they do not touch Markdown; the moment a stage needs to read or write a vault path, it **enqueues a writer job** rather than opening the file directly from arbitrary worker threads.
3. **Lock acquisition** uses a bounded timeout. On timeout, the job **fails without partial writes**, emits a structured diagnostic (`writer_lock_contended`, target path, holder hint), and leaves stage state dirty for retry—never a torn file.

Interaction of paths:

1. **Replay and live** must share the same queue ordering rules (generally FIFO per vault root with optional priority for operator-initiated commands over background sync).
2. **Reconcile repair** enqueues like any other writer; it does not bypass the lock.
3. If a human editor holds an external Obsidian lock the runtime cannot see, **mtime or content-hash preconditions** on writer commits still detect unexpected concurrent edits and abort with the same contended-writer diagnostic.

### 11. Performance and Cost-Control Layer

The project should explicitly control LLM spend and end-to-end throughput.

Key controls:

1. deterministic-first processing
2. selective LLM invocation
3. provider-aware configuration
4. usage and latency metrics
5. coalescing and batching
6. revision-based or content-hash skip
7. raw and model-result caching keyed by normalized content and policy version
8. chunking strategy for large documents
9. max tokens and max items per batch
10. budget caps per run and per day
11. graceful degradation into heuristics-only mode when budgets or provider limits are hit

### Live LLM Budget, Provider Routing, and Heuristics-Only Fallback

Live runs should treat LLM spend as a **metered resource** with the same observability as batch replay, but with **stricter pre-flight checks** because incremental work can silently accumulate calls.

Stages and eligibility:

1. **Intake, ledger, raw capture, and note rendering** must not call models in v1. **Selective distillation or reclassification** stages may call models only when policy marks an item as **LLM-eligible** (uncertainty, long context, multilingual disambiguation) and the item has not already consumed its per-task call budget.
2. Each eligible call passes through a **router** that chooses the active provider configuration; v1 uses **one active provider policy at a time** without blending.

Accounting and caps:

1. Every live run records **call count, token estimates, wall time, cache hits, fallbacks, and budget headroom** into run history and surfaces summarized totals in **doctor** output.
2. **Per-run and per-day caps** short-circuit remaining LLM-eligible work: items past the cap stay in **heuristics-only** outcomes (`needs_review`, degraded summary, or omitted optional summarization) with explicit **skip reasons** attached to tasks or run diagnostics—not silent truncation.

Fallback behavior:

1. When the provider returns **auth, quota, or hard errors**, the runtime switches that batch to **heuristics-only** for the remainder of the run and sets a **sticky degraded flag** visible to the operator until cleared.
2. **Heuristics-only mode** must still preserve **provenance and task visibility**; it must not promote extra aggression into Current Tasks to “compensate” for missing models.

### Live Throughput, Backpressure, Coalescing, and Load Shedding

Throughput controls complement **governance caps** (see ingestion caps): caps bound accepted volume; backpressure bounds how much accepted work is **actively processed** when downstream stages lag.

Chunking and batching:

1. **Large documents and long threads** use deterministic **chunking** with stable chunk IDs so cache entries, ledger references, and rerenders line up across retries.
2. **Coalescing windows** collapse bursty edits or reply storms into a single downstream batch per stream while still appending **immutable raw history** per revision rules; coalescing delays work but does not drop revisions.

Caching:

1. **Model-result and normalized-text caches** are keyed by `(ingest_key or content_hash, policy_version, provider_route)` so replay and live share invalidation semantics.

Backpressure and shedding:

1. Each stage exposes **queue depth and age metrics**. When depth crosses thresholds, the runtime applies **ordered shedding**: first defer optional rerenders or cosmetic refreshes, then defer non-urgent distillation extras, and only under extreme pressure defer intake processing—preferring to **honor caps and explicit deferral** over silent loss.
2. Shedding decisions are **operator-visible** in health output and tied to **per-source** backlog signals so noisy chats cannot starve quiet sources without showing up in metrics.
3. **Retries** honor the same cap and backpressure policy so retry storms cannot bypass limits.

## Canonical Local Data Model

The plan now needs a concrete local model so implementation can begin without inventing storage boundaries on the fly. The first version should include at least the following durable entities:

### 1. Watched Sources

Purpose: govern what is allowed into the pipeline.

Suggested fields:

1. `source_id`
2. `source_type`
3. `selection_mode`
4. `source_ref`
5. `display_name`
6. `paused`
7. `backfill_start`
8. `ignore_rules`
9. `redaction_policy`
10. `governance_version`
11. `run_cap`
12. `source_cap`
13. `cap_policy`
14. `last_cap_hit_at`

Field notes:

1. `selection_mode` should preserve whether the source came from direct link, explicit token, or name search.
2. `source_ref` should preserve the original operator input used for selection plus the canonical resolved identifier once available.

### 2. Intake Ledger

Purpose: unify polling and event-driven ingestion paths.

Suggested fields:

1. `ingest_key`
2. `source_id`
3. `source_stream_id`
4. `source_item_id`
5. `parent_item_id`
6. `revision_id`
7. `latest_revision_id`
8. `lifecycle_state`
9. `first_intake_path`
10. `last_intake_path`
11. `poll_seen_count`
12. `event_seen_count`
13. `first_seen_at`
14. `last_seen_at`
15. `coalesce_until`
16. `processing_state`
17. `raw_captured_at`
18. `distilled_at`
19. `task_upserted_at`
20. `rendered_at`
21. `stage_state`
22. `last_error`
23. `policy_version`

Notes:

1. The ledger row is the mutable operational state for one canonical revision-bearing item, not a second copy of immutable raw history.
2. `stage_state` may be materialized either as explicit per-stage timestamp columns plus status or as a structured payload, but it must answer whether raw capture, distillation, task upsert, and rendering have already succeeded for the current canonical revision.
3. `latest_revision_id` and `lifecycle_state` let the ledger represent the newest observed state for the logical item without rewriting prior raw records.
4. `first_intake_path` and `last_intake_path` preserve whether an item first arrived by polling or events and whether later sightings changed that path mix.

### 2b. Source Stream Checkpoints

Purpose: track replay-safe progress per watched stream without conflating stream position with item-level ledger state.

Suggested fields:

1. `source_id`
2. `source_stream_id`
3. `cursor_kind`
4. `cursor_value`
5. `last_seen_item_id`
6. `last_seen_revision_id`
7. `last_seen_timestamp`
8. `page_token`
9. `checkpoint_state`
10. `last_reconciled_at`
11. `updated_at`
12. `policy_version`

Notes:

1. Checkpoints are per source stream, not global, so a document comment stream, a DM stream, and a group-chat stream can advance independently.
2. The checkpoint should move only after the corresponding batch is durable enough to replay from local state rather than requiring ambiguous re-fetch behavior.
3. `cursor_kind` exists because some surfaces advance by timestamp, some by message or revision ID, and some by opaque server-issued paging tokens.
4. `checkpoint_state` should distinguish at least healthy, lagging, and needs_reconcile conditions so runtime repair can act without guessing.

### 3. Raw Records

Purpose: preserve immutable source history and replayability.

Suggested fields:

1. `raw_record_id`
2. `ingest_key`
3. `source_type`
4. `record_type`
5. `source_stream_id`
6. `source_item_id`
7. `parent_item_id`
8. `revision_id`
9. `lifecycle_state`
10. `canonical_link`
11. `author_ref`
12. `participants`
13. `created_at`
14. `updated_at`
15. `captured_at`
16. `content_hash`
17. `payload_json`
18. `normalized_text`
19. `policy_version`

### 4. Current Item State

Purpose: expose the latest active revision without mutating raw history.

Suggested fields:

1. `source_item_id`
2. `latest_raw_record_id`
3. `latest_revision_id`
4. `lifecycle_state`
5. `effective_hash`
6. `superseded_at`

### 5. Task Registry

Purpose: maintain stable task identity and lifecycle.

Suggested fields:

1. `task_id`
2. `fingerprint`
3. `title`
4. `status`
5. `classification`
6. `confidence_band`
7. `summary`
8. `reason_code`
9. `assignee_refs`
10. `due_at`
11. `manual_override_state`
12. `created_from_raw_record_id`
13. `last_updated_at`

### 6. Task Evidence

Purpose: attach repeated source evidence to an existing task instead of creating duplicates.

Suggested fields:

1. `task_id`
2. `raw_record_id`
3. `source_item_id`
4. `excerpt`
5. `confidence_delta`
6. `evidence_role`

### 7. Note Bindings

Purpose: track where a task or source record was rendered in the vault.

Suggested fields:

1. `binding_id`
2. `target_path`
3. `target_kind`
4. `stable_block_id`
5. `entity_type`
6. `entity_id`
7. `render_hash`
8. `last_rendered_at`

### 8. Feedback Events

Purpose: preserve structured review actions as reusable learning signals.

Suggested fields:

1. `feedback_id`
2. `target_type`
3. `target_id`
4. `action`
5. `comment`
6. `actor_ref`
7. `created_at`
8. `artifact_path`

### 9. Run History

Purpose: support operations, debugging, and cost monitoring.

Suggested fields:

1. `run_id`
2. `command_name`
3. `started_at`
4. `finished_at`
5. `items_seen`
6. `items_changed`
7. `tasks_created`
8. `tasks_updated`
9. `llm_calls`
10. `llm_tokens`
11. `error_count`
12. `quarantine_count`

## CLI Surface

The first version should expose a small, explicit CLI surface instead of scattering workflow logic across scripts.

Recommended commands:

1. `lark-to-notes sources list`
2. `lark-to-notes sources validate`
3. `lark-to-notes backfill`
4. `lark-to-notes sync once`
5. `lark-to-notes sync daemon`
6. `lark-to-notes replay`
7. `lark-to-notes reclassify`
8. `lark-to-notes render`
9. `lark-to-notes feedback import`
10. `lark-to-notes reconcile`
11. `lark-to-notes doctor`

### Operator Status, Inspect, and Doctor Command Surface (Live)

Operators must understand **what is watched, what is blocked, what is lagging, and what budgets or locks apply** without opening SQLite. Live sync therefore extends the CLI with explicit **read-mostly inspection** commands alongside mutating workflows.

Recommended additions (names may map to subcommands or flags, but the capabilities should exist):

1. **`sources status`** — per watched source: resolution state, pause, caps, last successful run, backlog or lag hints, auth or capability blockers, and last drift event for search rules.
2. **`runtime status`** — queue depths, writer lock state, active degraded modes (for example heuristics-only), coalescing backlog, and per-stage age metrics.
3. **`budget status`** — per day and per run: LLM headroom, calls skipped for caps, cache hit rates, and last provider error class.
4. **`inspect ledger|task|raw <id>`** — bounded, redaction-aware dumps of canonical rows and stage timestamps for support and debugging.
5. **`doctor --json` (or equivalent)** — machine-parseable superset including the above summaries plus migration honesty flags (which gates are satisfied, which source classes are unsupported).

Human versus machine output:

1. Default TTY output favors **short narratives with next actions**; `--json` favors **stable field names** for scripting and dashboards.
2. Partial or degraded support must be **explicit** (`unsupported`, `blocked`, `degraded`) rather than folding into generic success exit codes.

### Live command ergonomics

1. Long-running commands support **non-interactive** mode suitable for cron and agents: clear exit codes, log-friendly stderr, and `--limit` style bounds on inspect dumps.
2. Dangerous mutating commands continue to require **explicit flags** or **dry-run** where appropriate so inspect-first workflows remain safe.

## Rationale

### Why Include Documents and Comments

Important work often appears in document comments and collaborative docs, not only in chats. Excluding them would leave major knowledge sources outside the workflow.

### Why Define an Ingestion Contract

Hybrid intake is only useful if polling, events, retries, and replay all converge on the same result. Stable IDs, per-source cursors, and idempotent downstream writes prevent duplicate tasks and make crash recovery safe.

### Why Model Mutable Documents and Comments

Documents and comments are not static. Revision tracking, tombstones, and supersession let the system preserve history without confusing stale text with current work.

### Why Prefer Recall Over Precision

For this workflow, ignoring real work items is worse than producing some extra cleanup. The design should therefore lean toward capturing likely work, then use feedback and later refinement to improve quality.

### Why Add Task Identity and a Review Lane

Recall-first systems create noise. Stable task fingerprints, merge rules, and a `needs-review` lane preserve recall while limiting duplicate cleanup and making uncertainty visible.

### Why Keep Manual Completion at First

Automatically deciding whether a task is done is harder than identifying likely work from content. It is better to keep completion manual initially and focus the first iterations on reliable capture and promotion.

### Why Follow the Vault Contract

The vault already has a durable maintenance workflow. Writing raw first, then daily, then Current Tasks, then durable notes keeps automation aligned with how the vault is curated and reduces note sprawl.

### Why Use Machine-Owned Blocks

Automatic note updates are only trustworthy if they do not clobber user writing. Explicitly delimited generated regions make reruns safe and preserve manual edits.

### Why Prefer Structured Feedback Over Comments Alone

Free-text comments are helpful but hard to learn from at scale. Structured triage actions make high-volume review faster and turn feedback into reusable quality signals.

### Why Monitor Cost Early

If the system expands across many sources or large documents, LLM cost can grow quickly. Cost awareness and graceful degradation should therefore be designed in from the start rather than added after behavior becomes expensive.

### Why Use Plannotator First

Markdown and Obsidian are already central to the workflow, and plannotator can open Markdown artifacts and collect structured human feedback close to the generated content. That makes it a good first feedback surface without forcing a separate application immediately.

### Why Avoid Multi-Model Blending in V1

The main risk in this project is workflow correctness, provenance, and safe note mutation rather than frontier-model disagreement. A single-provider, deterministic-first approach is easier to reason about, easier to debug, cheaper to operate, and easier to replay consistently.

## Risks

1. Too many false positives can still create cleanup burden.
2. Too few captured tasks can cause neglected work.
3. Document and comment ingestion may introduce more volume than chat-only ingestion.
4. Mutable document history can create stale or conflicting derived notes if revision semantics are weak.
5. LLM cost may grow if invocation, batching, and budgets are not constrained.
6. Source permissions or auth expiry may limit complete visibility through `lark-cli`.
7. Automatic writes can still erode trust if machine-owned boundaries are unclear.
8. Overly aggressive cross-source merging can hide genuinely distinct tasks.
9. Privacy or sensitivity mistakes in watched sources can create durable notes that should not exist.

## Open Questions

The following questions remain open, but the defaults elsewhere in this plan are sufficient to start implementation.

1. Which additional documents should be watched after the initial seeded document link?
2. How aggressive should cross-source task merging be in the initial version beyond conservative evidence-linking?
3. What confirmed Copilot enterprise limits should be treated as hard operating constraints?
4. What redaction or retention policy is required for sensitive sources?
5. After initial use, does the YAML-sidecar feedback artifact remain sufficient, or does the workflow need a richer review artifact?
6. When, if ever, does the project outgrow plannotator and need a separate frontend?

## Testing and Validation Strategy

The project should be validated against a representative fixture corpus before it is trusted for everyday use.

### Fixture Corpus

Build a local test corpus that includes:

1. DMs, group-chat threads, documents, comments, and comment replies
2. English, Chinese, and mixed-language samples
3. edits, deletes, repeated asks, and near-duplicate asks
4. examples that should stay as context rather than becoming tasks

### Test Categories

1. unit tests for normalization, fingerprinting, heuristics, and block rendering
2. integration tests for `lark-cli` adapters and source normalization
3. replay tests proving idempotent note updates and task identity
4. golden-file tests for raw-note, daily-note, and Current Tasks rendering
5. locking tests for the single-writer note update model
6. failure-path tests for malformed payload quarantine and retry behavior
7. redaction tests for sensitive-source handling

## Phased Plan

### Phase 0: Source Access Baseline and Fixture Corpus

1. confirm the actual `lark-cli` retrieval commands and payload shapes for DMs, groups, documents, comments, and comment replies
2. capture representative sample payloads for each in-scope source class
3. build the initial local fixture corpus for replay and test development
4. document known capability gaps or permission blockers before implementation proceeds

### Phase 1: Governance, Scope, and Source Model

1. finalize watched source categories and explicit allowlist rules
2. define how documents are selected by link or name
3. define required source metadata and canonical external IDs
4. confirm `lark-cli` retrieval paths for messages, documents, comments, and comment replies
5. decide pause/resume, ignore, redaction, and backfill policies

### Phase 2: Intake Contract, Raw Capture, and Replay

1. implement or refine the shared intake ledger
2. persist immutable raw records, revisions, and tombstones
3. implement per-source cursors, checkpoints, and content-hash skip
4. implement replay, reclassification, and reconciliation support

### Phase 3: Task Identity and Action-Item Generation

1. implement configurable task-like patterns and multilingual heuristics
2. implement confidence bands and the `needs-review` lane
3. implement task fingerprinting, merge rules, and provenance fields
4. add selective LLM escalation only for uncertain or large-context cases
5. add feedback capture hooks and a minimal review artifact export for `needs-review` handling

### Phase 4: Vault-Safe Note Automation

1. automate raw note or raw context creation in `raw/`
2. automate daily note updates
3. automate Current Tasks promotion and updates
4. keep broader people, project, and area note updates manual or opt-in until the core rendered surfaces are stable
5. enforce machine-owned blocks, stable IDs, and backlink rules

### Phase 5: Feedback Workflow

1. harden the Markdown plus YAML feedback artifact for plannotator
2. define structured triage actions plus optional comments
3. connect plannotator feedback to tuning inputs and governance updates
4. capture reusable ideas in case a later frontend is needed

### Phase 6: Runtime, Performance, and Operations

1. add continuous background runtime
2. add event-driven updates where appropriate
3. add coalescing, batching, caching, and large-document chunking
4. add retries, backpressure, locking, and dead-letter handling
5. add observability, reconciliation, and budget enforcement

### Phase 7: Cost and Quality Refinement

1. tune provider routing and escalation thresholds
2. reduce unnecessary model calls
3. refine heuristics and configuration from structured feedback
4. track duplicate rate, dismiss rate, and confirm rate over time
5. refine the plannotator workflow and only consider a later frontend if needed

## Phase Exit Criteria

### Phase 0 Exit Criteria

1. each in-scope source class has at least one confirmed retrieval path or a documented blocker
2. the fixture corpus contains multilingual samples and lifecycle-change examples

### Phase 1 Exit Criteria

1. watched-source configuration can represent every intended source class
2. ingestion keys, cursor semantics, and policy versions are defined clearly enough to implement

### Phase 2 Exit Criteria

1. raw history is append-only and replayable across revisions, tombstones, and supersession
2. identical reruns or mixed poll/event sightings do not duplicate raw records or downstream note writes
3. source cursors advance only after durable raw capture plus stage handoff, and partial failures can resume from stored stage state without redefining item identity

### Phase 3 Exit Criteria

1. the fixture corpus produces stable task IDs on replay
2. lower-confidence items are visible through a review lane instead of silently disappearing

### Phase 4 Exit Criteria

1. rerendering updates machine-owned blocks without modifying nearby user-authored text
2. raw, daily, and Current Tasks outputs are stable under replay

### Phase 5 Exit Criteria

1. confirmations, dismissals, merges, snoozes, and missed-task actions round-trip cleanly through the feedback artifact
2. manual overrides persist across replay unless intentionally reset

### Phase 6 Exit Criteria

1. background sync can run without conflicting writers
2. reconciliation can detect and repair cursor drift or missed updates

### Phase 7 Exit Criteria

1. LLM usage remains within acceptable operating limits
2. dismiss rate and duplicate rate trend down as feedback is incorporated

## Acceptance Orientation

The project should later be evaluated against:

1. completeness of raw capture for in-scope sources
2. correctness of preserved metadata, revisions, and lifecycle state
3. idempotent reruns and replays that do not create duplicate tasks or note blocks
4. quality of task generation under multilingual input
5. usefulness of automatically updated notes in daily and Current Tasks views
6. ability to jump from an open task to its source context quickly through canonical links and excerpts
7. preservation of user-authored content outside machine-owned blocks
8. measurable sync health, queue lag, and reconciliation success
9. measurable LLM usage, cache effectiveness, and budget control
10. ability to improve quality over time through structured human feedback

## Implementation Readiness Checklist

The project is ready to start once the following are pinned down in code or fixtures:

1. the source-access matrix for every in-scope Lark surface
2. the canonical normalization contract
3. the SQLite schema for watched sources, ledger, raw records, tasks, note bindings, feedback, and run history
4. the machine-owned block format
5. the task fingerprinting policy
6. the feedback artifact format
7. the replay and reclassification semantics
8. the LLM routing and budget policy

## Repository Role

This file is the normative requirements and implementation-guidance document for the project. It should evolve as questions are answered and implementation begins. Prototypes such as `automation/lark_worker/` may inform it, but if a prototype conflicts with this document, this document wins until intentionally revised.
