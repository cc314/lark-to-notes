#!/usr/bin/env bash
# Offline end-to-end demo: fixture JSONL → SQLite → tasks → Markdown in your vault.
# Default vault: ~/projects/notes (matches repo lark-to-notes.toml vault_root).
# Usage:
#   ./scripts/demo_offline_vault.sh
#   VAULT_ROOT=/path/to/vault ./scripts/demo_offline_vault.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

VAULT_ROOT="${VAULT_ROOT:-$HOME/projects/notes}"
DB_PATH="${DB_PATH:-$VAULT_ROOT/var/lark-to-notes.db}"
RAW_FIXTURES="$ROOT/tests/fixtures/lark-worker"
CORPUS="$ROOT/tests/fixtures/lark-worker/fixture-corpus"

mkdir -p "$VAULT_ROOT/var" "$VAULT_ROOT/raw" "$VAULT_ROOT/area/current tasks" "$VAULT_ROOT/daily"

echo "== Lark-to-notes offline demo =="
echo "repo:       $ROOT"
echo "vault:      $VAULT_ROOT"
echo "database:   $DB_PATH"
echo "fixtures:   $RAW_FIXTURES"
echo

echo ">> replay (ingest fixture JSONL into SQLite)"
uv run lark-to-notes replay --db "$DB_PATH" --raw-dir "$RAW_FIXTURES" --json

echo
echo ">> reclassify (heuristics → task rows)"
uv run lark-to-notes reclassify --db "$DB_PATH" --limit 500 --json

echo
echo ">> render (tasks → vault Markdown)"
uv run lark-to-notes render --db "$DB_PATH" --vault-root "$VAULT_ROOT" --limit 500 --json

echo
echo ">> doctor (fixture corpus + runtime health)"
uv run lark-to-notes doctor --db "$DB_PATH" --fixture-corpus "$CORPUS" --json | head -c 400
echo
echo "… (doctor JSON truncated)"
echo
echo "Done. Open your vault (e.g. in Obsidian): $VAULT_ROOT"
echo "Look under raw/ and area/current tasks/ for new machine-owned output."
