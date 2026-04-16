#!/usr/bin/env bash
# Example operator wrapper: stream Lark WS events into `sync-events` stdin.
# Copy, chmod +x, set env vars, then point a LaunchAgent at this script.
set -euo pipefail
: "${LARK_TO_NOTES_DB:?set LARK_TO_NOTES_DB to the canonical SQLite path}"
: "${LARK_CHAT_SOURCE_ID:?set LARK_CHAT_SOURCE_ID to the watched dm:… or group source_id}"
exec lark-cli event +subscribe --format json-compact \
  | uv run lark-to-notes sync-events \
      --db "${LARK_TO_NOTES_DB}" \
      --source-id "${LARK_CHAT_SOURCE_ID}" \
      --json
