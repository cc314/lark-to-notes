"""Stable task fingerprinting for the task registry.

A fingerprint is a short deterministic hash derived from the normalised
content of a message, its source anchor, and a weekly time-window bucket
derived from the message's ``created_at`` timestamp.

Design goals:

* **Replay-stable**: re-processing the same raw record always yields the
  same fingerprint so task IDs do not drift between runs.
* **Conservative merging**: the weekly bucket means messages from the same
  source in the same week with sufficiently similar text map to the same
  fingerprint.  Cross-week or cross-source content always produces a
  different fingerprint.
* **Truncated hash**: 16 hex characters (64 bits) are long enough to be
  collision-free in practice while remaining readable in logs and UIs.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime

_PUNCT_RE = re.compile(r"[^\w\s@#\u4e00-\u9fff]")
_WS_RE = re.compile(r"\s+")

# Max normalised characters included in the fingerprint hash.
_CONTENT_WINDOW = 200


def derive_fingerprint(
    content: str,
    source_id: str,
    created_at: str,
    *,
    source_type: str = "",
) -> str:
    """Return a 16-character hex fingerprint for one raw message.

    The fingerprint is derived from the normalised leading text of
    *content*, source identity context, and the ISO week that *created_at*
    falls in.

    Args:
        content:    Full message text.
        source_id:  Watched-source identifier.
        created_at: Timestamp in either ``"YYYY-MM-DD HH:MM"`` (Lark
                    format) or ISO 8601 format.
        source_type: Optional source-surface discriminator (for example
                    ``dm_user``, ``chat``, ``doc_comment``). Including this
                    avoids cross-surface collisions when the same source
                    anchor carries different item classes.

    Returns:
        A 16-character lowercase hex string.
    """
    normalised = _normalize_text(content)[:_CONTENT_WINDOW]
    bucket = _week_bucket(created_at)
    stable_source_type = source_type.strip().lower()
    if stable_source_type:
        raw = f"{source_id}\x00{stable_source_type}\x00{bucket}\x00{normalised}"
    else:
        # Preserve the legacy fingerprint contract when no source_type is provided.
        raw = f"{source_id}\x00{bucket}\x00{normalised}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _normalize_text(text: str) -> str:
    """Lowercase, remove noise punctuation, collapse whitespace."""
    text = text.lower()
    text = _PUNCT_RE.sub(" ", text)
    return _WS_RE.sub(" ", text).strip()


def _week_bucket(created_at: str) -> str:
    """Return ``"YYYY-WWW"`` (ISO week) for *created_at*.

    Falls back to ``"unknown"`` for unparseable input so the fingerprint
    is always a valid string rather than raising.
    """
    try:
        if "T" in created_at:
            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        else:
            dt = datetime.strptime(created_at[:16], "%Y-%m-%d %H:%M")
        iso = dt.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    except (ValueError, AttributeError):
        return "unknown"
