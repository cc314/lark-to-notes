"""Deterministic **effective reaction state** from ordered add/remove events.

Used by tests and future vault/distillation code. Each logical key is
``(emoji_type, operator_key)`` where *operator_key* is typically ``open_id`` or
``user_id`` depending on upstream payload normalization.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter

from lark_to_notes.intake.reaction_model import ReactionKind

ReactionKey = tuple[str, str]


def apply_reaction_step(
    counts: Counter[ReactionKey],
    *,
    kind: ReactionKind,
    emoji_type: str,
    operator_key: str,
) -> None:
    """Mutate *counts* with one add or remove (remove clamps at zero)."""

    key = (emoji_type, operator_key)
    if kind == ReactionKind.ADD:
        counts[key] += 1
        return
    if kind == ReactionKind.REMOVE:
        counts[key] = max(0, counts[key] - 1)
        if counts[key] == 0:
            del counts[key]
        return
    raise ValueError(f"unsupported reaction kind: {kind!r}")


def materialize_effective_counts(
    steps: list[tuple[ReactionKind, str, str]],
) -> dict[ReactionKey, int]:
    """Return immutable snapshot of counts after applying *steps* in order."""

    c: Counter[ReactionKey] = Counter()
    for kind, emoji, op in steps:
        apply_reaction_step(c, kind=kind, emoji_type=emoji, operator_key=op)
    return dict(c)


def effective_reaction_set_fingerprint(counts: dict[ReactionKey, int]) -> str:
    """Stable SHA-256 digest of the sorted effective reaction multiset.

    Row order matches ``format_reaction_summary_markdown`` (same sort as the
    vault table) for replay vs summary checks (lw-pzj.10.11).
    """

    items = [
        {"emoji_type": emoji, "operator_key": op, "count": int(c)}
        for (emoji, op), c in sorted(counts.items(), key=lambda x: (x[0][0], x[0][1]))
    ]
    payload = json.dumps(items, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
