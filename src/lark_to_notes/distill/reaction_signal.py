"""``reaction_signal`` evidence objects for reaction → task distill (lw-pzj.8.2).

Stable :attr:`ReactionSignalEvidence.signal_id` is derived from
``(source_id, message_id, ruleset_version, effective_reaction_set_fingerprint)``
so replays and vault rerenders do not mint new identities for the same logical
evidence.  Optional :attr:`vault_projection_fingerprint` links machine-rendered
raw blocks when present.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ReactionSignalEvidence:
    """Conservative evidence bundle attached to a candidate task.

    Attributes:
        signal_id: Deterministic id from :func:`reaction_signal_id`.
        source_id: Watched source (e.g. ``dm:…``).
        message_id: Lark message id (e.g. ``om_…``).
        ruleset_version: :class:`~lark_to_notes.distill.reaction_rules.ReactionRuleset.version`.
        effective_reaction_set_fingerprint: Digest of effective reaction counts
            (see :func:`effective_reaction_set_fingerprint
            <lark_to_notes.intake.reaction_effective.effective_reaction_set_fingerprint>`).
        vault_projection_fingerprint: Optional digest from
            :func:`lark_to_notes.render.reaction_vault.reaction_vault_projection_fingerprint`.
        reason_codes: Machine codes explaining why this evidence was emitted.
    """

    signal_id: str
    source_id: str
    message_id: str
    ruleset_version: str
    effective_reaction_set_fingerprint: str
    vault_projection_fingerprint: str = ""
    reason_codes: tuple[str, ...] = ()


def reaction_signal_id(
    *,
    source_id: str,
    message_id: str,
    ruleset_version: str,
    effective_reaction_set_fingerprint: str,
) -> str:
    """Return stable ``rxsig_`` id for the quadruple above."""

    payload = json.dumps(
        {
            "effective_reaction_set_fingerprint": effective_reaction_set_fingerprint,
            "message_id": message_id.strip(),
            "ruleset_version": ruleset_version.strip(),
            "source_id": source_id.strip(),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"rxsig_{digest}"


def build_reaction_signal_evidence(
    *,
    source_id: str,
    message_id: str,
    ruleset_version: str,
    effective_reaction_set_fingerprint: str,
    vault_projection_fingerprint: str = "",
    reason_codes: tuple[str, ...] = (),
) -> ReactionSignalEvidence:
    """Construct a :class:`ReactionSignalEvidence` with a fresh ``signal_id``."""

    sid = reaction_signal_id(
        source_id=source_id,
        message_id=message_id,
        ruleset_version=ruleset_version,
        effective_reaction_set_fingerprint=effective_reaction_set_fingerprint,
    )
    return ReactionSignalEvidence(
        signal_id=sid,
        source_id=source_id.strip(),
        message_id=message_id.strip(),
        ruleset_version=ruleset_version.strip(),
        effective_reaction_set_fingerprint=effective_reaction_set_fingerprint,
        vault_projection_fingerprint=vault_projection_fingerprint.strip(),
        reason_codes=reason_codes,
    )
