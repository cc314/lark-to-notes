"""Unit tests for :mod:`lark_to_notes.intake.reaction_effective`."""

from __future__ import annotations

from lark_to_notes.intake.reaction_effective import materialize_effective_counts
from lark_to_notes.intake.reaction_model import ReactionKind


def test_remove_clamps_and_delete_zero() -> None:
    steps = [
        (ReactionKind.ADD, "OK", "ou_a"),
        (ReactionKind.REMOVE, "OK", "ou_a"),
        (ReactionKind.REMOVE, "OK", "ou_a"),
    ]
    assert materialize_effective_counts(steps) == {}


def test_multi_add_same_key() -> None:
    steps = [
        (ReactionKind.ADD, "OK", "ou_a"),
        (ReactionKind.ADD, "OK", "ou_a"),
        (ReactionKind.REMOVE, "OK", "ou_a"),
    ]
    assert materialize_effective_counts(steps) == {("OK", "ou_a"): 1}


def test_independent_operators() -> None:
    steps = [
        (ReactionKind.ADD, "OK", "ou_a"),
        (ReactionKind.ADD, "OK", "ou_b"),
        (ReactionKind.REMOVE, "OK", "ou_a"),
    ]
    assert materialize_effective_counts(steps) == {("OK", "ou_b"): 1}


def test_deterministic_same_sequence() -> None:
    seq = [
        (ReactionKind.ADD, "A", "u1"),
        (ReactionKind.ADD, "B", "u1"),
        (ReactionKind.REMOVE, "A", "u1"),
    ]
    assert materialize_effective_counts(seq) == materialize_effective_counts(list(seq))


def test_replaying_full_sequence_doubles_adds() -> None:
    seq = [(ReactionKind.ADD, "OK", "u")]
    assert materialize_effective_counts(seq) == {("OK", "u"): 1}
    assert materialize_effective_counts(seq + seq) == {("OK", "u"): 2}
