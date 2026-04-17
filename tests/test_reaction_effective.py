"""Unit tests for :mod:`lark_to_notes.intake.reaction_effective`."""

from __future__ import annotations

from collections import Counter
from itertools import product

from lark_to_notes.intake.reaction_effective import (
    apply_reaction_step,
    effective_reaction_set_fingerprint,
    materialize_effective_counts,
)
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


def test_effective_fingerprint_matches_golden_fixture_counts() -> None:
    """Digest for a known effective map (lw-pzj.10.4 golden counts)."""

    effective = {("THUMBSUP", "ou_alice"): 2, ("SMILE", "ou_bob"): 1}
    assert (
        effective_reaction_set_fingerprint(effective)
        == "7b12ec58338ef4dddac752f46f773c0b3d66e67fc3df2c62fb586565d35d05cf"
    )


def test_effective_fingerprint_stable_under_dict_insertion_order() -> None:
    a = {("B", "x"): 1, ("A", "y"): 2}
    b = {("A", "y"): 2, ("B", "x"): 1}
    assert effective_reaction_set_fingerprint(a) == effective_reaction_set_fingerprint(b)


def test_exhaustive_chunked_replay_matches_materialize_small_grid() -> None:
    """Every split of an ordered step list matches one-shot materialize (lw-pzj.10.11)."""

    emojis = ("A", "B")
    ops = ("u1", "u2")
    choices: list[tuple[ReactionKind, str, str]] = [
        (ReactionKind.ADD, e, o) for e in emojis for o in ops
    ] + [(ReactionKind.REMOVE, e, o) for e in emojis for o in ops]
    max_len = 4
    for length in range(max_len + 1):
        for seq in product(choices, repeat=length):
            steps = list(seq)
            expected = materialize_effective_counts(steps)
            fp = effective_reaction_set_fingerprint(expected)
            assert fp == effective_reaction_set_fingerprint(dict(expected.items()))
            for split in range(len(steps) + 1):
                c: Counter[tuple[str, str]] = Counter()
                for kind, emoji, op in steps[:split]:
                    apply_reaction_step(c, kind=kind, emoji_type=emoji, operator_key=op)
                for kind, emoji, op in steps[split:]:
                    apply_reaction_step(c, kind=kind, emoji_type=emoji, operator_key=op)
                assert dict(c) == expected
            doubled = materialize_effective_counts(steps + steps)
            c2: Counter[tuple[str, str]] = Counter()
            for kind, emoji, op in steps:
                apply_reaction_step(c2, kind=kind, emoji_type=emoji, operator_key=op)
            for kind, emoji, op in steps:
                apply_reaction_step(c2, kind=kind, emoji_type=emoji, operator_key=op)
            assert dict(c2) == doubled
            assert effective_reaction_set_fingerprint(dict(c2)) == effective_reaction_set_fingerprint(doubled)
