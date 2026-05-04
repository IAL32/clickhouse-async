"""Tests for the _nest / _flatten dict helpers in _json_helpers.py."""

from __future__ import annotations

from clickhouse_async.types._json_helpers import _flatten, _nest

# ---- _nest -----------------------------------------------------------------


def test_nest_helper_reconstructs_nested_dict() -> None:
    # BEGIN / WHEN: flat dict with dotted-path keys
    flat = {"user.id": 7, "user.name": "alice", "score": 99}

    # THEN: nested dict with proper hierarchy
    assert _nest(flat) == {"user": {"id": 7, "name": "alice"}, "score": 99}


def test_nest_helper_no_dots_passes_through() -> None:
    # BEGIN / WHEN: flat dict with no dotted keys
    flat = {"a": 1, "b": "x"}

    # THEN: same dict back (no nesting to apply)
    assert _nest(flat) == flat


def test_nest_helper_empty_dict() -> None:
    # BEGIN / WHEN / THEN: empty dict → empty dict
    assert _nest({}) == {}


def test_nest_helper_three_levels_deep() -> None:
    # BEGIN / WHEN: deeply nested key
    flat = {"a.b.c": 42}

    # THEN: three levels reconstructed
    assert _nest(flat) == {"a": {"b": {"c": 42}}}


# ---- _flatten --------------------------------------------------------------


def test_flatten_helper_round_trips_nested_dict() -> None:
    # BEGIN / WHEN: nested dict
    nested = {"user": {"id": 7, "name": "alice"}}

    # THEN: flattened to dotted-path keys
    assert _flatten(nested) == {"user.id": 7, "user.name": "alice"}


def test_flatten_is_noop_on_already_flat_dict() -> None:
    # BEGIN / WHEN: already-flat dict (no dict values)
    flat = {"a": 1, "b": "x"}

    # THEN: unchanged
    assert _flatten(flat) == flat


def test_flatten_empty_dict() -> None:
    # BEGIN / WHEN / THEN: empty dict → empty dict
    assert _flatten({}) == {}
