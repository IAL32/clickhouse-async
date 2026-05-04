"""Tests for ``_HostRotation`` — the round-robin + cooldown helper the
pool uses to spread acquires across the candidate hosts of a multi-host
DSN."""

from __future__ import annotations

import time

import pytest

from clickhouse_async._host_rotation import _HostRotation

# ---- property accessors -------------------------------------------------


def test_hosts_property_returns_configured_candidate_list() -> None:
    # BEGIN: a rotation with two hosts
    hosts = [("a", 9000), ("b", 9001)]
    rot = _HostRotation(hosts, cooldown=5.0)

    # WHEN / THEN: .hosts reflects what was passed in, as a tuple
    assert rot.hosts == (("a", 9000), ("b", 9001))


def test_cooldown_property_returns_configured_cooldown() -> None:
    # BEGIN: a rotation with a custom cooldown
    rot = _HostRotation([("a", 9000)], cooldown=30.0)

    # WHEN / THEN: .cooldown reflects the constructor argument
    assert rot.cooldown == 30.0


# ---- rotation pointer advances ------------------------------------------


def test_next_candidates_rotates_starting_position_each_call() -> None:
    # BEGIN: a rotation over three hosts with no failures
    hosts = [("a", 9000), ("b", 9000), ("c", 9000)]
    rot = _HostRotation(hosts, cooldown=5.0)

    # WHEN / THEN: successive calls each shift the start position by one
    assert rot.next_candidates() == (
        ("a", 9000),
        ("b", 9000),
        ("c", 9000),
    )
    assert rot.next_candidates() == (
        ("b", 9000),
        ("c", 9000),
        ("a", 9000),
    )
    assert rot.next_candidates() == (
        ("c", 9000),
        ("a", 9000),
        ("b", 9000),
    )
    # full cycle: back to the original ordering
    assert rot.next_candidates() == (
        ("a", 9000),
        ("b", 9000),
        ("c", 9000),
    )


def test_single_host_rotation_always_returns_same_list() -> None:
    # BEGIN: a single-host rotation
    rot = _HostRotation([("only", 9000)], cooldown=5.0)

    # WHEN / THEN: the rotation is a no-op for a single host
    assert rot.next_candidates() == (("only", 9000),)
    assert rot.next_candidates() == (("only", 9000),)


# ---- failures cool a host down ------------------------------------------


def test_recent_failure_skips_host_within_cooldown_window() -> None:
    # BEGIN: a 2-host rotation with a generous cooldown
    hosts = [("a", 9000), ("b", 9000)]
    rot = _HostRotation(hosts, cooldown=60.0)

    # WHEN: host ``a`` is recorded as just-failed
    rot.record_failure(("a", 9000))

    # THEN: the next rotation drops ``a`` and yields only ``b``
    assert rot.next_candidates() == (("b", 9000),)
    # and again — still in cooldown
    assert rot.next_candidates() == (("b", 9000),)


def test_success_clears_cooldown_for_that_host() -> None:
    # BEGIN: host a is in cooldown
    hosts = [("a", 9000), ("b", 9000)]
    rot = _HostRotation(hosts, cooldown=60.0)
    rot.record_failure(("a", 9000))
    assert rot.next_candidates() == (("b", 9000),)

    # WHEN: host a is later recorded as succeeded
    rot.record_success(("a", 9000))

    # THEN: a is back in the rotation immediately
    candidates = rot.next_candidates()
    assert ("a", 9000) in candidates
    assert ("b", 9000) in candidates


def test_all_hosts_cooled_down_returns_full_rotation_anyway() -> None:
    # BEGIN: every host has just failed
    hosts = [("a", 9000), ("b", 9000)]
    rot = _HostRotation(hosts, cooldown=60.0)
    rot.record_failure(("a", 9000))
    rot.record_failure(("b", 9000))

    # WHEN / THEN: the rotation returns the full list rather than empty —
    #              if every replica is dead, retrying is better than
    #              waiting for a cooldown that may never clear
    candidates = rot.next_candidates()
    assert set(candidates) == {("a", 9000), ("b", 9000)}


def test_cooldown_expires_with_time(monkeypatch: pytest.MonkeyPatch) -> None:
    # BEGIN: a rotation with a tight cooldown and a controllable clock
    hosts = [("a", 9000), ("b", 9000)]
    rot = _HostRotation(hosts, cooldown=1.0)

    fake_now = [0.0]

    def now() -> float:
        return fake_now[0]

    monkeypatch.setattr(time, "monotonic", now)

    fake_now[0] = 100.0
    rot.record_failure(("a", 9000))
    # Just inside the cooldown window
    fake_now[0] = 100.5
    assert rot.next_candidates() == (("b", 9000),)
    # Past the cooldown window
    fake_now[0] = 102.0
    candidates = rot.next_candidates()
    assert ("a", 9000) in candidates


# ---- input validation ----------------------------------------------------


def test_empty_host_list_rejected() -> None:
    # BEGIN / WHEN / THEN: an empty rotation is meaningless
    with pytest.raises(ValueError, match="at least one host"):
        _HostRotation([])


def test_negative_cooldown_rejected() -> None:
    # BEGIN / WHEN / THEN: cooldown must be non-negative
    with pytest.raises(ValueError, match="cooldown"):
        _HostRotation([("a", 9000)], cooldown=-1.0)


def test_low_monotonic_clock_does_not_cool_down_unfailed_hosts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # BEGIN: a freshly-booted runner where ``time.monotonic()`` is
    #        still smaller than the cooldown window. The rotation
    #        must not treat unfailed hosts as "in cooldown" just
    #        because (now - 0.0) is less than the cooldown — the
    #        sentinel-default approach was a real bug exposed by
    #        Linux CI runners with small uptimes.
    fake_now = [0.5]  # less than any reasonable cooldown
    monkeypatch.setattr(time, "monotonic", lambda: fake_now[0])
    rot = _HostRotation([("a", 9000), ("b", 9000)], cooldown=60.0)

    # WHEN: no failure has been recorded — every host is healthy
    fake_now[0] = 1.0
    candidates = rot.next_candidates()

    # THEN: both hosts come back, in the canonical rotation order;
    #       neither was filtered out by the small-clock quirk
    assert candidates == (("a", 9000), ("b", 9000))


def test_record_failure_for_unknown_host_is_a_noop() -> None:
    # BEGIN: a single-host rotation
    rot = _HostRotation([("a", 9000)], cooldown=5.0)

    # WHEN: a host outside the configured list is recorded as failed
    rot.record_failure(("ghost", 9000))

    # THEN: the next rotation is unaffected — the ghost host doesn't
    #       sneak into the candidate list, and the configured host is
    #       not penalised
    assert rot.next_candidates() == (("a", 9000),)
