"""Tests for the timezone/timestamp helpers in _datetime_helpers.py."""

from __future__ import annotations

from datetime import UTC, datetime, timezone
from zoneinfo import ZoneInfo

from clickhouse_async.types._datetime_helpers import _naive_utc_from_ts, _resolve_tz

# ---- _resolve_tz -----------------------------------------------------------


def test_resolve_tz_none_returns_none() -> None:
    # BEGIN / WHEN / THEN: None input → None (naive datetime mode)
    assert _resolve_tz(None) is None


def test_resolve_tz_utc_returns_utc_singleton() -> None:
    # BEGIN / WHEN: the string "UTC"
    result = _resolve_tz("UTC")

    # THEN: the stdlib UTC sentinel (not a ZoneInfo instance)
    assert result is UTC
    assert isinstance(result, timezone)


def test_resolve_tz_iana_name_returns_zoneinfo() -> None:
    # BEGIN / WHEN: a valid IANA timezone name
    result = _resolve_tz("Europe/Berlin")

    # THEN: a ZoneInfo object for that zone
    assert isinstance(result, ZoneInfo)
    assert str(result) == "Europe/Berlin"


# ---- _naive_utc_from_ts ----------------------------------------------------


def test_naive_utc_from_ts_epoch() -> None:
    # BEGIN / WHEN: Unix epoch (ts=0)
    result = _naive_utc_from_ts(0)

    # THEN: naive 1970-01-01 00:00:00 with no tzinfo
    assert result == datetime(1970, 1, 1, 0, 0, 0)
    assert result.tzinfo is None


def test_naive_utc_from_ts_known_value() -> None:
    # BEGIN / WHEN: a known Unix timestamp
    result = _naive_utc_from_ts(1_700_000_000)

    # THEN: matches the expected UTC datetime
    expected = datetime.fromtimestamp(1_700_000_000, tz=UTC).replace(tzinfo=None)
    assert result == expected
    assert result.tzinfo is None
