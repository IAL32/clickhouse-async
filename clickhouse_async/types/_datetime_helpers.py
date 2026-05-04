"""Pure utility helpers for timezone resolution and timestamp conversion.

Extracted from datetime.py so they can be tested independently of the
codec classes. Both helpers are used by DateTime and DateTime64.
"""

from __future__ import annotations

from datetime import UTC, datetime, timezone
from zoneinfo import ZoneInfo


def _resolve_tz(name: str | None) -> timezone | ZoneInfo | None:
    """Resolve a ClickHouse timezone name to a Python timezone object.

    Returns ``None`` for ``None`` input (naive datetime), ``UTC`` for
    the string ``"UTC"``, and a ``ZoneInfo`` for any other IANA name.
    """
    if name is None:
        return None
    if name == "UTC":
        return UTC
    return ZoneInfo(name)


def _naive_utc_from_ts(ts: int) -> datetime:
    """Naive datetime representing the UTC instant at ``ts`` Unix seconds.

    ``datetime.utcfromtimestamp`` is deprecated; we get the same value
    by resolving in UTC and stripping the tzinfo.
    """
    return datetime.fromtimestamp(ts, tz=UTC).replace(tzinfo=None)
