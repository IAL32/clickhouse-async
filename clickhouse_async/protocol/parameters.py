"""Format Python values into the wire form ClickHouse parameters expect.

Each parameter value travels as a single-quoted SQL string literal.
The server treats every parameter's storage type as a ``Field(String)``
on the way in, then *unquotes* the value and re-parses it according to
the placeholder's declared type at substitution time
(``{name:Date}``, ``{n:Int32}``, ``{tz:String}``, …). This means
``format_param`` always produces ``'<text>'``, regardless of the
Python type — internal single quotes and backslashes get escaped per
ClickHouse's ``readQuoted`` convention.

For v0 we cover the common types in ``DESIGN.md §7``:

- ``str`` — verbatim
- ``bool`` — ``"true"`` / ``"false"``
- ``int`` — ``str(value)``
- ``float`` — ``repr`` plus ``inf`` / ``-inf`` / ``nan`` for non-finite
- ``Decimal`` — ``str(value)``
- ``date`` — ``YYYY-MM-DD``
- ``datetime`` — ``YYYY-MM-DD HH:MM:SS`` (or ``…SS.ffffff`` if microseconds)
- ``UUID`` — canonical 8-4-4-4-12
- ``IPv4Address`` / ``IPv6Address`` — ``str(value)``
- ``bytes`` — hex-encoded

Anything else raises ``TypeError``. Custom types belong at the
higher-level ``Client`` where the type-aware conversion lives, not
here.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID


def format_param(value: object) -> str:
    """Format ``value`` as a single-quoted SQL string literal — the
    wire form parameter values must take regardless of the placeholder's
    declared type."""

    return _quote(_to_text(value))


def _to_text(value: object) -> str:
    """Render ``value`` into the unquoted text the server will parse
    after stripping the wire-level single quotes. Order of ``isinstance``
    checks matters: ``bool`` is a subclass of ``int``, so it must be
    checked first; ``Decimal`` is not a ``float`` but is checked
    before the numeric block for clarity."""

    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        # ``math.isnan`` over a self-comparison so PLR0124 doesn't fire;
        # they're equivalent (NaN is the only value that isn't equal to
        # itself, but isnan reads better and is what static analysis
        # expects).
        if math.isnan(value):
            return "nan"
        if value == float("inf"):
            return "inf"
        if value == float("-inf"):
            return "-inf"
        return repr(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        if value.microsecond:
            return value.strftime("%Y-%m-%d %H:%M:%S") + f".{value.microsecond:06d}"
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (IPv4Address, IPv6Address)):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(
        f"cannot format query parameter of type {type(value).__name__}: "
        f"{value!r}; pass a pre-stringified value or extend "
        f"format_param for a new Python type"
    )


def _quote(s: str) -> str:
    """Escape and wrap with single quotes per ClickHouse's
    ``readQuoted`` convention: backslashes and single quotes inside
    the value get a leading backslash."""
    escaped = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
