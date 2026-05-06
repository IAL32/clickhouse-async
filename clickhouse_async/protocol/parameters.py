"""Format Python values into the wire form ClickHouse parameters expect.

Each parameter value travels as a single-quoted SQL string literal.
The server treats every parameter's storage type as a ``Field(String)``
on the way in, then *unquotes* the value and re-parses it according to
the placeholder's declared type at substitution time
(``{name:Date}``, ``{n:Int32}``, ``{tz:String}``, …). ``format_param``
always produces ``'<text>'``, regardless of the Python type — internal
single quotes and backslashes get escaped per ClickHouse's ``readQuoted``
convention.

``None`` is a special case: the server's parameter parser recognises a
``NULL`` for ``Nullable(T)`` placeholders only when the unquoted body is
exactly the two-byte sequence ``\\N``. We therefore send the literal
string ``\\N`` through the same ``_quote`` path; on the wire it lands
as ``'\\\\N'`` (5 bytes), the server unquotes to ``\\N`` and resolves
it to SQL ``NULL``. Bare ``\\N`` without surrounding quotes is rejected
by the server's Field-dump parser.

For v0 we cover the common types in ``DESIGN.md §7``:

- ``None`` — ``'\\\\N'`` (ClickHouse NULL sentinel; only valid for ``Nullable(T)``)
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
- ``list`` / ``tuple`` — rendered as a ClickHouse array literal
  (``[1,2,3]`` for numerics; ``['a','b']`` for strings, with single quotes
  inside) and wrapped in the outer ``_quote``. ``None`` inside an array
  becomes the literal token ``NULL`` — the scalar ``\\N`` sentinel does
  not carry into array literals (the array-text parser silently coerces
  ``\\N`` to ``0`` / ``""`` instead of NULL).

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
    """Format ``value`` as a single-quoted SQL string literal — the wire
    form parameter values must take regardless of the placeholder's
    declared type. ``None`` becomes the ClickHouse NULL sentinel ``\\N``
    routed through the same quoting path: the wire bytes are ``'\\\\N'``,
    which the server unquotes to ``\\N`` and resolves to SQL ``NULL`` for
    any ``Nullable(T)`` placeholder."""

    if value is None:
        return _quote(r"\N")
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
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_to_array_text(v) for v in value) + "]"
    raise TypeError(
        f"cannot format query parameter of type {type(value).__name__}: "
        f"{value!r}; pass a pre-stringified value or extend "
        f"format_param for a new Python type"
    )


def _to_array_text(value: object) -> str:
    """Render one element inside an array literal. Numbers go bare,
    strings get single-quoted with inner-quote escaping, ``None`` becomes
    the literal token ``NULL`` (the scalar ``\\N`` sentinel does not
    parse to NULL inside an array — the array-text parser silently
    coerces it to a zero value instead). Anything else falls through to
    ``_to_text`` and inherits its formatting."""

    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return _to_text(value)


def _quote(s: str) -> str:
    """Escape and wrap with single quotes per ClickHouse's
    ``readQuoted`` convention: backslashes and single quotes inside
    the value get a leading backslash."""
    escaped = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
