"""Format Python values into the textual form ClickHouse parameter
parsing expects.

The Connection wire format for parameters is ``Mapping[str, str]`` —
the server re-parses each value according to the placeholder's
declared type (``{name:Date}``, ``{n:Int32}``, ``{tz:String}`` …). The
*type* lives in the SQL itself; we just need to convert the Python
value to a textual representation the server can re-parse.

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

from datetime import date, datetime
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID


def format_param(value: object) -> str:
    """Convert ``value`` to the textual form ClickHouse parameter
    parsing expects. Order of ``isinstance`` checks matters: ``bool``
    is a subclass of ``int``, so it must be checked first; ``Decimal``
    is not a ``float`` but is checked before the numeric block for
    clarity."""

    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value:  # NaN
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
