"""ClickHouse type system: parses type spec strings into column codecs.

The block header carries each column's type as a string (e.g.
``Array(Nullable(String))``). ``parse_type`` is the registry entry point
that turns one of those strings into a codec ready to read or write a
column body.

The parser handles:
- Bare names (``Int32``, ``String``).
- Parametric types whose params are themselves type specs (``Nullable(T)``).
- Parametric types whose params are integer literals (``FixedString(N)``,
  ``DateTime64(precision)``, ``Decimal(P, S)``).
- Parametric types whose params are single-quoted string literals
  (``DateTime('UTC')``).
- Mixed param lists (``DateTime64(3, 'UTC')``).
- Whitespace around tokens.

Enum's ``'a' = 1, 'b' = 2`` form lands in step 04e along with the codec.
"""

from __future__ import annotations

from collections.abc import Callable

from clickhouse_async.types.base import ColumnCodec
from clickhouse_async.types.composite import Nullable
from clickhouse_async.types.datetime import (
    Date,
    Date32,
    DateTime,
    DateTime64,
)
from clickhouse_async.types.decimal import (
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    make_decimal,
)
from clickhouse_async.types.net import UUID, IPv4, IPv6
from clickhouse_async.types.primitive import (
    Bool,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,
)
from clickhouse_async.types.string import FixedString, String

__all__ = ["ColumnCodec", "parse_type"]


# ---- registry --------------------------------------------------------------

_NULLARY: dict[str, Callable[[], ColumnCodec]] = {
    "Bool": Bool,
    "Date": Date,
    "Date32": Date32,
    "DateTime": DateTime,
    "Float32": Float32,
    "Float64": Float64,
    "IPv4": IPv4,
    "IPv6": IPv6,
    "Int8": Int8,
    "Int16": Int16,
    "Int32": Int32,
    "Int64": Int64,
    "Int128": Int128,
    "Int256": Int256,
    "String": String,
    "UInt8": UInt8,
    "UInt16": UInt16,
    "UInt32": UInt32,
    "UInt64": UInt64,
    "UInt128": UInt128,
    "UInt256": UInt256,
    "UUID": UUID,
}

# Each factory takes the heterogeneous params list and either returns a codec
# or raises ValueError for the wrong shape.
_Param = "ColumnCodec | int | str"
_PARAMETRIC: dict[str, Callable[[list[ColumnCodec | int | str]], ColumnCodec]] = {
    "Nullable": lambda p: Nullable(_one_type(p, "Nullable")),
    "FixedString": lambda p: FixedString(_one_int(p, "FixedString")),
    "DateTime": lambda p: DateTime(timezone=_one_str(p, "DateTime")),
    "DateTime64": lambda p: _make_datetime64(p),
    "Decimal": lambda p: _make_decimal(p),
    "Decimal32": lambda p: Decimal32(_one_int(p, "Decimal32")),
    "Decimal64": lambda p: Decimal64(_one_int(p, "Decimal64")),
    "Decimal128": lambda p: Decimal128(_one_int(p, "Decimal128")),
    "Decimal256": lambda p: Decimal256(_one_int(p, "Decimal256")),
}


def _one_type(params: list[ColumnCodec | int | str], where: str) -> ColumnCodec:
    if len(params) != 1 or not isinstance(params[0], ColumnCodec):
        raise ValueError(f"{where} takes one type parameter, got {params!r}")
    return params[0]


def _one_int(params: list[ColumnCodec | int | str], where: str) -> int:
    if len(params) != 1 or not isinstance(params[0], int) or isinstance(
        params[0], bool
    ):
        raise ValueError(f"{where} takes one integer parameter, got {params!r}")
    return params[0]


def _one_str(
    params: list[ColumnCodec | int | str], where: str
) -> str | None:
    # DateTime accepts zero or one string parameter (the timezone).
    if not params:
        return None
    if len(params) != 1 or not isinstance(params[0], str):
        raise ValueError(
            f"{where} takes zero or one string parameter, got {params!r}"
        )
    return params[0]


def _make_datetime64(params: list[ColumnCodec | int | str]) -> DateTime64:
    if not params or not isinstance(params[0], int):
        raise ValueError(
            f"DateTime64 takes (precision[, timezone]); got {params!r}"
        )
    precision = params[0]
    tz: str | None = None
    if len(params) == 2:
        if not isinstance(params[1], str):
            raise ValueError(
                f"DateTime64 timezone must be a string; got {params!r}"
            )
        tz = params[1]
    elif len(params) > 2:
        raise ValueError(
            f"DateTime64 takes at most two parameters; got {params!r}"
        )
    return DateTime64(precision=precision, timezone=tz)


def _make_decimal(params: list[ColumnCodec | int | str]) -> ColumnCodec:
    if (
        len(params) != 2
        or not isinstance(params[0], int)
        or not isinstance(params[1], int)
    ):
        raise ValueError(
            f"Decimal takes (precision, scale) integers; got {params!r}"
        )
    return make_decimal(precision=params[0], scale=params[1])


# ---- parser ---------------------------------------------------------------


def parse_type(spec: str) -> ColumnCodec:
    """Parse a ClickHouse type spec into a column codec.

    Raises ``ValueError`` for unknown type names or malformed specs.
    """
    return _Parser(spec).parse_top()


class _Parser:
    __slots__ = ("pos", "spec")

    def __init__(self, spec: str) -> None:
        self.spec = spec
        self.pos = 0

    def parse_top(self) -> ColumnCodec:
        codec = self._parse_one()
        self._skip_ws()
        if self.pos != len(self.spec):
            raise ValueError(
                f"trailing characters in type spec {self.spec!r}: "
                f"{self.spec[self.pos:]!r}"
            )
        return codec

    def _parse_one(self) -> ColumnCodec:
        self._skip_ws()
        name = self._read_identifier()
        self._skip_ws()
        if self._peek() == "(":
            self._consume("(")
            params = self._parse_params()
            self._consume(")")
            factory_p = _PARAMETRIC.get(name)
            if factory_p is None:
                raise ValueError(f"unknown parametric type: {name!r}")
            return factory_p(params)
        factory_n = _NULLARY.get(name)
        if factory_n is None:
            raise ValueError(f"unknown type: {name!r}")
        return factory_n()

    def _parse_params(self) -> list[ColumnCodec | int | str]:
        params: list[ColumnCodec | int | str] = []
        self._skip_ws()
        # Stop the loop on EOF as well so a missing ')' surfaces from
        # _consume(')') with the right diagnostic.
        while self._peek() not in (")", ""):
            params.append(self._parse_param())
            self._skip_ws()
            if self._peek() == ",":
                self._consume(",")
                self._skip_ws()
        return params

    def _parse_param(self) -> ColumnCodec | int | str:
        self._skip_ws()
        c = self._peek()
        if not c:
            raise ValueError(
                f"unexpected end of spec at position {self.pos} in {self.spec!r}"
            )
        if c == "'":
            return self._read_quoted_string()
        if c == "-" or c.isdigit():
            return self._read_integer()
        return self._parse_one()

    def _peek(self) -> str:
        if self.pos >= len(self.spec):
            return ""
        return self.spec[self.pos]

    def _consume(self, expected: str) -> None:
        if self._peek() != expected:
            raise ValueError(
                f"expected {expected!r} at position {self.pos} "
                f"in {self.spec!r}, got {self._peek()!r}"
            )
        self.pos += 1

    def _skip_ws(self) -> None:
        while self.pos < len(self.spec) and self.spec[self.pos] == " ":
            self.pos += 1

    def _read_identifier(self) -> str:
        start = self.pos
        while self.pos < len(self.spec):
            c = self.spec[self.pos]
            if c.isalnum() or c == "_":
                self.pos += 1
            else:
                break
        if start == self.pos:
            raise ValueError(
                f"expected identifier at position {start} in {self.spec!r}"
            )
        return self.spec[start : self.pos]

    def _read_integer(self) -> int:
        start = self.pos
        if self._peek() == "-":
            self.pos += 1
        while self.pos < len(self.spec) and self.spec[self.pos].isdigit():
            self.pos += 1
        if start == self.pos or (start + 1 == self.pos and self.spec[start] == "-"):
            raise ValueError(
                f"expected integer at position {start} in {self.spec!r}"
            )
        return int(self.spec[start : self.pos])

    def _read_quoted_string(self) -> str:
        # ClickHouse uses single-quoted strings; doubled quotes are not
        # part of server-emitted type strings (only user CREATE TABLE
        # syntax). We accept the simplest form for v0.
        self._consume("'")
        start = self.pos
        while self.pos < len(self.spec) and self.spec[self.pos] != "'":
            self.pos += 1
        if self.pos >= len(self.spec):
            raise ValueError(
                f"unterminated string literal starting at position {start - 1} "
                f"in {self.spec!r}"
            )
        value = self.spec[start : self.pos]
        self._consume("'")
        return value
