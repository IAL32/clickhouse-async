"""ClickHouse type system: parses type spec strings into column codecs.

The block header carries each column's type as a string (e.g.
``Array(Nullable(String))``). ``parse_type`` is the registry entry point
that turns one of those strings into a codec ready to read or write a
column body. Concrete codecs live in submodules; this module owns the
spec parser and dispatch table.

The parser handles the v0a shape — bare names and parametric types whose
parameters are themselves type specs (e.g. ``Nullable(T)``). Numeric and
quoted-string parameters (for ``FixedString(N)``, ``DateTime(tz)``,
``Enum8('a' = 1, ...)``) are added in later sub-steps as those codecs
land.
"""

from __future__ import annotations

from collections.abc import Callable

from clickhouse_async.types.base import ColumnCodec
from clickhouse_async.types.composite import Nullable
from clickhouse_async.types.primitive import Int32
from clickhouse_async.types.string import String

__all__ = ["ColumnCodec", "parse_type"]


_NULLARY: dict[str, Callable[[], ColumnCodec]] = {
    "Int32": Int32,
    "String": String,
}

_PARAMETRIC_WITH_TYPES: dict[str, Callable[[list[ColumnCodec]], ColumnCodec]] = {
    "Nullable": lambda params: Nullable(params[0]),
}


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
            params = self._parse_type_params()
            self._consume(")")
            factory = _PARAMETRIC_WITH_TYPES.get(name)
            if factory is None:
                raise ValueError(f"unknown parametric type: {name!r}")
            return factory(params)
        factory_nullary = _NULLARY.get(name)
        if factory_nullary is None:
            raise ValueError(f"unknown type: {name!r}")
        return factory_nullary()

    def _parse_type_params(self) -> list[ColumnCodec]:
        params: list[ColumnCodec] = []
        self._skip_ws()
        # Stop the loop on EOF as well so the missing ')' surfaces from
        # _consume(')') with the right diagnostic, not from an unrelated
        # _read_identifier() failure.
        while self._peek() not in (")", ""):
            params.append(self._parse_one())
            self._skip_ws()
            if self._peek() == ",":
                self._consume(",")
                self._skip_ws()
        return params

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
