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
- ``Enum8`` / ``Enum16`` bodies (``'label' = value, …``) — handled by a
  special-case branch since ``=`` isn't a general param separator.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from clickhouse_async.types.base import ColumnCodec
from clickhouse_async.types.composite import (
    Array,
    LowCardinality,
    Map,
    Nested,
    Nullable,
    Tuple,
)
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
from clickhouse_async.types.enums import Enum8, Enum16
from clickhouse_async.types.geo import MultiPolygon, Point, Polygon, Ring
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

if TYPE_CHECKING:
    from collections.abc import Callable

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
    # Geo aliases — pure sugar over Tuple(Float64, Float64) /
    # Array(Tuple(...)) shapes; codecs in ``types/geo.py``.
    "Point": Point,
    "Ring": Ring,
    "Polygon": Polygon,
    "MultiPolygon": MultiPolygon,
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
# or raises ValueError for the wrong shape. Many of these forward to a
# helper defined below; the lambdas defer the name lookup until call time
# so the helpers don't need to be hoisted above the dict literal. Ruff's
# PLW0108 (unnecessary lambda) is intentionally ignored project-wide for
# this reason — see ``pyproject.toml::[tool.ruff.lint.ignore]``.
_Param = "ColumnCodec | int | str"
# ``DateTime`` / ``DateTime64`` are not in this registry — the parser
# special-cases them in ``_parse_one`` so it can thread the connection's
# session timezone in as a fallback for bare specs.
_PARAMETRIC: dict[str, Callable[[list[ColumnCodec | int | str]], ColumnCodec]] = {
    "Array": lambda p: Array(_one_type(p, "Array")),
    "Decimal": lambda p: _make_decimal(p),
    "Decimal32": lambda p: Decimal32(_one_int(p, "Decimal32")),
    "Decimal64": lambda p: Decimal64(_one_int(p, "Decimal64")),
    "Decimal128": lambda p: Decimal128(_one_int(p, "Decimal128")),
    "Decimal256": lambda p: Decimal256(_one_int(p, "Decimal256")),
    "FixedString": lambda p: FixedString(_one_int(p, "FixedString")),
    "LowCardinality": lambda p: LowCardinality(_one_type(p, "LowCardinality")),
    "Map": lambda p: _make_map(p),
    "Nullable": lambda p: Nullable(_one_type(p, "Nullable")),
    "Tuple": lambda p: _make_tuple(p),
}


def _one_type(params: list[ColumnCodec | int | str], where: str) -> ColumnCodec:
    if len(params) != 1 or not isinstance(params[0], ColumnCodec):
        raise ValueError(f"{where} takes one type parameter, got {params!r}")
    return params[0]


def _one_int(params: list[ColumnCodec | int | str], where: str) -> int:
    if (
        len(params) != 1
        or not isinstance(params[0], int)
        or isinstance(params[0], bool)
    ):
        raise ValueError(f"{where} takes one integer parameter, got {params!r}")
    return params[0]


def _one_str(params: list[ColumnCodec | int | str], where: str) -> str | None:
    # DateTime accepts zero or one string parameter (the timezone).
    if not params:
        return None
    if len(params) != 1 or not isinstance(params[0], str):
        raise ValueError(f"{where} takes zero or one string parameter, got {params!r}")
    return params[0]


def _make_tuple(params: list[ColumnCodec | int | str]) -> Tuple:
    """Build an unnamed ``Tuple`` from a generic params list. Named
    Tuples have their own grammar — see ``_parse_tuple_params``."""
    if not params or any(not isinstance(p, ColumnCodec) for p in params):
        raise ValueError(f"Tuple takes one or more type parameters; got {params!r}")
    components: list[ColumnCodec] = [p for p in params if isinstance(p, ColumnCodec)]
    return Tuple(*components)


def _make_named_tuple(names: list[str | None], components: list[ColumnCodec]) -> Tuple:
    """Build a ``Tuple`` from a parallel list of ``(name, codec)``.

    Mixing named and unnamed components in the same Tuple is
    rejected — matches upstream's "all named or all unnamed" rule.
    """
    if not components:
        raise ValueError("Tuple requires at least one component")
    has_named = any(n is not None for n in names)
    has_unnamed = any(n is None for n in names)
    if has_named and has_unnamed:
        named_part = [f"{n!r}" if n is not None else "<unnamed>" for n in names]
        raise ValueError(
            "Tuple components must be all named or all unnamed; got mix: "
            + ", ".join(named_part)
        )
    if not has_named:
        return Tuple(*components)
    # All names are non-None; narrow for the type checker.
    name_tuple: tuple[str, ...] = tuple(n for n in names if n is not None)
    return Tuple(*components, names=name_tuple)


def _make_nested(names: list[str | None], components: list[ColumnCodec]) -> Nested:
    """Build a ``Nested`` from a parallel ``(name, codec)`` list. Names
    are mandatory — upstream rejects unnamed ``Nested`` server-side."""
    if not components:
        raise ValueError("Nested requires at least one component")
    if any(n is None for n in names):
        bad = [f"{n!r}" if n is not None else "<unnamed>" for n in names]
        raise ValueError(
            "Nested components must all be named (upstream rejects "
            f"unnamed forms server-side); got: {', '.join(bad)}"
        )
    name_tuple: tuple[str, ...] = tuple(n for n in names if n is not None)
    return Nested(*components, names=name_tuple)


def _make_map(params: list[ColumnCodec | int | str]) -> Map:
    if (
        len(params) != _MAP_PARAM_COUNT
        or not isinstance(params[0], ColumnCodec)
        or not isinstance(params[1], ColumnCodec)
    ):
        raise ValueError(f"Map takes (key_type, value_type); got {params!r}")
    return Map(params[0], params[1])


def _make_decimal(params: list[ColumnCodec | int | str]) -> ColumnCodec:
    if (
        len(params) != _DECIMAL_PARAM_COUNT
        or not isinstance(params[0], int)
        or not isinstance(params[1], int)
    ):
        raise ValueError(f"Decimal takes (precision, scale) integers; got {params!r}")
    return make_decimal(precision=params[0], scale=params[1])


# Param-count constants for the factories above — extracted so PLR2004
# stops flagging the literal ``2`` in the dispatcher signatures.
_MAP_PARAM_COUNT = 2
_DECIMAL_PARAM_COUNT = 2
_DT64_MAX_PARAMS = 2  # DateTime64(precision[, 'timezone'])


# ---- parser ---------------------------------------------------------------


def parse_type(spec: str, *, session_timezone: str | None = None) -> ColumnCodec:
    """Parse a ClickHouse type spec into a column codec.

    ``session_timezone`` (when given) is used as the fallback timezone
    for any bare ``DateTime`` / ``DateTime64(p)`` codec that doesn't
    carry an explicit timezone in its type spec. Threaded down by
    ``read_block`` so naive ``DateTime`` reads land in the server's
    negotiated session timezone rather than silently UTC.

    Raises ``ValueError`` for unknown type names or malformed specs.
    """
    return _Parser(spec, session_timezone=session_timezone).parse_top()


class _Parser:
    __slots__ = ("pos", "session_timezone", "spec")

    def __init__(self, spec: str, *, session_timezone: str | None = None) -> None:
        self.spec = spec
        self.pos = 0
        self.session_timezone = session_timezone

    def parse_top(self) -> ColumnCodec:
        codec = self._parse_one()
        self._skip_ws()
        if self.pos != len(self.spec):
            raise ValueError(
                f"trailing characters in type spec {self.spec!r}: "
                f"{self.spec[self.pos :]!r}"
            )
        return codec

    def _parse_one(self) -> ColumnCodec:
        self._skip_ws()
        name = self._read_identifier()
        self._skip_ws()
        if self._peek() == "(":
            self._consume("(")
            # Enum bodies have their own grammar — `'label' = value, …` —
            # not expressible in the generic param parser.
            if name in ("Enum8", "Enum16"):
                mapping = self._parse_enum_body()
                self._consume(")")
                return Enum8(mapping) if name == "Enum8" else Enum16(mapping)
            # Tuple has an optional named-field syntax that the generic
            # comma-separated param grammar can't see. ``Tuple(id Int32,
            # name String)`` is two components named ``id`` and ``name``;
            # ``Tuple(Int32, String)`` is the same two unnamed.
            if name == "Tuple":
                names, components = self._parse_tuple_params()
                self._consume(")")
                return _make_named_tuple(names, components)
            # ``Nested(name1 T1, name2 T2, …)`` is sugar for
            # ``Array(Tuple(...))`` with the names rendered as part of
            # the type-spec form. The grammar matches Tuple's named
            # branch; ``_make_nested`` enforces the "all named" rule
            # since upstream rejects unnamed Nested.
            if name == "Nested":
                names, components = self._parse_tuple_params()
                self._consume(")")
                return _make_nested(names, components)
            params = self._parse_params()
            self._consume(")")
            # ``DateTime`` / ``DateTime64`` need ``session_timezone``
            # threaded in as a fallback for bare specs (no explicit tz
            # parameter in the type spec). Other parametric types route
            # through the static registry.
            if name == "DateTime":
                return self._make_datetime(params)
            if name == "DateTime64":
                return self._make_datetime64(params)
            factory_p = _PARAMETRIC.get(name)
            if factory_p is None:
                raise ValueError(f"unknown parametric type: {name!r}")
            return factory_p(params)
        if name == "DateTime":
            # Bare ``DateTime`` (no parens) — still wants the session
            # timezone fallback when one is plumbed in.
            return DateTime(session_timezone=self.session_timezone)
        factory_n = _NULLARY.get(name)
        if factory_n is None:
            raise ValueError(f"unknown type: {name!r}")
        return factory_n()

    def _make_datetime(self, params: list[ColumnCodec | int | str]) -> DateTime:
        explicit = _one_str(params, "DateTime")
        return DateTime(timezone=explicit, session_timezone=self.session_timezone)

    def _make_datetime64(self, params: list[ColumnCodec | int | str]) -> DateTime64:
        if not params or not isinstance(params[0], int):
            raise ValueError(
                f"DateTime64 takes (precision[, timezone]); got {params!r}"
            )
        precision = params[0]
        explicit_tz: str | None = None
        # ``DateTime64(precision[, 'timezone'])`` — precision is mandatory,
        # timezone is the optional second arg.
        if len(params) == _DT64_MAX_PARAMS:
            if not isinstance(params[1], str):
                raise ValueError(
                    f"DateTime64 timezone must be a string; got {params!r}"
                )
            explicit_tz = params[1]
        elif len(params) > _DT64_MAX_PARAMS:
            raise ValueError(f"DateTime64 takes at most two parameters; got {params!r}")
        return DateTime64(
            precision=precision,
            timezone=explicit_tz,
            session_timezone=self.session_timezone,
        )

    def _parse_tuple_params(
        self,
    ) -> tuple[list[str | None], list[ColumnCodec]]:
        """Parse the params of a ``Tuple(...)`` form, supporting both
        named and unnamed components.

        Each component can be ``Type`` or ``name Type``. The
        disambiguation: peek past the leading identifier; if the next
        non-whitespace character is ``(``, ``,`` or ``)``, that
        identifier was the type itself (no field name). Otherwise the
        identifier is a field name and the remainder is the type
        spec.
        """
        names: list[str | None] = []
        components: list[ColumnCodec] = []
        self._skip_ws()
        while self._peek() not in (")", ""):
            field_name, codec = self._parse_tuple_component()
            names.append(field_name)
            components.append(codec)
            self._skip_ws()
            if self._peek() == ",":
                self._consume(",")
                self._skip_ws()
        return names, components

    def _parse_tuple_component(self) -> tuple[str | None, ColumnCodec]:
        """Parse one Tuple component. Returns ``(field_name, codec)``
        with ``field_name`` ``None`` when the component is unnamed."""
        self._skip_ws()
        save_pos = self.pos
        first_ident = self._read_identifier()
        self._skip_ws()
        nxt = self._peek()
        # ``(`` — first identifier was the type name (parametric).
        # ``,`` / ``)`` — first identifier was the type name (nullary,
        #     end of field). Either way, rewind and re-parse via the
        #     standard ``_parse_one`` so registry lookup happens.
        if nxt in ("(", ",", ")", ""):
            self.pos = save_pos
            return None, self._parse_one()
        # Otherwise the first identifier is a field name; the remainder
        # is the type spec (which can itself be a parametric type, a
        # nested Tuple, etc.).
        return first_ident, self._parse_one()

    def _parse_enum_body(self) -> dict[str, int]:
        mapping: dict[str, int] = {}
        self._skip_ws()
        while self._peek() not in (")", ""):
            label = self._read_quoted_string()
            self._skip_ws()
            self._consume("=")
            self._skip_ws()
            value = self._read_integer()
            if label in mapping:
                raise ValueError(f"duplicate Enum label {label!r} in {self.spec!r}")
            mapping[label] = value
            self._skip_ws()
            if self._peek() == ",":
                self._consume(",")
                self._skip_ws()
        return mapping

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
            raise ValueError(f"expected integer at position {start} in {self.spec!r}")
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
