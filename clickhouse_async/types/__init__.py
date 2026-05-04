"""ClickHouse type system: parses type spec strings into column codecs.

The block header carries each column's type as a string (e.g.
``Array(Nullable(String))``). ``parse_type`` is the registry entry point
that turns one of those strings into a codec ready to read or write a
column body.

The parser and registry live in ``types/_parser.py`` so that
``variant.py`` and ``json_type.py`` can import ``parse_type`` at the
top level without forming an import cycle.
"""

from __future__ import annotations

from clickhouse_async.types._parser import _JSONHint, parse_type
from clickhouse_async.types.aggregate import AggregateFunction
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
from clickhouse_async.types.json_type import JSON
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
from clickhouse_async.types.variant import Dynamic, Variant

__all__ = [
    "JSON",
    # Network
    "UUID",
    # Aggregate
    "AggregateFunction",
    # Composite
    "Array",
    # Primitives
    "Bool",
    # Base
    "ColumnCodec",
    # Date / time
    "Date",
    "Date32",
    "DateTime",
    "DateTime64",
    # Decimal
    "Decimal32",
    "Decimal64",
    "Decimal128",
    "Decimal256",
    "Dynamic",
    # Enums
    "Enum8",
    "Enum16",
    "FixedString",
    "Float32",
    "Float64",
    "IPv4",
    "IPv6",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Int128",
    "Int256",
    "LowCardinality",
    "Map",
    # Geo
    "MultiPolygon",
    "Nested",
    "Nullable",
    "Point",
    "Polygon",
    "Ring",
    # String
    "String",
    "Tuple",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "UInt128",
    "UInt256",
    # Variant / Dynamic / JSON
    "Variant",
    "_JSONHint",
    "make_decimal",
    # Parser entry point
    "parse_type",
]
