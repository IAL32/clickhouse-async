"""Round-trip and byte-layout tests for ``Array``, ``Tuple``, ``Map``."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.types import ColumnCodec, parse_type
from clickhouse_async.types.composite import Array, Map, Nested, Tuple
from clickhouse_async.types.primitive import Int32
from clickhouse_async.types.string import String

if TYPE_CHECKING:
    from collections.abc import Sequence


def _reader(data: bytes) -> AsyncBinaryReader:
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return AsyncBinaryReader(stream)


async def _round_trip(codec: ColumnCodec, values: Sequence[Any]) -> list[Any]:
    writer = BinaryWriter()
    codec.write(writer, values)
    return await codec.read(_reader(writer.getvalue()), len(values))


# ---- Array(T) ------------------------------------------------------------


@pytest.mark.parametrize(
    "values",
    [
        [],
        [[]],  # one empty array
        [[1]],
        [[1, 2, 3]],
        [[1], [2, 3], [], [4, 5, 6]],  # mixed lengths incl. empty
        [list(range(50))],  # one big array
    ],
)
async def test_array_int32_round_trip(values: list[list[int]]) -> None:
    # BEGIN: an Array(Int32) codec and arrays of varying length
    codec = parse_type("Array(Int32)")

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every row's array survives byte-for-byte
    assert isinstance(codec, Array)
    assert decoded == values


async def test_array_uses_cumulative_offsets() -> None:
    # BEGIN: an Array(Int8) codec and three arrays of known lengths
    codec = parse_type("Array(Int8)")
    values: list[list[int]] = [[1, 2, 3], [4, 5], [6]]

    # WHEN: writing and inspecting the encoded bytes
    writer = BinaryWriter()
    codec.write(writer, values)
    encoded = writer.getvalue()

    # THEN: the first 24 bytes are three UInt64 cumulative offsets
    #       (3, 5, 6) followed by six Int8 bytes (1..6)
    assert encoded[:8] == (3).to_bytes(8, "little")
    assert encoded[8:16] == (5).to_bytes(8, "little")
    assert encoded[16:24] == (6).to_bytes(8, "little")
    assert encoded[24:] == bytes([1, 2, 3, 4, 5, 6])


async def test_array_round_trips_through_nullable_inner() -> None:
    # BEGIN: an Array(Nullable(String)) codec mixing strings and nulls
    codec = parse_type("Array(Nullable(String))")
    values: list[list[str | None]] = [
        ["a", None, "b"],
        [],
        [None],
        ["c", "d"],
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: the Nullable mask inside each row's slice is preserved
    assert decoded == values


async def test_nested_array_of_array_round_trip() -> None:
    # BEGIN: an Array(Array(Int32)) codec and a 2-D ragged matrix
    codec = parse_type("Array(Array(Int32))")
    values: list[list[list[int]]] = [
        [[1], [2, 3]],
        [],
        [[], [4, 5, 6]],
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: both levels of nesting survive
    assert decoded == values


async def test_array_write_coerces_row_level_none_to_empty_array() -> None:
    # BEGIN: an Array(Nullable(String)) codec and a row source with one
    #        ``None`` (caller's "no array" sentinel) alongside a real list
    codec = parse_type("Array(Nullable(String))")
    values: list[Any] = [None, ["a", "b"], None]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: each ``None`` lands as the codec's ``null_value`` (``[]``),
    #       matching what the server itself does for a NULL inserted
    #       into an Array column at SQL level. The interior Nullable
    #       layer continues to handle element-level None on its own.
    assert decoded == [[], ["a", "b"], []]


# ---- Tuple(T1, T2, …) ----------------------------------------------------


async def test_tuple_round_trip_preserves_columns_in_order() -> None:
    # BEGIN: a Tuple(Int32, String) codec and rows with both components
    codec = parse_type("Tuple(Int32, String)")
    values: list[tuple[int, str]] = [
        (1, "alpha"),
        (-7, ""),
        (2**31 - 1, "café"),
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every row comes back as a tuple in declared order
    assert isinstance(codec, Tuple)
    assert decoded == values


async def test_tuple_writes_components_sequentially_not_row_major() -> None:
    # BEGIN: a Tuple(Int8, Int8) codec with two known rows
    codec = parse_type("Tuple(Int8, Int8)")
    values: list[tuple[int, int]] = [(1, 10), (2, 20), (3, 30)]

    # WHEN: writing and inspecting the encoded bytes
    writer = BinaryWriter()
    codec.write(writer, values)

    # THEN: the wire layout is column-major — three Int8s for component 0,
    #       then three Int8s for component 1 (NOT interleaved row-major)
    assert writer.getvalue() == bytes([1, 2, 3, 10, 20, 30])


async def test_tuple_write_coerces_row_level_none_to_default_tuple() -> None:
    # BEGIN: a Tuple(Int32, String) codec and a None among real tuples
    codec = parse_type("Tuple(Int32, String)")
    values: list[Any] = [(1, "a"), None, (3, "c")]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: the None lands as the codec's ``null_value`` — a tuple of
    #       per-component defaults (Int32 → 0, String → "")
    assert decoded == [(1, "a"), (0, ""), (3, "c")]


async def test_tuple_one_component_round_trip() -> None:
    # BEGIN: a 1-tuple — Tuple(Int32) — must still round-trip as a 1-tuple
    codec = parse_type("Tuple(Int32)")
    values: list[tuple[int]] = [(1,), (2,), (3,)]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every row is a 1-tuple (not the bare value)
    assert decoded == values


def test_tuple_requires_at_least_one_component() -> None:
    # BEGIN / WHEN / THEN: parsing Tuple() raises since the parser produces
    #     an empty params list and the factory rejects it
    with pytest.raises(ValueError, match="at least one component"):
        parse_type("Tuple()")


# ---- Named Tuple --------------------------------------------------------


def test_named_tuple_round_trips_through_parse_and_name() -> None:
    # BEGIN / WHEN: parsing a named Tuple spec
    codec = parse_type("Tuple(id UInt32, name String)")

    # THEN: the codec is named and ``codec.name`` reproduces the input
    assert isinstance(codec, Tuple)
    assert codec.named is True
    assert codec.names == ("id", "name")
    assert codec.name == "Tuple(id UInt32, name String)"


def test_unnamed_tuple_keeps_named_false() -> None:
    # BEGIN / WHEN: parsing an unnamed Tuple
    codec = parse_type("Tuple(UInt32, String)")

    # THEN: ``.named`` is False and the rendering matches input
    assert isinstance(codec, Tuple)
    assert codec.named is False
    assert codec.names is None
    assert codec.name == "Tuple(UInt32, String)"


def test_mixed_named_and_unnamed_tuple_components_rejected() -> None:
    # BEGIN / WHEN / THEN: ClickHouse requires Tuple components to be
    #     all named or all unnamed; mixed forms raise
    with pytest.raises(ValueError, match="all named or all unnamed"):
        parse_type("Tuple(id UInt32, String)")


def test_nested_named_tuple_parses_both_levels() -> None:
    # BEGIN / WHEN: a Tuple whose component is itself a named Tuple
    codec = parse_type("Tuple(meta Tuple(id UInt32, label String), value Int64)")

    # THEN: outer + inner names round-trip
    assert codec.name == ("Tuple(meta Tuple(id UInt32, label String), value Int64)")


async def test_named_tuple_round_trips_values_through_codec() -> None:
    # BEGIN: a named Tuple codec
    codec = parse_type("Tuple(id UInt32, name String)")
    values: list[tuple[object, ...]] = [
        (1, "alpha"),
        (2, "beta"),
        (3, "gamma"),
    ]

    # WHEN: round-tripping through the codec — wire format is identical
    #       to unnamed Tuple, so the existing read/write paths apply
    writer = BinaryWriter()
    codec.write(writer, values)
    written = writer.getvalue()

    stream = asyncio.StreamReader()
    stream.feed_data(written)
    stream.feed_eof()
    decoded = await codec.read(AsyncBinaryReader(stream), len(values))

    # THEN: every row comes back identically as a plain tuple — the
    #       names live only in the codec metadata, not in the values
    assert decoded == values


def test_named_tuple_constructor_validates_names_length() -> None:
    # BEGIN / WHEN / THEN: a names tuple of the wrong length is rejected
    with pytest.raises(ValueError, match="names length"):
        Tuple(Int32(), String(), names=("only_one",))


# ---- Nested(name T, ...) ------------------------------------------------


def test_nested_parses_and_renders_nested_form() -> None:
    # BEGIN / WHEN: parsing the Nested(...) sugar
    codec = parse_type("Nested(uid UInt32, label String)")

    # THEN: a Nested codec comes back with the original spelling on
    #       ``codec.name`` (not the desugared ``Array(Tuple(...))``)
    assert isinstance(codec, Nested)
    assert codec.name == "Nested(uid UInt32, label String)"
    assert codec.names == ("uid", "label")


def test_nested_rejects_unnamed_components() -> None:
    # BEGIN / WHEN / THEN: ClickHouse requires names in Nested; the
    #     parser surfaces that constraint instead of emitting an
    #     unnamed Tuple
    with pytest.raises(ValueError, match="all be named"):
        parse_type("Nested(UInt32, String)")


def test_nested_rejects_mixed_named_unnamed_components() -> None:
    # BEGIN / WHEN / THEN: same all-or-nothing rule as Tuple, restated
    #     for Nested with the Nested-specific message
    with pytest.raises(ValueError, match="all be named"):
        parse_type("Nested(uid UInt32, String)")


def test_nested_rejects_empty_components() -> None:
    # BEGIN / WHEN / THEN: zero components is meaningless for both
    #     spellings
    with pytest.raises(ValueError, match="at least one component"):
        parse_type("Nested()")


async def test_nested_round_trips_values_through_codec() -> None:
    # BEGIN: a Nested codec and rows holding lists of (uid, label)
    #        tuples — the wire format is identical to
    #        ``Array(Tuple(...))`` so this exercises the delegation
    codec = parse_type("Nested(uid UInt32, label String)")
    values: list[list[tuple[int, str]]] = [
        [(1, "alpha"), (2, "beta")],
        [],
        [(3, "gamma")],
    ]

    # WHEN: round-tripping through the codec
    writer = BinaryWriter()
    codec.write(writer, values)
    written = writer.getvalue()

    stream = asyncio.StreamReader()
    stream.feed_data(written)
    stream.feed_eof()
    decoded = await codec.read(AsyncBinaryReader(stream), len(values))

    # THEN: every row's array of tuples comes back identically
    assert decoded == values


def test_desugared_array_tuple_named_still_decodes() -> None:
    # BEGIN: the desugared form server-emits in some edge paths
    codec = parse_type("Array(Tuple(uid UInt32, label String))")

    # THEN: it parses to ``Array(Tuple(..., names=...))`` with the
    #       desugared rendering — the parser doesn't re-spell either
    #       way; whichever form arrived is what ``codec.name`` gives back
    assert isinstance(codec, Array)
    inner = codec.inner
    assert isinstance(inner, Tuple)
    assert inner.named is True
    assert codec.name == "Array(Tuple(uid UInt32, label String))"


# ---- Map(K, V) ----------------------------------------------------------


async def test_map_string_int_round_trip() -> None:
    # BEGIN: a Map(String, Int32) codec and dicts with varying entries
    codec = parse_type("Map(String, Int32)")
    values: list[dict[str, int]] = [
        {},
        {"a": 1},
        {"alpha": 1, "beta": 2, "gamma": 3},
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every dict survives — keys may permute since we go through a
    #       list of (k, v) pairs but content equality holds
    assert isinstance(codec, Map)
    assert decoded == values


async def test_map_write_coerces_row_level_none_to_empty_map() -> None:
    # BEGIN: a Map(String, Int32) codec and a None among real dicts
    codec = parse_type("Map(String, Int32)")
    values: list[Any] = [{"a": 1}, None, {"b": 2}]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: the None lands as the codec's ``null_value`` (``{}``), the
    #       same convention Array and Tuple now follow
    assert decoded == [{"a": 1}, {}, {"b": 2}]


async def test_map_wire_format_matches_array_of_tuple() -> None:
    # BEGIN: equivalent Map(Int8, Int8) and Array(Tuple(Int8, Int8)) codecs
    map_codec = parse_type("Map(Int8, Int8)")
    array_codec = parse_type("Array(Tuple(Int8, Int8))")

    rows: list[dict[int, int]] = [{1: 10, 2: 20}, {3: 30}]
    array_form: list[list[tuple[int, int]]] = [list(d.items()) for d in rows]

    # WHEN: encoding both forms
    map_writer = BinaryWriter()
    array_writer = BinaryWriter()
    map_codec.write(map_writer, rows)
    array_codec.write(array_writer, array_form)

    # THEN: the byte streams are identical — Map shares Array(Tuple(K, V))'s
    #       wire format
    assert map_writer.getvalue() == array_writer.getvalue()


# ---- empty-batch invariants ---------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [
        "Array(Int32)",
        "Array(Nullable(String))",
        "Tuple(Int32, String)",
        "Map(String, Int32)",
    ],
)
async def test_empty_batch_round_trip(spec: str) -> None:
    # BEGIN: a parsed composite codec
    codec = parse_type(spec)

    # WHEN: round-tripping zero rows
    decoded = await _round_trip(codec, [])

    # THEN: nothing is read or written, and an empty list comes back
    assert decoded == []


# ---- name round-tripping -------------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [
        "Array(Int32)",
        "Array(Nullable(String))",
        "Array(Array(Int32))",
        "Tuple(Int32, String)",
        "Tuple(Int32)",
        "Map(String, Int32)",
        "Map(Int64, Array(String))",
    ],
)
def test_codec_name_round_trips_through_parser(spec: str) -> None:
    # BEGIN: the canonical type spec
    # WHEN: parsing it
    codec = parse_type(spec)

    # THEN: the codec's `name` reproduces the spec verbatim — important
    #       so re-emitting the spec to the server doesn't alter it
    assert codec.name == spec
