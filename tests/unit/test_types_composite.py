"""Round-trip and byte-layout tests for ``Array``, ``Tuple``, ``Map``."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest

from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.types import ColumnCodec, parse_type
from clickhouse_async.types.composite import Array, Map, Tuple


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
    with pytest.raises(ValueError, match="one or more"):
        parse_type("Tuple()")


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
