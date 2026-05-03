"""Round-trip and parser tests for ``AggregateFunction`` state columns.

The codec rounds-trips a small allow-list of fixed-size aggregate
states (``avg``, ``count``) byte-for-byte; anything else raises a
clear ``NotImplementedError`` on read/write naming the function. The
parser handles both bare-identifier function calls (``avg``) and
parametric ones (``quantilesTDigest(0.5, 0.9)``).
"""

from __future__ import annotations

import asyncio
import struct

import pytest

from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.types import parse_type
from clickhouse_async.types.aggregate import AggregateFunction


def _reader(data: bytes) -> AsyncBinaryReader:
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return AsyncBinaryReader(stream)


# ---- parser surface -----------------------------------------------------


def test_aggregate_function_bare_identifier_round_trips_name() -> None:
    # BEGIN / WHEN: a simple AggregateFunction(avg, Float64) spec
    codec = parse_type("AggregateFunction(avg, Float64)")

    # THEN: the codec keeps the function call + arg types and renders
    #       the spec exactly as it came in
    assert isinstance(codec, AggregateFunction)
    assert codec.function_call == "avg"
    assert codec.function_name == "avg"
    assert codec.name == "AggregateFunction(avg, Float64)"


def test_aggregate_function_parametric_call_round_trips_name() -> None:
    # BEGIN / WHEN: a parametric aggregate call like
    #     ``quantilesTDigest(0.5, 0.9, 0.99)`` whose literal args are
    #     values, not types
    codec = parse_type("AggregateFunction(quantilesTDigest(0.5, 0.9, 0.99), Float64)")

    # THEN: the function-call substring is stored verbatim so the
    #       output spec round-trips, and the bare ``function_name``
    #       (used for the size-table lookup) drops the parens
    assert isinstance(codec, AggregateFunction)
    assert codec.function_call == "quantilesTDigest(0.5, 0.9, 0.99)"
    assert codec.function_name == "quantilesTDigest"
    assert codec.name == "AggregateFunction(quantilesTDigest(0.5, 0.9, 0.99), Float64)"


def test_aggregate_function_count_takes_no_arg_types() -> None:
    # BEGIN / WHEN: ``count`` is the unusual case with no arg types
    codec = parse_type("AggregateFunction(count)")

    # THEN: arg_types is empty and the spec rendering omits the trailing
    #       comma
    assert isinstance(codec, AggregateFunction)
    assert codec.function_call == "count"
    assert codec.arg_types == []
    assert codec.name == "AggregateFunction(count)"


# ---- round-trips: known fixed-size aggregates ---------------------------


async def test_aggregate_function_avg_state_round_trips_byte_for_byte() -> None:
    # BEGIN: an AggregateFunction(avg, Float64) codec and synthesised
    #        state values. ``avg`` state is variable-length: 8 bytes
    #        (Float64 numerator) + varuint (denominator). The codec
    #        knows the row boundary by parsing the varuint; ``write``
    #        is a passthrough so any byte string we read can be
    #        re-INSERTed verbatim.
    codec = parse_type("AggregateFunction(avg, Float64)")
    # Hand-craft three states with denominators that span 1, 2, and 3
    # byte varuint encodings (3 → 1B, 1000 → 2B, 1_000_000 → 3B).
    states: list[bytes] = [
        struct.pack("<d", 10.5) + b"\x03",
        struct.pack("<d", 499500.0) + b"\xe8\x07",  # 1000
        struct.pack("<d", -42.25) + b"\xc0\x84\x3d",  # 1_000_000
    ]

    # WHEN: writing then reading back through the codec
    writer = BinaryWriter()
    codec.write(writer, states)
    decoded = await codec.read(_reader(writer.getvalue()), len(states))

    # THEN: every state survives byte-for-byte (lengths and varuint
    #       payloads preserved across the round-trip)
    assert decoded == states


async def test_aggregate_function_count_state_round_trips() -> None:
    # BEGIN: an AggregateFunction(count) codec; state = single UInt64
    codec = parse_type("AggregateFunction(count)")
    states: list[bytes] = [
        struct.pack("<Q", 0),
        struct.pack("<Q", 1),
        struct.pack("<Q", 2**63),
    ]

    # WHEN: round-tripping
    writer = BinaryWriter()
    codec.write(writer, states)
    decoded = await codec.read(_reader(writer.getvalue()), len(states))

    # THEN: every 8-byte state round-trips
    assert decoded == states


async def test_aggregate_function_empty_column_round_trip() -> None:
    # BEGIN / WHEN: an empty column reads / writes zero bytes
    codec = parse_type("AggregateFunction(avg, Float64)")
    writer = BinaryWriter()
    codec.write(writer, [])
    decoded = await codec.read(_reader(writer.getvalue()), 0)
    assert decoded == []


# ---- error paths --------------------------------------------------------


async def test_aggregate_function_unknown_function_raises() -> None:
    # BEGIN: an aggregate whose state format isn't in the table
    codec = parse_type("AggregateFunction(uniqHLL12, String)")

    # WHEN / THEN: reading raises NotImplementedError naming the
    #              function and pointing the user at the workaround
    with pytest.raises(NotImplementedError, match="uniqHLL12"):
        await codec.read(_reader(b"\x00" * 100), 1)
    # And writing raises symmetrically — we don't pretend the bytes
    # would round-trip when we don't know the format
    with pytest.raises(NotImplementedError, match="uniqHLL12"):
        codec.write(BinaryWriter(), [b"\x00" * 16])
