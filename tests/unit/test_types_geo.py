"""Round-trip tests for `Point`, `Ring`, `Polygon`, `MultiPolygon`.

These are pure aliases over `Tuple(Float64, Float64)` and nested
`Array(...)` shapes, so the wire bytes match the desugared form
verbatim. The tests cover three things:

- Each alias parses and `codec.name` round-trips the alias spelling.
- Empty + non-empty values round-trip cleanly.
- Bytes from a `Point` codec match bytes from a hand-built
  `Tuple(Float64, Float64)` codec for the same value (pinning the
  desugaring so a future `Point` change can't silently desync).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.types import parse_type
from clickhouse_async.types.composite import Array, Tuple
from clickhouse_async.types.geo import MultiPolygon, Point, Polygon, Ring
from clickhouse_async.types.primitive import Float64

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.types import ColumnCodec


def _reader(data: bytes) -> AsyncBinaryReader:
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return AsyncBinaryReader(stream)


async def _round_trip(codec: ColumnCodec, values: Sequence[Any]) -> list[Any]:
    writer = BinaryWriter()
    codec.write(writer, values)
    return await codec.read(_reader(writer.getvalue()), len(values))


# ---- name + parser surface ---------------------------------------------


def test_geo_aliases_parse_and_render_their_alias_form() -> None:
    # BEGIN / WHEN / THEN: each alias parses to its own class and
    #     `.name` round-trips the alias spelling — not the
    #     desugared form
    point = parse_type("Point")
    assert isinstance(point, Point)
    assert point.name == "Point"

    ring = parse_type("Ring")
    assert isinstance(ring, Ring)
    assert ring.name == "Ring"

    polygon = parse_type("Polygon")
    assert isinstance(polygon, Polygon)
    assert polygon.name == "Polygon"

    multi = parse_type("MultiPolygon")
    assert isinstance(multi, MultiPolygon)
    assert multi.name == "MultiPolygon"


# ---- round-trips at every level ----------------------------------------


async def test_point_round_trip() -> None:
    # BEGIN: a Point codec and three (x, y) coords incl. negatives
    codec = parse_type("Point")
    values: list[tuple[float, float]] = [
        (0.0, 0.0),
        (1.5, -2.25),
        (-3.5, 4.5),
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every coordinate round-trips bit-for-bit
    assert decoded == values


async def test_point_empty_round_trip() -> None:
    # BEGIN / WHEN / THEN: an empty Point column reads / writes zero bytes
    codec = parse_type("Point")
    decoded = await _round_trip(codec, [])
    assert decoded == []


async def test_ring_round_trip_three_points() -> None:
    # BEGIN: a Ring codec and a single ring of three points
    codec = parse_type("Ring")
    values: list[list[tuple[float, float]]] = [
        [(0.0, 0.0), (1.0, 0.0), (0.5, 1.0)],
        [],
        [(2.0, 2.0)],
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: nested structure survives
    assert decoded == values


async def test_polygon_round_trip_two_rings() -> None:
    # BEGIN: a Polygon codec — a polygon with a hole (two rings)
    codec = parse_type("Polygon")
    values: list[list[list[tuple[float, float]]]] = [
        [
            # outer ring
            [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)],
            # inner hole
            [(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0)],
        ],
        [],
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: both rings preserved in order
    assert decoded == values


async def test_multipolygon_round_trip_two_polygons() -> None:
    # BEGIN: a MultiPolygon codec with two disjoint polygons
    codec = parse_type("MultiPolygon")
    values: list[list[list[list[tuple[float, float]]]]] = [
        [
            [[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]],
            [[(5.0, 5.0), (6.0, 5.0), (6.0, 6.0)]],
        ],
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: both polygons preserved
    assert decoded == values


# ---- desugaring pin ----------------------------------------------------


async def test_point_bytes_match_handbuilt_tuple_float64() -> None:
    # BEGIN: a Point codec and a hand-built `Tuple(Float64, Float64)`
    point = Point()
    handbuilt = Tuple(Float64(), Float64())
    values: list[tuple[float, float]] = [
        (1.5, -2.25),
        (-3.0, 4.0),
    ]

    # WHEN: encoding the same values through both
    w_point = BinaryWriter()
    point.write(w_point, values)
    w_tuple = BinaryWriter()
    handbuilt.write(w_tuple, values)

    # THEN: bytes are byte-for-byte identical — Point is a pure alias
    assert w_point.getvalue() == w_tuple.getvalue()


async def test_polygon_bytes_match_handbuilt_array_array_tuple() -> None:
    # BEGIN: a Polygon codec and the hand-built nested-array equivalent
    polygon = Polygon()
    handbuilt = Array(Array(Tuple(Float64(), Float64())))
    values: list[list[list[tuple[float, float]]]] = [
        [[(0.0, 0.0), (1.0, 1.0)]],
        [[(2.0, 2.0)], [(3.0, 3.0), (4.0, 4.0)]],
    ]

    # WHEN: encoding through both
    w_poly = BinaryWriter()
    polygon.write(w_poly, values)
    w_hand = BinaryWriter()
    handbuilt.write(w_hand, values)

    # THEN: bytes match — Polygon is a pure alias
    assert w_poly.getvalue() == w_hand.getvalue()
