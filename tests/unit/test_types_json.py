"""Parser tests for the v0.2 ``JSON`` parser-only stub.

The codec round-trips ``codec.name`` for every JSON spec the server
emits in block headers. ``read`` and ``write`` raise
``NotImplementedError`` with a diagnostic pointing at the v0.3
follow-up; the column-body wire format (5 concatenated substreams) is
out of v0.2 scope.
"""

from __future__ import annotations

import asyncio

import pytest

from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.types import ColumnCodec, parse_type
from clickhouse_async.types.json_type import JSON


def _reader(data: bytes) -> AsyncBinaryReader:
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return AsyncBinaryReader(stream)


# ---- parser surface -----------------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [
        "JSON",
        "JSON(max_dynamic_paths=512)",
        "JSON(max_dynamic_types=16)",
        "JSON(max_dynamic_paths=512, max_dynamic_types=16)",
        "JSON(SKIP user.email)",
        "JSON(SKIP REGEXP 'tmp_.*')",
        "JSON(max_dynamic_paths=128, SKIP foo, SKIP REGEXP 'rx')",
    ],
)
def test_codec_name_round_trips_for_every_spec_shape(spec: str) -> None:
    # BEGIN: a JSON spec the server might emit in a block header
    # WHEN: parsing through the registry
    codec = parse_type(spec)

    # THEN: the codec is a JSON instance and ``codec.name`` reproduces
    #       the spec verbatim — important so re-emitting INSERT headers
    #       on the wire doesn't alter the type spec
    assert isinstance(codec, JSON)
    assert codec.name == spec


def test_bare_json_has_no_hints() -> None:
    # BEGIN / WHEN: bare ``JSON`` (no parens)
    codec = parse_type("JSON")

    # THEN: the codec records no hints
    assert isinstance(codec, JSON)
    assert codec.hints == []


def test_json_with_hints_stores_them_verbatim() -> None:
    # BEGIN / WHEN: a JSON spec with a mix of named-int and SKIP hints
    codec = parse_type("JSON(max_dynamic_paths=128, SKIP foo)")

    # THEN: each hint is stored as raw text — the codec doesn't try to
    #       interpret semantic meaning
    assert isinstance(codec, JSON)
    assert [h.text for h in codec.hints] == [
        "max_dynamic_paths=128",
        "SKIP foo",
    ]


# ---- empty-batch invariants ---------------------------------------------


async def test_read_with_zero_rows_returns_empty_list() -> None:
    # BEGIN / WHEN: reading zero rows — the codec's empty-block
    #               short-circuit means no bytes are consumed
    codec = parse_type("JSON")
    decoded = await codec.read(_reader(b""), 0)

    # THEN: an empty list comes back
    assert decoded == []


def test_write_with_empty_values_writes_no_bytes() -> None:
    # BEGIN / WHEN: writing zero values — the early return fires
    #               before any bytes are emitted
    codec = parse_type("JSON")
    writer = BinaryWriter()
    codec.write(writer, [])

    # THEN: the writer is left untouched
    assert writer.getvalue() == b""


# ---- round-trip -----------------------------------------------------------


async def _round_trip(
    codec: ColumnCodec, values: list[dict[str, object]]
) -> list[dict[str, object]]:
    writer = BinaryWriter()
    codec.write(writer, values)
    return await codec.read(_reader(writer.getvalue()), len(values))


async def test_flat_dict_round_trip() -> None:
    # BEGIN: a JSON codec and a 2-row block with two paths "a" and "b"
    codec = parse_type("JSON")
    values: list[dict[str, object]] = [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every row's path → value mapping survives
    assert decoded == values


async def test_paths_missing_in_some_rows_round_trip() -> None:
    # BEGIN: a JSON codec where path "b" is absent from some rows
    codec = parse_type("JSON")
    values: list[dict[str, object]] = [
        {"a": 1, "b": "x"},
        {"a": 2},
        {"b": "z"},
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: missing paths come back as absent keys (not ``{path: None}``)
    assert decoded == values


async def test_dotted_path_keys_round_trip() -> None:
    # BEGIN: nested input pre-flattened to dotted-path keys —
    #        upstream stores nested JSON as ``user.id`` etc., so we
    #        accept that representation
    codec = parse_type("JSON")
    values: list[dict[str, object]] = [
        {"user.id": 7, "user.name": "alice"},
        {"user.id": 8, "user.name": "bob"},
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: dotted-path keys survive verbatim
    assert decoded == values


async def test_heterogeneous_values_per_path_round_trip() -> None:
    # BEGIN: same path "v" carries an int in one row and a string in
    #        another — the per-path Dynamic codec must declare both
    #        arms in the block prefix
    codec = parse_type("JSON")
    values: list[dict[str, object]] = [
        {"v": 1},
        {"v": "two"},
        {"v": 3},
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: per-path values come back with their original Python types
    assert decoded == values


async def test_all_empty_dicts_round_trip() -> None:
    # BEGIN: every row is ``{}`` — no paths at all
    codec = parse_type("JSON")
    values: list[dict[str, object]] = [{}, {}, {}]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every row comes back as an empty dict; the prefix declares
    #       zero dynamic paths
    assert decoded == values
