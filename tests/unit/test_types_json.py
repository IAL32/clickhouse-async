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
from clickhouse_async.types import parse_type
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


# ---- read / write stubs --------------------------------------------------


async def test_read_with_zero_rows_returns_empty_list_without_raising() -> None:
    # BEGIN: a JSON codec
    codec = parse_type("JSON")

    # WHEN: reading zero rows — the n_rows == 0 short-circuit applies
    #       BEFORE the NotImplementedError so empty SELECTs against a
    #       JSON column don't blow up the parser
    decoded = await codec.read(_reader(b""), 0)

    # THEN: an empty list comes back
    assert decoded == []


async def test_read_with_rows_raises_not_implemented_with_diagnostic() -> None:
    # BEGIN: a JSON codec asked to decode at least one row
    codec = parse_type("JSON")

    # WHEN / THEN: the v0.2 stub raises with a diagnostic pointing at
    #              the v0.3 follow-up and a workaround
    with pytest.raises(NotImplementedError, match="JSON column body decoding"):
        await codec.read(_reader(b""), 1)


async def test_write_with_empty_values_returns_without_raising() -> None:
    # BEGIN / WHEN: writing zero values — the early return fires before
    #               the NotImplementedError so the writer is left
    #               untouched
    codec = parse_type("JSON")
    writer = BinaryWriter()
    codec.write(writer, [])

    # THEN: no bytes were written
    assert writer.getvalue() == b""


def test_write_with_values_raises_not_implemented_with_diagnostic() -> None:
    # BEGIN: a JSON codec with at least one value
    codec = parse_type("JSON")

    # WHEN / THEN: the v0.2 stub raises with a diagnostic pointing at
    #              the v0.3 follow-up and the cast-to-String workaround
    with pytest.raises(NotImplementedError, match="JSON column body encoding"):
        codec.write(BinaryWriter(), [{"a": 1}])
