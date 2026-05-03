"""Tests for the type-spec parser and the v0a codec set (Int32, String,
Nullable)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.types import ColumnCodec, parse_type
from clickhouse_async.types.composite import Nullable
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


# ---- parser --------------------------------------------------------------


def test_parse_type_recognises_nullary_names() -> None:
    # BEGIN: the type spec for the simplest non-parametric codec
    spec = "Int32"

    # WHEN: parsing the spec
    codec = parse_type(spec)

    # THEN: the registry returns an Int32 codec whose name round-trips
    assert isinstance(codec, Int32)
    assert codec.name == "Int32"


def test_parse_type_handles_nested_nullable() -> None:
    # BEGIN: a nested type spec exercising the parser's recursion
    spec = "Nullable(Int32)"

    # WHEN: parsing the spec
    codec = parse_type(spec)

    # THEN: the outer codec is Nullable wrapping an Int32; the name
    #       reflects the nesting verbatim
    assert isinstance(codec, Nullable)
    assert isinstance(codec.inner, Int32)
    assert codec.name == "Nullable(Int32)"


def test_parse_type_tolerates_whitespace_around_tokens() -> None:
    # BEGIN: a spec padded with whitespace inside and around the parens
    spec = "  Nullable( String )  "

    # WHEN: parsing the spec
    codec = parse_type(spec)

    # THEN: the parser produces the same codec as the canonical form
    assert isinstance(codec, Nullable)
    assert isinstance(codec.inner, String)
    assert codec.name == "Nullable(String)"


@pytest.mark.parametrize(
    "spec,fragment",
    [
        ("Int999", "unknown type"),
        ("Nullable(Quux)", "unknown type"),
        ("Quux(Int32)", "unknown parametric type"),
        ("Nullable(Int32", r"expected '\)'"),
        ("Nullable Int32", "unknown type"),
        ("Int32 trailing", "trailing characters"),
        ("", "expected identifier"),
    ],
)
def test_parse_type_rejects_malformed_specs(spec: str, fragment: str) -> None:
    # BEGIN: a malformed type spec
    pass

    # WHEN: parsing the spec
    # THEN: a ValueError surfaces with a message naming what went wrong
    with pytest.raises(ValueError, match=fragment):
        parse_type(spec)


# ---- Int32 codec ---------------------------------------------------------


@pytest.mark.parametrize(
    "values",
    [
        [],
        [0],
        [1, -1, 2**31 - 1, -(2**31)],
        list(range(-50, 50)),
    ],
)
async def test_int32_round_trip(values: list[int]) -> None:
    # BEGIN: the Int32 codec and a list of edge-aligned integers
    codec = Int32()

    # WHEN: writing and reading back through the codec
    decoded = await _round_trip(codec, values)

    # THEN: the values round-trip identically
    assert decoded == values


async def test_int32_known_encoding_is_little_endian_packed() -> None:
    # BEGIN: an Int32 codec and a single value with a recognisable byte pattern
    codec = Int32()
    writer = BinaryWriter()

    # WHEN: writing one value
    codec.write(writer, [0x01020304])

    # THEN: the bytes appear low-byte-first (little-endian), no padding
    assert writer.getvalue() == b"\x04\x03\x02\x01"


# ---- String codec --------------------------------------------------------


@pytest.mark.parametrize(
    "values",
    [
        [],
        [""],
        ["a", "bb", "ccc"],
        ["", "non-empty", "", "café", "🦆"],
    ],
)
async def test_string_round_trip(values: list[str]) -> None:
    # BEGIN: the String codec and a list of mixed-length strings
    codec = String()

    # WHEN: writing and reading back through the codec
    decoded = await _round_trip(codec, values)

    # THEN: every string survives the round-trip verbatim
    assert decoded == values


# ---- Nullable codec -----------------------------------------------------


@pytest.mark.parametrize(
    "values",
    [
        [],
        [None],
        [1, None, 2, None, 3],
        [None, None, None],
        [0, 1, 2],
    ],
)
async def test_nullable_int32_round_trip(values: list[int | None]) -> None:
    # BEGIN: a Nullable(Int32) codec and a values list mixing ints and Nones
    codec = parse_type("Nullable(Int32)")

    # WHEN: writing and reading back through the codec
    decoded = await _round_trip(codec, values)

    # THEN: every position round-trips, including null-vs-zero distinctions
    assert decoded == values


async def test_nullable_string_distinguishes_null_from_empty() -> None:
    # BEGIN: a Nullable(String) codec where `None` and `""` must not collide
    codec = parse_type("Nullable(String)")
    values: list[str | None] = [None, "", "a", None, ""]

    # WHEN: writing and reading back through the codec
    decoded = await _round_trip(codec, values)

    # THEN: the null mask preserves the None/'' distinction
    assert decoded == values


async def test_nullable_emits_null_mask_then_inner_body() -> None:
    # BEGIN: a Nullable(Int32) codec and a known mixed sequence
    codec = parse_type("Nullable(Int32)")
    writer = BinaryWriter()

    # WHEN: writing the values and inspecting the byte layout
    codec.write(writer, [None, 7, None])
    encoded = writer.getvalue()

    # THEN: the first three bytes are the null mask; the next twelve are
    #       three little-endian Int32s, with `0` as the placeholder for
    #       the two null positions
    assert encoded[:3] == b"\x01\x00\x01"
    assert encoded[3:] == b"\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00"
