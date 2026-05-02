"""Round-trip and edge-case tests for the binary I/O primitives."""

from __future__ import annotations

import asyncio

import pytest

from clickhouse_async.errors import ProtocolError
from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter


def _reader(data: bytes) -> AsyncBinaryReader:
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return AsyncBinaryReader(stream)


# ---- varuint --------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        0,
        1,
        2**7 - 1,
        2**7,
        2**14 - 1,
        2**14,
        2**21 - 1,
        2**32 - 1,
        2**63 - 1,
        2**64 - 1,
    ],
)
async def test_varuint_round_trip(value: int) -> None:
    # BEGIN
    writer = BinaryWriter()

    # WHEN
    writer.write_varuint(value)
    decoded = await _reader(writer.getvalue()).read_varuint()

    # THEN
    assert decoded == value


async def test_varuint_known_encoding() -> None:
    # BEGIN
    # LEB128: 300 → 0xAC 0x02 (canonical fixture)
    writer = BinaryWriter()

    # WHEN
    writer.write_varuint(300)

    # THEN
    assert writer.getvalue() == b"\xac\x02"


async def test_varuint_zero_is_one_byte() -> None:
    # BEGIN
    writer = BinaryWriter()

    # WHEN
    writer.write_varuint(0)

    # THEN
    assert writer.getvalue() == b"\x00"


def test_varuint_negative_raises_value_error() -> None:
    # BEGIN
    writer = BinaryWriter()

    # WHEN / THEN
    with pytest.raises(ValueError, match="cannot be negative"):
        writer.write_varuint(-1)


async def test_varuint_too_long_raises_protocol_error() -> None:
    # BEGIN
    # 11 continuation bytes — one over the u64-safe limit
    bad = bytes([0xFF] * 11)
    reader = _reader(bad)

    # WHEN / THEN
    with pytest.raises(ProtocolError, match="varuint exceeds"):
        await reader.read_varuint()


async def test_varuint_truncated_raises_protocol_error() -> None:
    # BEGIN
    # A continuation byte (high bit set) with no follower
    reader = _reader(b"\xff")

    # WHEN / THEN
    with pytest.raises(ProtocolError, match="short read"):
        await reader.read_varuint()


# ---- fixed-width integers -------------------------------------------------


@pytest.mark.parametrize(
    "width,signed,value",
    [
        (1, False, 0),
        (1, False, 255),
        (1, True, -128),
        (1, True, 127),
        (2, True, -(2**15)),
        (2, False, 2**16 - 1),
        (4, True, -(2**31)),
        (4, False, 2**32 - 1),
        (8, True, -(2**63)),
        (8, False, 2**64 - 1),
        (16, False, 2**128 - 1),
        (16, True, -(2**127)),
        (32, False, 2**256 - 1),
        (32, True, -(2**255)),
    ],
)
async def test_int_round_trip(width: int, signed: bool, value: int) -> None:
    # BEGIN
    writer = BinaryWriter()

    # WHEN
    writer.write_int(value, width, signed=signed)
    encoded = writer.getvalue()
    decoded = await _reader(encoded).read_int(width, signed=signed)

    # THEN
    assert decoded == value
    assert len(encoded) == width


async def test_int_is_little_endian() -> None:
    # BEGIN
    writer = BinaryWriter()

    # WHEN
    writer.write_int(0x01020304, 4, signed=False)

    # THEN
    assert writer.getvalue() == b"\x04\x03\x02\x01"


# ---- length-prefixed string ----------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "",
        "a",
        "hello world",
        "café",
        "日本語",
        "🦆",
        "x" * (1024 * 1024),
    ],
)
async def test_string_round_trip(value: str) -> None:
    # BEGIN
    writer = BinaryWriter()

    # WHEN
    writer.write_string(value)
    decoded = await _reader(writer.getvalue()).read_string()

    # THEN
    assert decoded == value


async def test_string_invalid_utf8_raises_protocol_error() -> None:
    # BEGIN
    # Length 2, then bytes that are not valid UTF-8
    reader = _reader(b"\x02\xff\xfe")

    # WHEN / THEN
    with pytest.raises(ProtocolError, match="UTF-8"):
        await reader.read_string()


# ---- length-prefixed bytes -----------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        b"",
        b"\x00",
        b"\xff" * 10,
        b"\xaa\xbb\xcc\xdd",
        bytes(range(256)),
    ],
)
async def test_bytes_round_trip(value: bytes) -> None:
    # BEGIN
    writer = BinaryWriter()

    # WHEN
    writer.write_bytes(value)
    decoded = await _reader(writer.getvalue()).read_bytes()

    # THEN
    assert decoded == value


# ---- reader position tracking --------------------------------------------


async def test_position_tracks_bytes_consumed() -> None:
    # BEGIN
    reader = _reader(b"\x01\x02\x03\x04")

    # WHEN
    await reader.read_byte()
    await reader.read_byte()

    # THEN
    assert reader.position == 2


async def test_short_read_error_includes_offset() -> None:
    # BEGIN
    reader = _reader(b"\x01\x02")

    # WHEN
    await reader.read_byte()
    await reader.read_byte()

    # THEN
    with pytest.raises(ProtocolError, match="offset 2"):
        await reader.read_byte()


# ---- composability -------------------------------------------------------


async def test_compound_round_trip() -> None:
    # BEGIN
    writer = BinaryWriter()
    writer.write_varuint(7)
    writer.write_string("packet")
    writer.write_int(42, 4, signed=True)
    writer.write_bytes(b"\x00\xff")

    # WHEN
    reader = _reader(writer.getvalue())
    a = await reader.read_varuint()
    b = await reader.read_string()
    c = await reader.read_int(4, signed=True)
    d = await reader.read_bytes()

    # THEN
    assert (a, b, c, d) == (7, "packet", 42, b"\x00\xff")
