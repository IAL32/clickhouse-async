"""Round-trip and byte-layout tests for `UUID`, `IPv4`, `IPv6`."""

from __future__ import annotations

import uuid
from ipaddress import IPv4Address, IPv6Address
from typing import TYPE_CHECKING, Any

import pytest

from clickhouse_async.protocol.io import BinaryWriter
from clickhouse_async.protocol.io_sync import SyncBinaryReader
from clickhouse_async.types import ColumnCodec, parse_type
from clickhouse_async.types.net import UUID, IPv4, IPv6

if TYPE_CHECKING:
    from collections.abc import Sequence


def _reader(data: bytes) -> SyncBinaryReader:
    return SyncBinaryReader(bytes(data))


async def _round_trip(codec: ColumnCodec, values: Sequence[Any]) -> list[Any]:
    writer = BinaryWriter()
    codec.write(writer, values)
    return codec.read(_reader(writer.getvalue()), len(values))


# ---- UUID -----------------------------------------------------------------


async def test_uuid_round_trip_preserves_canonical_form() -> None:
    # BEGIN: a UUID codec and a known v4 UUID
    codec = parse_type("UUID")
    values = [
        uuid.UUID(int=0),
        uuid.UUID("12345678-1234-5678-9abc-def012345678"),
        uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: the canonical UUID values come back identical
    assert isinstance(codec, UUID)
    assert decoded == values


async def test_uuid_byte_layout_is_two_le_uint64_high_first() -> None:
    # BEGIN: a UUID whose high/low halves are easy to spot in the bytes
    codec = parse_type("UUID")
    high = 0x1122334455667788
    low = 0x99AABBCCDDEEFF00
    u = uuid.UUID(int=(high << 64) | low)

    # WHEN: writing a single value and inspecting the encoded bytes
    writer = BinaryWriter()
    codec.write(writer, [u])
    encoded = writer.getvalue()

    # THEN: bytes[0:8] are the high half little-endian, bytes[8:16] are the
    #       low half little-endian — the upstream CH on-wire layout
    assert encoded[:8] == high.to_bytes(8, "little")
    assert encoded[8:] == low.to_bytes(8, "little")


# ---- IPv4 -----------------------------------------------------------------


async def test_ipv4_round_trip() -> None:
    # BEGIN: an IPv4 codec and a small set of representative addresses
    codec = parse_type("IPv4")
    values = [
        IPv4Address("0.0.0.0"),
        IPv4Address("127.0.0.1"),
        IPv4Address("192.168.1.1"),
        IPv4Address("255.255.255.255"),
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every address survives identically
    assert isinstance(codec, IPv4)
    assert decoded == values


async def test_ipv4_byte_layout_is_uint32_little_endian() -> None:
    # BEGIN: 192.168.1.1 has integer value 0xC0A80101
    codec = parse_type("IPv4")
    addr = IPv4Address("192.168.1.1")

    # WHEN: writing one address
    writer = BinaryWriter()
    codec.write(writer, [addr])

    # THEN: the UInt32 is little-endian, so the byte sequence is the
    #       address octets reversed
    assert writer.getvalue() == b"\x01\x01\xa8\xc0"


# ---- IPv6 -----------------------------------------------------------------


async def test_ipv6_round_trip() -> None:
    # BEGIN: an IPv6 codec and a handful of representative addresses
    codec = parse_type("IPv6")
    values = [
        IPv6Address("::"),
        IPv6Address("::1"),
        IPv6Address("2001:db8::1"),
        IPv6Address("fe80::1"),
        IPv6Address("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
    ]

    # WHEN: round-tripping
    decoded = await _round_trip(codec, values)

    # THEN: every address survives identically
    assert isinstance(codec, IPv6)
    assert decoded == values


async def test_ipv6_byte_layout_is_network_order_packed() -> None:
    # BEGIN: an IPv6 codec and a known address
    codec = parse_type("IPv6")
    addr = IPv6Address("2001:db8::1")

    # WHEN: writing one address
    writer = BinaryWriter()
    codec.write(writer, [addr])

    # THEN: the bytes equal the address's `.packed` form (network byte order)
    assert writer.getvalue() == addr.packed


# ---- empty-batch invariants ---------------------------------------------


@pytest.mark.parametrize("spec", ["UUID", "IPv4", "IPv6"])
async def test_empty_batch_round_trip(spec: str) -> None:
    # BEGIN: a parsed network/UUID codec
    codec = parse_type(spec)

    # WHEN: round-tripping zero rows
    decoded = await _round_trip(codec, [])

    # THEN: nothing is read or written, and an empty list comes back
    assert decoded == []
