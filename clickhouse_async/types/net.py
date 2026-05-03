"""Codecs for ``UUID``, ``IPv4``, ``IPv6``.

On-wire layouts:

- ``UUID``: 16 bytes — two UInt64 little-endian halves, high half first.
  ``UUID.int = (high << 64) | low``. This is the upstream ClickHouse
  storage format; the byte sequence on the wire is *not* the canonical
  RFC 4122 byte order.
- ``IPv4``: 4 bytes — the address's UInt32 in little-endian. Note that
  the resulting byte sequence is the address octets reversed (since
  storing the integer little-endian flips the order versus the natural
  network-byte-order packing).
- ``IPv6``: 16 raw bytes in network byte order — equivalent to
  ``IPv6Address.packed``.
"""

from __future__ import annotations

import uuid
from ipaddress import IPv4Address, IPv6Address
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter

_NIL_UUID = uuid.UUID(int=0)
_ZERO_IPV4 = IPv4Address(0)
_ZERO_IPV6 = IPv6Address(0)


class UUID:
    name = "UUID"
    null_value: uuid.UUID = _NIL_UUID
    python_type: type = uuid.UUID

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[uuid.UUID]:
        if n_rows == 0:
            return []
        data = await reader.read_exact(16 * n_rows)
        out: list[uuid.UUID] = []
        for i in range(n_rows):
            base = i * 16
            high = int.from_bytes(data[base : base + 8], "little", signed=False)
            low = int.from_bytes(data[base + 8 : base + 16], "little", signed=False)
            out.append(uuid.UUID(int=(high << 64) | low))
        return out

    def write(self, writer: BinaryWriter, values: Sequence[uuid.UUID]) -> None:
        if not values:
            return
        out = bytearray()
        for v in values:
            high = (v.int >> 64) & ((1 << 64) - 1)
            low = v.int & ((1 << 64) - 1)
            out.extend(high.to_bytes(8, "little", signed=False))
            out.extend(low.to_bytes(8, "little", signed=False))
        writer.write_raw(bytes(out))


class IPv4:
    name = "IPv4"
    null_value: IPv4Address = _ZERO_IPV4
    python_type: type = IPv4Address

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[IPv4Address]:
        if n_rows == 0:
            return []
        data = await reader.read_exact(4 * n_rows)
        return [
            IPv4Address(
                int.from_bytes(data[i * 4 : (i + 1) * 4], "little", signed=False)
            )
            for i in range(n_rows)
        ]

    def write(self, writer: BinaryWriter, values: Sequence[IPv4Address]) -> None:
        if not values:
            return
        out = bytearray()
        for v in values:
            out.extend(int(v).to_bytes(4, "little", signed=False))
        writer.write_raw(bytes(out))


class IPv6:
    name = "IPv6"
    null_value: IPv6Address = _ZERO_IPV6
    python_type: type = IPv6Address

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[IPv6Address]:
        if n_rows == 0:
            return []
        data = await reader.read_exact(16 * n_rows)
        return [IPv6Address(bytes(data[i * 16 : (i + 1) * 16])) for i in range(n_rows)]

    def write(self, writer: BinaryWriter, values: Sequence[IPv6Address]) -> None:
        if not values:
            return
        out = bytearray()
        for v in values:
            out.extend(v.packed)
        writer.write_raw(bytes(out))
