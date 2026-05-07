"""Codecs for `UUID`, `IPv4`, `IPv6`.

On-wire layouts:

- `UUID`: 16 bytes — two UInt64 little-endian halves, high half first.
  `UUID.int = (high << 64) | low`. This is the upstream ClickHouse
  storage format; the byte sequence on the wire is *not* the canonical
  RFC 4122 byte order.
- `IPv4`: 4 bytes — the address's UInt32 in little-endian. Note that
  the resulting byte sequence is the address octets reversed (since
  storing the integer little-endian flips the order versus the natural
  network-byte-order packing).
- `IPv6`: 16 raw bytes in network byte order — equivalent to
  `IPv6Address.packed`.
"""

from __future__ import annotations

import struct
import uuid
from ipaddress import IPv4Address, IPv6Address
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import BinaryWriter
    from clickhouse_async.protocol.io_sync import SyncBinaryReader

_NIL_UUID = uuid.UUID(int=0)
_ZERO_IPV4 = IPv4Address(0)
_ZERO_IPV6 = IPv6Address(0)


class UUID:
    name = "UUID"
    null_value: uuid.UUID = _NIL_UUID
    python_type: type = uuid.UUID

    def read(self, reader: SyncBinaryReader, n_rows: int) -> list[uuid.UUID]:
        if n_rows == 0:
            return []
        data = reader.read_exact(16 * n_rows)
        # Bulk-unpack the two UInt64 halves of each UUID in one C-level
        # call; we get a flat 2*n_rows tuple back and stride pairwise.
        halves = struct.unpack(f"<{2 * n_rows}Q", data)
        cls = uuid.UUID
        return [
            cls(int=(halves[i] << 64) | halves[i + 1]) for i in range(0, 2 * n_rows, 2)
        ]

    def write(self, writer: BinaryWriter, values: Sequence[uuid.UUID]) -> None:
        if not values:
            return
        # Bulk-pack two UInt64 halves per UUID in one struct call.
        mask = (1 << 64) - 1
        halves: list[int] = []
        for v in values:
            i = v.int
            halves.append((i >> 64) & mask)
            halves.append(i & mask)
        writer.write_raw(struct.pack(f"<{len(halves)}Q", *halves))


class IPv4:
    name = "IPv4"
    null_value: IPv4Address = _ZERO_IPV4
    python_type: type = IPv4Address

    def read(self, reader: SyncBinaryReader, n_rows: int) -> list[IPv4Address]:
        if n_rows == 0:
            return []
        data = reader.read_exact(4 * n_rows)
        ints = struct.unpack(f"<{n_rows}I", data)
        cls = IPv4Address
        return [cls(v) for v in ints]

    def write(self, writer: BinaryWriter, values: Sequence[IPv4Address]) -> None:
        if not values:
            return
        ints = [int(v) for v in values]
        writer.write_raw(struct.pack(f"<{len(ints)}I", *ints))


class IPv6:
    name = "IPv6"
    null_value: IPv6Address = _ZERO_IPV6
    python_type: type = IPv6Address

    def read(self, reader: SyncBinaryReader, n_rows: int) -> list[IPv6Address]:
        if n_rows == 0:
            return []
        data = reader.read_exact(16 * n_rows)
        return [IPv6Address(bytes(data[i * 16 : (i + 1) * 16])) for i in range(n_rows)]

    def write(self, writer: BinaryWriter, values: Sequence[IPv6Address]) -> None:
        if not values:
            return
        out = bytearray()
        for v in values:
            out.extend(v.packed)
        writer.write_raw(bytes(out))
