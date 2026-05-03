"""Codecs for fixed-width primitive scalars.

Two implementation paths based on width:

- **≤ 64 bits** (Int{8,16,32,64}, UInt{8,16,32,64}, Float{32,64}, Bool):
  bulk encode/decode with ``struct`` over the whole column buffer at
  once. One ``await`` per batch on the read path; one ``write_raw`` on
  the write path.
- **128 / 256 bits**: ``struct`` has no format characters for these
  widths, so we use ``int.from_bytes`` / ``int.to_bytes`` per row, but
  still issue exactly one ``read_exact`` and one ``write_raw`` per batch.
"""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter


class _StructCodec:
    """Codec for any primitive whose Python value round-trips through a
    single ``struct`` format character."""

    name: str = ""
    null_value: Any = 0
    _format: str = ""
    _size: int = 0

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[Any]:
        if n_rows == 0:
            return []
        data = await reader.read_exact(self._size * n_rows)
        return list(struct.unpack(f"<{n_rows}{self._format}", data))

    def write(self, writer: BinaryWriter, values: Sequence[Any]) -> None:
        n = len(values)
        if n == 0:
            return
        writer.write_raw(struct.pack(f"<{n}{self._format}", *values))


# ---- signed integers ≤ 64 bits -------------------------------------------


class Int8(_StructCodec):
    name = "Int8"
    null_value: int = 0
    _format = "b"
    _size = 1


class Int16(_StructCodec):
    name = "Int16"
    null_value: int = 0
    _format = "h"
    _size = 2


class Int32(_StructCodec):
    name = "Int32"
    null_value: int = 0
    _format = "i"
    _size = 4


class Int64(_StructCodec):
    name = "Int64"
    null_value: int = 0
    _format = "q"
    _size = 8


# ---- unsigned integers ≤ 64 bits -----------------------------------------


class UInt8(_StructCodec):
    name = "UInt8"
    null_value: int = 0
    _format = "B"
    _size = 1


class UInt16(_StructCodec):
    name = "UInt16"
    null_value: int = 0
    _format = "H"
    _size = 2


class UInt32(_StructCodec):
    name = "UInt32"
    null_value: int = 0
    _format = "I"
    _size = 4


class UInt64(_StructCodec):
    name = "UInt64"
    null_value: int = 0
    _format = "Q"
    _size = 8


# ---- floats --------------------------------------------------------------


class Float32(_StructCodec):
    name = "Float32"
    null_value: float = 0.0
    _format = "f"
    _size = 4


class Float64(_StructCodec):
    name = "Float64"
    null_value: float = 0.0
    _format = "d"
    _size = 8


# ---- 128 / 256-bit integers ----------------------------------------------


class _BigIntCodec:
    """Codec for 128/256-bit signed or unsigned integers — outside ``struct``
    format coverage, encoded per-row via ``int.to_bytes``."""

    name: str = ""
    null_value: int = 0
    _size: int = 0
    _signed: bool = False

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[int]:
        if n_rows == 0:
            return []
        size = self._size
        signed = self._signed
        data = await reader.read_exact(size * n_rows)
        return [
            int.from_bytes(data[i * size : (i + 1) * size], "little", signed=signed)
            for i in range(n_rows)
        ]

    def write(self, writer: BinaryWriter, values: Sequence[int]) -> None:
        if not values:
            return
        size = self._size
        signed = self._signed
        out = bytearray()
        for v in values:
            out.extend(v.to_bytes(size, "little", signed=signed))
        writer.write_raw(bytes(out))


class Int128(_BigIntCodec):
    name = "Int128"
    _size = 16
    _signed = True


class UInt128(_BigIntCodec):
    name = "UInt128"
    _size = 16
    _signed = False


class Int256(_BigIntCodec):
    name = "Int256"
    _size = 32
    _signed = True


class UInt256(_BigIntCodec):
    name = "UInt256"
    _size = 32
    _signed = False


# ---- bool ----------------------------------------------------------------


class Bool:
    """ClickHouse ``Bool`` is on-wire identical to ``UInt8`` (0 or 1) but the
    Python side surfaces ``bool`` rather than ``int`` so users get the
    expected truthiness and identity semantics.
    """

    name = "Bool"
    null_value: bool = False

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[bool]:
        if n_rows == 0:
            return []
        data = await reader.read_exact(n_rows)
        return [b != 0 for b in data]

    def write(self, writer: BinaryWriter, values: Sequence[bool]) -> None:
        if not values:
            return
        writer.write_raw(bytes(1 if v else 0 for v in values))
