"""Codecs for fixed-width primitive scalars.

Bulk-encoded with ``struct.pack`` / ``struct.unpack`` over the whole
column buffer at once — one ``await`` for the whole batch on the read
path, one ``write_raw`` on the write path. Avoids per-row method-call
overhead, which dominates Python costs for tight numeric columns.
"""

from __future__ import annotations

import struct
from collections.abc import Sequence

from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter


class Int32:
    name = "Int32"
    null_value: int = 0
    _format: str = "i"  # signed 32-bit, little-endian (with `<` prefix)
    _size: int = 4

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[int]:
        if n_rows == 0:
            return []
        data = await reader.read_exact(self._size * n_rows)
        return list(struct.unpack(f"<{n_rows}{self._format}", data))

    def write(
        self, writer: BinaryWriter, values: Sequence[int]
    ) -> None:
        n = len(values)
        if n == 0:
            return
        writer.write_raw(struct.pack(f"<{n}{self._format}", *values))
