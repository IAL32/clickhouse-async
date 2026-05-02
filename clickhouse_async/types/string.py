"""Codec for variable-length UTF-8 ``String`` columns.

Each row is a varuint length followed by that many UTF-8 bytes.
``FixedString(N)`` is added in step 04b alongside the rest of the
primitive types.
"""

from __future__ import annotations

from collections.abc import Sequence

from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter


class String:
    name = "String"
    null_value: str = ""

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[str]:
        out: list[str] = []
        for _ in range(n_rows):
            out.append(await reader.read_string())
        return out

    def write(
        self, writer: BinaryWriter, values: Sequence[str]
    ) -> None:
        for v in values:
            writer.write_string(v)
