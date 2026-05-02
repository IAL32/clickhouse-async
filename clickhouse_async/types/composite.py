"""Composite codecs that wrap or combine other codecs.

For step 04a only ``Nullable(T)`` lives here. ``Array``, ``Tuple``,
``Map``, ``LowCardinality``, and ``Enum`` are added in later sub-steps.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.types.base import ColumnCodec


class Nullable:
    """Wraps another codec, prefixing the column body with a 1-byte-per-row
    null mask (``0`` = not null, ``1`` = null).

    The on-wire format requires the inner codec to write *every* row even
    when the row is null, so on writes we substitute ``inner.null_value``
    in place of ``None`` before delegating.
    """

    null_value: None = None

    def __init__(self, inner: ColumnCodec) -> None:
        self.inner = inner
        self.name = f"Nullable({inner.name})"

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[Any]:
        if n_rows == 0:
            return []
        mask = await reader.read_exact(n_rows)
        values = await self.inner.read(reader, n_rows)
        return [None if mask[i] else values[i] for i in range(n_rows)]

    def write(
        self, writer: BinaryWriter, values: Sequence[Any]
    ) -> None:
        n = len(values)
        if n == 0:
            return
        writer.write_raw(bytes(1 if v is None else 0 for v in values))
        replaced: list[Any] = [
            self.inner.null_value if v is None else v for v in values
        ]
        self.inner.write(writer, replaced)
