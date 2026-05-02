"""Composite codecs that wrap or combine other codecs.

- ``Nullable(T)`` — 1-byte-per-row null mask, then the inner body.
- ``Array(T)``    — UInt64 cumulative-offsets row, then the inner body
                    holding the flattened values.
- ``Tuple(T1, T2, …)`` — each component's full column body in order
                    (n_rows of T1, then n_rows of T2, …).
- ``Map(K, V)``   — same wire format as ``Array(Tuple(K, V))``.

``LowCardinality(T)`` and ``Enum8/16`` land in step 04e.
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


class Array:
    """``Array(T)`` — cumulative UInt64 offsets followed by the flattened
    inner column.

    The offsets row holds ``n_rows`` UInt64 values; offsets[i] is the
    running total of array lengths from row 0 through row i (inclusive).
    The inner body then holds ``offsets[-1]`` flat values to be sliced
    back into per-row arrays.
    """

    null_value: list[Any]

    def __init__(self, inner: ColumnCodec) -> None:
        self.inner = inner
        self.name = f"Array({inner.name})"
        self.null_value = []

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[list[Any]]:
        if n_rows == 0:
            return []
        # Cumulative offsets — one UInt64 per row.
        offsets_data = await reader.read_exact(8 * n_rows)
        offsets = [
            int.from_bytes(offsets_data[i * 8 : (i + 1) * 8], "little", signed=False)
            for i in range(n_rows)
        ]
        total = offsets[-1]
        flat = await self.inner.read(reader, total)
        out: list[list[Any]] = []
        prev = 0
        for end in offsets:
            out.append(list(flat[prev:end]))
            prev = end
        return out

    def write(
        self, writer: BinaryWriter, values: Sequence[Sequence[Any]]
    ) -> None:
        n = len(values)
        if n == 0:
            return
        # Cumulative offsets.
        offsets = bytearray()
        running = 0
        for v in values:
            running += len(v)
            offsets.extend(running.to_bytes(8, "little", signed=False))
        writer.write_raw(bytes(offsets))
        # Flat inner body.
        flat: list[Any] = [item for row in values for item in row]
        self.inner.write(writer, flat)


class Tuple:
    """``Tuple(T1, T2, …)`` — each component's full column body in order.

    The wire layout is *not* row-major; each component contributes its
    ``n_rows``-long body sequentially. We read each component's column,
    then ``zip`` them into Python tuples.
    """

    null_value: tuple[Any, ...]

    def __init__(self, *components: ColumnCodec) -> None:
        if not components:
            raise ValueError("Tuple requires at least one component")
        self.components = components
        self.name = f"Tuple({', '.join(c.name for c in components)})"
        self.null_value = tuple(c.null_value for c in components)

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[tuple[Any, ...]]:
        if n_rows == 0:
            return []
        columns: list[list[Any]] = []
        for component in self.components:
            columns.append(await component.read(reader, n_rows))
        return [
            tuple(columns[c][i] for c in range(len(self.components)))
            for i in range(n_rows)
        ]

    def write(
        self, writer: BinaryWriter, values: Sequence[Sequence[Any]]
    ) -> None:
        n = len(values)
        if n == 0:
            return
        for c, component in enumerate(self.components):
            component.write(writer, [row[c] for row in values])


class Map:
    """``Map(K, V)`` — same wire format as ``Array(Tuple(K, V))``.

    The Python representation is a ``dict[K, V]`` per row. Repeated keys
    in the on-wire payload are not preserved by the dict conversion;
    ClickHouse Map columns aren't supposed to contain duplicate keys.
    """

    null_value: dict[Any, Any]

    def __init__(self, key: ColumnCodec, value: ColumnCodec) -> None:
        self.key = key
        self.value = value
        self.name = f"Map({key.name}, {value.name})"
        # Reuse Array(Tuple(K, V)) for the actual wire work.
        self._inner: Array = Array(Tuple(key, value))
        self.null_value = {}

    async def read(
        self, reader: AsyncBinaryReader, n_rows: int
    ) -> list[dict[Any, Any]]:
        rows = await self._inner.read(reader, n_rows)
        return [dict(row) for row in rows]

    def write(
        self, writer: BinaryWriter, values: Sequence[dict[Any, Any]]
    ) -> None:
        rows = [list(d.items()) for d in values]
        self._inner.write(writer, rows)
