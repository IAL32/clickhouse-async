"""The protocol every column codec implements.

Each codec encodes/decodes one ClickHouse column type. The reader and
writer call into codecs row-batch-at-a-time: ``read(reader, n_rows)``
returns a list of Python values, ``write(writer, values)`` emits them.

``null_value`` is the placeholder a codec uses for itself when wrapped
in ``Nullable(T)`` — the on-wire format requires a value at every row
position even when the row is null, so ``Nullable`` substitutes
``inner.null_value`` for ``None`` before delegating the write.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable

from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter


@runtime_checkable
class ColumnCodec(Protocol):
    """Encode and decode a single ClickHouse column type.

    Method signatures use ``Any`` because the registry returns codecs
    keyed on a runtime spec string. Concrete codecs narrow their own
    signatures to the specific Python type they handle (e.g. ``int``,
    ``str``, ``datetime``).
    """

    name: str
    null_value: Any

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[Any]: ...

    def write(self, writer: BinaryWriter, values: Sequence[Any]) -> None: ...
