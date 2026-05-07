"""Codecs for `Enum8` and `Enum16`.

On-wire identical to signed `Int8` / `Int16`; the codec layer maps the
underlying integers to their named labels (`str`) on the Python side.

The block-header spec uses the form `Enum8('first' = 1, 'second' = 2)`,
which the parser recognises specially since the `=` separator is unique
to enum bodies — there is no general `key = value` param syntax.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from clickhouse_async.errors import ProtocolError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter


class _EnumCodec:
    """Common implementation for both Enum widths."""

    null_value: str = ""
    python_type: type = str
    _size: int = 0

    def __init__(self, mapping: dict[str, int]) -> None:
        if not mapping:
            raise ValueError("Enum requires at least one entry")
        self.mapping = dict(mapping)
        self._reverse = {v: k for k, v in mapping.items()}
        if len(self._reverse) != len(self.mapping):
            raise ValueError(f"Enum has duplicate values: {sorted(mapping.values())}")
        entries = ", ".join(f"'{k}' = {v}" for k, v in self.mapping.items())
        self.name = f"Enum{self._size * 8}({entries})"
        self.null_value = next(iter(self.mapping))

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[str]:
        if n_rows == 0:
            return []
        size = self._size
        data = await reader.read_exact(size * n_rows)
        out: list[str] = []
        for i in range(n_rows):
            v = int.from_bytes(data[i * size : (i + 1) * size], "little", signed=True)
            label = self._reverse.get(v)
            if label is None:
                raise ProtocolError(f"unknown {self.name} value at row {i}: {v}")
            out.append(label)
        return out

    def write(self, writer: BinaryWriter, values: Sequence[str]) -> None:
        if not values:
            return
        size = self._size
        out = bytearray()
        for label in values:
            v = self.mapping.get(label)
            if v is None:
                raise ValueError(f"unknown {self.name} label: {label!r}")
            out.extend(v.to_bytes(size, "little", signed=True))
        writer.write_raw(bytes(out))


class Enum8(_EnumCodec):
    _size = 1


class Enum16(_EnumCodec):
    _size = 2
