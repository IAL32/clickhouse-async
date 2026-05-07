"""Codecs for `Enum8` and `Enum16`.

On-wire identical to signed `Int8` / `Int16`; the codec layer maps the
underlying integers to their named labels (`str`) on the Python side.

The block-header spec uses the form `Enum8('first' = 1, 'second' = 2)`,
which the parser recognises specially since the `=` separator is unique
to enum bodies — there is no general `key = value` param syntax.
"""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING

from clickhouse_async.errors import ProtocolError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import BinaryWriter
    from clickhouse_async.protocol.io_sync import SyncBinaryReader


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

    def read(self, reader: SyncBinaryReader, n_rows: int) -> list[str]:
        if n_rows == 0:
            return []
        size = self._size
        data = reader.read_exact(size * n_rows)
        # Bulk-unpack with struct so the per-row decode is just a dict
        # lookup. Width 1 → "b" (Int8), width 2 → "h" (Int16).
        fmt = "b" if size == 1 else "h"
        ints = struct.unpack(f"<{n_rows}{fmt}", data)
        reverse = self._reverse
        out: list[str] = [""] * n_rows
        for i, v in enumerate(ints):
            label = reverse.get(v)
            if label is None:
                raise ProtocolError(f"unknown {self.name} value at row {i}: {v}")
            out[i] = label
        return out

    def write(self, writer: BinaryWriter, values: Sequence[str]) -> None:
        if not values:
            return
        size = self._size
        mapping = self.mapping
        ints: list[int] = []
        for label in values:
            v = mapping.get(label)
            if v is None:
                raise ValueError(f"unknown {self.name} label: {label!r}")
            ints.append(v)
        # Width 1 → "b" (Int8), width 2 → "h" (Int16); same as the
        # read-side dispatch.
        fmt = "b" if size == 1 else "h"
        writer.write_raw(struct.pack(f"<{len(ints)}{fmt}", *ints))


class Enum8(_EnumCodec):
    _size = 1


class Enum16(_EnumCodec):
    _size = 2
