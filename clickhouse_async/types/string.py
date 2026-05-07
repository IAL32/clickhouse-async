"""Codecs for `String` and `FixedString(N)`.

`String` is varuint-prefixed UTF-8 per row; `FixedString(N)` is
exactly N raw bytes per row, NUL-padded on writes when the input is
shorter and surfaced as `bytes` on reads (FixedString columns are
often binary, not text — returning bytes preserves that).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from clickhouse_async import _fast_read
from clickhouse_async.protocol.io_sync import _VARUINT_CONTINUATION_BIT

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import BinaryWriter
    from clickhouse_async.protocol.io_sync import SyncBinaryReader


class String:
    name = "String"
    null_value: str = ""
    python_type: type = str

    def read(self, reader: SyncBinaryReader, n_rows: int) -> list[str]:
        if n_rows == 0:
            return []
        rows, new_pos = _fast_read.decode_strings(reader._buf, reader._pos, n_rows)
        reader._pos = new_pos
        return rows

    def write(self, writer: BinaryWriter, values: Sequence[str]) -> None:
        # Hot path: build the whole column body in a local bytearray
        # via inline UTF-8 encode + varuint emit, then `write_raw` the
        # whole thing in one call. Avoids per-row method overhead
        # (`write_string` → `write_varuint` → bytearray.append) which
        # dominated the 100k-row insert benchmark.
        if not values:
            return
        out = bytearray()
        extend = out.extend
        append = out.append
        cont = _VARUINT_CONTINUATION_BIT
        for v in values:
            data = v.encode("utf-8")
            n = len(data)
            # Inline LEB128 varuint for the length prefix.
            while n >= cont:
                append((n & 0x7F) | cont)
                n >>= 7
            append(n)
            extend(data)
        writer.write_raw(bytes(out))


class FixedString:
    null_value: bytes
    python_type: type = bytes

    def __init__(self, length: int) -> None:
        if length <= 0:
            raise ValueError(f"FixedString length must be positive, got {length}")
        self.length = length
        self.name = f"FixedString({length})"
        self.null_value = b"\x00" * length

    def read(self, reader: SyncBinaryReader, n_rows: int) -> list[bytes]:
        if n_rows == 0:
            return []
        n = self.length
        data = reader.read_exact(n * n_rows)
        return [bytes(data[i * n : (i + 1) * n]) for i in range(n_rows)]

    def write(self, writer: BinaryWriter, values: Sequence[bytes]) -> None:
        n = self.length
        out = bytearray()
        for v in values:
            if len(v) > n:
                raise ValueError(
                    f"FixedString({n}) value of length {len(v)} exceeds capacity"
                )
            out.extend(v)
            if len(v) < n:
                out.extend(b"\x00" * (n - len(v)))
        writer.write_raw(bytes(out))
