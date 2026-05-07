"""Codecs for `String` and `FixedString(N)`.

`String` is varuint-prefixed UTF-8 per row; `FixedString(N)` is
exactly N raw bytes per row, NUL-padded on writes when the input is
shorter and surfaced as `bytes` on reads (FixedString columns are
often binary, not text — returning bytes preserves that).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from clickhouse_async._fast import decode_strings
from clickhouse_async.protocol.io import _VARUINT_CONTINUATION_BIT

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter


class String:
    name = "String"
    null_value: str = ""
    python_type: type = str

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[str]:
        if n_rows == 0:
            return []
        # Wire layout per row is ``[varuint length, body bytes]``. Copy
        # the on-wire bytes verbatim into a single bytearray and hand
        # it to Rust, which parses both varuints and UTF-8 in one tight
        # loop. The alternative shape — passing a parsed lengths list
        # plus a joined body buffer — costs an extra ``b"".join`` over
        # the per-row chunks, which dominates for short-string columns
        # and erases the Rust speedup.
        buf = bytearray()
        for _ in range(n_rows):
            # Read varuint bytewise; each byte goes into ``buf`` so
            # Rust can re-walk the same encoding without a re-encode
            # step on the Python side.
            n = 0
            shift = 0
            while True:
                b = await reader.read_byte()
                buf.append(b)
                n |= (b & 0x7F) << shift
                if not (b & _VARUINT_CONTINUATION_BIT):
                    break
                shift += 7
            buf.extend(await reader.read_exact(n))
        return decode_strings(bytes(buf), n_rows)

    def write(self, writer: BinaryWriter, values: Sequence[str]) -> None:
        for v in values:
            writer.write_string(v)


class FixedString:
    null_value: bytes
    python_type: type = bytes

    def __init__(self, length: int) -> None:
        if length <= 0:
            raise ValueError(f"FixedString length must be positive, got {length}")
        self.length = length
        self.name = f"FixedString({length})"
        self.null_value = b"\x00" * length

    async def read(self, reader: AsyncBinaryReader, n_rows: int) -> list[bytes]:
        if n_rows == 0:
            return []
        n = self.length
        data = await reader.read_exact(n * n_rows)
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
