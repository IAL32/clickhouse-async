"""Low-level binary I/O for the ClickHouse native protocol.

`AsyncBinaryReader` wraps an `asyncio.StreamReader` with the codec helpers
the rest of the protocol layer reads against, and tracks bytes consumed so
errors can name an offset. `BinaryWriter` builds a packet in an in-memory
`bytearray`; the connection writes the whole buffer in one `transport.write`
to keep packets atomic on the wire.
"""

from __future__ import annotations

import asyncio

from clickhouse_async.errors import ProtocolError

# ceil(64 / 7) — the longest LEB128 unsigned encoding for a u64.
_VARUINT_MAX_BYTES = 10


class AsyncBinaryReader:
    """Async reader over an `asyncio.StreamReader` with ClickHouse codecs."""

    __slots__ = ("_pos", "_stream")

    def __init__(self, stream: asyncio.StreamReader) -> None:
        self._stream = stream
        self._pos = 0

    @classmethod
    def from_bytes(cls, data: bytes) -> AsyncBinaryReader:
        """Wrap an in-memory byte buffer as a reader. Used by the
        compression layer (06h) to feed a decompressed frame into the
        rest of the codec stack without spinning up a real socket."""
        stream = asyncio.StreamReader()
        stream.feed_data(data)
        stream.feed_eof()
        return cls(stream)

    @property
    def position(self) -> int:
        return self._pos

    async def read_exact(self, n: int) -> bytes:
        if n == 0:
            return b""
        try:
            data = await self._stream.readexactly(n)
        except asyncio.IncompleteReadError as exc:
            raise ProtocolError(
                f"short read at offset {self._pos}: "
                f"wanted {n} bytes, got {len(exc.partial)}"
            ) from exc
        self._pos += n
        return data

    async def read_byte(self) -> int:
        data = await self.read_exact(1)
        return data[0]

    async def read_int(self, width: int, *, signed: bool) -> int:
        data = await self.read_exact(width)
        return int.from_bytes(data, byteorder="little", signed=signed)

    async def read_varuint(self) -> int:
        result = 0
        shift = 0
        for _ in range(_VARUINT_MAX_BYTES):
            b = await self.read_byte()
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                return result
            shift += 7
        raise ProtocolError(
            f"varuint exceeds {_VARUINT_MAX_BYTES} bytes at offset {self._pos}"
        )

    async def read_string(self) -> str:
        n = await self.read_varuint()
        start = self._pos
        data = await self.read_exact(n)
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError(
                f"invalid UTF-8 in length-prefixed string at offset {start}"
            ) from exc

    async def read_bytes(self) -> bytes:
        n = await self.read_varuint()
        return await self.read_exact(n)


class BinaryWriter:
    """Builds a ClickHouse packet in an in-memory bytearray."""

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def __len__(self) -> int:
        return len(self._buf)

    def getvalue(self) -> bytes:
        return bytes(self._buf)

    def write_raw(self, data: bytes) -> None:
        """Append raw bytes with no framing — used by column codecs flushing
        bulk-packed buffers (struct.pack, null masks, etc.)."""
        self._buf.extend(data)

    def write_byte(self, b: int) -> None:
        self._buf.append(b)

    def write_int(self, value: int, width: int, *, signed: bool) -> None:
        self._buf.extend(value.to_bytes(width, byteorder="little", signed=signed))

    def write_varuint(self, value: int) -> None:
        if value < 0:
            raise ValueError(f"varuint cannot be negative: {value}")
        while value >= 0x80:
            self._buf.append((value & 0x7F) | 0x80)
            value >>= 7
        self._buf.append(value)

    def write_string(self, s: str) -> None:
        data = s.encode("utf-8")
        self.write_varuint(len(data))
        self._buf.extend(data)

    def write_bytes(self, data: bytes) -> None:
        self.write_varuint(len(data))
        self._buf.extend(data)
