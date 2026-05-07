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
# LEB128 / varuint: bit 7 of each byte is the "continuation" marker;
# the low 7 bits hold the payload.
_VARUINT_CONTINUATION_BIT = 0x80
_VARUINT_PAYLOAD_MASK = 0x7F


class AsyncBinaryReader:
    """Async reader over an `asyncio.StreamReader` with ClickHouse codecs.

    Carries a pushback buffer alongside the stream. The buffered Block
    reader (`compression.read_block_buffered`) drains more bytes than
    the codec layer ends up consuming — the leftover belongs to the
    *next* packet, so it gets pushed back here to be returned by the
    next read in FIFO order.
    """

    __slots__ = ("_pos", "_pushback", "_pushback_pos", "_stream")

    def __init__(self, stream: asyncio.StreamReader) -> None:
        self._stream = stream
        self._pos = 0
        # Pushback uses a (bytes, position) pair so reading off the
        # front is O(1) — slicing the front off a bytearray on every
        # 1-byte varuint read would dominate the hot path.
        self._pushback: bytes = b""
        self._pushback_pos: int = 0

    @classmethod
    def from_bytes(cls, data: bytes) -> AsyncBinaryReader:
        """Wrap an in-memory byte buffer as a reader. Used by tests
        and helpers that feed a literal buffer into the codec stack
        without spinning up a real socket."""
        stream = asyncio.StreamReader()
        stream.feed_data(data)
        stream.feed_eof()
        return cls(stream)

    @property
    def position(self) -> int:
        return self._pos

    def _pushback_remaining(self) -> int:
        return len(self._pushback) - self._pushback_pos

    def push_back(self, data: bytes) -> None:
        """Stash bytes the codec layer drained but didn't consume back
        at the front of the read queue. Subsequent reads pull from
        pushback before the underlying stream."""
        if not data:
            return
        if self._pushback_pos == len(self._pushback):
            self._pushback = data
            self._pushback_pos = 0
        else:
            self._pushback = data + self._pushback[self._pushback_pos :]
            self._pushback_pos = 0
        self._pos -= len(data)

    async def read_exact(self, n: int) -> bytes:
        if n == 0:
            return b""
        avail = self._pushback_remaining()
        if avail >= n:
            data = self._pushback[self._pushback_pos : self._pushback_pos + n]
            self._pushback_pos += n
            if self._pushback_pos == len(self._pushback):
                self._pushback = b""
                self._pushback_pos = 0
            self._pos += n
            return data
        if avail:
            front = self._pushback[self._pushback_pos :]
            self._pushback = b""
            self._pushback_pos = 0
            try:
                rest = await self._stream.readexactly(n - len(front))
            except asyncio.IncompleteReadError as exc:
                raise ProtocolError(
                    f"short read at offset {self._pos}: "
                    f"wanted {n} bytes, got {len(front) + len(exc.partial)}"
                ) from exc
            self._pos += n
            return front + bytes(rest)
        try:
            data = await self._stream.readexactly(n)
        except asyncio.IncompleteReadError as exc:
            raise ProtocolError(
                f"short read at offset {self._pos}: "
                f"wanted {n} bytes, got {len(exc.partial)}"
            ) from exc
        self._pos += n
        return data

    async def read_available(self, max_n: int) -> bytes:
        """Return up to `max_n` bytes, blocking only when the read
        queue is completely empty.

        Sized for the buffered Block reader's initial drain: it wants
        whatever the server has already pushed but mustn't block on
        more than the protocol has emitted (a small DATA packet may be
        the entire payload). Pushback is consumed first; otherwise a
        single `StreamReader.read(max_n)` returns whatever's currently
        buffered (or blocks for the first byte)."""
        if max_n == 0:
            return b""
        avail = self._pushback_remaining()
        if avail:
            take = min(avail, max_n)
            data = self._pushback[self._pushback_pos : self._pushback_pos + take]
            self._pushback_pos += take
            if self._pushback_pos == len(self._pushback):
                self._pushback = b""
                self._pushback_pos = 0
            self._pos += take
            return data
        chunk = await self._stream.read(max_n)
        self._pos += len(chunk)
        return chunk

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

    __slots__ = ("_buf", "revision")

    def __init__(self, revision: int = 0) -> None:
        self._buf = bytearray()
        self.revision = revision

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
        while value >= _VARUINT_CONTINUATION_BIT:
            self._buf.append(
                (value & _VARUINT_PAYLOAD_MASK) | _VARUINT_CONTINUATION_BIT
            )
            value >>= 7
        self._buf.append(value)

    def write_string(self, s: str) -> None:
        data = s.encode("utf-8")
        self.write_varuint(len(data))
        self._buf.extend(data)

    def write_bytes(self, data: bytes) -> None:
        self.write_varuint(len(data))
        self._buf.extend(data)
