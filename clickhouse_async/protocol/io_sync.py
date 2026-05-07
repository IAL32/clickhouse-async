"""Synchronous binary reader + the underflow sentinel codecs raise.

The codec layer is synchronous: codecs read from a buffer that's
already in memory. The only async work the protocol layer does
happens at the **transport** boundary — pulling bytes off the
socket and (for compressed connections) decompressing frames into
the buffer the codec sees. Avoiding an `await` on every codec
primitive keeps per-row overhead off the hot read path.

`SyncBinaryReader` is the "frozen-bytes" reader codecs consume.
`read_exact` slices the buffer; when there aren't enough bytes,
`BufferUnderflow` is raised and the outer async wrapper handles
the refill + retry (see `protocol/compression.py::read_block_buffered`).

Mirror of `protocol/io.py::AsyncBinaryReader` minus the async; same
method names so a codec written against the sync reader looks
nearly identical to its async predecessor.
"""

from __future__ import annotations

from clickhouse_async.errors import ProtocolError

# Mirrors `protocol/io.py`'s constants — duplicated rather than
# imported across modules to keep the sync path's import chain small
# (no transitive `asyncio`).
_VARUINT_MAX_BYTES = 10
_VARUINT_CONTINUATION_BIT = 0x80


class BufferUnderflow(Exception):  # noqa: N818 — sentinel, not an Error
    """Raised when a sync reader is asked for more bytes than the
    buffer holds. Caught by `read_block_buffered` so it can pull
    another compressed frame / socket chunk and retry the parse.

    Carries `needed` (bytes still wanted) and `available` (bytes
    remaining in the current buffer) so the outer wrapper can size
    the next pull intelligently.
    """

    __slots__ = ("available", "needed")

    def __init__(self, *, needed: int, available: int) -> None:
        super().__init__(
            f"sync reader needs {needed} bytes but only {available} are buffered"
        )
        self.needed = needed
        self.available = available


class SyncBinaryReader:
    """Read codec primitives off an in-memory bytes buffer.

    No I/O. `read_exact` raises `BufferUnderflow` when the buffer is
    short — it's the caller's job to top up and retry.
    """

    __slots__ = ("_buf", "_pos")

    def __init__(self, buf: bytes, pos: int = 0) -> None:
        self._buf = buf
        self._pos = pos

    @property
    def position(self) -> int:
        return self._pos

    def read_exact(self, n: int) -> bytes:
        if n == 0:
            return b""
        end = self._pos + n
        if end > len(self._buf):
            raise BufferUnderflow(needed=n, available=len(self._buf) - self._pos)
        data = self._buf[self._pos : end]
        self._pos = end
        return data

    def read_byte(self) -> int:
        return self.read_exact(1)[0]

    def read_int(self, width: int, *, signed: bool) -> int:
        return int.from_bytes(self.read_exact(width), byteorder="little", signed=signed)

    def read_varuint(self) -> int:
        result = 0
        shift = 0
        for _ in range(_VARUINT_MAX_BYTES):
            b = self.read_byte()
            result |= (b & 0x7F) << shift
            if not (b & _VARUINT_CONTINUATION_BIT):
                return result
            shift += 7
        raise ProtocolError(
            f"varuint exceeds {_VARUINT_MAX_BYTES} bytes at offset {self._pos}"
        )

    def read_string(self) -> str:
        n = self.read_varuint()
        start = self._pos
        data = self.read_exact(n)
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError(
                f"invalid UTF-8 in length-prefixed string at offset {start}"
            ) from exc

    def read_bytes(self) -> bytes:
        n = self.read_varuint()
        return self.read_exact(n)
