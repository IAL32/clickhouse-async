"""LZ4 / ZSTD per-block compressed framing.

Wire layout, per upstream `Compression/CompressedReadBuffer.cpp` and
`CompressedWriteBuffer.cpp`:

| Bytes  | Field                                                       |
| ------ | ----------------------------------------------------------- |
| 16     | CityHash128 over the rest of the frame                      |
|  1     | method byte (`0x82` LZ4, `0x90` ZSTD, `0x02` NONE)    |
|  4     | `compressed_size` UInt32 LE — total framed bytes from the |
|        | method byte onward (so 1 + 4 + 4 + len(compressed_payload)) |
|  4     | `decompressed_size` UInt32 LE                             |
|  …     | compressed payload                                          |

The CityHash128 is computed over the bytes starting at the method byte.
The method byte `NONE` (`0x02`) is a passthrough — the payload is
the bytes verbatim, framed and checksummed but not compressed. Useful
for blocks the server decides not to compress per-frame.

**Optional dependencies.** The compression libraries (`lz4`,
`zstandard`) and the `clickhouse_cityhash` binding are extras —
the bare install must remain import-clean. Imports happen lazily, on
first use of each codec or hash; importing this module on a bare
install never raises. `MissingExtraError` surfaces with the exact
`pip install clickhouse-async[<extra>]` command if the matching
library isn't installed.
"""

from __future__ import annotations

import importlib
from enum import IntEnum
from typing import TYPE_CHECKING

from clickhouse_async.errors import MissingExtraError, ProtocolError
from clickhouse_async.protocol.block import Block, read_block, write_block
from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.protocol.io_sync import BufferUnderflow, SyncBinaryReader

if TYPE_CHECKING:
    from types import ModuleType


class CompressionMethod(IntEnum):
    """Compression method byte that travels with each compressed frame."""

    NONE = 0x02
    LZ4 = 0x82
    ZSTD = 0x90


_HASH_BYTES = 16
_HEADER_BYTES = 1 + 4 + 4  # method + compressed_size + decompressed_size


def _require(extra: str, module: str) -> ModuleType:
    """Lazy-import `module`, surfacing a documented
    `MissingExtraError` if the matching extra wasn't installed."""
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        raise MissingExtraError(
            f"{module!r} is required for {extra} support and is not "
            f"installed. Install with: pip install clickhouse-async[{extra}]"
        ) from exc


def _cityhash_128(
    data: bytes,
) -> bytes:  # pragma: no cover — requires clickhouse-cityhash extra
    """CityHash128 over `data`, packed as 16 LE bytes (low64 first,
    matching upstream `writeBinaryLittleEndian(checksum.low64);
    writeBinaryLittleEndian(checksum.high64)`).

    `clickhouse_cityhash.CityHash128` returns a 128-bit Python integer
    as `(low64 << 64) | high64` — the opposite of what the name
    suggests — so we extract the halves explicitly rather than using a
    plain `to_bytes` on the whole value.
    """
    mod = _require("compression", "clickhouse_cityhash.cityhash")
    value = int(mod.CityHash128(data))
    # The binding packs (low64 << 64) | high64, so mask accordingly.
    high64 = value & 0xFFFFFFFFFFFFFFFF
    low64 = value >> 64
    return low64.to_bytes(8, "little") + high64.to_bytes(8, "little")


def _compress(method: CompressionMethod, payload: bytes) -> bytes:
    if method == CompressionMethod.LZ4:  # pragma: no cover — requires lz4 extra
        mod = _require("lz4", "lz4.block")
        return mod.compress(payload, store_size=False)
    if method == CompressionMethod.ZSTD:  # pragma: no cover — requires zstd extra
        mod = _require("zstd", "zstandard")
        return mod.ZstdCompressor().compress(payload)
    if method == CompressionMethod.NONE:
        return payload
    raise ValueError(
        f"unsupported compression method: {method!r}"
    )  # pragma: no cover — defensive


def _decompress(
    method: CompressionMethod, payload: bytes, decompressed_size: int
) -> bytes:
    if method == CompressionMethod.LZ4:  # pragma: no cover — requires lz4 extra
        mod = _require("lz4", "lz4.block")
        return mod.decompress(payload, uncompressed_size=decompressed_size)
    if method == CompressionMethod.ZSTD:  # pragma: no cover — requires zstd extra
        mod = _require("zstd", "zstandard")
        return mod.ZstdDecompressor().decompress(
            payload, max_output_size=decompressed_size
        )
    if method == CompressionMethod.NONE:
        return payload
    raise ProtocolError(
        f"unknown compression method byte: 0x{int(method):02x}"
    )  # pragma: no cover — defensive


class CompressedBlockReader:
    """Reads one compressed frame from an `AsyncBinaryReader`,
    verifies the CityHash128 checksum, and returns the decompressed
    payload."""

    __slots__ = ("_reader",)

    def __init__(
        self, reader: AsyncBinaryReader
    ) -> None:  # pragma: no cover — requires cityhash extra
        self._reader = reader

    async def read_payload(self) -> bytes:  # pragma: no cover — requires cityhash extra
        expected_hash = await self._reader.read_exact(_HASH_BYTES)
        method_byte = await self._reader.read_byte()
        compressed_size = await self._reader.read_int(4, signed=False)
        decompressed_size = await self._reader.read_int(4, signed=False)

        if compressed_size < _HEADER_BYTES:
            raise ProtocolError(
                f"compressed_size {compressed_size} is smaller than the "
                f"frame header ({_HEADER_BYTES} bytes)"
            )
        body_size = compressed_size - _HEADER_BYTES
        compressed_payload = await self._reader.read_exact(body_size)

        # CityHash128 covers [method byte | compressed_size | decompressed_size | body]
        framed = (
            bytes((method_byte,))
            + compressed_size.to_bytes(4, "little", signed=False)
            + decompressed_size.to_bytes(4, "little", signed=False)
            + compressed_payload
        )
        actual_hash = _cityhash_128(framed)
        if actual_hash != expected_hash:
            raise ProtocolError(
                f"CityHash128 mismatch in compressed frame at offset "
                f"{self._reader.position}: expected {expected_hash.hex()}, "
                f"got {actual_hash.hex()}"
            )

        try:
            method = CompressionMethod(method_byte)
        except ValueError as exc:
            raise ProtocolError(
                f"unknown compression method byte: 0x{method_byte:02x}"
            ) from exc

        return _decompress(method, compressed_payload, decompressed_size)


class CompressedBlockWriter:
    """Writes one compressed frame to a `BinaryWriter`: compresses the
    payload, builds the header, computes the CityHash128, emits the
    full framed bytes in one `write_raw`."""

    __slots__ = ("_method", "_writer")

    def __init__(self, writer: BinaryWriter, method: CompressionMethod) -> None:
        self._writer = writer
        self._method = method

    def write_payload(
        self, payload: bytes
    ) -> None:  # pragma: no cover — requires lz4/zstd extra
        compressed = _compress(self._method, payload)
        compressed_size = _HEADER_BYTES + len(compressed)
        framed = (
            bytes((self._method.value,))
            + compressed_size.to_bytes(4, "little", signed=False)
            + len(payload).to_bytes(4, "little", signed=False)
            + compressed
        )
        self._writer.write_raw(_cityhash_128(framed) + framed)


# ---- compression-aware Block helpers -----------------------------------


class _MultiFrameReader(AsyncBinaryReader):
    """Wraps a raw `AsyncBinaryReader` and reads compressed frames on
    demand, presenting the concatenated decompressed stream to its
    callers.

    Used for compressed packet bodies that aren't blocks — e.g. the
    `TABLE_COLUMNS` body at revision 54481+, which the server wraps
    in compression frames but isn't a Block (so the sync block-parse
    path doesn't apply). Block reads use `read_block_buffered` instead.
    """

    __slots__ = ("_buf", "_raw")

    def __init__(self, raw: AsyncBinaryReader) -> None:  # pragma: no cover
        self._raw = raw
        self._buf: bytearray = bytearray()
        self._pos = 0
        self._pushback = b""
        self._pushback_pos = 0

    async def read_exact(self, n: int) -> bytes:  # pragma: no cover
        if n == 0:
            return b""
        while len(self._buf) < n:
            frame = await CompressedBlockReader(self._raw).read_payload()
            self._buf.extend(frame)
        data = bytes(self._buf[:n])
        del self._buf[:n]
        self._pos += n
        return data


# Initial drain cap for an uncompressed block read. We block only for
# the first byte and then take whatever the kernel has buffered up to
# this size — too small means many tiny refills, too large risks
# stranding bytes in the asyncio buffer that we'd rather have parsed.
# 1 MiB matches typical ClickHouse `network_compression_method` frame
# sizes and easily covers the default `max_block_size` row counts.
_UNCOMPRESSED_INITIAL_CAP = 1 << 20


async def read_block_buffered(
    reader: AsyncBinaryReader,
    *,
    revision: int,
    compression: CompressionMethod,
    session_timezone: str | None = None,
    json_nested: bool = False,
) -> Block:
    """Read a Block — framed-and-compressed when compression is on,
    raw otherwise. Used for the DATA / TOTALS / EXTREMES packet bodies
    on a connection that negotiated compression.

    Drains bytes off the async reader into an in-memory buffer, then
    hands the buffer to the synchronous `read_block` codec stack via
    `SyncBinaryReader`. On `BufferUnderflow` we top up the buffer
    (one more compressed frame for the framed path; an exact-sized
    refill for the raw path) and re-parse from the start. The block's
    encoded size isn't on the wire, so we can't pre-size the buffer;
    after a successful parse we push any unconsumed bytes back to the
    reader to belong to the next packet.

    `session_timezone` flows through to the inner `read_block` so
    bare `DateTime` codecs in the block's column specs honour the
    Connection's session timezone fallback.

    `json_nested` flows through to configure `JSON` codecs to
    return nested dicts on read.
    """
    buf = bytearray()
    if compression == CompressionMethod.NONE:
        # Drain whatever's currently buffered without over-asking past
        # what the server has emitted — a small one-shot DATA packet
        # would deadlock if we blocked for a fixed minimum. The cap is
        # set high enough that typical large blocks arrive in one read.
        buf.extend(await reader.read_available(_UNCOMPRESSED_INITIAL_CAP))
        refill = _refill_uncompressed
    else:  # pragma: no cover — requires extras
        # Compressed framing is self-describing; drain at least one
        # frame before the first parse attempt.
        buf.extend(await CompressedBlockReader(reader).read_payload())
        refill = _refill_compressed

    while True:
        sync = SyncBinaryReader(bytes(buf))
        try:
            block = read_block(
                sync,
                revision=revision,
                session_timezone=session_timezone,
                json_nested=json_nested,
            )
        except BufferUnderflow as exc:
            await refill(reader, buf, exc.needed)
            continue
        # Hand the bytes our codec didn't consume back to the reader
        # so the next packet sees them at the head of its read queue.
        leftover = bytes(buf[sync.position :])
        if leftover:
            reader.push_back(leftover)
        return block


async def _refill_uncompressed(
    reader: AsyncBinaryReader, buf: bytearray, needed: int
) -> None:
    """Top up `buf` from the raw socket by exactly `needed` bytes —
    the size of the read that just underflowed. Reading any more
    risks blocking past what the server has emitted (a small one-shot
    DATA packet would deadlock waiting for bytes that aren't coming);
    the outer retry loop will call us again if the next parse pass
    needs even more."""
    buf.extend(await reader.read_exact(needed))


async def _refill_compressed(
    reader: AsyncBinaryReader,
    buf: bytearray,
    needed: int,  # kept to match the refill signature; unused here
) -> None:  # pragma: no cover — requires extras
    del needed
    """Top up `buf` by exactly one compressed frame. The `needed`
    argument is unused here: the outer `read_block_buffered` retry
    loop re-parses after each refill and will call us again if more
    bytes are still missing. Looping over multiple frames inside
    this helper would over-drain on the boundary case where one
    frame plus the bytes we *already* have is enough — we'd then
    pull the next packet's raw header and misinterpret it as a
    compressed frame."""
    frame = await CompressedBlockReader(reader).read_payload()
    buf.extend(frame)


def write_block_framed(
    writer: BinaryWriter,
    block: Block,
    *,
    revision: int,
    compression: CompressionMethod,
) -> None:
    """Write a Block — framed-and-compressed when compression is on,
    raw otherwise. Counterpart to `read_block_buffered`."""
    if compression == CompressionMethod.NONE:
        write_block(writer, block, revision=revision)
        return
    inner = BinaryWriter()  # pragma: no cover — requires lz4/zstd extra
    write_block(
        inner, block, revision=revision
    )  # pragma: no cover — requires lz4/zstd extra
    CompressedBlockWriter(writer, method=compression).write_payload(
        inner.getvalue()
    )  # pragma: no cover — requires lz4/zstd extra
