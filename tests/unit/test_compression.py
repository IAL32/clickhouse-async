"""Compression layer tests.

Covers:
- Bare-install discipline: importing the compression module never
  raises, even with all compression libs absent. The error only fires
  when a codec method is actually invoked.
- `MissingExtraError` paths via `monkeypatch.setitem` on
  `sys.modules` so we can exercise the failure mode without
  uninstalling anything.
- Round-trip + multi-block round-trip per method (LZ4, ZSTD, NONE)
  via `pytest.importorskip` so the tests skip on bare installs.
- End-to-end `Connection` wiring: the query packet's compression
  flag flips, and `send_data` / `iter_packets` round-trip blocks
  through the framed format when compression is on.
"""

from __future__ import annotations

import sys
from importlib import reload
from typing import TYPE_CHECKING

import pytest

from clickhouse_async.connection import Connection, State
from clickhouse_async.errors import MissingExtraError, ProtocolError
from clickhouse_async.protocol import compression as compression_module
from clickhouse_async.protocol.block import (
    Block,
    BlockInfo,
    make_column,
    read_block,
    write_block,
)
from clickhouse_async.protocol.compression import (
    CompressedBlockReader,
    CompressedBlockWriter,
    CompressionMethod,
)
from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.protocol.io_sync import SyncBinaryReader
from clickhouse_async.protocol.packets import (
    OUR_REVISION,
    ClientPacket,
    ServerPacket,
)

from ._mock_transport import ScriptedTransport
from ._scripted_packets import encode_server_end_of_stream, encode_server_hello

if TYPE_CHECKING:
    from collections.abc import Iterator


def _reader_over(data: bytes) -> SyncBinaryReader:
    return SyncBinaryReader(bytes(data))


def _async_reader_over(data: bytes) -> AsyncBinaryReader:
    return AsyncBinaryReader.from_bytes(bytes(data))


# ---- bare-install discipline -------------------------------------------


def test_importing_compression_module_does_not_pull_extras() -> None:
    # BEGIN: a fresh module reload to drop any cached optional imports
    reload(compression_module)

    # WHEN / THEN: the module's own namespace must not contain the
    #              optional libs — they live behind lazy `_require`
    #              calls that fire only when a codec method is invoked
    assert "lz4" not in compression_module.__dict__
    assert "zstandard" not in compression_module.__dict__
    assert "clickhouse_cityhash" not in compression_module.__dict__


def test_method_enum_carries_documented_byte_values() -> None:
    # BEGIN / WHEN / THEN: the upstream method byte for each codec
    assert CompressionMethod.NONE == 0x02
    assert CompressionMethod.LZ4 == 0x82
    assert CompressionMethod.ZSTD == 0x90


# ---- MissingExtraError on lazy import failure --------------------------


@pytest.fixture
def _isolated_modules(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Drop every compression-related module from sys.modules so the
    next `import_module` call inside `_require` actually re-runs
    against whatever `sys.modules` looks like."""
    for name in [
        "lz4",
        "lz4.block",
        "zstandard",
        "clickhouse_cityhash",
        "clickhouse_cityhash.cityhash",
    ]:
        monkeypatch.delitem(sys.modules, name, raising=False)
    yield


def test_missing_lz4_raises_named_extra(
    monkeypatch: pytest.MonkeyPatch, _isolated_modules: None
) -> None:
    # BEGIN: simulate lz4 being uninstalled by poisoning sys.modules
    monkeypatch.setitem(sys.modules, "lz4", None)
    monkeypatch.setitem(sys.modules, "lz4.block", None)

    # WHEN: trying to compress through the LZ4 path
    # THEN: MissingExtraError fires naming the extra and the pip command
    writer = BinaryWriter()
    cw = CompressedBlockWriter(writer, method=CompressionMethod.LZ4)
    with pytest.raises(MissingExtraError) as exc_info:
        cw.write_payload(b"hello")
    msg = str(exc_info.value)
    assert "lz4" in msg
    assert "pip install clickhouse-async[lz4]" in msg


def test_missing_zstd_raises_named_extra(
    monkeypatch: pytest.MonkeyPatch, _isolated_modules: None
) -> None:
    # BEGIN: simulate zstandard being uninstalled
    monkeypatch.setitem(sys.modules, "zstandard", None)

    # WHEN: trying to compress through the ZSTD path
    # THEN: MissingExtraError fires naming the extra
    writer = BinaryWriter()
    cw = CompressedBlockWriter(writer, method=CompressionMethod.ZSTD)
    with pytest.raises(MissingExtraError) as exc_info:
        cw.write_payload(b"hello")
    msg = str(exc_info.value)
    assert "zstandard" in msg
    assert "pip install clickhouse-async[zstd]" in msg


def test_missing_cityhash_raises_named_extra(
    monkeypatch: pytest.MonkeyPatch, _isolated_modules: None
) -> None:
    # BEGIN: simulate clickhouse_cityhash being uninstalled
    monkeypatch.setitem(sys.modules, "clickhouse_cityhash", None)
    monkeypatch.setitem(sys.modules, "clickhouse_cityhash.cityhash", None)

    # WHEN: trying to write a NONE-method frame (still requires the hash)
    # THEN: MissingExtraError surfaces — the hash is part of the framing
    #       and is required for every method including NONE
    writer = BinaryWriter()
    cw = CompressedBlockWriter(writer, method=CompressionMethod.NONE)
    with pytest.raises(MissingExtraError) as exc_info:
        cw.write_payload(b"hello")
    msg = str(exc_info.value)
    assert "clickhouse_cityhash" in msg
    assert "pip install clickhouse-async[compression]" in msg


# ---- Round-trip per method (skipped on bare installs) ------------------


def _has_extras(*modules: str) -> bool:
    for m in modules:
        try:
            __import__(m)
        except ImportError:
            return False
    return True


_LZ4_AVAILABLE = _has_extras("lz4.block", "clickhouse_cityhash.cityhash")
_ZSTD_AVAILABLE = _has_extras("zstandard", "clickhouse_cityhash.cityhash")
_HASH_AVAILABLE = _has_extras("clickhouse_cityhash.cityhash")


@pytest.mark.parametrize(
    "method",
    [
        pytest.param(
            CompressionMethod.LZ4,
            marks=pytest.mark.skipif(not _LZ4_AVAILABLE, reason="requires [lz4] extra"),
        ),
        pytest.param(
            CompressionMethod.ZSTD,
            marks=pytest.mark.skipif(
                not _ZSTD_AVAILABLE, reason="requires [zstd] extra"
            ),
        ),
        pytest.param(
            CompressionMethod.NONE,
            marks=pytest.mark.skipif(
                not _HASH_AVAILABLE, reason="requires [compression] extra"
            ),
        ),
    ],
)
async def test_compressed_frame_round_trip(method: CompressionMethod) -> None:
    # BEGIN: a writer + a payload large enough to exercise the codec
    payload = b"the quick brown fox jumps over the lazy dog. " * 50

    # WHEN: framing the payload and reading it back through the reader
    writer = BinaryWriter()
    CompressedBlockWriter(writer, method=method).write_payload(payload)
    reader = CompressedBlockReader(_async_reader_over(writer.getvalue()))
    decoded = await reader.read_payload()

    # THEN: every byte round-trips
    assert decoded == payload


@pytest.mark.skipif(not _LZ4_AVAILABLE, reason="requires [lz4] extra")
async def test_multi_frame_round_trip_over_one_megabyte() -> None:
    # BEGIN: three back-to-back compressed frames totalling > 1 MiB
    payloads = [
        b"a" * (400 * 1024),
        b"".join(bytes((i % 256,)) for i in range(400 * 1024)),
        b"alpha-beta-gamma " * (300 * 1024 // len("alpha-beta-gamma ")),
    ]

    # WHEN: writing each frame in turn and reading them back in order
    writer = BinaryWriter()
    for p in payloads:
        CompressedBlockWriter(writer, method=CompressionMethod.LZ4).write_payload(p)
    rdr = _async_reader_over(writer.getvalue())
    decoded = [await CompressedBlockReader(rdr).read_payload() for _ in payloads]

    # THEN: each frame's CityHash128 verifies and the payloads round-trip
    assert decoded == payloads


@pytest.mark.skipif(not _HASH_AVAILABLE, reason="requires [compression] extra")
async def test_corrupted_frame_raises_protocol_error_with_hex_diagnostic() -> None:
    # BEGIN: a freshly-framed payload that we then mutate to break the hash
    writer = BinaryWriter()
    CompressedBlockWriter(writer, method=CompressionMethod.NONE).write_payload(
        b"hello world"
    )
    framed = bytearray(writer.getvalue())
    # Flip a byte inside the compressed body (after the 16-byte hash + header)
    framed[-1] ^= 0x01
    rdr = _async_reader_over(bytes(framed))

    # WHEN: reading the frame
    # THEN: CityHash mismatch surfaces as a ProtocolError naming both
    #       the expected and actual hashes in hex
    with pytest.raises(ProtocolError, match="CityHash128 mismatch"):
        await CompressedBlockReader(rdr).read_payload()


# ---- Connection-level wiring ------------------------------------------


@pytest.mark.skipif(not _LZ4_AVAILABLE, reason="requires [lz4] extra")
async def test_send_query_compression_flag_flips_when_lz4_enabled() -> None:
    # BEGIN: a connection opened with LZ4 compression
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    conn = Connection(
        [("h", 9000)], compression=CompressionMethod.LZ4, transport_factory=transport
    )
    await conn.open()

    # WHEN: sending a query
    await conn.send_query("SELECT 1")

    # THEN: walking the Query packet in the documented field order, the
    #       compression flag is 1 (sits right after the query stage)
    rdr = _reader_over(transport.written())
    # Drain Hello packet
    assert rdr.read_varuint() == ClientPacket.HELLO
    rdr.read_string()  # client name
    rdr.read_varuint()  # version major
    rdr.read_varuint()  # version minor
    rdr.read_varuint()  # revision
    rdr.read_string()  # database
    rdr.read_string()  # user
    rdr.read_string()  # password
    rdr.read_string()  # addendum: quota_key (empty)
    rdr.read_string()  # addendum: proto_send_chunked
    rdr.read_string()  # addendum: proto_recv_chunked
    rdr.read_varuint()  # addendum: parallel_replicas_protocol_version
    # Drain Query packet up to the compression flag
    assert rdr.read_varuint() == ClientPacket.QUERY
    rdr.read_string()  # query_id
    # ClientInfo block at OUR_REVISION
    rdr.read_byte()  # query_kind
    rdr.read_string()  # initial_user
    rdr.read_string()  # initial_query_id
    rdr.read_string()  # initial_address
    rdr.read_int(8, signed=True)  # initial_query_start_time
    rdr.read_byte()  # interface = TCP
    rdr.read_string()  # os_user
    rdr.read_string()  # hostname
    rdr.read_string()  # client_name
    rdr.read_varuint()  # client version major
    rdr.read_varuint()  # client version minor
    rdr.read_varuint()  # revision
    rdr.read_string()  # quota_key
    rdr.read_varuint()  # distributed_depth
    rdr.read_varuint()  # client_version_patch
    rdr.read_byte()  # has_otel
    rdr.read_varuint()  # parallel_replicas: collaborate
    rdr.read_varuint()  # parallel_replicas: count
    rdr.read_varuint()  # parallel_replicas: replica idx
    rdr.read_varuint()  # script_query_number
    rdr.read_varuint()  # script_line_number
    rdr.read_byte()  # have_jwt
    rdr.read_string()  # settings terminator
    rdr.read_string()  # extra_roles (empty for non-interserver)
    rdr.read_string()  # interserver secret
    rdr.read_varuint()  # stage
    assert rdr.read_varuint() == 1  # compression flag flipped on


@pytest.mark.skipif(not _LZ4_AVAILABLE, reason="requires [lz4] extra")
async def test_send_data_round_trips_compressed_block_through_loopback() -> None:
    # BEGIN: a connection past handshake with LZ4 compression
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    conn = Connection(
        [("h", 9000)], compression=CompressionMethod.LZ4, transport_factory=transport
    )
    await conn.open()
    await conn.send_query("INSERT INTO t VALUES")
    pre = len(transport.written())

    # WHEN: sending a non-trivial block via send_data
    spec, vals = make_column("id", "Int32", list(range(1, 1001)))
    block = Block(info=BlockInfo(), columns=[spec], n_rows=1000, data=[vals])
    await conn.send_data(block)
    captured = transport.written()[pre:]

    # THEN: parsing the captured bytes — Data packet id, empty name,
    #       then a compressed frame that decodes to the original block
    rdr = _reader_over(captured)
    assert rdr.read_varuint() == ClientPacket.DATA
    assert rdr.read_string() == ""
    async_rdr = AsyncBinaryReader.from_bytes(bytes(captured)[rdr.position :])
    payload = await CompressedBlockReader(async_rdr).read_payload()
    decoded = read_block(_reader_over(payload), revision=conn.negotiated_revision)
    assert decoded.n_rows == 1000
    assert decoded.data[0][:5] == [1, 2, 3, 4, 5]


@pytest.mark.skipif(not _LZ4_AVAILABLE, reason="requires [lz4] extra")
async def test_iter_packets_decodes_compressed_data_block() -> None:
    # BEGIN: a connection with LZ4 on; a server-emitted Data packet
    #        with a compressed block + EndOfStream queued
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    conn = Connection(
        [("h", 9000)], compression=CompressionMethod.LZ4, transport_factory=transport
    )
    await conn.open()
    await conn.send_query("SELECT 1")

    spec, vals = make_column("x", "Int32", [10, 20, 30])
    block = Block(info=BlockInfo(), columns=[spec], n_rows=3, data=[vals])
    # Build a compressed Data packet by hand: id + empty name + a
    # compressed frame over the block bytes.
    out = BinaryWriter()
    out.write_varuint(ServerPacket.DATA)
    out.write_string("")
    inner = BinaryWriter()
    write_block(inner, block, revision=OUR_REVISION)
    CompressedBlockWriter(out, method=CompressionMethod.LZ4).write_payload(
        inner.getvalue()
    )
    transport.feed(out.getvalue())
    transport.feed(encode_server_end_of_stream())

    # WHEN: draining
    streamed = [s async for s in conn.iter_packets()]

    # THEN: the compressed Data block decodes correctly via the
    #       compression-aware read path
    assert len(streamed) == 1
    assert streamed[0].kind == "data"
    assert streamed[0].block.data == [[10, 20, 30]]
    assert conn.state == State.READY


# ---- Default behaviour: compression off --------------------------------


async def test_default_connection_uses_no_compression() -> None:
    # BEGIN: a connection opened without specifying a compression method
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    conn = Connection([("h", 9000)], transport_factory=transport)
    await conn.open()

    # WHEN: sending a non-trivial block via send_data after a query
    await conn.send_query("INSERT INTO t VALUES")
    pre = len(transport.written())
    spec, vals = make_column("id", "Int32", [1, 2, 3])
    block = Block(info=BlockInfo(), columns=[spec], n_rows=3, data=[vals])
    await conn.send_data(block)

    # THEN: the bytes after the Data packet header decode as a raw
    #       Block (no 16-byte hash prefix), confirming compression
    #       defaults to NONE on a bare-install connection
    captured = transport.written()[pre:]
    rdr = _reader_over(captured)
    assert rdr.read_varuint() == ClientPacket.DATA
    assert rdr.read_string() == ""
    decoded = read_block(rdr, revision=conn.negotiated_revision)
    assert decoded.n_rows == 3
