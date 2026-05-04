"""Tests for ``Client`` resilience under connection-death scenarios.

Connections die for many reasons beyond the server sending a clean
``Exception`` packet.  These tests classify the causes and verify that
the client surfaces the right exception, sets ``is_alive = False``, and
allows ``close()`` to complete without raising.

Causes covered
--------------
1.  **Server idle / receive timeout** — server closes its write end
    after the client is silent too long.  Manifests as EOF on the next
    read.
2.  **TCP reset (RST)** — abrupt peer closure: load-balancer timeout,
    NAT session expiry, server OOM-kill, Docker network restart, NIC
    flap.  Manifests as ``ConnectionResetError`` on the next write or
    ``OSError`` / EOF on the next read (asyncio wraps it).
3.  **Truncated packet body** — server writes a valid packet header but
    crashes before the full body arrives.  The ``AsyncBinaryReader``
    raises ``ProtocolError`` inside the body read.
4.  **Write-buffer overflow / slow peer** — the OS send-buffer fills;
    ``drain()`` raises ``BrokenPipeError`` / ``ConnectionResetError``.
    Can happen mid-``send_query`` or mid-``send_data`` (INSERT stream).
5.  **Firewall / NAT transparent drop** — the TCP session is silently
    terminated by a stateful firewall.  The client sees the connection
    as live until the next IO attempt, which then gets a TCP RST.
6.  **Half-open connection** — the server's read end is gone but TCP
    state hasn't propagated.  The client's next write raises
    ``BrokenPipeError``; the next read hangs then RST/EOF arrives.
7.  **Server memory-limit exceeded mid-query** — server kills the query
    executor thread and drops the TCP connection instead of sending an
    ``Exception`` packet.  The client sees EOF mid-result.
8.  **Malformed / corrupt bytes on the wire** — in-transit data
    corruption (rare) results in ``ProtocolError`` inside the packet
    dispatcher (the unknown-packet-id branch or inside a packet body).

For each scenario we assert:
  * A ``ClickHouseError`` (``ProtocolError``) or transport error
    propagates to the caller — the error is never silently swallowed.
  * ``client.is_alive`` is ``False`` immediately after the failure.
  * ``await client.close()`` does not raise.
"""

from __future__ import annotations

import pytest

from clickhouse_async import connect
from clickhouse_async.errors import ProtocolError
from clickhouse_async.protocol.block import Block, BlockInfo, make_column
from clickhouse_async.protocol.io import BinaryWriter
from clickhouse_async.protocol.packets import ServerPacket

from ._mock_transport import ScriptedTransport
from ._scripted_packets import (
    encode_server_data,
    encode_server_hello,
    encode_server_progress,
)

# ---------------------------------------------------------------------------
# Transport helpers
# ---------------------------------------------------------------------------


def _header_block() -> Block:
    spec, _ = make_column("n", "Int32", [])
    return Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])


def _data_block(values: list[int]) -> Block:
    spec, _ = make_column("n", "Int32", [])
    return Block(info=BlockInfo(), columns=[spec], n_rows=len(values), data=[values])


# ---------------------------------------------------------------------------
# 1. Server idle timeout / connection closed by server (EOF mid-query)
# ---------------------------------------------------------------------------


async def test_execute_raises_and_marks_broken_on_eof_before_any_packet() -> None:
    # BEGIN: server closes the connection immediately after the handshake
    #        (simulates idle timeout or process crash before sending results)
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())

    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        transport.feed_eof()

        # WHEN: the client tries to execute a query
        # THEN: ProtocolError is raised (the read hit EOF)
        with pytest.raises(ProtocolError):
            await client.execute("SELECT n FROM t")

        # THEN: the client reports itself as dead
        assert not client.is_alive

        # THEN: explicit close does not raise
        await client.close()


async def test_execute_raises_and_marks_broken_on_eof_mid_result() -> None:
    # BEGIN: server sends the header block then drops the connection
    #        (simulates server crash mid-query / memory-limit exceeded)
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    transport.feed(encode_server_data(_header_block()))
    # No EndOfStream — the server goes away here

    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        transport.feed_eof()

        # WHEN: the client drains a query whose results are cut off
        # THEN: ProtocolError is raised
        with pytest.raises(ProtocolError):
            await client.execute("SELECT n FROM t")

        # THEN: connection is not alive
        assert not client.is_alive


async def test_iter_blocks_raises_and_marks_broken_on_eof_mid_stream() -> None:
    # BEGIN: server sends one data block then drops the connection
    #        (simulates server OOM-kill or load-balancer timeout mid-stream)
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    transport.feed(encode_server_data(_header_block()))
    transport.feed(encode_server_data(_data_block([1, 2, 3])))
    # Connection drops before the next packet

    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        blocks = []
        # WHEN: iterating and the server drops the connection after one block
        # THEN: ProtocolError is raised (not silently swallowed by the
        #       cancel-on-break finally clause)
        with pytest.raises(ProtocolError):
            async for block in client.iter_blocks("SELECT n FROM t"):
                blocks.append(block)
                if len(blocks) == 1:
                    transport.feed_eof()

        # THEN: the first block was received before the failure
        assert len(blocks) == 1

        # THEN: connection is not alive
        assert not client.is_alive


# ---------------------------------------------------------------------------
# 2. TCP reset / write failure (ConnectionResetError on drain)
# ---------------------------------------------------------------------------


async def test_execute_raises_and_marks_broken_on_write_failure() -> None:
    # BEGIN: the server (or an intermediary) resets the TCP connection
    #        right before the client writes its query.  Simulated by
    #        closing the ScriptedWriter before send_query.
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())

    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        # Close the writer so the next write() call raises ConnectionResetError
        transport._writer.close()  # type: ignore[attr-defined]

        # WHEN: the client tries to write a query
        # THEN: ConnectionResetError is raised (the write hit a dead socket)
        with pytest.raises(ConnectionResetError):
            await client.execute("SELECT n FROM t")

        # THEN: the client reports itself as dead
        assert not client.is_alive

        # THEN: explicit close does not raise
        await client.close()


async def test_insert_raises_and_marks_broken_on_write_failure_during_data() -> None:
    # BEGIN: server accepts the INSERT header handshake but then the TCP
    #        connection resets while the client is streaming data blocks
    #        (simulates NAT timeout, router restart, or server OOM mid-INSERT)
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    # Server responds to INSERT with a header block (column definitions)
    transport.feed(encode_server_data(_header_block()))

    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:

        def _rows() -> list[tuple[int]]:
            # Emit the first row, then kill the writer to simulate reset
            transport._writer.close()  # type: ignore[attr-defined]
            return [(42,)]

        # WHEN: the INSERT data write fails
        # THEN: ConnectionResetError (or BrokenPipeError) propagates
        with pytest.raises((ConnectionResetError, BrokenPipeError, OSError)):
            await client.insert(
                "INSERT INTO t VALUES",
                rows=_rows(),
                column_names=["n"],
            )

        # THEN: connection is dead
        assert not client.is_alive


# ---------------------------------------------------------------------------
# 3. Truncated packet body (server crashes after writing packet header)
# ---------------------------------------------------------------------------


async def test_execute_raises_on_truncated_packet_body() -> None:
    # BEGIN: server writes a DATA packet header (varuint id = 1) but only
    #        sends 3 bytes of the block body, then drops the connection.
    #        Simulates corruption or a server that crashes mid-serialise.
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())

    # Queue the DATA packet id then only 3 bytes of body (not a valid block)
    w = BinaryWriter()
    w.write_varuint(ServerPacket.DATA)
    transport.feed(w.getvalue())
    transport.feed(b"\x00\x01\x02")  # truncated block body
    transport.feed_eof()

    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        # WHEN: the packet body read raises ProtocolError
        with pytest.raises(ProtocolError):
            await client.execute("SELECT n FROM t")

        # THEN: connection is BROKEN
        assert not client.is_alive

        # THEN: close does not raise
        await client.close()


# ---------------------------------------------------------------------------
# 4. Corrupt packet id (wire corruption / router corruption)
# ---------------------------------------------------------------------------


async def test_execute_raises_on_unknown_packet_id() -> None:
    # BEGIN: in-transit bit-flip changes the packet id to a value the
    #        client doesn't recognise (0xFF = 255, not a valid packet).
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())

    w = BinaryWriter()
    w.write_varuint(0xFF)  # unknown packet id
    transport.feed(w.getvalue())

    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        # WHEN: the dispatcher hits the unknown-id branch
        with pytest.raises(ProtocolError, match="unexpected packet id"):
            await client.execute("SELECT 1")

        # THEN: connection is BROKEN
        assert not client.is_alive

        # THEN: close does not raise
        await client.close()


# ---------------------------------------------------------------------------
# 5. Ping against a dead connection (firewall transparent drop / half-open)
# ---------------------------------------------------------------------------


async def test_ping_raises_and_marks_broken_on_eof() -> None:
    # BEGIN: the connection appears alive (READY) but the server has
    #        silently gone away — typical of a firewall that drops idle
    #        TCP sessions without sending RST.  The client discovers this
    #        on the next Ping (health-check path used by the pool).
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())

    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        # Simulate the peer vanishing silently
        transport.feed_eof()

        # WHEN: the pool (or caller) pings to verify liveness
        # THEN: ProtocolError is raised
        with pytest.raises(ProtocolError):
            await client.ping()

        # THEN: connection is BROKEN (not silently left as READY)
        assert not client.is_alive

        # THEN: close is safe
        await client.close()


async def test_ping_raises_and_marks_broken_on_write_failure() -> None:
    # BEGIN: TCP connection is half-open — the server's receive buffer
    #        is gone, so the first write attempt gets BrokenPipeError.
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())

    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        transport._writer.close()  # type: ignore[attr-defined]

        # WHEN: ping cannot write the Ping packet
        with pytest.raises((ConnectionResetError, BrokenPipeError, OSError)):
            await client.ping()

        # THEN: connection is BROKEN
        assert not client.is_alive


# ---------------------------------------------------------------------------
# 6. Connection recovers: error is isolated to one query, close still works
# ---------------------------------------------------------------------------


async def test_close_after_broken_connection_does_not_raise() -> None:
    # BEGIN: a series of different fatal errors; each time we want to
    #        confirm ``close()`` completes without a secondary exception.
    for _, setup in [
        ("eof_before_query", lambda t: t.feed_eof()),
        (
            "eof_mid_result",
            lambda t: (t.feed(encode_server_data(_header_block())), t.feed_eof()),
        ),  # type: ignore[func-returns-value]
    ]:
        transport = ScriptedTransport()
        transport.feed(encode_server_hello())

        async with connect(
            "clickhouse://default:@host/db", transport_factory=transport
        ) as client:
            setup(transport)
            with pytest.raises(ProtocolError):
                await client.execute("SELECT 1")

            # WHEN: calling close after a fatal error
            # THEN: no secondary exception
            await client.close()  # must not raise


# ---------------------------------------------------------------------------
# 7. Partial progress then EOF (server crash after partial result delivery)
# ---------------------------------------------------------------------------


async def test_execute_raises_after_partial_progress_and_eof() -> None:
    # BEGIN: server emits progress updates (so the connection is clearly
    #        in IN_FLIGHT with packets flowing) then drops the connection
    #        before EndOfStream.  Simulates server memory-limit abort or
    #        network partition after partial delivery.
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    transport.feed(encode_server_data(_header_block()))
    transport.feed(encode_server_progress(read_rows=100, read_bytes=4096))
    transport.feed(encode_server_data(_data_block([1, 2, 3])))
    transport.feed(encode_server_progress(read_rows=200, read_bytes=8192))
    # Connection drops here — no EndOfStream

    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        transport.feed_eof()

        # WHEN: execute drains the partial result then hits EOF
        with pytest.raises(ProtocolError):
            await client.execute("SELECT n FROM big_table")

        # THEN: connection is not alive
        assert not client.is_alive
