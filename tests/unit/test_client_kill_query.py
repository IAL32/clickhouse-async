"""Tests for ``Client.kill_query`` and ``Pool.kill_query``.

The kill_query call issues ``KILL QUERY WHERE query_id = {qid:String}``
(with optional ``SYNC``) on a side-channel connection. Tests assert
the SQL hits the wire correctly, validation rejects garbage input,
and the busy-connection fallback opens a fresh client.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

import clickhouse_async as ch
from clickhouse_async import connect
from clickhouse_async.protocol.block import Block, BlockInfo, make_column
from clickhouse_async.protocol.io import AsyncBinaryReader
from clickhouse_async.protocol.packets import ClientPacket

from ._mock_transport import ScriptedTransport
from ._scripted_packets import (
    encode_server_data,
    encode_server_end_of_stream,
    encode_server_hello,
)

if TYPE_CHECKING:
    import ssl

    from clickhouse_async.connection import _WriterLike


def _reader_over(data: bytes) -> AsyncBinaryReader:
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return AsyncBinaryReader(stream)


def _kill_query_response(transport: ScriptedTransport, *, killed: int) -> None:
    """Queue the typical ``KILL QUERY`` response shape: header block +
    one data block with ``killed`` rows + EndOfStream.

    We don't care about the column values for the SQL-on-the-wire
    test; ``Client.execute`` returns ``row_count`` based on the block
    row count.
    """
    spec, _ = make_column("query_id", "String", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    transport.feed(encode_server_data(header))
    if killed > 0:
        data = Block(
            info=BlockInfo(),
            columns=[spec],
            n_rows=killed,
            data=[[f"q-{i}" for i in range(killed)]],
        )
        transport.feed(encode_server_data(data))
    transport.feed(encode_server_end_of_stream())


# ---- input validation ---------------------------------------------------


async def test_kill_query_rejects_empty_string() -> None:
    # BEGIN / WHEN / THEN: empty query_id raises ValueError before any
    #                     bytes hit the wire
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        pre = len(transport.written())
        with pytest.raises(ValueError, match="non-empty"):
            await client.kill_query("")
        assert len(transport.written()) == pre


async def test_kill_query_rejects_whitespace_only() -> None:
    # BEGIN / WHEN / THEN: whitespace-only query_id is also rejected
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        pre = len(transport.written())
        with pytest.raises(ValueError, match="non-empty"):
            await client.kill_query("   \t\n")
        assert len(transport.written()) == pre


# ---- SQL on the wire ----------------------------------------------------


async def test_kill_query_emits_sync_sql_by_default() -> None:
    # BEGIN: a connected client + a scripted KILL QUERY response
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _kill_query_response(transport, killed=1)

    # WHEN: kill_query("q-123") — defaults to sync=True
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        n = await client.kill_query("q-123")

    # THEN: the wire carries SQL ending in " SYNC", and the parameters
    #       block holds the quoted query_id under name "qid"
    sql, params = await _decode_query_sql_and_params_async(
        _reader_over(transport.written())
    )
    assert sql == "KILL QUERY WHERE query_id = {qid:String} SYNC"
    assert params == {"qid": "'q-123'"}
    # And the row count from the response surfaces
    assert n == 1


async def test_kill_query_drops_sync_when_sync_false() -> None:
    # BEGIN: a connected client + a scripted KILL QUERY response
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    _kill_query_response(transport, killed=0)

    # WHEN: kill_query with sync=False
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        n = await client.kill_query("q-456", sync=False)

    # THEN: the wire SQL has no trailing " SYNC"; parameters unchanged
    sql, params = await _decode_query_sql_and_params_async(
        _reader_over(transport.written())
    )
    assert sql == "KILL QUERY WHERE query_id = {qid:String}"
    assert params == {"qid": "'q-456'"}
    assert n == 0


# ---- busy-connection fallback ------------------------------------------


class _MultiTransport:
    """Hands out a fresh ``ScriptedTransport`` per open call. Tests can
    pre-populate the next transport's reply queue via ``next_reply``.
    """

    def __init__(self) -> None:
        self.transports: list[ScriptedTransport] = []
        self.pending: list[bytes] = []

    def queue_open_reply(self, *replies: bytes) -> None:
        """Bytes to feed the *next* opened transport in order."""
        self.pending.append(b"".join(replies))

    async def __call__(
        self,
        _host: str,
        _port: int,
        _ssl_context: ssl.SSLContext | None,
    ) -> tuple[asyncio.StreamReader, _WriterLike]:
        t = ScriptedTransport()
        if self.pending:
            t.feed(self.pending.pop(0))
        self.transports.append(t)
        return await t(_host, _port, _ssl_context)


async def test_kill_query_opens_fresh_client_when_primary_is_busy() -> None:
    # BEGIN: a Client whose primary connection has been driven into
    #        IN_FLIGHT (we send a query without draining the response).
    factory = _MultiTransport()
    # First transport: handshake only — we leave the connection in
    # IN_FLIGHT after a SELECT 1 send
    factory.queue_open_reply(encode_server_hello())
    # Second transport: handshake + kill response
    spec, _ = make_column("query_id", "String", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    data = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[["q-789"]])
    factory.queue_open_reply(
        encode_server_hello(),
        encode_server_data(header),
        encode_server_data(data),
        encode_server_end_of_stream(),
    )

    client = ch.Client("clickhouse://default:@host/db", transport_factory=factory)
    await client.open()
    try:
        # Drive the primary connection into IN_FLIGHT without draining
        await client._conn.send_query("SELECT sleep(99)")
        assert client._conn.state.name == "IN_FLIGHT"

        # WHEN: calling kill_query while primary is busy
        n = await client.kill_query("q-789")

        # THEN: a second transport was opened (the fresh side-channel)
        assert len(factory.transports) == 2
        # And the kill went through: row count from the response surfaces
        assert n == 1
        # The primary connection is still in IN_FLIGHT — kill_query
        # didn't disturb it
        assert client._conn.state.name == "IN_FLIGHT"
    finally:
        # Don't await close on a connection mid-IN_FLIGHT; just drop.
        await client._conn.close()


# ---- Pool.kill_query pass-through --------------------------------------


async def test_pool_kill_query_acquires_a_client_and_delegates() -> None:
    # BEGIN: a pool whose acquired client will see a single KILL QUERY
    factory = _MultiTransport()
    spec, _ = make_column("query_id", "String", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    data = Block(info=BlockInfo(), columns=[spec], n_rows=2, data=[["a", "b"]])
    factory.queue_open_reply(
        encode_server_hello(),
        encode_server_data(header),
        encode_server_data(data),
        encode_server_end_of_stream(),
    )

    pool = ch.create_pool(
        "clickhouse://default:@host/db",
        max_size=1,
        transport_factory=factory,
        idle_check_interval=999,  # don't perturb during tests
        health_check_after=999,
    )

    # WHEN: kill_query on the pool
    async with pool:
        n = await pool.kill_query("q-pool")

    # THEN: returns the row count from the result
    assert n == 2
    # And exactly one connection was opened to deliver the kill
    assert len(factory.transports) == 1


# ---- helpers ------------------------------------------------------------


async def _decode_query_sql_and_params_async(
    rdr: AsyncBinaryReader,
) -> tuple[str, dict[str, str]]:
    # ---- Hello ----
    assert await rdr.read_varuint() == ClientPacket.HELLO
    await rdr.read_string()  # client_name
    await rdr.read_varuint()  # version major
    await rdr.read_varuint()  # version minor
    await rdr.read_varuint()  # revision
    await rdr.read_string()  # database
    await rdr.read_string()  # user
    await rdr.read_string()  # password
    await rdr.read_string()  # addendum: quota_key (empty)
    await rdr.read_string()  # addendum: proto_send_chunked
    await rdr.read_string()  # addendum: proto_recv_chunked
    await rdr.read_varuint()  # addendum: parallel_replicas_protocol_version
    # ---- Query ----
    assert await rdr.read_varuint() == ClientPacket.QUERY
    await rdr.read_string()  # query_id
    # ClientInfo
    assert await rdr.read_byte() == 1  # query_kind
    await rdr.read_string()  # initial_user
    await rdr.read_string()  # initial_query_id
    await rdr.read_string()  # initial_address
    await rdr.read_int(8, signed=True)  # initial_query_start_time
    assert await rdr.read_byte() == 1  # interface = TCP
    await rdr.read_string()  # os_user
    await rdr.read_string()  # hostname
    await rdr.read_string()  # client_name
    await rdr.read_varuint()  # version major
    await rdr.read_varuint()  # version minor
    await rdr.read_varuint()  # revision
    await rdr.read_string()  # quota_key
    await rdr.read_varuint()  # distributed_depth
    await rdr.read_varuint()  # client_version_patch
    await rdr.read_byte()  # has_otel
    await rdr.read_varuint()  # parallel_replicas: collaborate
    await rdr.read_varuint()  # parallel_replicas: count
    await rdr.read_varuint()  # parallel_replicas: replica idx
    await rdr.read_varuint()  # script_query_number
    await rdr.read_varuint()  # script_line_number
    await rdr.read_byte()  # have_jwt
    # Tail of Query packet
    await rdr.read_string()  # settings terminator
    await rdr.read_string()  # extra_roles (empty for non-interserver)
    await rdr.read_string()  # interserver_secret
    await rdr.read_varuint()  # stage
    await rdr.read_varuint()  # compression
    sql = await rdr.read_string()
    params: dict[str, str] = {}
    while True:
        name = await rdr.read_string()
        if not name:
            break
        await rdr.read_varuint()  # CUSTOM flag (0x02)
        params[name] = await rdr.read_string()
    return sql, params
