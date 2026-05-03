"""Tests for ``Connection.send_query`` + ``iter_packets``."""

from __future__ import annotations

import asyncio

import pytest

from clickhouse_async.connection import Connection, State
from clickhouse_async.errors import ConcurrentQueryError, ServerError
from clickhouse_async.protocol.block import (
    Block,
    BlockInfo,
    make_column,
    write_block,
)
from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.protocol.packets import (
    OUR_REVISION,
    ClientPacket,
)
from clickhouse_async.protocol.query_packet import QueryStage

from ._mock_transport import ScriptedTransport
from ._scripted_packets import (
    encode_server_data,
    encode_server_end_of_stream,
    encode_server_exception,
    encode_server_hello,
)


def _reader_over(data: bytes) -> AsyncBinaryReader:
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return AsyncBinaryReader(stream)


async def _connect(transport: ScriptedTransport) -> Connection:
    """Open a connection that's already through the handshake — saves
    boilerplate in tests focused on send_query."""
    transport.feed(encode_server_hello())
    conn = Connection([("h", 9000)], transport_factory=transport)
    await conn.open(user="alice")
    return conn


async def _drain_client_hello(rdr: AsyncBinaryReader) -> None:
    """Walk past the bytes ``write_client_hello`` writes plus the
    post-Hello addendum so subsequent reads land at the next packet."""
    assert await rdr.read_varuint() == ClientPacket.HELLO
    await rdr.read_string()  # client name
    await rdr.read_varuint()  # version major
    await rdr.read_varuint()  # version minor
    await rdr.read_varuint()  # revision
    await rdr.read_string()  # database
    await rdr.read_string()  # user
    await rdr.read_string()  # password
    await rdr.read_string()  # addendum: quota_key (empty)


# ---- send_query bytes ---------------------------------------------------


async def test_send_query_writes_query_packet_id_and_query_id_first() -> None:
    # BEGIN: a connection past the handshake
    transport = ScriptedTransport()
    conn = await _connect(transport)

    # WHEN: sending a SELECT 1 with an explicit query id
    await conn.send_query("SELECT 1", query_id="qid-1")

    # THEN: the first bytes after the Hello prefix are ClientPacket.QUERY
    #       followed by the query_id string
    rdr = _reader_over(transport.written())
    await _drain_client_hello(rdr)
    assert await rdr.read_varuint() == ClientPacket.QUERY
    assert await rdr.read_string() == "qid-1"


async def test_send_query_emits_documented_field_order_through_sql() -> None:
    # BEGIN: a connection past the handshake, sending a SELECT 1
    transport = ScriptedTransport()
    conn = await _connect(transport)
    await conn.send_query("SELECT 1")

    # WHEN: walking the bytes after Hello in the documented order
    rdr = _reader_over(transport.written())
    await _drain_client_hello(rdr)
    assert await rdr.read_varuint() == ClientPacket.QUERY
    assert await rdr.read_string() == ""  # default empty query_id

    # ClientInfo block — query_kind=InitialQuery(1), initial_user/qid/addr
    assert await rdr.read_byte() == 1
    assert await rdr.read_string() == "alice"  # initial_user (the open() user)
    assert await rdr.read_string() == ""  # initial_query_id
    assert await rdr.read_string() == "0.0.0.0:0"  # initial_address
    await rdr.read_int(8, signed=True)  # initial_query_start_time microseconds
    assert await rdr.read_byte() == 1  # interface = TCP
    await rdr.read_string()  # os_user (env-dependent)
    await rdr.read_string()  # hostname (env-dependent)
    await rdr.read_string()  # client_name
    await rdr.read_varuint()  # client version major
    await rdr.read_varuint()  # client version minor
    assert await rdr.read_varuint() == OUR_REVISION
    await rdr.read_string()  # quota_key (empty)
    await rdr.read_varuint()  # distributed_depth (0)
    await rdr.read_varuint()  # client_version_patch (0)
    assert await rdr.read_byte() == 0  # OTel has-otel flag (0)
    assert await rdr.read_varuint() == 0  # parallel_replicas: collaborate
    assert await rdr.read_varuint() == 0  # parallel_replicas: count
    assert await rdr.read_varuint() == 0  # parallel_replicas: replica idx

    # THEN: the rest of the Query packet matches the documented layout —
    #       empty settings + empty interserver secret + Complete stage +
    #       compression flag 0 + SQL string + empty parameters terminator
    assert await rdr.read_string() == ""  # settings terminator
    assert await rdr.read_string() == ""  # interserver_secret (empty)
    assert await rdr.read_varuint() == QueryStage.COMPLETE
    assert await rdr.read_varuint() == 0  # compression flag
    assert await rdr.read_string() == "SELECT 1"
    assert await rdr.read_string() == ""  # parameters terminator


# ---- iter_packets — happy path -----------------------------------------


async def test_iter_packets_yields_data_then_terminates_on_eos() -> None:
    # BEGIN: a connected connection with a scripted SELECT response —
    #        a header block (0 rows) + a data block (1 row of Int32) +
    #        EndOfStream
    transport = ScriptedTransport()
    conn = await _connect(transport)
    spec, _ = make_column("number", "Int32", [])
    header = Block(info=BlockInfo(), columns=[spec], n_rows=0, data=[[]])
    data = Block(info=BlockInfo(), columns=[spec], n_rows=1, data=[[42]])
    transport.feed(encode_server_data(header))
    transport.feed(encode_server_data(data))
    transport.feed(encode_server_end_of_stream())

    # WHEN: sending the query and draining the iterator
    await conn.send_query("SELECT 42")
    streamed = [s async for s in conn.iter_packets()]

    # THEN: two blocks come back, both tagged "data" (no Totals/Extremes
    #       in this scenario), and the connection returns to READY
    assert [s.kind for s in streamed] == ["data", "data"]
    assert streamed[0].block.n_rows == 0
    assert streamed[1].block.n_rows == 1
    assert streamed[1].block.data == [[42]]
    assert conn.state == State.READY


# ---- iter_packets — error path -----------------------------------------


async def test_iter_packets_raises_server_error_and_returns_to_ready() -> None:
    # BEGIN: a server that emits an Exception mid-query (e.g. table not found)
    transport = ScriptedTransport()
    conn = await _connect(transport)
    transport.feed(
        encode_server_exception(
            code=60, name="UNKNOWN_TABLE", display_text="Table foo doesn't exist"
        )
    )

    # WHEN: sending a query and iterating
    await conn.send_query("SELECT * FROM foo")
    with pytest.raises(ServerError) as exc_info:
        async for _ in conn.iter_packets():
            pass

    # THEN: the structured error fields surface and the connection is
    #       READY (reusable, not BROKEN — a query-level Exception isn't
    #       a transport failure)
    err = exc_info.value
    assert err.code == 60
    assert err.name == "UNKNOWN_TABLE"
    assert "foo" in err.display_text
    assert conn.state == State.READY


# Coverage for unrecognised packet ids lives in the parametrised
# `test_distributed_read_packets_break_the_connection` — that test
# walks every distributed-read packet id and asserts the same
# state-and-error contract.


# ---- state guards -------------------------------------------------------


async def test_send_query_from_in_flight_raises_concurrent_query_error() -> None:
    # BEGIN: a connection that just sent a query and is still IN_FLIGHT
    transport = ScriptedTransport()
    conn = await _connect(transport)
    await conn.send_query("SELECT 1")
    assert conn.state == State.IN_FLIGHT

    # WHEN: trying to send a second query without consuming the first
    # THEN: ConcurrentQueryError surfaces — one in-flight query per conn
    with pytest.raises(ConcurrentQueryError, match="another query"):
        await conn.send_query("SELECT 2")


async def test_send_query_from_idle_raises() -> None:
    # BEGIN: a brand-new connection that never opened
    transport = ScriptedTransport()
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN: trying to send a query without opening
    # THEN: a RuntimeError surfaces naming the current state
    with pytest.raises(RuntimeError, match="IDLE"):
        await conn.send_query("SELECT 1")


async def test_iter_packets_from_ready_raises() -> None:
    # BEGIN: a connection in READY (handshake done, no query sent)
    transport = ScriptedTransport()
    conn = await _connect(transport)
    assert conn.state == State.READY

    # WHEN: trying to iterate without sending a query first
    # THEN: a RuntimeError surfaces
    with pytest.raises(RuntimeError, match="IN_FLIGHT"):
        async for _ in conn.iter_packets():
            pass


# ---- trailing empty block in the Query packet --------------------------


async def test_query_packet_ends_with_empty_data_block() -> None:
    # BEGIN: a connection past the handshake
    transport = ScriptedTransport()
    conn = await _connect(transport)

    # WHEN: sending a trivial SELECT
    await conn.send_query("SELECT 1")
    written = transport.written()

    # THEN: the very last bytes are a Client.DATA packet with empty
    #       table name and an empty block (BlockInfo defaults + 0
    #       columns + 0 rows). Reconstruct the canonical empty-block
    #       suffix and check the buffer ends with it.
    suffix_w = BinaryWriter()
    suffix_w.write_varuint(ClientPacket.DATA)
    suffix_w.write_string("")
    write_block(
        suffix_w,
        Block(info=BlockInfo(), columns=[], n_rows=0, data=[]),
        revision=conn.negotiated_revision,
    )
    assert written.endswith(suffix_w.getvalue())
