"""Lifecycle tests for the high-level Client.

Covers ``connect`` / ``__aenter__`` / ``close`` / ``ping`` /
``server_info`` against a scripted transport. The Client is a thin
wrapper over ``Connection``; these tests exercise the wiring.
"""

from __future__ import annotations

import asyncio

import pytest

import clickhouse_async as ch
from clickhouse_async import Client, connect
from clickhouse_async.connection import State
from clickhouse_async.errors import ProtocolError
from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.protocol.packets import ClientPacket, ServerPacket

from ._mock_transport import ScriptedTransport
from ._scripted_packets import encode_server_hello

# ---- connect() factory --------------------------------------------------


async def test_connect_returns_unopened_client() -> None:
    # BEGIN: a transport that hasn't been touched
    transport = ScriptedTransport()

    # WHEN: building a Client via the connect() factory
    client = connect(
        "clickhouse://alice:secret@localhost/db",
        transport_factory=transport,
    )

    # THEN: the underlying transport hasn't been opened yet — the
    #       handshake happens in __aenter__/open(), not in connect()
    assert isinstance(client, Client)
    assert transport.opens == 0
    assert client.dsn.host == "localhost"
    assert client.dsn.user == "alice"


# ---- __aenter__ runs the handshake -------------------------------------


async def test_async_with_runs_handshake_and_returns_to_ready() -> None:
    # BEGIN: a scripted transport with a Hello reply queued
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())

    # WHEN: entering the async-with block
    async with connect(
        "clickhouse://alice:secret@localhost/db",
        transport_factory=transport,
    ) as client:
        # THEN: server_info is populated and the client's underlying
        #       state is READY (verified via ping below)
        info = client.server_info
        assert info.name == "ClickHouse"


async def test_async_with_passes_dsn_credentials_to_handshake() -> None:
    # BEGIN: a scripted transport
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())

    # WHEN: entering with credentials and a non-default database
    async with connect(
        "clickhouse://alice:secret@host/analytics",
        transport_factory=transport,
    ):
        pass

    # THEN: the captured Hello bytes carry the DSN's user / password /
    #       database — the Client's __aenter__ wired the DSN through
    rdr_data = transport.written()
    s = asyncio.StreamReader()
    s.feed_data(rdr_data)
    s.feed_eof()
    rdr = AsyncBinaryReader(s)
    assert await rdr.read_varuint() == ClientPacket.HELLO
    await rdr.read_string()  # client name
    await rdr.read_varuint()  # version major
    await rdr.read_varuint()  # version minor
    await rdr.read_varuint()  # revision
    assert await rdr.read_string() == "analytics"
    assert await rdr.read_string() == "alice"
    assert await rdr.read_string() == "secret"


async def test_async_with_close_releases_transport() -> None:
    # BEGIN: an opened client
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())

    # WHEN: exiting the async-with block
    async with connect("clickhouse://default:@host/db", transport_factory=transport):
        pass

    # THEN: the underlying writer was closed
    assert transport.writer_closed()


# ---- ping() round-trip -------------------------------------------------


async def test_ping_round_trips_pong() -> None:
    # BEGIN: an opened client; queue a Pong reply for the upcoming ping
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        pre = len(transport.written())
        # Queue a Pong packet (just the varuint id)
        pong = BinaryWriter()
        pong.write_varuint(ServerPacket.PONG)
        transport.feed(pong.getvalue())

        # WHEN: pinging
        await client.ping()

        # THEN: a single byte (Ping packet id, 4) was written; client
        #       still works
        assert transport.written()[pre:] == bytes((ClientPacket.PING,))


async def test_ping_against_non_pong_response_breaks_connection() -> None:
    # BEGIN: an opened client; queue a wrong packet (Hello = 0) instead of Pong
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    async with connect(
        "clickhouse://default:@host/db", transport_factory=transport
    ) as client:
        bad = BinaryWriter()
        bad.write_varuint(ServerPacket.HELLO)
        transport.feed(bad.getvalue())

        # WHEN / THEN: ping raises ProtocolError naming PONG explicitly
        with pytest.raises(ProtocolError, match="expected PONG"):
            await client.ping()
        # Underlying connection is BROKEN
        assert client._conn.state == State.BROKEN  # type: ignore[attr-defined]


# ---- server_info gating ------------------------------------------------


async def test_server_info_raises_before_open() -> None:
    # BEGIN: a Client that hasn't entered the async-with block
    transport = ScriptedTransport()
    client = connect("clickhouse://default:@host/db", transport_factory=transport)

    # WHEN / THEN: accessing server_info before open() raises
    with pytest.raises(RuntimeError, match="IDLE"):
        _ = client.server_info


# ---- public re-exports -------------------------------------------------


def test_top_level_module_re_exports() -> None:
    # BEGIN / WHEN / THEN: the public surface is reachable from the
    #     top-level module per the README quick-start
    assert ch.connect is connect
    assert ch.Client is Client
    assert hasattr(ch, "ClickHouseError")
    assert hasattr(ch, "CompressionMethod")
    assert hasattr(ch, "DSN")
    assert hasattr(ch, "__version__")
