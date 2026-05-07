"""Hello-handshake tests for `Connection.open()`.

The handshake takes `CONNECTING → READY` on success, or
`CONNECTING → BROKEN` on a server-side rejection (server replies
`Exception`) or any handshake-time IO/protocol failure.
"""

from __future__ import annotations

import asyncio

import pytest

from clickhouse_async.connection import Connection, State
from clickhouse_async.errors import ProtocolError, ServerError
from clickhouse_async.protocol.handshake import (
    CLIENT_NAME,
    CLIENT_VERSION_MAJOR,
    CLIENT_VERSION_MINOR,
)
from clickhouse_async.protocol.io import AsyncBinaryReader
from clickhouse_async.protocol.packets import OUR_REVISION, ClientPacket

from ._mock_transport import ScriptedTransport
from ._scripted_packets import (
    encode_exception_body_only,
    encode_server_exception,
    encode_server_hello,
)


def _reader_over(data: bytes) -> AsyncBinaryReader:
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return AsyncBinaryReader(stream)


# ---- successful handshake -----------------------------------------------


async def test_handshake_populates_server_info_and_promotes_to_ready() -> None:
    # BEGIN: a transport with a Hello reply queued
    transport = ScriptedTransport()
    transport.feed(
        encode_server_hello(
            name="ClickHouse",
            version_major=24,
            version_minor=8,
            revision=OUR_REVISION,
            timezone="Europe/Madrid",
            display_name="my-server",
            version_patch=12,
        )
    )
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN: opening with credentials
    await conn.open(user="alice", password="secret", database="analytics")

    # THEN: the connection is READY with the parsed server identity
    assert conn.state == State.READY
    info = conn.server_info
    assert info.name == "ClickHouse"
    assert info.version_major == 24
    assert info.version_minor == 8
    assert info.revision == OUR_REVISION
    assert info.timezone == "Europe/Madrid"
    assert info.display_name == "my-server"
    assert info.version_patch == 12


async def test_client_hello_bytes_match_documented_field_order() -> None:
    # BEGIN: a connection ready to handshake
    transport = ScriptedTransport()
    transport.feed(encode_server_hello())
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN: opening with credentials
    await conn.open(user="alice", password="secret", database="db")

    # THEN: the bytes we wrote follow the documented Hello layout — id,
    #       client_name, version major/minor, our revision, then DB / user
    #       / password
    reader = _reader_over(transport.written())
    assert await reader.read_varuint() == ClientPacket.HELLO
    assert await reader.read_string() == CLIENT_NAME
    assert await reader.read_varuint() == CLIENT_VERSION_MAJOR
    assert await reader.read_varuint() == CLIENT_VERSION_MINOR
    assert await reader.read_varuint() == OUR_REVISION
    assert await reader.read_string() == "db"
    assert await reader.read_string() == "alice"
    assert await reader.read_string() == "secret"


# ---- revision negotiation ------------------------------------------------


async def test_negotiated_revision_caps_at_our_revision_for_newer_servers() -> None:
    # BEGIN: a server claiming a revision newer than ours
    transport = ScriptedTransport()
    transport.feed(encode_server_hello(revision=OUR_REVISION + 5))
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN: handshaking
    await conn.open()

    # THEN: negotiated_revision is OUR_REVISION (we never claim more than
    #       what we implement)
    assert conn.server_info.revision == OUR_REVISION + 5
    assert conn.negotiated_revision == OUR_REVISION


async def test_negotiated_revision_follows_server_for_older_servers() -> None:
    # BEGIN: a server claiming a revision below ours but still above all
    #        gates we declare (54469 - 30 = 54439, still above the highest
    #        gate we currently use)
    older = OUR_REVISION - 30
    transport = ScriptedTransport()
    transport.feed(encode_server_hello(revision=older))
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN: handshaking
    await conn.open()

    # THEN: negotiated_revision matches the server's — gated reads/writes
    #       in later substeps will use this older value
    assert conn.negotiated_revision == older


# ---- server-side rejection ----------------------------------------------


async def test_server_exception_during_handshake_raises_and_breaks_connection() -> None:
    # BEGIN: a server that rejects the Hello with an Exception packet
    transport = ScriptedTransport()
    transport.feed(
        encode_server_exception(
            code=192,
            name="UNKNOWN_USER",
            display_text="User 'alice' does not exist",
        )
    )
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN: handshaking against the rejecting server
    # THEN: a ServerError surfaces with the documented fields, and the
    #       connection is BROKEN (writer closed, no reader)
    with pytest.raises(ServerError) as exc_info:
        await conn.open(user="alice")
    err = exc_info.value
    assert err.code == 192
    assert err.name == "UNKNOWN_USER"
    assert "alice" in err.display_text
    assert err.nested is None
    assert conn.state == State.BROKEN
    assert transport.writer_closed()


async def test_nested_server_exception_round_trips() -> None:
    # BEGIN: a server emitting a nested-exception chain (outer wrapping
    #        an inner cause)
    inner = encode_exception_body_only(
        code=999, name="INNER", display_text="root cause"
    )
    transport = ScriptedTransport()
    transport.feed(
        encode_server_exception(
            code=42, name="OUTER", display_text="failed to do X", nested=inner
        )
    )
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN: handshake fails
    with pytest.raises(ServerError) as exc_info:
        await conn.open()

    # THEN: the outer exception carries a populated `nested` whose code
    #       and name match the encoded inner body
    err = exc_info.value
    assert err.name == "OUTER"
    assert err.nested is not None
    assert err.nested.code == 999
    assert err.nested.name == "INNER"
    assert err.nested.display_text == "root cause"


async def test_unexpected_first_packet_raises_protocol_error_and_breaks() -> None:
    # BEGIN: a server that emits some other packet id where we expect a
    #        Hello or Exception (e.g. Pong = 4)
    transport = ScriptedTransport()
    transport.feed(b"\x04")  # ServerPacket.PONG = 4
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN / THEN: the connection rejects the unexpected packet and is BROKEN
    with pytest.raises(ProtocolError, match="unexpected packet id 4"):
        await conn.open()
    assert conn.state == State.BROKEN


# ---- server_info gating -------------------------------------------------


async def test_server_info_raises_before_handshake() -> None:
    # BEGIN: an IDLE connection
    transport = ScriptedTransport()
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN / THEN: accessing server_info before handshake raises with the
    #              current state in the message
    with pytest.raises(RuntimeError, match="IDLE"):
        _ = conn.server_info


# ---- old-server fields gating -------------------------------------------


async def test_old_server_omitting_optional_fields_still_handshakes() -> None:
    # BEGIN: a server claiming a revision below the timezone gate (54058)
    #        — display_name and version_patch are also absent at that age
    pre_timezone = 54057
    transport = ScriptedTransport()
    transport.feed(encode_server_hello(revision=pre_timezone))
    conn = Connection([("h", 9000)], transport_factory=transport)

    # WHEN: handshaking
    await conn.open()

    # THEN: the optional fields default; the connection still reaches READY
    info = conn.server_info
    assert info.timezone is None
    assert info.display_name is None
    assert info.version_patch == 0
    assert conn.state == State.READY
