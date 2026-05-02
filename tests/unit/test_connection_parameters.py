"""Server-side parameter binding for ``Connection.send_query``.

Covers ``send_query(..., params=...)`` and the ``format_param``
registry it uses to coerce typed Python values into the textual form
ClickHouse expects.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID

import pytest

from clickhouse_async.connection import Connection
from clickhouse_async.errors import UnsupportedFeatureError
from clickhouse_async.protocol.io import AsyncBinaryReader
from clickhouse_async.protocol.packets import (
    DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS,
    ClientPacket,
)
from clickhouse_async.protocol.parameters import format_param

from ._mock_transport import ScriptedTransport
from ._scripted_packets import encode_server_hello


def _reader_over(data: bytes) -> AsyncBinaryReader:
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return AsyncBinaryReader(stream)


async def _drain_client_hello(rdr: AsyncBinaryReader) -> None:
    assert await rdr.read_varuint() == ClientPacket.HELLO
    await rdr.read_string()
    await rdr.read_varuint()
    await rdr.read_varuint()
    await rdr.read_varuint()
    await rdr.read_string()
    await rdr.read_string()
    await rdr.read_string()


async def _drain_query_through_sql(
    rdr: AsyncBinaryReader, *, expected_user: str = "alice"
) -> None:
    """Walk through the Query packet up to (and including) the SQL string,
    leaving the cursor at the start of the parameters block."""
    assert await rdr.read_varuint() == ClientPacket.QUERY
    await rdr.read_string()  # query_id
    # ClientInfo block at OUR_REVISION
    assert await rdr.read_byte() == 1                # query_kind
    assert await rdr.read_string() == expected_user  # initial_user
    await rdr.read_string()                          # initial_query_id
    await rdr.read_string()                          # initial_address
    await rdr.read_int(8, signed=True)               # initial_query_start_time
    assert await rdr.read_byte() == 1                # interface = TCP
    await rdr.read_string()                          # os_user
    await rdr.read_string()                          # hostname
    await rdr.read_string()                          # client_name
    await rdr.read_varuint()                         # version major
    await rdr.read_varuint()                         # version minor
    await rdr.read_varuint()                         # revision
    await rdr.read_string()                          # quota_key
    await rdr.read_varuint()                         # distributed_depth
    await rdr.read_varuint()                         # client_version_patch
    await rdr.read_byte()                            # has_otel
    await rdr.read_varuint()                         # parallel_replicas: collaborate
    await rdr.read_varuint()                         # parallel_replicas: count
    await rdr.read_varuint()                         # parallel_replicas: replica idx
    # settings terminator + interserver_secret + stage + compression + sql
    await rdr.read_string()  # settings terminator
    await rdr.read_string()  # interserver_secret
    await rdr.read_varuint()  # stage
    await rdr.read_varuint()  # compression
    await rdr.read_string()  # sql


async def _connect(transport: ScriptedTransport, *, revision: int | None = None) -> Connection:
    transport.feed(
        encode_server_hello(revision=revision)
        if revision is not None
        else encode_server_hello()
    )
    conn = Connection("h", 9000, transport_factory=transport)
    await conn.open(user="alice")
    return conn


# ---- format_param registry ---------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("hello", "hello"),
        ("", ""),
        (True, "true"),
        (False, "false"),
        (0, "0"),
        (-42, "-42"),
        (2**63 - 1, str(2**63 - 1)),
        (Decimal("3.14"), "3.14"),
        (Decimal("-0.0001"), "-0.0001"),
        (date(2026, 5, 3), "2026-05-03"),
        (datetime(2026, 5, 3, 12, 34, 56), "2026-05-03 12:34:56"),
        (
            datetime(2026, 5, 3, 12, 34, 56, microsecond=123_456),
            "2026-05-03 12:34:56.123456",
        ),
        (UUID(int=0), "00000000-0000-0000-0000-000000000000"),
        (IPv4Address("192.168.1.1"), "192.168.1.1"),
        (IPv6Address("::1"), "::1"),
        (b"\xde\xad\xbe\xef", "deadbeef"),
    ],
)
def test_format_param_round_trip(value: object, expected: str) -> None:
    # BEGIN: a representative value of every supported Python type
    # WHEN: formatting via the registry
    # THEN: the textual form matches the documented representation
    assert format_param(value) == expected


def test_format_param_handles_special_floats() -> None:
    # BEGIN: the IEEE-754 specials that need bespoke string forms
    # WHEN / THEN: each surfaces with the documented spelling
    assert format_param(float("nan")) == "nan"
    assert format_param(float("inf")) == "inf"
    assert format_param(float("-inf")) == "-inf"


def test_format_param_rejects_unsupported_types() -> None:
    # BEGIN / WHEN / THEN: unknown Python types raise TypeError naming
    #     the offending type
    with pytest.raises(TypeError, match="cannot format query parameter"):
        format_param([1, 2, 3])
    with pytest.raises(TypeError, match="cannot format query parameter"):
        format_param({"k": "v"})


# ---- send_query parameter wire format ---------------------------------


async def test_send_query_emits_parameters_block_with_custom_flag() -> None:
    # BEGIN: a connection past handshake at OUR_REVISION
    transport = ScriptedTransport()
    conn = await _connect(transport)

    # WHEN: sending a parametric SELECT with three typed values
    await conn.send_query(
        "SELECT * FROM t WHERE day = {d:Date} AND id = {n:Int32} AND name = {s:String}",
        params={
            "d": date(2026, 5, 3),
            "n": 42,
            "s": "alice",
        },
    )

    # THEN: the parameters block sits after the SQL string with each
    #       entry as `string name + varuint flag (CUSTOM = 0x02) +
    #       string value`, terminated by an empty name
    rdr = _reader_over(transport.written())
    await _drain_client_hello(rdr)
    await _drain_query_through_sql(rdr)
    seen: dict[str, str] = {}
    while True:
        name = await rdr.read_string()
        if not name:
            break
        flag = await rdr.read_varuint()
        assert flag == 0x02, f"parameter {name!r} flag must be CUSTOM (0x02)"
        seen[name] = await rdr.read_string()
    assert seen == {"d": "2026-05-03", "n": "42", "s": "alice"}


async def test_send_query_omits_parameters_block_when_no_params_passed() -> None:
    # BEGIN: a connection past handshake at OUR_REVISION
    transport = ScriptedTransport()
    conn = await _connect(transport)

    # WHEN: sending a non-parametric query
    await conn.send_query("SELECT 1")

    # THEN: walking past SQL lands at the parameters terminator (empty
    #       string), no parameter entries between
    rdr = _reader_over(transport.written())
    await _drain_client_hello(rdr)
    await _drain_query_through_sql(rdr)
    assert await rdr.read_string() == ""  # parameters terminator


# ---- revision gating ---------------------------------------------------


async def test_send_query_with_params_below_gate_raises_unsupported_feature() -> None:
    # BEGIN: a connection negotiated below the parameters revision (54459)
    older_revision = DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS - 1
    transport = ScriptedTransport()
    conn = await _connect(transport, revision=older_revision)
    assert conn.negotiated_revision == older_revision

    # WHEN: trying to use server-side parameters
    # THEN: UnsupportedFeatureError surfaces, naming the gate and the
    #       negotiated revision; no bytes were written for this Query
    pre = len(transport.written())
    with pytest.raises(UnsupportedFeatureError) as exc_info:
        await conn.send_query("SELECT {n:Int32}", params={"n": 1})
    assert str(DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS) in str(exc_info.value)
    assert str(older_revision) in str(exc_info.value)
    assert len(transport.written()) == pre


async def test_send_query_below_gate_without_params_still_works() -> None:
    # BEGIN: a connection below the parameters revision, no params
    older_revision = DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS - 1
    transport = ScriptedTransport()
    conn = await _connect(transport, revision=older_revision)

    # WHEN: sending a non-parametric query
    pre = len(transport.written())
    await conn.send_query("SELECT 1")

    # THEN: it goes through — only the *use* of params is gated, not
    #       send_query itself; bytes were written
    assert len(transport.written()) > pre


# ---- typed values flow through send_query ------------------------------


async def test_send_query_typed_values_format_via_registry() -> None:
    # BEGIN: a connection past handshake
    transport = ScriptedTransport()
    conn = await _connect(transport)

    # WHEN: sending parameters with typed values that exercise the
    #       registry's special cases (datetime with microseconds,
    #       UUID, bytes)
    await conn.send_query(
        "SELECT 1",
        params={
            "ts": datetime(2026, 5, 3, 12, 0, 0, microsecond=500_000, tzinfo=UTC),
            "id": UUID("12345678-1234-5678-9abc-def012345678"),
            "blob": b"\x00\xff",
        },
    )

    # THEN: each value reaches the wire in its registry-formatted form
    rdr = _reader_over(transport.written())
    await _drain_client_hello(rdr)
    await _drain_query_through_sql(rdr)
    seen: dict[str, str] = {}
    while True:
        name = await rdr.read_string()
        if not name:
            break
        await rdr.read_varuint()  # flag
        seen[name] = await rdr.read_string()
    assert seen["ts"] == "2026-05-03 12:00:00.500000"
    assert seen["id"] == "12345678-1234-5678-9abc-def012345678"
    assert seen["blob"] == "00ff"


async def test_send_query_unsupported_param_type_raises_type_error() -> None:
    # BEGIN: a connection past handshake
    transport = ScriptedTransport()
    conn = await _connect(transport)

    # WHEN: passing a parameter of a type the registry doesn't cover
    # THEN: TypeError surfaces from format_param via send_query;
    #       nothing was written for this Query
    pre = len(transport.written())
    with pytest.raises(TypeError, match="cannot format query parameter"):
        await conn.send_query("SELECT 1", params={"x": [1, 2, 3]})
    assert len(transport.written()) == pre
