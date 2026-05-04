"""Multi-host failover tests for ``Connection.open()``.

The connection takes a candidate list of ``(host, port)`` pairs and
walks them in order on each open. Per-host failures are recorded;
the call returns on the first successful handshake. If every
candidate fails the connection raises ``ConnectError`` (multi-host)
or surfaces the underlying error directly (single-host) and ends
in ``BROKEN``.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from clickhouse_async.connection import Connection, State, _WriterLike
from clickhouse_async.errors import ConnectError, ProtocolError, ServerError

from ._mock_transport import _ScriptedWriter
from ._scripted_packets import encode_server_exception, encode_server_hello

if TYPE_CHECKING:
    import ssl


class _PerHostTransport:
    """A transport factory that returns scripted readers per (host, port).

    For each candidate the test arms either a ``bytes`` payload (which
    becomes the server's reply, fed into a fresh ``StreamReader``) or
    an ``Exception`` that the factory raises on the open call. Every
    call is recorded for assertion.
    """

    def __init__(self) -> None:
        self.scripted: dict[tuple[str, int], bytes | Exception] = {}
        self.calls: list[tuple[str, int]] = []
        # Hold writers for inspection — typically tests only need the
        # last one but we keep them all.
        self.writers: list[_ScriptedWriter] = []

    def arm(self, host: tuple[str, int], outcome: bytes | Exception) -> None:
        self.scripted[host] = outcome

    async def __call__(
        self,
        host: str,
        port: int,
        _ssl_context: ssl.SSLContext | None,
    ) -> tuple[asyncio.StreamReader, _WriterLike]:
        key = (host, port)
        self.calls.append(key)
        outcome = self.scripted.get(key)
        if isinstance(outcome, Exception):
            raise outcome
        if outcome is None:
            # Default: feed an empty reader so the handshake's first
            # read fails with EOF. Tests should arm every candidate
            # they care about.
            outcome = b""
        reader = asyncio.StreamReader()
        reader.feed_data(outcome)
        reader.feed_eof()
        writer = _ScriptedWriter(bytearray())
        self.writers.append(writer)
        return reader, writer


# ---- failover walks the candidate list ----------------------------------


async def test_open_returns_on_first_successful_candidate() -> None:
    # BEGIN: three candidates; only the third has a valid Hello reply
    transport = _PerHostTransport()
    transport.arm(("a", 9000), ConnectionRefusedError("a down"))
    transport.arm(("b", 9000), ConnectionRefusedError("b down"))
    transport.arm(("c", 9000), encode_server_hello())
    conn = Connection(
        [("a", 9000), ("b", 9000), ("c", 9000)], transport_factory=transport
    )

    # WHEN: opening the connection
    await conn.open()

    # THEN: the connection ends in READY against the third candidate;
    #       the first two were tried in order before the win
    assert conn.state == State.READY
    assert transport.calls == [("a", 9000), ("b", 9000), ("c", 9000)]
    assert conn.host == "c"
    assert conn.port == 9000


async def test_open_stops_after_first_success() -> None:
    # BEGIN: two candidates; the first has a valid Hello reply
    transport = _PerHostTransport()
    transport.arm(("a", 9000), encode_server_hello())
    transport.arm(("b", 9000), ConnectionRefusedError("never reached"))
    conn = Connection([("a", 9000), ("b", 9000)], transport_factory=transport)

    # WHEN: opening the connection
    await conn.open()

    # THEN: only the first candidate was tried — the second isn't even
    #       contacted once the first succeeds
    assert conn.state == State.READY
    assert transport.calls == [("a", 9000)]
    assert conn.host == "a"


# ---- all-fail surface ----------------------------------------------------


async def test_all_candidates_fail_raises_connect_error_with_chain() -> None:
    # BEGIN: three candidates that all reject the connection in distinct
    #        ways — transport-level error, malformed packet, server exception
    transport = _PerHostTransport()
    transport.arm(("a", 9000), ConnectionRefusedError("a refused"))
    # Malformed first byte → ProtocolError during handshake
    transport.arm(("b", 9000), bytes([0x05]) + b"\x00" * 64)
    transport.arm(("c", 9000), encode_server_exception(code=42, name="X"))

    conn = Connection(
        [("a", 9000), ("b", 9000), ("c", 9000)], transport_factory=transport
    )

    # WHEN / THEN: open() raises ConnectError naming every candidate
    with pytest.raises(ConnectError) as exc_info:
        await conn.open()

    err = exc_info.value
    # THEN: the error message names every host:port
    assert "a:9000" in str(err)
    assert "b:9000" in str(err)
    assert "c:9000" in str(err)
    # THEN: each per-host exception is reachable via host_errors
    assert len(err.host_errors) == 3
    types = [type(e).__name__ for _h, _p, e in err.host_errors]
    assert "ConnectionRefusedError" in types
    assert "ProtocolError" in types
    assert "ServerError" in types
    # THEN: __cause__ is the last underlying error (ServerError) so
    #       Python's traceback still shows at least one underlying cause
    assert isinstance(err.__cause__, ServerError)
    # THEN: state is BROKEN
    assert conn.state == State.BROKEN


async def test_single_host_failure_surfaces_underlying_error() -> None:
    # BEGIN: a single-host candidate list that fails at handshake
    transport = _PerHostTransport()
    transport.arm(("a", 9000), encode_server_exception(code=7, name="Boom"))
    conn = Connection([("a", 9000)], transport_factory=transport)

    # WHEN / THEN: open() raises ServerError directly — not ConnectError —
    #              so existing single-host call sites keep their semantics
    with pytest.raises(ServerError):
        await conn.open()
    assert conn.state == State.BROKEN


async def test_mixed_failure_then_success_reports_winner() -> None:
    # BEGIN: first candidate raises during handshake (protocol error),
    #        second candidate has a valid Hello reply
    transport = _PerHostTransport()
    transport.arm(("a", 9000), bytes([0x05]) + b"\x00" * 64)  # bad packet id
    transport.arm(("b", 9000), encode_server_hello())
    conn = Connection([("a", 9000), ("b", 9000)], transport_factory=transport)

    # WHEN: opening
    await conn.open()

    # THEN: the connection lands on candidate b
    assert conn.state == State.READY
    assert (conn.host, conn.port) == ("b", 9000)
    # THEN: the first candidate's failure didn't get suppressed silently —
    #       the transport saw both calls
    assert transport.calls == [("a", 9000), ("b", 9000)]


# ---- on_host_attempt callback feeds the rotation ------------------------


async def test_on_host_attempt_called_for_every_candidate() -> None:
    # BEGIN: a Connection with an on_host_attempt hook and a 2-candidate
    #        list where the first fails and the second succeeds
    transport = _PerHostTransport()
    transport.arm(("a", 9000), ConnectionRefusedError("nope"))
    transport.arm(("b", 9000), encode_server_hello())
    attempts: list[tuple[tuple[str, int], type[BaseException] | None]] = []

    def hook(host: tuple[str, int], exc: BaseException | None) -> None:
        attempts.append((host, type(exc) if exc else None))

    conn = Connection(
        [("a", 9000), ("b", 9000)],
        transport_factory=transport,
        on_host_attempt=hook,
    )

    # WHEN: opening
    await conn.open()

    # THEN: the hook fires once per candidate with the right outcome
    assert attempts == [
        (("a", 9000), ConnectionRefusedError),
        (("b", 9000), None),
    ]


# ---- input validation ---------------------------------------------------


async def test_empty_host_list_is_rejected_at_construction() -> None:
    # BEGIN / WHEN / THEN: zero hosts is meaningless; the constructor
    #                     refuses rather than failing late at open()
    with pytest.raises(ValueError, match="at least one host"):
        Connection([])


# ---- ProtocolError on every candidate is still wrapped ------------------


async def test_all_protocol_errors_still_wrapped_as_connect_error() -> None:
    # BEGIN: two candidates that both reply with a malformed first packet
    transport = _PerHostTransport()
    transport.arm(("a", 9000), bytes([0x05]) + b"\x00" * 64)
    transport.arm(("b", 9000), bytes([0x05]) + b"\x00" * 64)
    conn = Connection([("a", 9000), ("b", 9000)], transport_factory=transport)

    # WHEN / THEN: ConnectError surfaces with both ProtocolErrors attached
    with pytest.raises(ConnectError) as exc_info:
        await conn.open()
    types = {type(e) for _h, _p, e in exc_info.value.host_errors}
    assert types == {ProtocolError}


# ---- ConnectError constructor validation --------------------------------


def test_connect_error_empty_list_raises_value_error() -> None:
    # WHEN: / THEN: ConnectError requires at least one failure entry
    with pytest.raises(ValueError, match="at least one"):
        ConnectError([])


# ---- connect_timeout -------------------------------------------------------


async def test_connect_timeout_raises_when_handshake_hangs() -> None:
    # BEGIN: a transport that opens successfully but never feeds any bytes
    #        (simulates a server that accepts the TCP connection but ignores
    #        our Hello — the handshake read blocks forever without a timeout)
    async def _hanging_factory(
        _host: str,
        _port: int,
        _ssl: object,
    ) -> tuple[asyncio.StreamReader, _WriterLike]:
        return asyncio.StreamReader(), _ScriptedWriter(bytearray())

    conn = Connection(
        [("h", 9000)],
        transport_factory=_hanging_factory,
        connect_timeout=0.05,
    )

    # WHEN / THEN: open() raises because the handshake times out
    with pytest.raises((TimeoutError, asyncio.TimeoutError)):
        await conn.open()
    assert conn.state == State.BROKEN


async def test_connect_timeout_falls_through_to_next_host() -> None:
    # BEGIN: first host hangs; second host has a valid Hello
    async def _hanging_factory(
        host: str,
        port: int,
        _ssl: object,
    ) -> tuple[asyncio.StreamReader, _WriterLike]:
        if host == "slow":
            return asyncio.StreamReader(), _ScriptedWriter(bytearray())
        reader = asyncio.StreamReader()
        reader.feed_data(encode_server_hello())
        reader.feed_eof()
        return reader, _ScriptedWriter(bytearray())

    conn = Connection(
        [("slow", 9000), ("fast", 9000)],
        transport_factory=_hanging_factory,
        connect_timeout=0.05,
    )

    # WHEN: opening the connection
    await conn.open()

    # THEN: the second host won; connection is READY
    assert conn.state == State.READY
    assert conn.host == "fast"
