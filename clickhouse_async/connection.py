"""The native-protocol Connection.

A ``Connection`` owns one TCP socket and one protocol state machine. It
is the layer the high-level ``Client`` is thin over. Responsibilities:
TCP open/close, the Hello handshake (which sets ``server_info`` and
``negotiated_revision``), the Query packet, the steady-state packet
loop with optional callbacks for the non-yielded packets, the INSERT
``send_data`` path, server-side parameter binding, cooperative
cancellation with bounded drain, and per-block LZ4 / ZSTD framing
when compression is enabled.

INSERT sequence (orchestrated by the higher-level ``Client``):

1. ``await conn.send_query("INSERT INTO t VALUES", ...)``
2. ``async for streamed in conn.iter_packets(): ...`` — the server
   replies with a header-only Data block (``n_rows == 0``) describing
   the table schema. ``break`` out of the loop after consuming it.
3. For each batch: ``await conn.send_data(block)`` where ``block``'s
   columns match the header.
4. ``await conn.send_data(None)`` to write the empty terminator block.
5. Re-enter the iterator to drain any remaining server packets
   (``Progress``, ``ProfileInfo``, eventually ``EndOfStream``); the
   connection returns to READY.

Single-task model: the connection never spawns a background reader
task. Every read happens on the calling task, so cancellation lives
in one place and there's no "who owns the buffer" problem when a user
breaks out of a streamed iterator. Adding a reader task later would
undo the cancellation reasoning we get for free here.
"""

from __future__ import annotations

import asyncio
import logging
import ssl as _ssl_module
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import IntEnum
from typing import Literal, Protocol

from clickhouse_async.errors import (
    ConcurrentQueryError,
    ConnectError,
    ProtocolError,
    QueryCancellationError,
    ServerError,
    UnsupportedFeatureError,
)
from clickhouse_async.protocol.block import Block, BlockInfo
from clickhouse_async.protocol.compression import (
    CompressionMethod,
    write_block_framed,
)
from clickhouse_async.protocol.exception_packet import read_exception_body
from clickhouse_async.protocol.handshake import (
    ServerInfo,
    read_server_hello,
    write_client_hello,
)
from clickhouse_async.protocol.io import AsyncBinaryReader, BinaryWriter
from clickhouse_async.protocol.packets import (
    DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY,
    OUR_REVISION,
    ClientPacket,
    ServerPacket,
)
from clickhouse_async.protocol.parameters import format_param
from clickhouse_async.protocol.query_packet import write_query_packet
from clickhouse_async.protocol.server_packets import (
    ProfileInfo,
    ProgressInfo,
    read_block_packet_body,
    read_profile_info,
    read_progress,
    read_table_columns,
    read_timezone_update,
)

_logger = logging.getLogger(__name__)


class State(IntEnum):
    IDLE = 0
    CONNECTING = 1
    READY = 2
    IN_FLIGHT = 3
    BROKEN = 4
    CLOSED = 5


BlockKind = Literal["data", "totals", "extremes"]


@dataclass
class StreamedBlock:
    """A block yielded by ``iter_packets`` together with its kind tag.

    ClickHouse can interleave the regular result rows (``DATA``) with
    aggregate ``TOTALS`` and ``EXTREMES`` blocks before ``EndOfStream``;
    the kind tag lets callers route them appropriately without parsing
    packet ids themselves.
    """

    kind: BlockKind
    block: Block


class _WriterLike(Protocol):
    """The slice of ``asyncio.StreamWriter`` we actually call.

    A test ``ScriptedTransport`` matches this protocol structurally so
    we can drive the connection without a real socket.
    """

    def write(self, data: bytes) -> None: ...
    def close(self) -> None: ...
    def is_closing(self) -> bool: ...
    async def drain(self) -> None: ...
    async def wait_closed(self) -> None: ...


TransportFactory = Callable[
    [str, int, _ssl_module.SSLContext | None],
    Awaitable[tuple[asyncio.StreamReader, _WriterLike]],
]


async def _default_transport_factory(
    host: str,
    port: int,
    ssl_context: _ssl_module.SSLContext | None,
) -> tuple[asyncio.StreamReader, _WriterLike]:
    return await asyncio.open_connection(host, port, ssl=ssl_context)


class Connection:
    """Native-protocol connection skeleton.

    ``open()`` brings the transport up and runs the Hello handshake;
    on success the connection ends in ``READY``. With multiple
    candidate hosts the open walks them in order, returning on the
    first success and raising ``ConnectError`` (with every per-host
    error attached) only if every candidate fails. ``close()`` is
    idempotent and safe from any state, including mid-open.
    """

    def __init__(
        self,
        hosts: Sequence[tuple[str, int]],
        *,
        ssl_context: _ssl_module.SSLContext | None = None,
        compression: CompressionMethod = CompressionMethod.NONE,
        transport_factory: TransportFactory | None = None,
        on_host_attempt: Callable[[tuple[str, int], BaseException | None], None]
        | None = None,
    ) -> None:
        if not hosts:
            raise ValueError("Connection requires at least one host")
        self._hosts: tuple[tuple[str, int], ...] = tuple(hosts)
        self._ssl_context = ssl_context
        self._compression = compression
        self._transport_factory: TransportFactory = (
            transport_factory or _default_transport_factory
        )
        # Hook called once per candidate at end-of-attempt. ``exc`` is
        # ``None`` on success, the underlying exception on failure.
        # Used by the pool's ``_HostRotation`` to track per-host
        # cooldowns without having to peek at private state.
        self._on_host_attempt = on_host_attempt
        self._state: State = State.IDLE
        self._reader: AsyncBinaryReader | None = None
        self._writer: _WriterLike | None = None
        # Set on successful open(); None until then.
        self._connected_host: tuple[str, int] | None = None
        self._server_info: ServerInfo | None = None
        self._negotiated_revision: int = 0
        self._user: str = ""
        # Set while a cancel() is mid-flight so iter_packets's recursive
        # invocation from inside cancel doesn't itself try to cancel.
        self._cancel_in_flight: bool = False
        # Callback hooks for the non-yielded server packets. Assignable
        # by callers; default no-ops.
        self.on_progress: Callable[[ProgressInfo], None] | None = None
        self.on_profile_info: Callable[[ProfileInfo], None] | None = None
        self.on_profile_events: Callable[[Block], None] | None = None
        self.on_log: Callable[[Block], None] | None = None
        self.on_table_columns: Callable[[str, str], None] | None = None
        self.on_timezone_update: Callable[[str], None] | None = None
        # The server's last-emitted session timezone via the
        # ``TIMEZONE_UPDATE`` packet (or the handshake's static
        # ``ServerInfo.timezone`` until that fires). Threaded down to
        # ``read_block_packet_body`` so naive ``DateTime`` columns
        # honour the session zone instead of silently UTC.
        self._session_timezone: str | None = None
        # (from_state, to_state, reason) per transition — load-bearing for
        # tests and a useful debugging breadcrumb in production logs.
        self._transitions: list[tuple[State, State, str]] = []

    # ---- introspection ---------------------------------------------------

    @property
    def state(self) -> State:
        return self._state

    @property
    def hosts(self) -> tuple[tuple[str, int], ...]:
        """Candidate host list as configured. ``open()`` walks these
        in order; whichever wins is exposed via ``host`` / ``port``."""
        return self._hosts

    @property
    def host(self) -> str:
        """Host of the connected peer if open succeeded; the first
        candidate otherwise (a defensive default for callers that
        introspect before / during connect)."""
        if self._connected_host is not None:
            return self._connected_host[0]
        return self._hosts[0][0]

    @property
    def port(self) -> int:
        """Port of the connected peer if open succeeded; the first
        candidate's port otherwise."""
        if self._connected_host is not None:
            return self._connected_host[1]
        return self._hosts[0][1]

    @property
    def transitions(self) -> list[tuple[State, State, str]]:
        """Read-only view of the state transitions this connection has
        gone through. Tests use this to assert the right reasons fired."""
        return list(self._transitions)

    @property
    def server_info(self) -> ServerInfo:
        """Server identity captured during the Hello handshake.

        Raises if the connection hasn't completed the handshake yet —
        otherwise we'd hand callers a half-populated value.
        """
        if self._server_info is None:
            raise RuntimeError(
                f"server_info is not available in state {self._state.name}"
            )
        return self._server_info

    @property
    def negotiated_revision(self) -> int:
        """``min(OUR_REVISION, server_revision)`` — the revision used to
        gate every wire-format decision past the handshake."""
        return self._negotiated_revision

    @property
    def session_timezone(self) -> str | None:
        """The server's session timezone — seeded from the handshake's
        ``ServerInfo.timezone`` and refined by any ``TIMEZONE_UPDATE``
        packets received mid-query.

        ``None`` until the handshake completes; thereafter usually the
        IANA zone name the server reports (``"UTC"`` /
        ``"Europe/Berlin"`` / etc.). ``read_block`` threads this
        through ``parse_type`` so naive ``DateTime`` reads land in
        this zone instead of silently UTC.
        """
        return self._session_timezone

    # ---- lifecycle -------------------------------------------------------

    async def open(
        self,
        *,
        user: str = "default",
        password: str = "",
        database: str = "default",
    ) -> None:
        """Open the transport and run the Hello handshake.

        With multiple candidate hosts, walks them in order and returns
        on the first successful handshake. Per-host failures are
        recorded; if every candidate fails the call raises
        ``ConnectError`` whose message names every attempted
        ``host:port`` and the underlying error class.

        On success the connection ends in ``READY`` with ``server_info``
        populated and ``negotiated_revision`` set to
        ``min(OUR_REVISION, server.revision)``. ``host`` / ``port``
        report the candidate that won.

        A server-side rejection of our Hello (the server replies
        ``Exception``) counts as a per-host failure and falls through
        to the next candidate just like a transport error. The
        connection is left ``BROKEN`` with the writer closed when every
        candidate fails; cancellation during open propagates as
        ``CancelledError`` without converting to ``ConnectError``.
        """
        if self._state != State.IDLE:
            raise RuntimeError(f"open() requires IDLE state, got {self._state.name}")
        self._user = user
        self._transition(State.CONNECTING, "open()")

        host_errors: list[tuple[str, int, str, BaseException]] = []
        for host, port in self._hosts:
            stage = "transport open"
            try:
                reader, writer = await self._transport_factory(
                    host, port, self._ssl_context
                )
                self._reader = AsyncBinaryReader(reader)
                self._writer = writer
                stage = "handshake"
                await self._do_handshake(
                    user=user, password=password, database=database
                )
            except asyncio.CancelledError:
                # Cancellation isn't a per-host failure — propagate.
                self._transition(
                    State.BROKEN, f"cancelled during {stage} for {host}:{port}"
                )
                await self._cleanup_writer()
                raise
            except BaseException as exc:
                host_errors.append((host, port, stage, exc))
                if self._on_host_attempt is not None:
                    self._on_host_attempt((host, port), exc)
                # Drop the half-opened transport so the next candidate
                # starts from a clean slate.
                await self._cleanup_writer()
                continue
            # Success.
            self._connected_host = (host, port)
            if self._on_host_attempt is not None:
                self._on_host_attempt((host, port), None)
            return

        # Every candidate failed — mark broken. The BROKEN reason names
        # the failing stage so the transition log matches v0's shape.
        if len(host_errors) == 1:
            host, port, stage, exc = host_errors[0]
            self._transition(
                State.BROKEN,
                f"{stage} failed for {host}:{port}: {exc!r}",
            )
            # Surface the underlying error directly so callers can
            # ``except ServerError`` / ``except ConnectionRefusedError``
            # the way v0 documented.
            raise exc
        self._transition(
            State.BROKEN,
            f"all {len(host_errors)} candidate host(s) failed",
        )
        # Multi-host: wrap so the message names every attempted candidate
        # and the per-host exceptions are preserved on ``host_errors``.
        raise ConnectError([(h, p, e) for (h, p, _stage, e) in host_errors])

    async def _do_handshake(self, *, user: str, password: str, database: str) -> None:
        assert self._reader is not None and self._writer is not None
        # Send client Hello as one buffered write — the protocol expects
        # the whole packet to arrive together.
        out = BinaryWriter()
        write_client_hello(out, user=user, password=password, database=database)
        self._writer.write(out.getvalue())
        await self._writer.drain()

        # Read the server's response. The packet id determines whether
        # the server accepted us (HELLO) or rejected us (EXCEPTION).
        packet_id = await self._reader.read_varuint()
        if packet_id == ServerPacket.HELLO:
            info = await read_server_hello(self._reader)
            self._server_info = info
            self._negotiated_revision = min(OUR_REVISION, info.revision)
            # Seed the session timezone from the handshake; the server
            # may later refine it via a TIMEZONE_UPDATE packet mid-query.
            self._session_timezone = info.timezone or None
            await self._send_addendum()
            self._transition(
                State.READY,
                f"handshake complete (server={info.name!r} "
                f"revision={info.revision}, "
                f"negotiated={self._negotiated_revision})",
            )
        elif packet_id == ServerPacket.EXCEPTION:
            raise await read_exception_body(self._reader)
        else:
            raise ProtocolError(
                f"unexpected packet id {packet_id} during handshake "
                f"(expected HELLO={ServerPacket.HELLO.value} or "
                f"EXCEPTION={ServerPacket.EXCEPTION.value})"
            )

    # ---- queries ---------------------------------------------------------

    async def send_query(
        self,
        sql: str,
        *,
        query_id: str = "",
        settings: Mapping[str, str] | None = None,
        params: Mapping[str, object] | None = None,
    ) -> None:
        """Send a Query packet for ``sql``. Transitions ``READY → IN_FLIGHT``.

        ``params`` are server-side query parameters: each value is
        formatted via ``format_param`` and emitted in the parameters
        block of the Query packet. The placeholder *type* lives in the
        SQL itself (``WHERE day = {d:Date}``); the wire only carries
        textual values. The negotiated revision must be at or above
        ``DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS`` — older servers
        raise ``UnsupportedFeatureError`` rather than silently falling
        back to client-side string interpolation.

        Concurrent calls on the same connection raise
        ``ConcurrentQueryError`` rather than queueing or fanning out.
        """
        if self._state == State.IN_FLIGHT:
            raise ConcurrentQueryError(
                "another query is already in flight on this connection"
            )
        if self._state != State.READY:
            raise RuntimeError(
                f"send_query() requires READY state, got {self._state.name}"
            )
        assert self._writer is not None  # READY implies handshake completed

        formatted_params: dict[str, str] | None = None
        if params:
            if self._negotiated_revision < DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS:
                raise UnsupportedFeatureError(
                    f"server-side query parameters require revision "
                    f"{DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS}; the "
                    f"negotiated revision with this server is "
                    f"{self._negotiated_revision}. Upgrade the server "
                    f"or remove `params=...` from the call."
                )
            formatted_params = {
                name: format_param(value) for name, value in params.items()
            }

        out = BinaryWriter()
        write_query_packet(
            out,
            sql=sql,
            query_id=query_id,
            user=self._user,
            revision=self._negotiated_revision,
            settings=settings,
            parameters=formatted_params,
            compression=self._compression != CompressionMethod.NONE,
        )
        self._writer.write(out.getvalue())
        await self._writer.drain()
        self._transition(
            State.IN_FLIGHT,
            f"send_query(query_id={query_id!r}, len(sql)={len(sql)})",
        )

    async def send_data(self, block: Block | None) -> None:
        """Send a Data packet during an INSERT.

        Pass ``None`` for the empty-block terminator that signals
        end-of-data. State stays IN_FLIGHT — the call returns to READY
        only when the user resumes ``iter_packets`` and the server
        emits ``EndOfStream``.

        v0 does not validate the block's columns against the header
        the server emitted; misaligned schemas surface as a
        ``ServerError`` from the next ``iter_packets`` read. Header
        validation lives at the higher-level ``Client`` where the
        full header→block flow is owned.
        """
        if self._state != State.IN_FLIGHT:
            raise RuntimeError(
                f"send_data() requires IN_FLIGHT state, got {self._state.name}"
            )
        assert self._writer is not None

        out = BinaryWriter()
        out.write_varuint(ClientPacket.DATA)
        out.write_string("")  # external table name (empty = main table)
        write_block_framed(
            out,
            block if block is not None else Block(info=BlockInfo()),
            revision=self._negotiated_revision,
            compression=self._compression,
        )
        self._writer.write(out.getvalue())
        await self._writer.drain()

    async def ping(self) -> None:
        """Send a ``Ping`` packet and read the matching ``Pong``.

        Used by the pool's health-check path on acquire and by user
        code that wants to verify the connection is alive without
        running a query. Requires ``READY``; remains ``READY`` on
        success. A non-Pong response, IO error, or unexpected packet
        marks the connection ``BROKEN`` and raises.
        """
        if self._state != State.READY:
            raise RuntimeError(f"ping() requires READY state, got {self._state.name}")
        assert self._reader is not None and self._writer is not None

        out = BinaryWriter()
        out.write_varuint(ClientPacket.PING)
        try:
            self._writer.write(out.getvalue())
            await self._writer.drain()
        except BaseException as exc:
            self._transition(State.BROKEN, f"ping send failed: {exc!r}")
            await self._cleanup_writer()
            raise

        packet_id = await self._reader.read_varuint()
        if packet_id != ServerPacket.PONG:
            self._transition(
                State.BROKEN, f"unexpected packet id {packet_id} after Ping"
            )
            raise ProtocolError(
                f"expected PONG ({int(ServerPacket.PONG)}) after Ping, "
                f"got packet id {packet_id}"
            )

    async def cancel(self, *, drain_timeout: float = 5.0) -> None:
        """Cancel the current query.

        Always raises ``QueryCancellationError`` describing the outcome
        — never returns silently. The reason on the raised error tells
        the caller which path the cancel took:

        - ``READY``: no-op return (the only path that doesn't raise).
          Cancel is meaningful only mid-query.
        - ``IN_FLIGHT``: send a ``Cancel`` packet, drain whatever the
          server emits afterwards (Progress / EndOfStream / Exception)
          bounded by ``drain_timeout``. Clean drain → ``READY`` plus
          ``reason="drained"``. Timeout → close the writer, transition
          to ``BROKEN``, raise ``reason="timeout"``.
        - Another ``cancel()`` is in flight: raise
          ``reason="already_cancelled"`` without disturbing the first.
        - ``BROKEN`` / ``CLOSED``: raise ``reason="not_in_flight"`` —
          there's nothing to cancel.

        Cancellation safety: every ``await`` inside leaves the
        connection in a state callers can reason about — either still
        mid-cancel (we'll raise on completion) or ``BROKEN`` (the
        writer is closed). No half-broken state.
        """
        if self._state == State.READY:
            return
        if self._cancel_in_flight:
            raise QueryCancellationError(
                reason="already_cancelled",
                message="another cancel is already in flight on this connection",
            )
        if self._state != State.IN_FLIGHT:
            raise QueryCancellationError(
                reason="not_in_flight",
                message=(
                    f"cancel() requires IN_FLIGHT (or READY for no-op), "
                    f"got {self._state.name}"
                ),
            )
        assert self._writer is not None

        self._cancel_in_flight = True
        try:
            # Send the Cancel packet — just the varuint id, no body.
            try:
                out = BinaryWriter()
                out.write_varuint(ClientPacket.CANCEL)
                self._writer.write(out.getvalue())
                await self._writer.drain()
            except BaseException as exc:
                self._transition(State.BROKEN, f"cancel send failed: {exc!r}")
                await self._cleanup_writer()
                raise

            # Drain the rest of the query, bounded by drain_timeout.
            # Any ServerError mid-drain is the server's cancel-ack
            # (typically code QUERY_WAS_CANCELLED) — swallow it; the
            # connection has already moved to READY in iter_packets.
            try:
                async with asyncio.timeout(drain_timeout):
                    try:
                        async for _ in self.iter_packets():
                            pass
                    except ServerError:
                        pass
            except TimeoutError:
                self._transition(
                    State.BROKEN, f"cancel drain timed out after {drain_timeout}s"
                )
                await self._cleanup_writer()
                raise QueryCancellationError(
                    reason="timeout",
                    message=(
                        f"server did not finish the cancelled query within "
                        f"{drain_timeout}s; connection is now BROKEN"
                    ),
                ) from None

            # Clean drain — connection is READY (set by iter_packets).
            raise QueryCancellationError(
                reason="drained",
                message="query cancelled cleanly; connection is READY",
            )
        finally:
            self._cancel_in_flight = False

    async def iter_packets(self) -> AsyncIterator[StreamedBlock]:
        """Yield each block-bearing packet until ``EndOfStream`` or
        ``Exception``.

        Block-bearing packets — ``DATA`` / ``TOTALS`` / ``EXTREMES`` —
        are yielded as ``StreamedBlock`` so callers can route them by
        kind without parsing packet ids.

        Non-yielding packets fire the matching callback (``on_progress``,
        ``on_profile_info``, ``on_profile_events``, ``on_log``,
        ``on_table_columns``, ``on_timezone_update``) and the loop
        continues. Setting a callback to ``None`` (the default) drops
        the data on the floor — every packet's body is still consumed
        so the wire stays in sync.

        ``EXCEPTION`` mid-query raises ``ServerError`` and returns the
        connection to READY (a query-level error isn't a transport
        failure). ``END_OF_STREAM`` terminates cleanly. Distributed-read
        packets (``PART_UUIDS``, ``READ_TASK_REQUEST``,
        ``MERGE_TREE_*``) and any unrecognised id mark the connection
        BROKEN — initial-query connections shouldn't see those.
        """
        if self._state != State.IN_FLIGHT:
            raise RuntimeError(
                f"iter_packets() requires IN_FLIGHT state, got {self._state.name}"
            )
        assert self._reader is not None
        revision = self._negotiated_revision
        # DATA / TOTALS / EXTREMES blocks travel through the connection's
        # negotiated compression; LOG / PROFILE_EVENTS are always raw
        # (upstream sendLogs / sendProfileEvents bypass the compression
        # layer regardless of the per-query compression flag).
        compression = self._compression
        while True:
            # Re-read on each iteration so a TIMEZONE_UPDATE packet
            # mid-query updates the codec's view for subsequent blocks.
            session_tz = self._session_timezone
            packet_id = await self._reader.read_varuint()
            if packet_id == ServerPacket.DATA:
                _, block = await read_block_packet_body(
                    self._reader,
                    revision=revision,
                    compression=compression,
                    session_timezone=session_tz,
                )
                yield StreamedBlock(kind="data", block=block)
            elif packet_id == ServerPacket.TOTALS:
                _, block = await read_block_packet_body(
                    self._reader,
                    revision=revision,
                    compression=compression,
                    session_timezone=session_tz,
                )
                yield StreamedBlock(kind="totals", block=block)
            elif packet_id == ServerPacket.EXTREMES:
                _, block = await read_block_packet_body(
                    self._reader,
                    revision=revision,
                    compression=compression,
                    session_timezone=session_tz,
                )
                yield StreamedBlock(kind="extremes", block=block)
            elif packet_id == ServerPacket.END_OF_STREAM:
                self._transition(State.READY, "EndOfStream")
                return
            elif packet_id == ServerPacket.EXCEPTION:
                err = await read_exception_body(self._reader)
                self._transition(State.READY, f"server exception: {err.name}")
                raise err
            elif packet_id == ServerPacket.PROGRESS:
                progress = await read_progress(self._reader, revision=revision)
                if self.on_progress is not None:
                    self.on_progress(progress)
            elif packet_id == ServerPacket.PROFILE_INFO:
                pinfo = await read_profile_info(self._reader, revision=revision)
                if self.on_profile_info is not None:
                    self.on_profile_info(pinfo)
            elif packet_id == ServerPacket.PROFILE_EVENTS:
                _, block = await read_block_packet_body(
                    self._reader,
                    revision=revision,
                    session_timezone=session_tz,
                )
                if self.on_profile_events is not None:
                    self.on_profile_events(block)
            elif packet_id == ServerPacket.LOG:
                _, block = await read_block_packet_body(
                    self._reader,
                    revision=revision,
                    session_timezone=session_tz,
                )
                if self.on_log is not None:
                    self.on_log(block)
            elif packet_id == ServerPacket.TABLE_COLUMNS:
                default_table_name, columns = await read_table_columns(self._reader)
                if self.on_table_columns is not None:
                    self.on_table_columns(default_table_name, columns)
            elif packet_id == ServerPacket.TIMEZONE_UPDATE:
                tz = await read_timezone_update(self._reader)
                # Capture before firing the user callback so a hook
                # that introspects ``conn.session_timezone`` sees the
                # new value, not the stale one.
                self._session_timezone = tz or None
                if self.on_timezone_update is not None:
                    self.on_timezone_update(tz)
            else:
                self._transition(State.BROKEN, f"unexpected packet id {packet_id}")
                raise ProtocolError(
                    f"unexpected packet id {packet_id} during query "
                    f"(distributed-read packets PART_UUIDS / "
                    f"READ_TASK_REQUEST / MERGE_TREE_* are not expected on "
                    f"initial-query connections)"
                )

    async def _send_addendum(self) -> None:
        """Send the post-Hello addendum the server expects from clients
        whose negotiated revision is at or above
        ``DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM``.

        At ``OUR_REVISION`` the addendum is just one length-prefixed
        string — the quota key, which we don't use — so we always emit
        an empty string. Newer revisions add chunked-protocol
        negotiation and parallel-replicas versioning; both gates sit
        above ``OUR_REVISION`` and surface as no-ops here.
        """
        if self._negotiated_revision < DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM:
            return
        assert self._writer is not None
        out = BinaryWriter()
        if self._negotiated_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY:
            out.write_string("")  # empty quota_key — we don't use quotas
        if len(out) == 0:
            return
        self._writer.write(out.getvalue())
        await self._writer.drain()

    async def _cleanup_writer(self) -> None:
        writer = self._writer
        self._writer = None
        self._reader = None
        if writer is None or writer.is_closing():
            return
        writer.close()
        try:
            await writer.wait_closed()
        except (OSError, asyncio.CancelledError):
            pass

    async def close(self) -> None:
        """Close the transport and transition to ``CLOSED``.

        Idempotent and safe to call from any state, including mid-open.
        """
        if self._state == State.CLOSED:
            return
        self._transition(State.CLOSED, "close()")
        writer = self._writer
        self._writer = None
        self._reader = None
        if writer is not None and not writer.is_closing():
            writer.close()
            try:
                await writer.wait_closed()
            except (OSError, asyncio.CancelledError):
                # Already-closed sockets and cancellation during shutdown
                # aren't load-bearing — we're closing anyway.
                pass

    # ---- internals -------------------------------------------------------

    def _transition(self, new_state: State, reason: str) -> None:
        old = self._state
        self._state = new_state
        self._transitions.append((old, new_state, reason))
        _logger.debug(
            "connection state %s -> %s (%s)", old.name, new_state.name, reason
        )
