"""Bounded async connection pool around ``Client``.

Ergonomics modeled on asyncpg's:

    pool = ch.create_pool("clickhouse://...", min_size=2, max_size=8)
    async with pool:
        async with pool.acquire() as client:
            rows = await client.fetch_all("SELECT 1")

The pool is lazy — connections are opened on first ``acquire()``, never
at ``create_pool`` time — bounded by ``max_size``, and dispenses
connections in FIFO order to waiters. ``acquire_timeout`` caps how long
``acquire()`` will wait when every connection is in use.

What the pool deliberately does **not** do (per ``DESIGN.md`` §5):

- Automatic query retry — INSERTs are not idempotent, SELECTs may be
  expensive; silently re-running them is a footgun. Connection-level
  reconnect on acquire is fine; query-level retry is the caller's.
- Multiplex — one client per acquire; the wire protocol does not
  multiplex queries on a single connection.
- Multi-host load balancing — roadmap.
"""

from __future__ import annotations

import asyncio
import dataclasses
import ssl as _ssl_module
import time
from collections.abc import Mapping
from dataclasses import dataclass
from types import TracebackType

from clickhouse_async._host_rotation import _HostRotation
from clickhouse_async.client import Client, QueryResult
from clickhouse_async.connection import TransportFactory
from clickhouse_async.dsn import DSN, parse_dsn
from clickhouse_async.errors import (
    ClickHouseError,
    PoolClosedError,
    PoolTimeoutError,
)


@dataclass
class _PoolEntry:
    """A pooled client plus the metadata the pool uses for health
    checks (``last_returned_at``) and lifetime caps (``opened_at``)."""

    client: Client
    opened_at: float
    last_returned_at: float


class Pool:
    """Lazy-fill, bounded async pool of ``Client`` instances."""

    def __init__(
        self,
        dsn: str | DSN,
        *,
        min_size: int = 0,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
        max_lifetime: float = 600.0,
        health_check_after: float = 30.0,
        host_failover_cooldown: float = 5.0,
        ssl_context: _ssl_module.SSLContext | None = None,
        transport_factory: TransportFactory | None = None,
    ) -> None:
        if max_size < 1:
            raise ValueError(f"max_size must be ≥ 1, got {max_size}")
        if min_size < 0 or min_size > max_size:
            raise ValueError(
                f"min_size must be in [0, max_size]; got {min_size} "
                f"with max_size={max_size}"
            )
        if max_lifetime <= 0:
            raise ValueError(f"max_lifetime must be positive, got {max_lifetime}")
        if health_check_after < 0:
            raise ValueError(
                f"health_check_after must be ≥ 0, got {health_check_after}"
            )
        if host_failover_cooldown < 0:
            raise ValueError(
                f"host_failover_cooldown must be ≥ 0, got {host_failover_cooldown}"
            )
        self._dsn: DSN = dsn if isinstance(dsn, DSN) else parse_dsn(dsn)
        self._min_size = min_size
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._max_lifetime = max_lifetime
        self._health_check_after = health_check_after
        self._ssl_context = ssl_context
        self._transport_factory = transport_factory
        self._rotation = _HostRotation(self._dsn.hosts, cooldown=host_failover_cooldown)

        # Free queue's FIFO semantics give us per-waiter fairness for free.
        self._free: asyncio.Queue[_PoolEntry] = asyncio.Queue(maxsize=max_size)
        # Open + opening connections. Bumped under _lock before opening,
        # decremented if the open fails or on release-after-close.
        self._size: int = 0
        self._lock = asyncio.Lock()
        self._closed: bool = False

    # ---- introspection --------------------------------------------------

    @property
    def size(self) -> int:
        """Number of connections the pool currently owns (open + opening)."""
        return self._size

    @property
    def free_size(self) -> int:
        """Number of idle connections currently in the free queue."""
        return self._free.qsize()

    @property
    def is_closed(self) -> bool:
        return self._closed

    # ---- context manager ------------------------------------------------

    async def __aenter__(self) -> Pool:
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        await self.close()

    # ---- acquire / release ----------------------------------------------

    def acquire(self) -> _PoolAcquireContext:
        """Borrow a ``Client`` for the duration of the ``async with``.

        On exit the client returns to the pool, or is closed if the
        pool itself was closed in the meantime or if the connection is
        no longer healthy.
        """
        return _PoolAcquireContext(self)

    async def _acquire(self) -> Client:
        if self._closed:
            raise PoolClosedError("pool is closed")

        deadline = time.monotonic() + self._acquire_timeout

        while True:
            if self._closed:
                raise PoolClosedError("pool is closed")

            # Hot path: an idle entry is available right now.
            entry: _PoolEntry | None = None
            try:
                entry = self._free.get_nowait()
            except asyncio.QueueEmpty:
                entry = None

            if entry is not None:
                healthy = await self._verify_or_discard(entry)
                if healthy is not None:
                    return healthy
                # Health check failed; loop to acquire afresh.
                continue

            # Reserve a slot if we're below max_size; open lazily off
            # the lock so concurrent open()s don't serialise.
            opened: bool
            async with self._lock:
                if self._size < self._max_size:
                    self._size += 1
                    opened = True
                else:
                    opened = False

            if opened:
                try:
                    return await self._open_client()
                except BaseException:
                    async with self._lock:
                        self._size -= 1
                    raise

            # All slots in use — wait for a release.
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PoolTimeoutError(
                    f"acquire timed out after {self._acquire_timeout}s "
                    f"(max_size={self._max_size}, in_use="
                    f"{self._size - self.free_size})"
                )
            try:
                async with asyncio.timeout(remaining):
                    waited = await self._free.get()
            except TimeoutError:
                raise PoolTimeoutError(
                    f"acquire timed out after {self._acquire_timeout}s "
                    f"(max_size={self._max_size}, in_use="
                    f"{self._size - self.free_size})"
                ) from None
            healthy = await self._verify_or_discard(waited)
            if healthy is not None:
                return healthy
            # The just-released entry failed health check; loop.

    async def _verify_or_discard(self, entry: _PoolEntry) -> Client | None:
        """Return the underlying client if the entry passes the health
        check (and isn't past its lifetime cap). Otherwise close it,
        decrement size, and return ``None`` so the caller loops back
        to acquire again."""
        now = time.monotonic()

        # Lifetime cap: connections older than max_lifetime are recycled
        # on the way *out* of the pool too — defends against
        # session-timeout surprises and DNS rotation.
        if now - entry.opened_at > self._max_lifetime:
            await self._discard(entry.client)
            return None

        if not entry.client.is_alive:
            await self._discard(entry.client)
            return None

        # Health check via Ping/Pong if the entry has been idle long
        # enough that the socket might have been quietly closed.
        idle_for = now - entry.last_returned_at
        if idle_for >= self._health_check_after:
            try:
                await entry.client.ping()
            except (ClickHouseError, OSError):
                await self._discard(entry.client)
                return None

        return entry.client

    async def _discard(self, client: Client) -> None:
        await client.close()
        async with self._lock:
            self._size -= 1

    async def _release(self, client: Client) -> None:
        now = time.monotonic()

        # Pool was closed while client was in use — close the client.
        if self._closed:
            await self._discard(client)
            return

        # Discard if no longer healthy (BROKEN from a server reset,
        # CLOSED from a manual client.close inside the `with`).
        if not client.is_alive:
            await self._discard(client)
            return

        # Lifetime cap on release — stale connections never go back in.
        opened_at = self._opened_at_for(client)
        if now - opened_at > self._max_lifetime:
            await self._discard(client)
            return

        # Healthy — back into the free queue. put_nowait can't actually
        # block: the queue's maxsize matches max_size, and our _size
        # invariant means the queue can never overflow.
        self._free.put_nowait(
            _PoolEntry(
                client=client,
                opened_at=opened_at,
                last_returned_at=now,
            )
        )

    def _opened_at_for(self, client: Client) -> float:
        """Read the per-client ``_pool_opened_at`` annotation that
        ``_open_client`` stamps on freshly-minted clients. Falls back
        to ``now`` for clients we somehow didn't stamp (defensive)."""
        return getattr(client, "_pool_opened_at", time.monotonic())

    async def _open_client(self) -> Client:
        # Pull a freshly-rotated, cooldown-filtered candidate list from
        # the rotation; hand it to the Client (and onward to its
        # Connection) by overriding the DSN's hosts for this open. The
        # ``on_host_attempt`` callback feeds per-host outcomes back into
        # the rotation so dead replicas earn a cooldown.
        candidates = self._rotation.next_candidates()
        per_open_dsn = dataclasses.replace(self._dsn, hosts=candidates)

        def _on_attempt(host: tuple[str, int], exc: BaseException | None) -> None:
            if exc is None:
                self._rotation.record_success(host)
            else:
                self._rotation.record_failure(host)

        client = Client(
            per_open_dsn,
            ssl_context=self._ssl_context,
            transport_factory=self._transport_factory,
            on_host_attempt=_on_attempt,
        )
        await client.open()
        # Stamp the open timestamp on the client itself so we can read
        # it back at release time without threading state through every
        # `with pool.acquire()` block. setattr-by-name keeps ty happy
        # about Client's defined surface.
        setattr(client, "_pool_opened_at", time.monotonic())  # noqa: B010
        return client

    # ---- shutdown -------------------------------------------------------

    async def close(self) -> None:
        """Close the pool. Marks closed (no new acquires); closes every
        idle client. In-use clients are closed when released."""
        self._closed = True
        while not self._free.empty():
            entry = self._free.get_nowait()
            await entry.client.close()
            async with self._lock:
                self._size -= 1

    # ---- pass-through one-shots ----------------------------------------

    async def execute(
        self,
        sql: str,
        *,
        params: Mapping[str, object] | None = None,
        settings: Mapping[str, str] | None = None,
        query_id: str = "",
    ) -> QueryResult:
        """Acquire, run ``sql``, release. Returns the full
        ``QueryResult``."""
        async with self.acquire() as client:
            return await client.execute(
                sql, params=params, settings=settings, query_id=query_id
            )

    async def fetch_all(
        self,
        sql: str,
        *,
        params: Mapping[str, object] | None = None,
        settings: Mapping[str, str] | None = None,
        query_id: str = "",
    ) -> list[tuple[object, ...]]:
        """Acquire, run ``sql``, release. Returns just the rows."""
        async with self.acquire() as client:
            return await client.fetch_all(
                sql, params=params, settings=settings, query_id=query_id
            )

    async def fetch_one(
        self,
        sql: str,
        *,
        params: Mapping[str, object] | None = None,
        settings: Mapping[str, str] | None = None,
        query_id: str = "",
    ) -> tuple[object, ...] | None:
        """Acquire, run ``sql``, release. Returns the first row or
        ``None`` for an empty result."""
        async with self.acquire() as client:
            return await client.fetch_one(
                sql, params=params, settings=settings, query_id=query_id
            )


class _PoolAcquireContext:
    """Async context manager returned by ``Pool.acquire()``. Wrapping
    is in its own class so ``async with pool.acquire() as client`` is
    the natural shape, regardless of whether ``acquire()`` is
    implemented as async or sync."""

    __slots__ = ("_client", "_pool")

    def __init__(self, pool: Pool) -> None:
        self._pool = pool
        self._client: Client | None = None

    async def __aenter__(self) -> Client:
        self._client = await self._pool._acquire()
        return self._client

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        client = self._client
        self._client = None
        if client is not None:
            await self._pool._release(client)


def create_pool(
    dsn: str | DSN,
    *,
    min_size: int = 0,
    max_size: int = 10,
    acquire_timeout: float = 30.0,
    max_lifetime: float = 600.0,
    health_check_after: float = 30.0,
    host_failover_cooldown: float = 5.0,
    ssl_context: _ssl_module.SSLContext | None = None,
    transport_factory: TransportFactory | None = None,
) -> Pool:
    """Build an unopened pool. Connections open on first ``acquire()``.

    - ``max_lifetime``: connections older than this (seconds) are
      recycled on release. Defends against server-side session
      timeouts and DNS rotation.
    - ``health_check_after``: idle connections older than this are
      pinged on the way out of the pool; failed pings → discard +
      open fresh.
    - ``host_failover_cooldown``: for multi-host DSNs, how long
      (seconds) to skip a host that just failed before considering it
      again. Best-effort: if every host is in cooldown the rotation
      retries them all.

    ``transport_factory`` is a test-only injection point for the
    underlying socket pair; production callers should leave it unset.
    """
    return Pool(
        dsn,
        min_size=min_size,
        max_size=max_size,
        acquire_timeout=acquire_timeout,
        max_lifetime=max_lifetime,
        health_check_after=health_check_after,
        host_failover_cooldown=host_failover_cooldown,
        ssl_context=ssl_context,
        transport_factory=transport_factory,
    )
