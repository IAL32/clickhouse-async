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
import ssl as _ssl_module
from types import TracebackType

from clickhouse_async.client import Client
from clickhouse_async.connection import TransportFactory
from clickhouse_async.dsn import DSN, parse_dsn
from clickhouse_async.errors import PoolClosedError, PoolTimeoutError


class Pool:
    """Lazy-fill, bounded async pool of ``Client`` instances."""

    def __init__(
        self,
        dsn: str | DSN,
        *,
        min_size: int = 0,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
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
        self._dsn: DSN = dsn if isinstance(dsn, DSN) else parse_dsn(dsn)
        self._min_size = min_size
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._ssl_context = ssl_context
        self._transport_factory = transport_factory

        # Free queue's FIFO semantics give us per-waiter fairness for free.
        self._free: asyncio.Queue[Client] = asyncio.Queue(maxsize=max_size)
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

        # Hot path: an idle client is available right now.
        try:
            return self._free.get_nowait()
        except asyncio.QueueEmpty:
            pass

        # Reserve a slot if we're below max_size; open lazily off the lock.
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

        # All slots in use — wait for a release. Queue.get is FIFO across
        # waiters, so fairness is automatic.
        try:
            async with asyncio.timeout(self._acquire_timeout):
                return await self._free.get()
        except TimeoutError:
            raise PoolTimeoutError(
                f"acquire timed out after {self._acquire_timeout}s "
                f"(max_size={self._max_size}, in_use="
                f"{self._size - self.free_size})"
            ) from None

    async def _release(self, client: Client) -> None:
        # Pool was closed while client was in use — close the client.
        if self._closed:
            await client.close()
            async with self._lock:
                self._size -= 1
            return

        # Health check: only return READY clients to the pool. Anything
        # else (BROKEN from a server reset, CLOSED from a manual
        # client.close inside the `with`) is discarded.
        if not client.is_alive:
            await client.close()
            async with self._lock:
                self._size -= 1
            return

        # Healthy — back into the free queue. put_nowait can't actually
        # block: the queue's maxsize matches max_size, and our _size
        # invariant means the queue can never overflow.
        self._free.put_nowait(client)

    async def _open_client(self) -> Client:
        client = Client(
            self._dsn,
            ssl_context=self._ssl_context,
            transport_factory=self._transport_factory,
        )
        await client.open()
        return client

    # ---- shutdown -------------------------------------------------------

    async def close(self) -> None:
        """Close the pool. Marks closed (no new acquires); closes every
        idle client. In-use clients are closed when released."""
        self._closed = True
        while not self._free.empty():
            client = self._free.get_nowait()
            await client.close()
            async with self._lock:
                self._size -= 1


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
    ssl_context: _ssl_module.SSLContext | None = None,
    transport_factory: TransportFactory | None = None,
) -> Pool:
    """Build an unopened pool. Connections open on first ``acquire()``.

    ``transport_factory`` is a test-only injection point for the
    underlying socket pair; production callers should leave it unset.
    """
    return Pool(
        dsn,
        min_size=min_size,
        max_size=max_size,
        acquire_timeout=acquire_timeout,
        ssl_context=ssl_context,
        transport_factory=transport_factory,
    )
