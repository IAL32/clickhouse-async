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
import logging
import ssl as _ssl_module
import time
from collections import deque
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

_logger = logging.getLogger(__name__)


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
        max_idle_time: float = 300.0,
        idle_check_interval: float = 30.0,
        enable_reaper: bool = True,
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
        if max_idle_time <= 0:
            raise ValueError(f"max_idle_time must be positive, got {max_idle_time}")
        if idle_check_interval <= 0:
            raise ValueError(
                f"idle_check_interval must be positive, got {idle_check_interval}"
            )
        if health_check_after < 0:
            raise ValueError(
                f"health_check_after must be ≥ 0, got {health_check_after}"
            )
        if host_failover_cooldown < 0:
            raise ValueError(
                f"host_failover_cooldown must be ≥ 0, got {host_failover_cooldown}"
            )
        # ``min_size`` only gets actively warmed by the background reaper —
        # without one nothing opens connections proactively. Refusing the
        # contradiction here is friendlier than silently leaving the
        # parameter unenforced.
        if min_size > 0 and not enable_reaper:
            raise ValueError(
                f"min_size={min_size} requires enable_reaper=True; the "
                f"reaper is what keeps the pool warm to min_size"
            )
        self._dsn: DSN = dsn if isinstance(dsn, DSN) else parse_dsn(dsn)
        self._min_size = min_size
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._max_lifetime = max_lifetime
        self._max_idle_time = max_idle_time
        self._idle_check_interval = idle_check_interval
        self._enable_reaper = enable_reaper
        self._health_check_after = health_check_after
        self._ssl_context = ssl_context
        self._transport_factory = transport_factory
        self._rotation = _HostRotation(self._dsn.hosts, cooldown=host_failover_cooldown)

        # FIFO deque + Condition: the reaper needs to scan entries by
        # last_returned_at without consuming them, which Queue can't do.
        # Condition gives us the wakeup signalling Queue did for free —
        # release notifies, acquire wait_for()s, the per-waiter ordering
        # falls out of asyncio's FIFO future scheduling.
        self._free: deque[_PoolEntry] = deque()
        self._cond: asyncio.Condition = asyncio.Condition()
        # Open + opening connections. Bumped under the cond's lock before
        # opening, decremented if the open fails or on release-after-close.
        self._size: int = 0
        self._closed: bool = False
        # Lazily started on first acquire so an idle Pool doesn't burn
        # a task. Cancelled and awaited in close().
        self._reaper_task: asyncio.Task[None] | None = None

    # ---- introspection --------------------------------------------------

    @property
    def size(self) -> int:
        """Number of connections the pool currently owns (open + opening)."""
        return self._size

    @property
    def free_size(self) -> int:
        """Number of idle connections currently in the free queue."""
        return len(self._free)

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

        self._start_reaper()
        deadline = time.monotonic() + self._acquire_timeout

        while True:
            if self._closed:
                raise PoolClosedError("pool is closed")

            # Decide what to do under the cond's lock: claim a free
            # entry, reserve a slot for opening, or wait for a release.
            action: str
            entry: _PoolEntry | None = None
            async with self._cond:
                if self._free:
                    entry = self._free.popleft()
                    action = "verify"
                elif self._size < self._max_size:
                    self._size += 1
                    action = "open"
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeoutError(
                            f"acquire timed out after {self._acquire_timeout}s "
                            f"(max_size={self._max_size}, in_use="
                            f"{self._size - self.free_size})"
                        )
                    try:
                        await asyncio.wait_for(self._cond.wait(), timeout=remaining)
                    except TimeoutError:
                        raise PoolTimeoutError(
                            f"acquire timed out after {self._acquire_timeout}s "
                            f"(max_size={self._max_size}, in_use="
                            f"{self._size - self.free_size})"
                        ) from None
                    continue

            # Long-running operations happen *outside* the lock so other
            # tasks can interleave on the pool while this one verifies
            # / opens.
            if action == "verify":
                assert entry is not None
                healthy = await self._verify_or_discard(entry)
                if healthy is not None:
                    return healthy
                continue
            # action == "open"
            try:
                return await self._open_client()
            except BaseException:
                async with self._cond:
                    self._size -= 1
                    self._cond.notify_all()
                raise

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
        async with self._cond:
            self._size -= 1
            # A waiter blocked on capacity can now reserve our slot.
            self._cond.notify_all()

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

        # Healthy — back into the free deque under the lock so a waiter
        # sees the deque's update and the cond notification atomically.
        async with self._cond:
            self._free.append(
                _PoolEntry(
                    client=client,
                    opened_at=opened_at,
                    last_returned_at=now,
                )
            )
            self._cond.notify()

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

    # ---- reaper ---------------------------------------------------------

    def _start_reaper(self) -> None:
        """Spawn the idle reaper task on first ``acquire()``. Idempotent.

        Runs only when the pool has actually been used so an idle
        ``create_pool`` doesn't burn an event-loop task. A no-op if
        ``enable_reaper=False`` was passed at construction.
        ``close()`` cancels and awaits the task before returning.
        """
        if self._reaper_task is not None or self._closed or not self._enable_reaper:
            return
        self._reaper_task = asyncio.create_task(
            self._reaper_loop(), name="clickhouse-async pool reaper"
        )

    async def _reaper_loop(self) -> None:
        """Periodic reaper: closes connections idle past ``max_idle_time``
        while keeping the population at or above ``min_size``.

        Errors during a pass are logged, never raised — the reaper is
        best-effort and shouldn't take the pool down for a transient
        server issue.
        """
        try:
            while True:
                try:
                    await asyncio.sleep(self._idle_check_interval)
                except asyncio.CancelledError:
                    return
                if self._closed:
                    return
                try:
                    await self._reaper_pass()
                except asyncio.CancelledError:
                    return
                except Exception:
                    _logger.exception("pool reaper pass raised")
        finally:
            # Defensive: if we exit the loop for any reason, leave a
            # breadcrumb so a developer staring at a dead pool has
            # something to grep for.
            _logger.debug("pool reaper task finished")

    async def _reaper_pass(self) -> None:
        """One sweep: drop stale free entries above ``min_size``, then
        warm to ``min_size`` if the pool has fewer connections."""
        if self._closed:
            return
        now = time.monotonic()

        # Phase 1: drop stale entries while size > min_size. We rebuild
        # the deque with survivors; closing happens off the lock.
        to_close: list[Client] = []
        async with self._cond:
            survivors: deque[_PoolEntry] = deque()
            for entry in self._free:
                if (
                    self._size > self._min_size
                    and (now - entry.last_returned_at) > self._max_idle_time
                ):
                    to_close.append(entry.client)
                    self._size -= 1
                else:
                    survivors.append(entry)
            self._free = survivors
            if to_close:
                self._cond.notify_all()

        for client in to_close:
            try:
                await client.close()
            except Exception:
                _logger.exception("pool reaper: close failed for stale entry")

        # Phase 2: warm the pool back up to min_size. One open per
        # iteration; bail on the first failure so a flapping server
        # doesn't burn the reaper in a tight loop.
        while True:
            if self._closed:
                return
            async with self._cond:
                if self._size >= self._min_size:
                    return
                self._size += 1
            try:
                client = await self._open_client()
            except Exception:
                _logger.warning(
                    "pool reaper: warm open failed; will retry on next pass",
                    exc_info=True,
                )
                async with self._cond:
                    self._size -= 1
                    self._cond.notify_all()
                return
            warm_now = time.monotonic()
            async with self._cond:
                if self._closed:
                    # Pool closed while we were opening — drop the new
                    # client cleanly.
                    self._size -= 1
                    self._cond.notify_all()
                    await client.close()
                    return
                self._free.append(
                    _PoolEntry(
                        client=client,
                        opened_at=warm_now,
                        last_returned_at=warm_now,
                    )
                )
                self._cond.notify()

    # ---- shutdown -------------------------------------------------------

    async def close(self) -> None:
        """Close the pool. Marks closed (no new acquires); cancels the
        reaper; closes every idle client. In-use clients are closed
        when released."""
        self._closed = True

        # Wake any acquire() waiters so they observe the closed flag.
        async with self._cond:
            self._cond.notify_all()

        if self._reaper_task is not None:
            self._reaper_task.cancel()
            try:
                await asyncio.wait_for(self._reaper_task, timeout=2.0)
            except (TimeoutError, asyncio.CancelledError):
                pass
            self._reaper_task = None

        # Drain idle entries.
        while True:
            async with self._cond:
                if not self._free:
                    break
                entry = self._free.popleft()
                self._size -= 1
            try:
                await entry.client.close()
            except Exception:
                _logger.exception("pool close: client.close() raised")

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

    async def kill_query(self, query_id: str, *, sync: bool = True) -> int:
        """Cancel a running query identified by ``query_id`` from a
        side-channel connection borrowed from this pool.

        Equivalent to ``async with pool.acquire() as c:
        await c.kill_query(query_id, sync=sync)`` — pulled out so the
        common case of "I have a query_id, please kill it" doesn't
        need its own ``async with`` block at the call site.

        Returns the number of queries the server confirmed it
        targeted (each row in the ``KILL QUERY`` result corresponds
        to one targeted query). Permissions and sync semantics match
        ``Client.kill_query``; see that docstring for the details.
        """
        async with self.acquire() as client:
            return await client.kill_query(query_id, sync=sync)


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
    max_idle_time: float = 300.0,
    idle_check_interval: float = 30.0,
    enable_reaper: bool = True,
    health_check_after: float = 30.0,
    host_failover_cooldown: float = 5.0,
    ssl_context: _ssl_module.SSLContext | None = None,
    transport_factory: TransportFactory | None = None,
) -> Pool:
    """Build an unopened pool. Connections open on first ``acquire()``.

    - ``max_lifetime``: connections older than this (seconds) are
      recycled on release. Defends against server-side session
      timeouts and DNS rotation.
    - ``max_idle_time``: idle connections sitting in the free deque
      longer than this are closed by the background reaper, provided
      ``size > min_size``. Defaults to 5 minutes.
    - ``idle_check_interval``: how often (seconds) the reaper sweeps
      the free deque. Default 30 s.
    - ``enable_reaper``: when ``False``, the background idle reaper
      task is never started — ``max_idle_time`` and the ``min_size``
      warm aspect become no-ops. Useful in test harnesses or
      short-lived scripts where the per-acquire health check
      (``health_check_after``) and per-release lifetime cap
      (``max_lifetime``) are sufficient. Default ``True``.
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
        max_idle_time=max_idle_time,
        idle_check_interval=idle_check_interval,
        enable_reaper=enable_reaper,
        health_check_after=health_check_after,
        host_failover_cooldown=host_failover_cooldown,
        ssl_context=ssl_context,
        transport_factory=transport_factory,
    )
