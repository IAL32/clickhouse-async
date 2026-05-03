"""Tests for the connection pool: lazy fill, bounded acquire, FIFO,
acquire_timeout, close."""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from clickhouse_async import Pool, create_pool
from clickhouse_async.connection import _WriterLike
from clickhouse_async.errors import PoolClosedError, PoolTimeoutError

from ._mock_transport import ScriptedTransport
from ._scripted_packets import encode_server_hello


class _FreshTransports:
    """Test-only transport factory that mints a fresh ``ScriptedTransport``
    per client and pre-feeds a Hello reply so the handshake completes.

    Tests can grab a transport after open via ``transports[i]`` to feed
    query-shape responses.
    """

    def __init__(self) -> None:
        self.transports: list[ScriptedTransport] = []

    async def __call__(
        self,
        _host: str,
        _port: int,
        _ssl_context: ssl.SSLContext | None,
    ) -> tuple[asyncio.StreamReader, _WriterLike]:
        t = ScriptedTransport()
        t.feed(encode_server_hello())
        self.transports.append(t)
        return await t(_host, _port, _ssl_context)


@asynccontextmanager
async def _fresh_pool(
    *, max_size: int = 3, acquire_timeout: float = 30.0
) -> AsyncIterator[tuple[Pool, _FreshTransports]]:
    factory = _FreshTransports()
    pool = create_pool(
        "clickhouse://default:@host/db",
        max_size=max_size,
        acquire_timeout=acquire_timeout,
        transport_factory=factory,
    )
    async with pool:
        yield pool, factory


# ---- creation -----------------------------------------------------------


async def test_create_pool_does_not_open_connections_eagerly() -> None:
    # BEGIN: a pool factory that records every transport it creates
    factory = _FreshTransports()
    pool = create_pool(
        "clickhouse://default:@host/db",
        max_size=5,
        transport_factory=factory,
    )

    # WHEN: just creating and entering the pool's context
    async with pool:
        # THEN: no connections were opened — fill is lazy
        assert pool.size == 0
        assert pool.free_size == 0
        assert factory.transports == []


def test_create_pool_validates_size_arguments() -> None:
    # BEGIN / WHEN / THEN: max_size < 1 is rejected
    with pytest.raises(ValueError, match="max_size"):
        create_pool("clickhouse://host", max_size=0)
    # min_size > max_size is rejected
    with pytest.raises(ValueError, match="min_size"):
        create_pool("clickhouse://host", min_size=5, max_size=2)
    # negative min_size is rejected
    with pytest.raises(ValueError, match="min_size"):
        create_pool("clickhouse://host", min_size=-1)


# ---- acquire opens up to max_size --------------------------------------


async def test_acquire_opens_a_fresh_client_when_no_idle_available() -> None:
    # BEGIN: a fresh pool
    async with _fresh_pool() as (pool, factory):
        # WHEN: acquiring a client
        async with pool.acquire() as client:
            # THEN: one connection is open, one transport was minted,
            #       client is alive (handshake completed)
            assert pool.size == 1
            assert pool.free_size == 0
            assert len(factory.transports) == 1
            assert client.is_alive

        # After release the client is back in the free queue
        assert pool.size == 1
        assert pool.free_size == 1


async def test_acquire_reuses_idle_clients() -> None:
    # BEGIN: a pool with a previously-acquired client now idle in the queue
    async with _fresh_pool() as (pool, factory):
        async with pool.acquire():
            pass
        assert pool.free_size == 1
        assert len(factory.transports) == 1

        # WHEN: acquiring again
        async with pool.acquire():
            # THEN: the same connection is reused (no new transport)
            assert len(factory.transports) == 1
            assert pool.size == 1


async def test_acquire_opens_up_to_max_size_concurrently() -> None:
    # BEGIN: a pool with max_size=3
    async with _fresh_pool(max_size=3) as (pool, factory):
        # WHEN: acquiring three clients in parallel
        cm1 = pool.acquire()
        cm2 = pool.acquire()
        cm3 = pool.acquire()
        c1 = await cm1.__aenter__()
        c2 = await cm2.__aenter__()
        c3 = await cm3.__aenter__()

        # THEN: three distinct clients, three transports, pool full
        assert {id(c1), id(c2), id(c3)} == {id(c1), id(c2), id(c3)}
        assert len(factory.transports) == 3
        assert pool.size == 3
        assert pool.free_size == 0

        await cm1.__aexit__(None, None, None)
        await cm2.__aexit__(None, None, None)
        await cm3.__aexit__(None, None, None)
        assert pool.free_size == 3


# ---- bounded behaviour --------------------------------------------------


async def test_acquire_beyond_max_size_blocks_until_release() -> None:
    # BEGIN: a pool sized 1, with the only connection in use
    async with _fresh_pool(max_size=1, acquire_timeout=5.0) as (pool, _):
        first_cm = pool.acquire()
        await first_cm.__aenter__()

        # WHEN: a second acquire fires concurrently with a delayed release
        async def second_acquirer() -> str:
            async with pool.acquire():
                return "got it"

        second_task = asyncio.create_task(second_acquirer())
        # Yield once so the second task reaches its acquire wait
        await asyncio.sleep(0)
        assert not second_task.done()

        # THEN: releasing the first connection unblocks the second
        await first_cm.__aexit__(None, None, None)
        result = await second_task
        assert result == "got it"


async def test_acquire_timeout_raises_pool_timeout_error() -> None:
    # BEGIN: a pool sized 1 with a tight acquire_timeout
    async with _fresh_pool(max_size=1, acquire_timeout=0.05) as (pool, _):
        async with pool.acquire():
            # WHEN: a second acquire fires while the first is still held
            # THEN: PoolTimeoutError surfaces after the deadline, naming
            #       max_size and how many are in use
            with pytest.raises(PoolTimeoutError) as exc_info:
                async with pool.acquire():
                    pass
            msg = str(exc_info.value)
            assert "max_size=1" in msg
            assert "in_use=1" in msg


# ---- FIFO fairness ------------------------------------------------------


async def test_waiters_get_served_in_fifo_order() -> None:
    # BEGIN: a pool sized 1 with the only connection in use
    async with _fresh_pool(max_size=1, acquire_timeout=5.0) as (pool, _):
        first_cm = pool.acquire()
        await first_cm.__aenter__()

        order: list[int] = []

        async def waiter(idx: int) -> None:
            async with pool.acquire():
                order.append(idx)
                # Release immediately so the next waiter can proceed
                await asyncio.sleep(0)

        # WHEN: three waiters queue in order, then we release the first
        t1 = asyncio.create_task(waiter(1))
        await asyncio.sleep(0)
        t2 = asyncio.create_task(waiter(2))
        await asyncio.sleep(0)
        t3 = asyncio.create_task(waiter(3))
        await asyncio.sleep(0)

        await first_cm.__aexit__(None, None, None)
        await asyncio.gather(t1, t2, t3)

        # THEN: they completed in declared order — Queue.get() is FIFO
        assert order == [1, 2, 3]


# ---- broken connections are discarded ----------------------------------


async def test_broken_client_on_release_is_discarded_not_recycled() -> None:
    # BEGIN: a pool sized 1 with a connection that's been forced into BROKEN
    async with _fresh_pool(max_size=1) as (pool, factory):
        cm = pool.acquire()
        client = await cm.__aenter__()
        # Simulate a broken connection — close the underlying writer so
        # the next op would fail; the simplest way to flip is_alive is
        # to call client.close() (state → CLOSED, not READY).
        await client.close()
        assert not client.is_alive

        # WHEN: releasing the closed/broken client
        await cm.__aexit__(None, None, None)

        # THEN: the pool didn't put it back in the free queue and
        #       decremented size; a fresh acquire opens a new one
        assert pool.size == 0
        assert pool.free_size == 0
        async with pool.acquire():
            assert len(factory.transports) == 2


# ---- close --------------------------------------------------------------


async def test_close_drains_idle_clients_and_blocks_new_acquires() -> None:
    # BEGIN: a pool with two idle clients (holding both concurrently
    #        forces the pool to actually open two distinct connections,
    #        rather than reusing the first across sequential acquires)
    async with _fresh_pool(max_size=3) as (pool, _):
        cm1 = pool.acquire()
        cm2 = pool.acquire()
        await cm1.__aenter__()
        await cm2.__aenter__()
        await cm1.__aexit__(None, None, None)
        await cm2.__aexit__(None, None, None)
        assert pool.free_size == 2

        # WHEN: closing the pool
        await pool.close()

        # THEN: idle clients are drained, new acquires raise PoolClosedError
        assert pool.is_closed
        assert pool.free_size == 0
        with pytest.raises(PoolClosedError):
            async with pool.acquire():
                pass


async def test_close_via_async_with_exit() -> None:
    # BEGIN: a pool with one idle client
    factory = _FreshTransports()
    pool = create_pool(
        "clickhouse://default:@host/db", transport_factory=factory
    )
    async with pool:
        async with pool.acquire():
            pass
        assert pool.free_size == 1

    # WHEN / THEN: __aexit__ closed the pool
    assert pool.is_closed
    assert pool.free_size == 0


# ---- public re-exports -------------------------------------------------


def test_pool_re_exported_from_top_level() -> None:
    # BEGIN / WHEN / THEN: Pool / create_pool / pool errors are
    #     reachable from the top-level module per the README quick-start
    import clickhouse_async as ch

    assert ch.create_pool is create_pool
    assert ch.Pool is Pool
    assert ch.PoolError is not None
    assert ch.PoolTimeoutError is PoolTimeoutError
    assert ch.PoolClosedError is PoolClosedError
