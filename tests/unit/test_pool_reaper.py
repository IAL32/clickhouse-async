"""Reaper tests: idle close + min_size warm + lifecycle.

The reaper sweeps every `idle_check_interval` seconds, closing any
free entry whose `last_returned_at` is older than `max_idle_time`
provided the pool's `size` stays above `min_size`. After the
close pass it re-warms the pool back up to `min_size`.

Tests use very small intervals (sub-second) so the suite stays fast
while still exercising the real timing path.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

import clickhouse_async as ch

from ._mock_transport import ScriptedTransport
from ._scripted_packets import encode_server_hello

if TYPE_CHECKING:
    import ssl

    from clickhouse_async.connection import _WriterLike

# A Pong packet is a single varuint with the PONG packet id (4).
_PONG = bytes([4])


class _CountingTransports:
    """Transport factory that pre-feeds Hello + a bunch of Pong replies
    per minted client (so ping-on-acquire health checks survive)."""

    def __init__(self, fail_after: int | None = None) -> None:
        self.opens = 0
        self.fail_after = fail_after
        self.transports: list[ScriptedTransport] = []

    async def __call__(
        self,
        _host: str,
        _port: int,
        _ssl_context: ssl.SSLContext | None,
    ) -> tuple[asyncio.StreamReader, _WriterLike]:
        self.opens += 1
        if self.fail_after is not None and self.opens > self.fail_after:
            raise ConnectionRefusedError("transport factory configured to fail")
        t = ScriptedTransport()
        t.feed(encode_server_hello())
        # Plenty of Pongs queued so no health check ever runs short.
        for _ in range(64):
            t.feed(_PONG)
        self.transports.append(t)
        return await t(_host, _port, _ssl_context)


# ---- idle reap brings size down to min_size ---------------------------


async def test_reaper_closes_stale_entries_above_min_size() -> None:
    # BEGIN: a pool with min_size=1, max_size=3, max_idle_time tiny
    factory = _CountingTransports()
    pool = ch.create_pool(
        "clickhouse://default:@host/db",
        min_size=1,
        max_size=3,
        max_idle_time=0.05,
        idle_check_interval=0.05,
        health_check_after=999,  # never trigger ping path during tests
        transport_factory=factory,
    )
    async with pool:
        # WHEN: three concurrent acquires force three opens, then they
        #       all release back to the free deque
        ctxs = [pool.acquire() for _ in range(3)]
        for c in ctxs:
            await c.__aenter__()
        for c in ctxs:
            await c.__aexit__(None, None, None)
        assert pool.size == 3
        assert pool.free_size == 3

        # WHEN: we wait long enough for at least one reaper pass after
        #       max_idle_time has elapsed
        await asyncio.sleep(0.4)

        # THEN: the reaper closed two stale entries, leaving size at
        #       min_size
        assert pool.size == 1
        assert pool.free_size == 1


# ---- min_size warm fills the pool from cold ---------------------------


async def test_min_size_warm_after_first_acquire() -> None:
    # BEGIN: a pool with min_size=2 and a tight reap interval
    factory = _CountingTransports()
    pool = ch.create_pool(
        "clickhouse://default:@host/db",
        min_size=2,
        max_size=4,
        max_idle_time=10.0,  # generous so the warm clients don't get reaped
        idle_check_interval=0.05,
        health_check_after=999,
        transport_factory=factory,
    )
    async with pool:
        # WHEN: a single acquire/release round-trip — this is what
        #       starts the reaper task; cold pool would otherwise stay
        #       at size 0
        async with pool.acquire():
            pass
        # And give the reaper enough time for one full pass
        await asyncio.sleep(0.3)

        # THEN: the reaper has warmed the pool to exactly min_size
        assert pool.size == 2
        assert pool.free_size == 2


# ---- reaper survives open errors during warm --------------------------


async def test_reaper_survives_warm_open_failures() -> None:
    # BEGIN: a transport that succeeds for the first open (so the
    #        reaper task can start), then fails — every warm attempt
    #        hits ConnectionRefusedError. The reaper should log and
    #        keep running, not die.
    factory = _CountingTransports(fail_after=1)
    pool = ch.create_pool(
        "clickhouse://default:@host/db",
        min_size=2,
        max_size=4,
        max_idle_time=10.0,
        idle_check_interval=0.05,
        health_check_after=999,
        transport_factory=factory,
    )
    async with pool:
        # WHEN: the first acquire opens client 1 successfully
        async with pool.acquire():
            pass
        # And we let several reaper passes run
        await asyncio.sleep(0.3)

        # THEN: the reaper task is still alive (would be done() if it
        #       had crashed) — and the pool sits at the one healthy
        #       entry while the warm attempts keep failing
        assert pool._reaper_task is not None
        assert not pool._reaper_task.done()
        # The reaper attempted to warm up but every attempt past the
        # first failed; the pool stays at one entry rather than
        # spiraling.
        assert pool.size <= 1


# ---- close() shuts the reaper down deterministically ------------------


async def test_close_cancels_reaper_task() -> None:
    # BEGIN: a pool whose reaper has been started by an acquire
    factory = _CountingTransports()
    pool = ch.create_pool(
        "clickhouse://default:@host/db",
        max_size=2,
        idle_check_interval=0.05,
        health_check_after=999,
        transport_factory=factory,
    )
    async with pool.acquire():
        pass
    assert pool._reaper_task is not None
    reaper = pool._reaper_task

    # WHEN: closing the pool
    await pool.close()

    # THEN: the reaper task has been cancelled and awaited; its done()
    #       is True and a second close() is a no-op
    assert reaper.done()
    assert pool._reaper_task is None
    # second close: idempotent
    await pool.close()


# ---- close() drains the free deque ------------------------------------


async def test_concurrent_acquire_stress_test() -> None:
    # BEGIN: a small pool with min_size=2, max_size=8 — the deque +
    #        condition rewrite needs to survive a high acquire/release
    #        churn without deadlocking, and the reaper running
    #        concurrently must not interfere with the steady-state
    #        flow.
    factory = _CountingTransports()
    pool = ch.create_pool(
        "clickhouse://default:@host/db",
        min_size=2,
        max_size=8,
        max_idle_time=10.0,
        idle_check_interval=0.05,
        health_check_after=999,
        transport_factory=factory,
    )

    async def worker(n: int) -> None:
        for _ in range(n):
            async with pool.acquire():
                # Yield once so concurrent tasks interleave.
                await asyncio.sleep(0)

    async with pool:
        # WHEN: 50 concurrent tasks each do 60 acquires (3000 total)
        async with asyncio.TaskGroup() as tg:
            for _ in range(50):
                tg.create_task(worker(60))

        # Give the reaper a beat to settle.
        await asyncio.sleep(0.2)

        # THEN: no deadlock; the pool's invariants hold; min_size is
        #       respected — the reaper has either kept it warm or just
        #       finished a pass.
        assert pool.size <= pool._max_size
        assert pool.free_size <= pool.size
        assert pool.size >= pool._min_size


async def test_enable_reaper_false_skips_starting_the_task() -> None:
    # BEGIN: a pool with enable_reaper=False — useful in test harnesses
    #        / short-lived scripts where the per-acquire health check
    #        is enough and we don't want a background task ticking
    factory = _CountingTransports()
    pool = ch.create_pool(
        "clickhouse://default:@host/db",
        max_size=2,
        enable_reaper=False,
        idle_check_interval=0.05,
        max_idle_time=0.05,
        health_check_after=999,
        transport_factory=factory,
    )

    async with pool:
        # WHEN: a couple of acquires + releases happen
        async with pool.acquire():
            pass
        async with pool.acquire():
            pass
        # And we wait long enough that the reaper *would* have closed
        # everything had it been running
        await asyncio.sleep(0.3)

        # THEN: no reaper task was ever started
        assert pool._reaper_task is None
        # And idle entries are still in the deque — they weren't reaped
        assert pool.free_size == 1


async def test_min_size_with_disabled_reaper_is_rejected() -> None:
    # BEGIN / WHEN / THEN: `min_size` only gets enforced by the
    #                     reaper, so combining the two is meaningless;
    #                     refuse at construction rather than silently
    #                     accept a parameter we won't honour
    with pytest.raises(ValueError, match=r"min_size.*requires enable_reaper"):
        ch.create_pool(
            "clickhouse://default:@host/db",
            min_size=2,
            max_size=4,
            enable_reaper=False,
        )


async def test_close_drains_free_deque_and_decrements_size() -> None:
    # BEGIN: a pool with two open clients sitting in the free deque
    factory = _CountingTransports()
    pool = ch.create_pool(
        "clickhouse://default:@host/db",
        max_size=2,
        idle_check_interval=999,  # don't let the reaper interfere
        health_check_after=999,
        transport_factory=factory,
    )
    async with pool:
        c1 = pool.acquire()
        c2 = pool.acquire()
        await c1.__aenter__()
        await c2.__aenter__()
        await c1.__aexit__(None, None, None)
        await c2.__aexit__(None, None, None)
        assert pool.free_size == 2
        assert pool.size == 2

    # WHEN: the context manager exits → close() runs
    # THEN: the deque is empty and size is back to zero
    assert pool.free_size == 0
    assert pool.size == 0
    assert pool.is_closed
