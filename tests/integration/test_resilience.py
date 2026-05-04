"""Integration tests for connection resilience under proxy-simulated failures.

A lightweight asyncio TCP proxy intercepts the wire between the client
and the real ClickHouse server.  ``_KillingProxy.kill_all()`` severs
every active forwarded connection either with a clean TCP FIN or an
abrupt TCP RST, covering the two most common real-world connection-death
modes:

  * **FIN** — server / load-balancer sends a graceful close (idle
    timeout, graceful shutdown, NAT expiry propagated as FIN).
  * **RST** — abrupt peer reset: container OOM-kill, network partition,
    NIC flap, kernel kill of the server process.

For each scenario we assert:
  * An error propagates to the caller — never silently swallowed.
  * ``client.is_alive`` is ``False`` immediately after the failure.
  * A ``Pool`` re-establishes a live connection on the next acquire
    without the caller seeing any error.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import struct
from typing import TYPE_CHECKING

import pytest

import clickhouse_async as ch
from clickhouse_async.errors import ProtocolError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

# Pool health-check interval used for proxy-pool tests.  Must be shorter
# than the sleep we take after kill_all() so the health check fires on
# the next acquire and discards the dead connection.
_HEALTH_CHECK_AFTER = 0.1  # seconds

# How long to wait after kill_all() before issuing the next acquire.
# Three multiples of _HEALTH_CHECK_AFTER gives ample margin.
_KILL_SETTLE = _HEALTH_CHECK_AFTER * 3


# ---------------------------------------------------------------------------
# Proxy implementation
# ---------------------------------------------------------------------------


class _KillingProxy:
    """Transparent TCP proxy with on-demand connection termination.

    Forwards bytes unchanged between clients and the real ClickHouse.
    ``kill_all()`` severs every active forwarded pair gracefully (FIN) or
    abruptly (RST via ``SO_LINGER`` with ``l_linger = 0``).
    """

    def __init__(self, target_host: str, target_port: int) -> None:
        self._host = target_host
        self._port = target_port
        self._server: asyncio.Server | None = None
        self._pairs: list[tuple[asyncio.StreamWriter, asyncio.StreamWriter]] = []

    @property
    def port(self) -> int:
        assert self._server is not None
        sockets = self._server.sockets
        assert sockets
        return sockets[0].getsockname()[1]

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", 0)

    async def _accept(
        self,
        client_r: asyncio.StreamReader,
        client_w: asyncio.StreamWriter,
    ) -> None:
        up_r, up_w = await asyncio.open_connection(self._host, self._port)
        self._pairs.append((client_w, up_w))
        await asyncio.gather(
            _pipe(client_r, up_w),
            _pipe(up_r, client_w),
            return_exceptions=True,
        )

    async def kill_all(self, *, ungraceful: bool = False) -> None:
        for client_w, up_w in self._pairs:
            if ungraceful:
                for w in (client_w, up_w):
                    raw: socket.socket | None = w.get_extra_info("socket")
                    if raw is not None:
                        with contextlib.suppress(OSError):
                            raw.setsockopt(
                                socket.SOL_SOCKET,
                                socket.SO_LINGER,
                                struct.pack("ii", 1, 0),
                            )
            client_w.close()
            up_w.close()
        self._pairs.clear()

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._server.wait_closed(), timeout=1.0)


async def _pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
    try:
        while True:
            chunk = await src.read(65536)
            if not chunk:
                break
            dst.write(chunk)
            await dst.drain()
    except (OSError, asyncio.IncompleteReadError):
        pass
    finally:
        with contextlib.suppress(Exception):
            dst.close()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def proxy(dsn: str) -> AsyncIterator[_KillingProxy]:
    """A fresh proxy per test, forwarding to the session's ClickHouse."""
    parsed = ch.parse_dsn(dsn)
    p = _KillingProxy(parsed.host, parsed.port)
    await p.start()
    yield p
    await p.stop()


@pytest.fixture
async def proxy_dsn(dsn: str, proxy: _KillingProxy) -> str:
    """DSN that routes through the per-test killing proxy."""
    parsed = ch.parse_dsn(dsn)
    return (
        f"clickhouse://{parsed.user}:{parsed.password}"
        f"@127.0.0.1:{proxy.port}/{parsed.database}"
    )


# ---------------------------------------------------------------------------
# 1. Client raises and marks itself broken after the connection is killed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ungraceful", [False, True], ids=["fin", "rst"])
async def test_client_raises_and_marks_broken_after_connection_killed(
    proxy: _KillingProxy,
    proxy_dsn: str,
    ungraceful: bool,
) -> None:
    # BEGIN: a Client connected through the proxy; the first query
    #        completes successfully, leaving the connection in READY state
    async with ch.connect(proxy_dsn) as client:
        assert await client.fetch_all("SELECT 1") == [(1,)]

        # WHEN: the proxy drops all connections (FIN or RST)
        await proxy.kill_all(ungraceful=ungraceful)
        await asyncio.sleep(_KILL_SETTLE)

        # THEN: the next query surfaces an error — never silently stalls
        with pytest.raises(
            (ProtocolError, ConnectionResetError, BrokenPipeError, OSError)
        ):
            await client.fetch_all("SELECT 1")

        # THEN: the connection marks itself dead
        assert not client.is_alive


# ---------------------------------------------------------------------------
# 2. Client raises when the connection is killed mid-query
# ---------------------------------------------------------------------------


async def test_client_raises_when_connection_killed_mid_query(
    proxy: _KillingProxy,
    proxy_dsn: str,
) -> None:
    # BEGIN: a Client with a long-running query in flight through the proxy
    async with ch.connect(proxy_dsn) as client:

        async def _kill_soon() -> None:
            await asyncio.sleep(0.05)
            await proxy.kill_all()

        kill_task = asyncio.create_task(_kill_soon())

        # WHEN: the proxy drops the connection while the query is in flight
        # THEN: the error propagates to the caller — never silently stalls
        with pytest.raises(
            (ProtocolError, ConnectionResetError, BrokenPipeError, OSError)
        ):
            await client.fetch_all(
                "SELECT sleepEachRow(0.5) FROM numbers(5)",
                settings={"function_sleep_max_microseconds_per_block": "5000000"},
            )

        await kill_task

        # THEN: the connection marks itself dead
        assert not client.is_alive


# ---------------------------------------------------------------------------
# 3. Pool reconnects transparently after idle connections are killed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ungraceful", [False, True], ids=["fin", "rst"])
async def test_pool_reconnects_after_idle_connection_killed(
    proxy: _KillingProxy,
    proxy_dsn: str,
    ungraceful: bool,
) -> None:
    # BEGIN: a pool with a short health_check_after, connected through
    #        the proxy; one query runs and the connection returns to idle
    async with ch.create_pool(
        proxy_dsn,
        min_size=0,
        max_size=2,
        health_check_after=_HEALTH_CHECK_AFTER,
    ) as pool:
        async with pool.acquire() as client:
            assert await client.fetch_all("SELECT 1") == [(1,)]

        # WHEN: the proxy kills all idle connections and enough time
        #       elapses for health_check_after to trigger on the next acquire
        await proxy.kill_all(ungraceful=ungraceful)
        await asyncio.sleep(_KILL_SETTLE)

        # THEN: the pool detects the dead connection, opens a fresh one
        #       through the proxy, and the query succeeds transparently
        async with pool.acquire() as client:
            rows = await client.fetch_all("SELECT 1")

    assert rows == [(1,)]
