"""Multi-host pool tests: rotation across acquires + failover semantics
when some replicas are dead.
"""

from __future__ import annotations

import asyncio
import ssl

import clickhouse_async as ch
from clickhouse_async.connection import _WriterLike

from ._mock_transport import _ScriptedWriter
from ._scripted_packets import encode_server_hello


class _RotatingTransport:
    """Transport factory that hands out per-host scripted readers.

    Each (host, port) maps to either a Hello payload (success) or an
    exception (immediate failure on connect). Every call is logged so
    tests can assert the rotation visited the expected sequence.
    """

    def __init__(
        self, outcomes: dict[tuple[str, int], bytes | Exception]
    ) -> None:
        self.outcomes = outcomes
        self.calls: list[tuple[str, int]] = []

    async def __call__(
        self,
        host: str,
        port: int,
        _ssl_context: ssl.SSLContext | None,
    ) -> tuple[asyncio.StreamReader, _WriterLike]:
        key = (host, port)
        self.calls.append(key)
        outcome = self.outcomes.get(key, b"")
        if isinstance(outcome, Exception):
            raise outcome
        reader = asyncio.StreamReader()
        reader.feed_data(outcome)
        reader.feed_eof()
        return reader, _ScriptedWriter(bytearray())


# ---- rotation across acquires --------------------------------------------


async def test_pool_rotates_first_choice_host_across_acquires() -> None:
    # BEGIN: a 3-host DSN where every host is healthy; the pool's
    #        rotation should hand each acquire a different first
    #        candidate so concurrent acquires fan out.
    transport = _RotatingTransport(
        {
            ("a", 9000): encode_server_hello(),
            ("b", 9000): encode_server_hello(),
            ("c", 9000): encode_server_hello(),
        }
    )
    pool = ch.create_pool(
        "clickhouse://default:@a:9000,b:9000,c:9000/db",
        max_size=3,
        transport_factory=transport,
    )

    # WHEN: acquiring three connections (each forces a fresh open
    #       because the previous one is still checked out)
    async with pool:
        ctx_a = pool.acquire()
        await ctx_a.__aenter__()
        ctx_b = pool.acquire()
        await ctx_b.__aenter__()
        ctx_c = pool.acquire()
        await ctx_c.__aenter__()

        # THEN: each open hit a different first-choice host because
        #       the rotation pointer advances per acquire
        assert len(transport.calls) == 3
        assert set(transport.calls) == {
            ("a", 9000),
            ("b", 9000),
            ("c", 9000),
        }

        # Pool.__aexit__ closes everything; release explicitly so
        # the pool's queue is drained the same way real callers do.
        await ctx_a.__aexit__(None, None, None)
        await ctx_b.__aexit__(None, None, None)
        await ctx_c.__aexit__(None, None, None)


async def test_pool_skips_known_dead_host_on_subsequent_acquire() -> None:
    # BEGIN: a 2-host DSN where host ``a`` always refuses and host ``b``
    #        is healthy
    transport = _RotatingTransport(
        {
            ("a", 9000): ConnectionRefusedError("a is gone"),
            ("b", 9000): encode_server_hello(),
        }
    )
    pool = ch.create_pool(
        "clickhouse://default:@a:9000,b:9000/db",
        max_size=2,
        host_failover_cooldown=60.0,  # plenty long for the test
        transport_factory=transport,
    )

    async with pool:
        # WHEN: the first acquire walks (a, b) and lands on b — failure
        #       on ``a`` records a cooldown
        ctx_first = pool.acquire()
        await ctx_first.__aenter__()
        first_pass = list(transport.calls)
        assert ("a", 9000) in first_pass
        assert ("b", 9000) in first_pass

        # WHEN: a second acquire forces a fresh open (the first client
        #       is still checked out, so the pool can't recycle it).
        #       The rotation should skip ``a`` because it's cooled
        #       down from the previous failure.
        prev_calls = len(transport.calls)
        ctx_second = pool.acquire()
        await ctx_second.__aenter__()
        second_pass = transport.calls[prev_calls:]

        # THEN: the second open went straight to ``b`` — ``a`` was
        #       skipped for being in cooldown
        assert ("a", 9000) not in second_pass
        assert second_pass == [("b", 9000)]

        # Cleanup
        await ctx_first.__aexit__(None, None, None)
        await ctx_second.__aexit__(None, None, None)
