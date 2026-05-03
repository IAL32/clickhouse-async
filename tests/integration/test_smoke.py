"""Smoke tests against a real ClickHouse server.

Walks through the end-to-end golden path so wire-format regressions
that the unit suite would miss (something subtly wrong in our
encoding that the loopback round-trips because we use the same buggy
code on both sides) surface here against the real server's parser.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import date

import clickhouse_async as ch


async def test_handshake_against_real_server_populates_server_info(
    client: ch.Client,
) -> None:
    # BEGIN: a Client connected to a live ClickHouse server
    info = client.server_info

    # WHEN / THEN: the server identifies itself and reports a sane
    #              negotiated revision
    assert "ClickHouse" in info.name
    assert info.revision > 0
    assert info.timezone is not None  # gated above 54058 — covered by 24.x


async def test_select_one_round_trips(client: ch.Client) -> None:
    # BEGIN: a connected client
    # WHEN: running the universal smoke query
    rows = await client.fetch_all("SELECT 1")

    # THEN: a single row containing the integer 1 comes back
    assert rows == [(1,)]


async def test_insert_then_select_round_trips_via_pool(
    pool: ch.Pool,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: an empty Memory-engine table named after the test, plus
    #        three rows to insert
    table = "test_insert_select_round_trip"
    await fresh_table(
        table,
        "(id UInt64, name String, day Date) ENGINE = Memory",
    )
    rows_in: list[tuple[object, ...]] = [
        (1, "alpha", date(2026, 5, 1)),
        (2, "beta", date(2026, 5, 2)),
        (3, "gamma", date(2026, 5, 3)),
    ]

    # WHEN: inserting via the pool's pass-through, then reading back
    async with pool.acquire() as client:
        n = await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=rows_in,
            column_names=["id", "name", "day"],
        )
    assert n == 3

    rows_out = await pool.fetch_all(f"SELECT id, name, day FROM {table} ORDER BY id")

    # THEN: every row round-tripped — including the Date conversion
    assert rows_out == rows_in


async def test_server_side_parameter_binding(
    pool: ch.Pool,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: a small table populated for parametric lookups
    table = "test_param_binding"
    await fresh_table(table, "(id UInt64, name String) ENGINE = Memory")
    async with pool.acquire() as client:
        await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=[(1, "alpha"), (2, "beta"), (3, "gamma")],
            column_names=["id", "name"],
        )

    # WHEN: querying with a typed parameter
    rows = await pool.fetch_all(
        f"SELECT id, name FROM {table} WHERE id = {{lookup:UInt64}}",
        params={"lookup": 2},
    )

    # THEN: the server-side parameter parser resolved the placeholder
    #       and returned the matching row
    assert rows == [(2, "beta")]


async def test_streaming_iter_rows_returns_to_ready(pool: ch.Pool) -> None:
    # BEGIN: a connected client streaming over a generated row source
    n_rows = 100
    async with pool.acquire() as client:
        # WHEN: consuming the full stream
        rows: list[tuple[object, ...]] = []
        async for row in client.iter_rows(
            f"SELECT number FROM system.numbers LIMIT {n_rows}"
        ):
            rows.append(row)

        # THEN: every number arrived in order; the connection is
        #       reusable for the next operation
        assert [r[0] for r in rows] == list(range(n_rows))
        # A follow-up trivial query confirms the connection survived
        assert await client.fetch_all("SELECT 1") == [(1,)]


async def test_kill_query_cancels_query_running_on_another_connection(
    pool: ch.Pool,
) -> None:
    # BEGIN: a long-running SELECT with a known query_id launched on
    #        one pool client; the kill goes through a *different* pool
    #        client so we exercise the cross-connection cancel path.
    qid = "test-kill-query-cross-conn"

    async def runner() -> BaseException | None:
        try:
            async with pool.acquire() as client:
                # ``sleepEachRow(2.5)`` is the longest single sleep
                # ClickHouse permits per call; we chain 8 rows under a
                # raised ``function_sleep_max_microseconds_per_block``
                # cap so the query takes ~20 s. That leaves ample
                # headroom for the killer task to run.
                await client.fetch_all(
                    "SELECT sleepEachRow(2.5) FROM numbers(8)",
                    query_id=qid,
                    settings={
                        "function_sleep_max_microseconds_per_block": "30000000",
                    },
                )
        except BaseException as exc:
            return exc
        return None

    runner_task = asyncio.create_task(runner())

    # Poll system.processes until the runner's query is registered.
    # Faster and more robust than a fixed sleep — the pool acquire
    # plus first-block round-trip can take 100 ms on a cold pool.
    deadline = asyncio.get_event_loop().time() + 5.0
    found = False
    while asyncio.get_event_loop().time() < deadline:
        procs = await pool.fetch_all(
            "SELECT query_id FROM system.processes WHERE query_id = {qid:String}",
            params={"qid": qid},
        )
        if procs:
            found = True
            break
        await asyncio.sleep(0.05)
    if not found:
        runner_task.cancel()
        raise AssertionError(
            "runner's query never showed up in system.processes within 5 s"
        )

    # WHEN: the runner is in flight, a separate pool.kill_query
    #       cancels it from a fresh connection
    try:
        async with asyncio.timeout(15):
            n = await pool.kill_query(qid)
    except TimeoutError:
        runner_task.cancel()
        raise

    # THEN: the server confirmed at least one query targeted (sync
    #       waits for the kill to actually land)
    assert n >= 1

    # And the original task surfaces ServerError code 394
    # (QUERY_WAS_CANCELLED).
    result = await asyncio.wait_for(runner_task, timeout=15)
    assert isinstance(result, ch.ServerError)
    assert result.code == 394, (
        f"expected QUERY_WAS_CANCELLED (394), got {result.code} "
        f"({result.name}): {result.display_text}"
    )


async def test_low_cardinality_string_round_trips_via_server(
    pool: ch.Pool,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: a Memory-engine table with a plain LowCardinality(String) column
    table = "test_low_cardinality_string"
    await fresh_table(
        table,
        "(id UInt64, label LowCardinality(String)) ENGINE = Memory",
    )
    rows_in: list[tuple[object, ...]] = [
        (1, "alpha"),
        (2, "beta"),
        (3, "alpha"),
        (4, "gamma"),
    ]

    # WHEN: inserting via the pool, then reading back
    async with pool.acquire() as client:
        n = await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=rows_in,
            column_names=["id", "label"],
        )
    assert n == 4
    rows_out = await pool.fetch_all(f"SELECT id, label FROM {table} ORDER BY id")

    # THEN: every row round-trips
    assert rows_out == rows_in


async def test_low_cardinality_nullable_string_round_trips_via_server(
    pool: ch.Pool,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: a Memory-engine table with a LowCardinality(Nullable(String))
    #        column — the most common shape in real ClickHouse schemas
    table = "test_low_cardinality_nullable_string"
    await fresh_table(
        table,
        "(id UInt64, label LowCardinality(Nullable(String))) ENGINE = Memory",
    )
    rows_in: list[tuple[object, ...]] = [
        (1, "alpha"),
        (2, None),
        (3, "beta"),
        (4, "alpha"),
        (5, None),
        (6, "gamma"),
    ]

    # WHEN: inserting via the pool, then reading back
    async with pool.acquire() as client:
        n = await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=rows_in,
            column_names=["id", "label"],
        )
    assert n == 6
    rows_out = await pool.fetch_all(f"SELECT id, label FROM {table} ORDER BY id")

    # THEN: every row round-trips — Nones travel through dictionary
    #       slot 0 and come back as Python None
    assert rows_out == rows_in


async def test_named_tuple_column_round_trips_via_server(
    pool: ch.Pool,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: a Memory-engine table with a named-tuple column. The
    #        server emits the named-form type spec back in the block
    #        header on SELECT, so this exercises both the parser and
    #        the codec round-trip end-to-end.
    table = "test_named_tuple_column"
    await fresh_table(
        table,
        "(id UInt64, meta Tuple(uid UInt32, label String)) ENGINE = Memory",
    )
    rows_in: list[tuple[object, ...]] = [
        (1, (10, "alpha")),
        (2, (20, "beta")),
        (3, (30, "gamma")),
    ]

    # WHEN: inserting via the pool, then reading back
    async with pool.acquire() as client:
        n = await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=rows_in,
            column_names=["id", "meta"],
        )
    assert n == 3
    rows_out = await pool.fetch_all(f"SELECT id, meta FROM {table} ORDER BY id")

    # THEN: every row round-trips — the named-tuple values come back
    #       as plain Python tuples (the names live in the codec, not
    #       in the row representation, until a future Client surfaces
    #       NamedTuple rows).
    assert rows_out == rows_in


async def test_multi_host_dsn_falls_through_dead_first_host(dsn: str) -> None:
    # BEGIN: a multi-host DSN whose first candidate is unreachable
    #        (a port nothing's listening on) and second candidate is
    #        the real ClickHouse from the session DSN
    real = ch.parse_dsn(dsn)
    real_host, real_port = real.host, real.port
    multi = (
        f"clickhouse://{real.user}:{real.password}"
        f"@localhost:9999,{real_host}:{real_port}/{real.database}"
    )

    # WHEN: connecting via the multi-host DSN
    async with ch.connect(multi) as client:
        # THEN: the connection landed on the real (second) candidate
        assert client.dsn.hosts == (
            ("localhost", 9999),
            (real_host, real_port),
        )
        # And the connection is fully functional
        assert await client.fetch_all("SELECT 1") == [(1,)]
