"""Smoke tests against a real ClickHouse server.

Walks through the end-to-end golden path so wire-format regressions
that the unit suite would miss (something subtly wrong in our
encoding that the loopback round-trips because we use the same buggy
code on both sides) surface here against the real server's parser.
"""

from __future__ import annotations

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
