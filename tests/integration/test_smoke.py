"""Smoke tests against a real ClickHouse server.

Walks through the end-to-end golden path so wire-format regressions
that the unit suite would miss (something subtly wrong in our
encoding that the loopback round-trips because we use the same buggy
code on both sides) surface here against the real server's parser.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

import clickhouse_async as ch
from clickhouse_async import ColumnarBlock, ColumnarResult
from clickhouse_async.types.datetime import HighPrecisionTimestamp

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


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


async def test_server_side_parameter_binding_none_for_nullable(
    pool: ch.Pool,
) -> None:
    # BEGIN: no fixture data needed — exercise the binding via SELECT
    # WHEN: passing ``None`` against a ``Nullable(T)`` placeholder for
    #       both string and integer types
    rows_str = await pool.fetch_all(
        "SELECT {x:Nullable(String)} AS v",
        params={"x": None},
    )
    rows_int = await pool.fetch_all(
        "SELECT {x:Nullable(Int64)} AS v",
        params={"x": None},
    )
    rows_str_value = await pool.fetch_all(
        "SELECT {x:Nullable(String)} AS v",
        params={"x": "hi"},
    )

    # THEN: the server resolves the ``\N`` sentinel to SQL NULL on the
    #       way in, and a non-null value through the same placeholder
    #       still binds cleanly — the None path doesn't poison the
    #       Nullable code path
    assert rows_str == [(None,)]
    assert rows_int == [(None,)]
    assert rows_str_value == [("hi",)]


async def test_streaming_iter_rows_returns_to_ready(pool: ch.Pool) -> None:
    # BEGIN: a connected client streaming over a generated row source
    n_rows = 100
    async with pool.acquire() as client:
        # WHEN: consuming the full stream
        rows: list[tuple[object, ...]] = [
            row
            async for row in client.iter_rows(
                f"SELECT number FROM system.numbers LIMIT {n_rows}"
            )
        ]

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


async def test_datetime64_nanosecond_precision_round_trips(
    pool: ch.Pool,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: a Memory-engine table with a ``DateTime64(9)`` column —
    #        nanosecond ticks. Python's ``datetime`` can't hold sub-
    #        microsecond resolution, so the high_precision codec
    #        surfaces values as ``HighPrecisionTimestamp`` instead.
    table = "test_datetime64_nano"
    await fresh_table(
        table,
        "(id UInt64, ts DateTime64(9, 'UTC')) ENGINE = Memory",
    )
    rows_in: list[tuple[object, ...]] = [
        (1, HighPrecisionTimestamp(ticks=1_756_812_345_123_456_789, scale=9)),
        (2, HighPrecisionTimestamp(ticks=1_756_812_345_999_999_999, scale=9)),
    ]

    # WHEN: inserting via the pool, then reading back
    async with pool.acquire() as client:
        n = await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=rows_in,
            column_names=["id", "ts"],
        )
    assert n == 2
    rows_out = await pool.fetch_all(f"SELECT id, ts FROM {table} ORDER BY id")

    # THEN: the nanosecond ticks survive byte-for-byte — no microsecond
    #       truncation at the Python boundary
    assert rows_out == rows_in


async def test_datetime_session_timezone_seeded_from_handshake(
    pool: ch.Pool,
) -> None:
    # BEGIN: the Connection's session_timezone is seeded from the
    #        handshake's ``ServerInfo.timezone``. Naked ``DateTime``
    #        columns then come back tz-aware in that zone instead of
    #        as naive ``datetime`` (the v0 behaviour).
    async with pool.acquire() as client:
        # The test container reports UTC at handshake, so the seed
        # value is "UTC".
        assert client._conn.session_timezone == "UTC"

        # WHEN: a naked ``DateTime`` SELECT
        rows = await client.fetch_all("SELECT toDateTime('2026-05-02 12:00:00') AS t")

    # THEN: the result is aware in UTC, not naive
    assert len(rows) == 1
    t = rows[0][0]
    assert isinstance(t, datetime)
    assert t.utcoffset() == timedelta(0)


async def test_insert_column_name_mismatch_raises_before_any_data_sent(
    pool: ch.Pool,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: a table that already has rows; an INSERT whose SQL is
    #        valid (no explicit column list, so the server emits the
    #        full header) but whose ``column_names=`` disagrees with
    #        the header should raise locally — before any DATA bytes
    #        leave the wire — and leave the existing rows untouched.
    table = "test_insert_column_name_mismatch"
    await fresh_table(table, "(id UInt64, name String) ENGINE = Memory")
    # Seed one row so we can verify nothing else lands.
    async with pool.acquire() as client:
        await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=[(1, "alpha")],
            column_names=["id", "name"],
        )

    # WHEN: ``column_names`` swaps the order on us
    with pytest.raises(ValueError, match="column names mismatch"):
        async with pool.acquire() as client:
            await client.insert(
                f"INSERT INTO {table} VALUES",
                rows=[(2, "beta"), (3, "gamma")],
                column_names=["name", "id"],  # reversed vs. table order
            )

    # THEN: no rows were sent — the table still has only the seed row.
    rows = await pool.fetch_all(f"SELECT id, name FROM {table} ORDER BY id")
    assert rows == [(1, "alpha")]


async def test_insert_returns_server_confirmed_written_rows_via_real_server(
    pool: ch.Pool,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: the canonical 3-row insert against a real server. The
    #        return value should be the server's count, which for a
    #        plain Memory engine matches the client-side count.
    table = "test_insert_written_rows"
    await fresh_table(table, "(id UInt64, name String) ENGINE = Memory")
    rows_in: list[tuple[object, ...]] = [
        (1, "alpha"),
        (2, "beta"),
        (3, "gamma"),
    ]

    # WHEN: inserting via the pool
    async with pool.acquire() as client:
        n = await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=rows_in,
            column_names=["id", "name"],
        )

    # THEN: server-confirmed count agrees with the input length —
    #       the v0 contract still holds for the simple case
    assert n == 3
    # And the rows are queryable (sanity)
    out = await pool.fetch_all(f"SELECT id, name FROM {table} ORDER BY id")
    assert out == rows_in


async def test_aggregate_function_avg_state_round_trip_through_merge(
    pool: ch.Pool,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: a Memory-engine table holding ``AggregateFunction(avg,
    #        Float64)`` state bytes, plus a parallel raw-values table
    #        we'll aggregate from server-side. The pipeline:
    #
    #          values → avgState (server) → state column
    #              ↓ SELECT state column over our client
    #              ↓ INSERT same bytes into a sibling state column
    #          → avgMerge (server) → final average
    #
    #        round-trips opaque state bytes through Python without
    #        the client ever interpreting them.
    src = "test_aggfn_src"
    state_a = "test_aggfn_state_a"
    state_b = "test_aggfn_state_b"
    await fresh_table(src, "(x Float64) ENGINE = Memory")
    await fresh_table(
        state_a,
        "(s AggregateFunction(avg, Float64)) ENGINE = Memory",
    )
    await fresh_table(
        state_b,
        "(s AggregateFunction(avg, Float64)) ENGINE = Memory",
    )

    # Seed the source table; values average to 4.0 (= (2+4+6)/3).
    async with pool.acquire() as client:
        await client.insert(
            f"INSERT INTO {src} VALUES",
            rows=[(2.0,), (4.0,), (6.0,)],
            column_names=["x"],
        )
        # Build state bytes server-side and store in state_a.
        await client.execute(f"INSERT INTO {state_a} SELECT avgState(x) FROM {src}")

    # WHEN: pull the state bytes through our client and re-INSERT them
    #       into state_b
    state_rows = await pool.fetch_all(f"SELECT s FROM {state_a}")
    assert len(state_rows) == 1
    state_bytes = state_rows[0][0]
    assert isinstance(state_bytes, bytes)
    # avg state on the wire = Float64 numerator (8 B) + varuint
    # denominator. For our 3-row source the denominator is 3 → 1 B,
    # giving a 9-byte state.
    assert len(state_bytes) == 9

    async with pool.acquire() as client:
        n = await client.insert(
            f"INSERT INTO {state_b} VALUES",
            rows=[(state_bytes,)],
            column_names=["s"],
        )
    assert n == 1

    # THEN: the server reads our re-inserted bytes back through
    #       avgMerge and produces 4.0 — proving the bytes survived
    #       the Python round-trip identically
    merged = await pool.fetch_one(f"SELECT avgMerge(s) FROM {state_b}")
    assert merged is not None
    assert merged[0] == pytest.approx(4.0)


async def test_polygon_column_round_trips_via_server(
    pool: ch.Pool,
    fresh_table: Callable[[str, str], Awaitable[None]],
) -> None:
    # BEGIN: a Memory-engine table with a ``Polygon`` column. The
    #        server happily stores polygons as the geo alias and
    #        emits ``Polygon`` (not the desugared
    #        ``Array(Array(Tuple(Float64, Float64)))``) in the
    #        block header, so round-tripping a polygon-with-hole
    #        plus a single-ring polygon exercises the parser, the
    #        ``Polygon`` codec's delegation, and the server's view.
    table = "test_polygon_column"
    await fresh_table(
        table,
        "(id UInt64, shape Polygon) ENGINE = Memory",
    )
    rows_in: list[tuple[object, ...]] = [
        (
            1,
            [
                # outer ring
                [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)],
                # inner hole
                [(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0)],
            ],
        ),
        (2, [[(100.0, 100.0), (101.0, 100.0), (101.0, 101.0)]]),
    ]

    # WHEN: inserting via the pool, then reading back
    async with pool.acquire() as client:
        n = await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=rows_in,
            column_names=["id", "shape"],
        )
    assert n == 2
    rows_out = await pool.fetch_all(f"SELECT id, shape FROM {table} ORDER BY id")

    # THEN: every polygon's nested ring structure round-trips
    assert rows_out == rows_in


async def test_nested_column_round_trips_via_server(pool: ch.Pool) -> None:
    # BEGIN: a Memory-engine table with a ``Nested`` column created
    #        under ``flatten_nested = 0`` so the column stays a
    #        single ``Nested(uid UInt32, tag String)`` instead of
    #        being flattened to dotted sub-Arrays. With the default
    #        ``flatten_nested = 1`` the type spec is decomposed at
    #        CREATE TABLE time and the server never emits the
    #        ``Nested(...)`` form back, so we couldn't exercise the
    #        sugar.
    table = "test_nested_column"
    async with pool.acquire() as client:
        await client.execute(f"DROP TABLE IF EXISTS {table}")
        await client.execute(
            f"CREATE TABLE {table} "
            f"(id UInt64, events Nested(uid UInt32, tag String)) "
            f"ENGINE = Memory",
            settings={"flatten_nested": "0"},
        )

    rows_in: list[tuple[object, ...]] = [
        (1, [(10, "click"), (11, "view")]),
        (2, []),
        (3, [(30, "purchase")]),
    ]

    # WHEN: inserting via the pool, then reading back
    async with pool.acquire() as client:
        n = await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=rows_in,
            column_names=["id", "events"],
        )
    assert n == 3
    rows_out = await pool.fetch_all(f"SELECT id, events FROM {table} ORDER BY id")

    # THEN: every row's nested array of tuples round-trips
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


async def test_variant_column_round_trips_via_server(pool: ch.Pool) -> None:
    # BEGIN: a Memory-engine table with a ``Variant(Int64, String,
    #        Date)`` column. Variant is still gated behind the
    #        ``allow_experimental_variant_type`` setting on 24.x, so
    #        each statement that touches the column hands the setting
    #        in. Mixed-arm rows + a NULL exercise the discriminator
    #        stream and per-arm body slicing end-to-end against the
    #        server.
    table = "test_variant_column"
    variant_settings = {"allow_experimental_variant_type": "1"}
    async with pool.acquire() as client:
        await client.execute(f"DROP TABLE IF EXISTS {table}")
        await client.execute(
            f"CREATE TABLE {table} (id UInt64, v Variant(Int64, String, Date)) "
            f"ENGINE = Memory",
            settings=variant_settings,
        )

    rows_in: list[tuple[object, ...]] = [
        (1, 42),
        (2, "hello"),
        (3, date(2026, 5, 3)),
        (4, None),
    ]

    # WHEN: inserting via the pool, then reading back. The Variant
    #       column needs the experimental flag on both INSERT and
    #       SELECT — the type-spec parse fails otherwise.
    async with pool.acquire() as client:
        n = await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=rows_in,
            column_names=["id", "v"],
            settings=variant_settings,
        )
    assert n == 4
    rows_out = await pool.fetch_all(
        f"SELECT id, v FROM {table} ORDER BY id",
        settings=variant_settings,
    )

    # THEN: every arm survives end-to-end; the NULL row comes back
    #       as None (NULL discriminator → Python None)
    assert rows_out == rows_in


async def test_dynamic_column_round_trips_via_server(pool: ch.Pool) -> None:
    # BEGIN: a Memory-engine table with a ``Dynamic`` column.
    #        ``Dynamic`` is gated behind ``allow_experimental_dynamic_type``
    #        on 24.x — same per-statement opt-in as Variant. Mixing
    #        Int64 and String values across rows exercises the
    #        per-block prefix declaring active types and the implicit
    #        ``SharedVariant`` (String) tail-arm that ClickHouse always
    #        carries on the wire for V1 serialization.
    table = "test_dynamic_column"
    dynamic_settings = {"allow_experimental_dynamic_type": "1"}
    async with pool.acquire() as client:
        await client.execute(f"DROP TABLE IF EXISTS {table}")
        await client.execute(
            f"CREATE TABLE {table} (id UInt64, d Dynamic) ENGINE = Memory",
            settings=dynamic_settings,
        )

    rows_in: list[tuple[object, ...]] = [
        (1, 42),
        (2, "hello"),
        (3, None),
        (4, 99),
    ]

    # WHEN: inserting via the pool, then reading back. Both sides need
    #       the experimental flag — the type-spec parse fails otherwise.
    async with pool.acquire() as client:
        n = await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=rows_in,
            column_names=["id", "d"],
            settings=dynamic_settings,
        )
    assert n == 4
    rows_out = await pool.fetch_all(
        f"SELECT id, d FROM {table} ORDER BY id",
        settings=dynamic_settings,
    )

    # THEN: every row's value lands in the right discriminator
    assert rows_out == rows_in


async def test_json_column_round_trips_via_server(pool: ch.Pool) -> None:
    # BEGIN: a Memory-engine table with a ``JSON`` column. ``JSON`` is
    #        gated behind ``allow_experimental_json_type`` on 24.x.
    #        Multi-row mixed-path-set INSERT exercises the full
    #        SerializationObject substream cascade end-to-end:
    #        ``ObjectStructure`` prefix (V1 + path list) → per-path
    #        Dynamic body → shared-data ``Array(Tuple(String, String))``
    #        body. Heterogeneous values per path (Int64 in one row,
    #        String in another) exercise the per-path Dynamic codec's
    #        multi-arm support.
    table = "test_json_column"
    json_settings = {"allow_experimental_json_type": "1"}
    async with pool.acquire() as client:
        await client.execute(f"DROP TABLE IF EXISTS {table}")
        await client.execute(
            f"CREATE TABLE {table} (id UInt64, j JSON) ENGINE = Memory",
            settings=json_settings,
        )

    rows_in: list[tuple[object, ...]] = [
        (1, {"a": 42, "b": "hello"}),
        (2, {"a": 99}),
        (3, {"b": "world"}),
        (4, {}),
    ]

    # WHEN: inserting via the pool, then reading back. Both sides need
    #       the experimental flag — the type-spec parser succeeds
    #       without it, but read/write fail at the codec.
    async with pool.acquire() as client:
        n = await client.insert(
            f"INSERT INTO {table} VALUES",
            rows=rows_in,
            column_names=["id", "j"],
            settings=json_settings,
        )
    assert n == 4
    rows_out = await pool.fetch_all(
        f"SELECT id, j FROM {table} ORDER BY id",
        settings=json_settings,
    )

    # THEN: every row's path → value mapping survives end-to-end. The
    #       server may emit paths in a normalised order so we compare
    #       per-row dicts, not the raw type spec.
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


async def test_fetch_columns_round_trip(client: ch.Client) -> None:
    # BEGIN: a query that returns two typed columns over five rows
    # WHEN: running fetch_columns instead of execute
    result = await client.fetch_columns(
        "SELECT number, toString(number) AS s FROM system.numbers LIMIT 5"
    )

    # THEN: ColumnarResult with column-major data — no row-tuple transpose
    assert isinstance(result, ColumnarResult)
    assert [c.name for c in result.columns] == ["number", "s"]
    assert result.rows == 5
    assert result.data[0] == [0, 1, 2, 3, 4]
    assert result.data[1] == ["0", "1", "2", "3", "4"]
    assert result.elapsed >= 0.0


async def test_iter_column_blocks_large_result(client: ch.Client) -> None:
    # BEGIN: a 100k-row query that forces multiple server blocks
    # WHEN: streaming via iter_column_blocks
    total_rows = 0
    block_count = 0
    async for block in client.iter_column_blocks(
        "SELECT number FROM system.numbers LIMIT 100000"
    ):
        # THEN: each yielded value is a ColumnarBlock with column-major data
        assert isinstance(block, ColumnarBlock)
        assert block.n_rows == len(block.data[0])
        total_rows += block.n_rows
        block_count += 1

    assert total_rows == 100000
    assert block_count > 1  # server split into multiple blocks
