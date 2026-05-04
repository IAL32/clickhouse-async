"""Shared fixtures for scenario tests.

Session-scoped fixtures load three public ClickHouse datasets once per
session and yield the table name string to each test. Data is loaded via
``INSERT INTO ... SELECT ... FROM url/s3(...)`` so ClickHouse pulls from
the source directly; no Python-side downloads.

Every test under ``tests/scenarios/`` is auto-marked ``scenarios`` and
is excluded from the default ``pytest`` run (which filters
``-m 'not integration and not scenarios'``). Run with:
    pytest tests/scenarios --localdb
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import clickhouse_async as ch

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    scenarios_dir = Path(__file__).resolve().parent
    for item in items:
        try:
            item_path = Path(str(item.path))
        except Exception:
            continue
        if scenarios_dir in item_path.parents or item_path.parent == scenarios_dir:
            item.add_marker(pytest.mark.scenarios)


@pytest.fixture
async def client(dsn: str) -> AsyncIterator[ch.Client]:
    """Function-scoped client — each test gets a fresh connection."""
    async with ch.connect(dsn) as c:
        yield c


# ---------------------------------------------------------------------------
# Session-scoped dataset fixtures.
#
# Each fixture is sync and uses asyncio.run() to avoid the session-scoped
# async event-loop complexity in pytest-asyncio; only a plain string (the
# table name) crosses the boundary into function-scoped tests.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def covid19(dsn: str) -> str:
    """Load the COVID-19 epidemiology dataset (~12.5 M rows) once per session."""
    table = "scenarios_covid19"

    async def _load() -> None:
        async with ch.create_pool(dsn, min_size=0, max_size=4) as pool:
            async with pool.acquire() as c:
                await c.execute(f"DROP TABLE IF EXISTS {table}")
                await c.execute(f"""
                    CREATE TABLE {table} (
                        date                  Date,
                        location_key          LowCardinality(String),
                        new_confirmed         Int32,
                        new_deceased          Int32,
                        new_recovered         Int32,
                        new_tested            Int32,
                        cumulative_confirmed  Int32,
                        cumulative_deceased   Int32,
                        cumulative_recovered  Int32,
                        cumulative_tested     Int32
                    ) ENGINE = MergeTree ORDER BY (location_key, date)
                """)
                await c.execute(
                    f"""
                    INSERT INTO {table}
                    SELECT * FROM url(
                        'https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv',
                        CSVWithNames
                    )
                    SETTINGS max_http_get_redirects = 5
                    """,
                    settings={"max_execution_time": "300"},
                )
                result = await c.execute(f"SELECT count() FROM {table}")
                n = result.rows[0][0]
                assert isinstance(n, int) and n > 0, "COVID-19 load produced no rows"

    asyncio.run(_load())
    return table


@pytest.fixture(scope="session")
def cell_towers(dsn: str) -> str:
    """Load 500 000 cell tower records (OpenCelliD) once per session."""
    table = "scenarios_cell_towers"

    async def _load() -> None:
        async with ch.create_pool(dsn, min_size=0, max_size=4) as pool:
            async with pool.acquire() as c:
                await c.execute(f"DROP TABLE IF EXISTS {table}")
                await c.execute(f"""
                    CREATE TABLE {table} (
                        radio         Enum8('CDMA' = 0, 'GSM' = 1, 'LTE' = 2,
                                           'NR' = 3, 'UMTS' = 4),
                        mcc           UInt16,
                        net           UInt16,
                        area          UInt32,
                        cell          UInt64,
                        unit          Int16,
                        lon           Float64,
                        lat           Float64,
                        range         UInt32,
                        samples       UInt32,
                        changeable    UInt8,
                        created       DateTime,
                        updated       DateTime,
                        averageSignal UInt8
                    ) ENGINE = MergeTree ORDER BY (radio, mcc, cell)
                """)
                await c.execute(
                    f"""
                    INSERT INTO {table}
                    SELECT * FROM s3(
                        'https://datasets-documentation.s3.amazonaws.com/cell_towers/cell_towers.csv.xz',
                        CSVWithNames
                    )
                    LIMIT 500000
                    SETTINGS max_download_threads = 4
                    """,
                    settings={"max_execution_time": "300"},
                )
                result = await c.execute(f"SELECT count() FROM {table}")
                n = result.rows[0][0]
                assert isinstance(n, int) and n > 0, "cell_towers load produced no rows"

    asyncio.run(_load())
    return table


@pytest.fixture(scope="session")
def hackernews(dsn: str) -> str:
    """Load 200 000 Hacker News posts (Parquet) once per session."""
    table = "scenarios_hackernews"

    async def _load() -> None:
        async with ch.create_pool(dsn, min_size=0, max_size=4) as pool:
            async with pool.acquire() as c:
                await c.execute(f"DROP TABLE IF EXISTS {table}")
                await c.execute(f"""
                    CREATE TABLE {table} (
                        id          UInt32,
                        deleted     UInt8,
                        type        LowCardinality(String),
                        by          LowCardinality(String),
                        time        DateTime,
                        text        String,
                        dead        UInt8,
                        parent      UInt32,
                        poll        UInt32,
                        kids        Array(UInt32),
                        url         String,
                        score       Int32,
                        title       String,
                        parts       Array(UInt32),
                        descendants Int32
                    ) ENGINE = MergeTree ORDER BY (type, time)
                """)
                await c.execute(
                    f"""
                    INSERT INTO {table}
                    SELECT * FROM s3(
                        'https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/hacknernews.parquet',
                        Parquet
                    )
                    LIMIT 200000
                    """,
                    settings={"max_execution_time": "300"},
                )
                result = await c.execute(f"SELECT count() FROM {table}")
                n = result.rows[0][0]
                assert isinstance(n, int) and n > 0, "hackernews load produced no rows"

    asyncio.run(_load())
    return table
