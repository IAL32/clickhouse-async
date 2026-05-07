"""``clickhouse-async`` (this project) — native TCP, async-only."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import clickhouse_async as ch

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence


class _CAClient:
    def __init__(self, client: ch.Client) -> None:
        self._client = client

    async def select_one(self, sql: str) -> Any:
        return await self._client.fetch_one(sql)

    async def select_rows(self, sql: str) -> int:
        rows = await self._client.fetch_all(sql)
        return len(rows)

    async def insert_rows(
        self,
        table: str,
        rows: Sequence[Sequence[Any]],
        columns: Sequence[str],
    ) -> int:
        return await self._client.insert(
            f"INSERT INTO {table} VALUES",
            rows=list(rows),
            column_names=list(columns),
        )


class ClickhouseAsyncAdapter:
    """Adapter for clickhouse-async. Uses the default LZ4 compression
    when the ``[compression]`` extra is installed (it is in the
    benchmark venv)."""

    name = "clickhouse-async"

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[_CAClient]:
        async with ch.connect(self._dsn) as client:
            yield _CAClient(client)
