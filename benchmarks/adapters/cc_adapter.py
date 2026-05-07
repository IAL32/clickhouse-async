"""``clickhouse-connect`` (official driver) — HTTP, async wrapper."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import clickhouse_connect

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence


def _parse_dsn(dsn: str) -> dict[str, Any]:
    """``http://user:pass@host:port/db`` → clickhouse-connect kwargs.

    The benchmark passes the HTTP DSN explicitly (port 8123) so the
    parsing here is a thin wrapper that keeps user/pass/database in
    sync with the native-protocol adapters.
    """
    parsed = urlparse(dsn)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"clickhouse-connect needs an HTTP DSN: {dsn!r}")
    return {
        "interface": parsed.scheme,
        "host": parsed.hostname or "localhost",
        "port": parsed.port or (8443 if parsed.scheme == "https" else 8123),
        "username": parsed.username or "default",
        "password": parsed.password or "",
        "database": parsed.path.lstrip("/") or "default",
    }


class _CCClient:
    def __init__(self, client: Any) -> None:
        self._client = client

    async def select_one(self, sql: str) -> Any:
        result = await self._client.query(sql)
        rows = result.result_rows
        return rows[0] if rows else None

    async def select_rows(self, sql: str) -> int:
        result = await self._client.query(sql)
        return len(result.result_rows)

    async def insert_rows(
        self,
        table: str,
        rows: Sequence[Sequence[Any]],
        columns: Sequence[str],
    ) -> int:
        # ``client.insert`` is the bulk path; it accepts a list of rows
        # and an explicit column-name list. The driver returns a
        # ``QuerySummary`` whose ``written_rows`` is the server-confirmed
        # count.
        summary = await self._client.insert(
            table=table,
            data=list(rows),
            column_names=list(columns),
        )
        return getattr(summary, "written_rows", None) or len(rows)


class ClickhouseConnectAdapter:
    """Adapter for clickhouse-connect's async client. Compression is on
    by default (``lz4`` for the HTTP transport when the lz4 library is
    available — it is in the benchmark venv via the parent's
    ``[compression]`` extra)."""

    name = "clickhouse-connect"

    def __init__(self, dsn: str) -> None:
        self._kwargs = _parse_dsn(dsn)

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[_CCClient]:
        client = await clickhouse_connect.get_async_client(**self._kwargs)
        try:
            yield _CCClient(client)
        finally:
            await client.close()
