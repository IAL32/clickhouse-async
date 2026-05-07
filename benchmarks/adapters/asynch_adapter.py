"""``asynch`` (long2ice/asynch) — native TCP, async-only, DBAPI-style.

Same module is used for both the PyPI release and the tacto fork; the
choice of which is installed is driven by the benchmark's pyproject
extras (see ``benchmarks/README.md``). Both share the same INSERT
quirk that the drain helper below works around.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from asynch import Connection
from asynch.proto.protocol import ServerPacket

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence


def _parse_dsn(dsn: str) -> dict[str, Any]:
    """``clickhouse://user:pass@host:port/db`` → asynch kwargs.

    asynch does take a DSN-shaped string via ``connect(dsn=...)`` in
    recent versions, but parsing it ourselves makes the connection
    parameters identical across libraries — same host, port, user,
    password, database — instead of relying on slightly different DSN
    grammars between drivers.
    """
    parsed = urlparse(dsn)
    if parsed.scheme not in ("clickhouse", "clickhouses"):
        raise ValueError(f"unexpected DSN scheme {parsed.scheme!r}: {dsn!r}")
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 9000,
        "user": parsed.username or "default",
        "password": parsed.password or "",
        "database": parsed.path.lstrip("/") or "default",
    }


class _AsynchClient:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    async def select_one(self, sql: str) -> Any:
        async with self._conn.cursor() as cursor:
            await cursor.execute(sql)
            row = await cursor.fetchone()
            return row

    async def select_rows(self, sql: str) -> int:
        async with self._conn.cursor() as cursor:
            await cursor.execute(sql)
            rows = await cursor.fetchall()
            return len(rows)

    async def insert_rows(
        self,
        table: str,
        rows: Sequence[Sequence[Any]],
        columns: Sequence[str],
    ) -> int:
        # ``executemany`` is asynch's documented bulk-INSERT path. The
        # column names are part of the SQL prefix, mirroring the wire-
        # level VALUES header the server expects.
        col_list = ", ".join(columns)
        async with self._conn.cursor() as cursor:
            await cursor.executemany(
                f"INSERT INTO {table} ({col_list}) VALUES",
                list(rows),
            )
            rowcount = cursor.rowcount or len(rows)
        # asynch quirk (PyPI 0.3.1 and tacto fork): ``process_insert_query``
        # only awaits one packet after sending data. The server's trailing
        # END_OF_STREAM stays in the buffer, which leaves the proto-level
        # ``is_query_executing`` flag stuck at ``True`` and corrupts the
        # next operation on the same connection. Drain to END_OF_STREAM
        # so the connection is reusable.
        proto = self._conn._connection  # type: ignore[attr-defined]
        while proto.is_query_executing:
            packet = await proto._receive_packet()
            if packet.type == ServerPacket.END_OF_STREAM:
                # ``_receive_packet`` already flips the flag for this
                # packet; the loop guard re-checks and exits.
                break
        return rowcount


class AsynchAdapter:
    """Adapter for asynch. Default config — no compression toggle."""

    name = "asynch"

    def __init__(self, dsn: str) -> None:
        self._kwargs = _parse_dsn(dsn)

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[_AsynchClient]:
        conn = Connection(**self._kwargs)
        await conn.connect()
        try:
            yield _AsynchClient(conn)
        finally:
            await conn.close()
