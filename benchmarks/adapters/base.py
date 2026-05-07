"""The uniform async interface every adapter implements."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Sequence
    from contextlib import AbstractAsyncContextManager


@runtime_checkable
class AdapterClient(Protocol):
    """One open connection / session against a ClickHouse server.

    The methods cover only what the scenarios call:

    - ``select_one(sql)`` — single-row read; returns the row or ``None``
    - ``select_rows(sql)`` — large read; returns the row count after
      the result has been fully consumed
    - ``insert_rows(table, rows, columns)`` — bulk insert; returns the
      number of rows the server confirmed (or ``len(rows)`` for
      libraries that don't surface a server count)
    """

    async def select_one(self, sql: str) -> Any: ...

    async def select_rows(self, sql: str) -> int: ...

    async def insert_rows(
        self, table: str, rows: Sequence[Sequence[Any]], columns: Sequence[str]
    ) -> int: ...


class Adapter(Protocol):
    """The factory each library exposes — `connect()` is an async
    context manager yielding an :class:`AdapterClient`."""

    name: str

    def connect(self) -> AbstractAsyncContextManager[AdapterClient]: ...
