"""Integration-test fixtures and the ``integration`` marker.

Every test under ``tests/integration/`` is auto-marked
``integration`` so the default ``pytest`` invocation (which runs with
``-m 'not integration'`` per ``pyproject.toml``) skips them. To run
them explicitly: ``pytest tests/integration`` or ``pytest -m integration``.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import clickhouse_async as ch

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Auto-mark every test under ``tests/integration/`` with the
    ``integration`` marker so the default unit run filters them out
    via ``-m 'not integration'``."""
    integration_dir = Path(__file__).resolve().parent
    for item in items:
        try:
            item_path = Path(str(item.path))
        except Exception:
            continue
        if integration_dir in item_path.parents:
            item.add_marker(pytest.mark.integration)


@pytest.fixture
async def client(dsn: str) -> AsyncIterator[ch.Client]:
    """A freshly-opened, freshly-closed ``Client`` per test."""
    async with ch.connect(dsn, connect_timeout=10.0) as c:
        yield c


@pytest.fixture
async def pool(dsn: str) -> AsyncIterator[ch.Pool]:
    """A pool tuned to a small size for speed; one per test."""
    async with ch.create_pool(dsn, min_size=0, max_size=4, connect_timeout=10.0) as p:
        yield p


@pytest.fixture
async def fresh_table(
    pool: ch.Pool,
) -> AsyncIterator[Callable[[str, str], Awaitable[None]]]:
    """Return a coroutine that drops + recreates a server-side table.

    Integration tests clean up **before** they run; the helper is a
    thin wrapper around ``DROP TABLE IF EXISTS`` + ``CREATE TABLE`` so
    each test owns its own table named after itself (avoid collisions
    under ``pytest-xdist`` / random ordering).
    """

    async def _fresh(name: str, schema: str) -> None:
        async with pool.acquire() as c:
            await c.execute(f"DROP TABLE IF EXISTS {name}")
            await c.execute(f"CREATE TABLE {name} {schema}")

    yield _fresh
