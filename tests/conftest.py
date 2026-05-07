"""Shared pytest configuration: `--localdb` flag and the `dsn`
fixture.

Most tests live under `tests/unit/` and never reach for `dsn` —
they don't need a server. Integration tests under
`tests/integration/` consume `dsn` (or one of the higher-level
`client` / `pool` fixtures defined alongside them) and skip
gracefully when neither a local server nor docker is available.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.containers.clickhouse import ClickHouseContainer

if TYPE_CHECKING:
    from collections.abc import Iterator

# Canonical default — kept in sync with CLAUDE.md and
# scripts/clickhouse.sh. Bumping in one place needs a bump in all.
DEFAULT_LOCAL_DSN = "clickhouse://clickhouse:clickhouse@localhost:9000/clickhouse"


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add `--localdb[=DSN]` so integration tests can hit a local
    server instead of starting a container.

    Behaviour matches CLAUDE.md exactly:
    - `--localdb` (no value) → use the canonical default DSN.
    - `--localdb=clickhouse://...` → use that DSN verbatim.
    - flag absent → start a `ClickHouseContainer` for the session.
    """
    parser.addoption(
        "--localdb",
        action="store",
        nargs="?",
        const=DEFAULT_LOCAL_DSN,
        default=None,
        metavar="DSN",
        help=(
            "Run integration tests against a local ClickHouse instead "
            "of starting a testcontainers instance. Bare flag uses the "
            "canonical default DSN; pass --localdb=<dsn> for a custom "
            "one."
        ),
    )


@pytest.fixture(scope="session")
def dsn(request: pytest.FixtureRequest) -> Iterator[str]:
    """Session-scoped DSN string for integration tests.

    Either points at a developer-provided local server (`--localdb`)
    or spins up a `ClickHouseContainer` shared across the session. If
    neither path works (no `--localdb`, docker unavailable), the
    fixture skips so a developer without docker installed can still
    run `pytest tests/integration` and see a clean message.
    """
    local: str | None = request.config.getoption("localdb")
    if local is not None:
        yield local
        return

    try:
        container = ClickHouseContainer().start()
    except Exception as exc:
        pytest.skip(
            f"docker is not available for integration tests: {exc!r}. "
            f"Start one yourself and pass --localdb to point at it."
        )

    try:
        yield container.dsn
    finally:
        container.stop()
