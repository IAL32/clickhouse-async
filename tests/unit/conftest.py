"""Unit-test configuration.

Pin the default compression to NONE so that unit tests that build
``Connection`` objects without an explicit ``compression=`` argument
get a predictable wire layout regardless of which optional extras are
installed in the test environment. Tests in
``test_compression_default.py`` manage the env var themselves via
``monkeypatch`` and are unaffected by this fixture.
"""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Apply a 1-second timeout to every unit test."""
    timeout_mark = pytest.mark.timeout(1)
    for item in items:
        item.add_marker(timeout_mark, append=False)


@pytest.fixture(autouse=True)
def _pin_default_compression(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLICKHOUSE_ASYNC_DEFAULT_COMPRESSION", "off")
