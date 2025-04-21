"""Tests for the ClickHouse client."""

import pytest

from clickhouse_async.client import ClickHouseClient


@pytest.mark.asyncio
async def test_client_initialization() -> None:
    """Test that the client can be initialized with default parameters."""
    client = ClickHouseClient()
    assert client.host == "localhost"
    assert client.port == 8123
    assert client.user == "default"
    assert client.password == ""
    assert client.database == "default"


@pytest.mark.asyncio
async def test_execute_returns_empty_list() -> None:
    """Test that the execute method returns an empty list (placeholder implementation)."""
    client = ClickHouseClient()
    result = await client.execute("SELECT 1")
    assert isinstance(result, list)
    assert len(result) == 0
