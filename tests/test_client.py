"""Tests for the ClickHouse client."""

from unittest.mock import AsyncMock

import pytest

from clickhouse_async.client import ClickHouseClient
from clickhouse_async.client_options import ClientOptions


async def test_client_initialization() -> None:
    """Test that the client can be initialized with default parameters."""
    client = ClickHouseClient()
    assert client.options.host == "localhost"
    assert client.options.port == 9000  # Default port is now 9000 (native protocol)
    assert client.options.user == "default"
    assert client.options.password == ""
    assert client.options.database == "default"
    assert client.connection is None
    assert client.server_info is None


async def test_execute_returns_empty_list() -> None:
    """Test that the execute method returns an empty list (placeholder implementation)."""
    client = ClickHouseClient()

    # Use patch to mock the connect method
    with pytest.MonkeyPatch.context() as monkeypatch:
        mock_connect = AsyncMock()
        monkeypatch.setattr(client, "connect", mock_connect)

        result = await client.execute("SELECT 1")
        assert isinstance(result, list)
        assert len(result) == 0

        # Verify that connect was called
        mock_connect.assert_called_once()


async def test_connect_and_disconnect(
    clickhouse_connection_params: ClientOptions,
) -> None:
    """Test connecting to and disconnecting from the server.
    Args:
        clickhouse_connection_params: Connection parameters for the ClickHouse container
    """
    client = ClickHouseClient(clickhouse_connection_params)

    # Connect to the server
    await client.connect()

    # Verify that server_info was set
    assert client.server_info is not None
    assert client.server_info.name == "ClickHouse"
    assert client.server_info.version_major > 0

    # Disconnect from the server
    await client.disconnect()

    # Verify that connection and server_info were reset
    assert client.connection is None
    assert client.server_info is None


async def test_ping(clickhouse_connection_params: ClientOptions) -> None:
    """Test pinging the server.
    Args:
        clickhouse_connection_params: Connection parameters for the ClickHouse container
    """
    client = ClickHouseClient(clickhouse_connection_params)

    # Connect to the server
    await client.connect()

    # Ping the server
    result = await client.ping()

    # Verify that ping returned True
    assert result is True

    # Disconnect from the server
    await client.disconnect()


async def test_context_manager(clickhouse_connection_params: ClientOptions) -> None:
    """Test using the client as a context manager.
    Args:
        clickhouse_connection_params: Connection parameters for the ClickHouse container
    """
    # Use the client as a context manager
    async with ClickHouseClient(clickhouse_connection_params) as client:
        # Verify that server_info was set
        assert client.server_info is not None
        assert client.server_info.name == "ClickHouse"
        assert client.server_info.version_major > 0

        # Ping the server
        assert await client.ping()
