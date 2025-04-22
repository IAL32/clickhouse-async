"""Integration tests for the ClickHouse client."""

from clickhouse_async.client import ClickHouseClient
from clickhouse_async.client_options import ClientOptions


async def test_connect_and_ping(clickhouse_connection_params: ClientOptions) -> None:
    """Test connecting to the server and pinging it.
    Args:
        clickhouse_connection_params: Connection parameters for the ClickHouse container
    """
    # Print connection parameters for debugging

    # Create client with longer timeout
    client = ClickHouseClient(
        clickhouse_connection_params,
        connect_timeout=30.0,
        send_receive_timeout=30.0,
    )

    # Connect to the server
    await client.connect()

    try:
        # Check that we can connect and ping
        assert client.server_info is not None
        assert client.server_info.name == "ClickHouse"
        assert client.server_info.version_major > 0

        # Ping the server
        assert await client.ping()
    finally:
        # Disconnect from the server
        await client.disconnect()


async def test_simple_select(clickhouse_connection_params: ClientOptions) -> None:
    """Test executing a simple SELECT query.
    Args:
        clickhouse_connection_params: Connection parameters for the ClickHouse container
    """
    # Create client with longer timeout
    client = ClickHouseClient(
        clickhouse_connection_params,
        connect_timeout=30.0,
        send_receive_timeout=30.0,
    )

    # Connect to the server
    await client.connect()

    try:
        # Execute a simple SELECT query
        result = await client.execute("SELECT 1")
        assert len(result) == 1
        assert 1 in result[0].values()
    finally:
        # Disconnect from the server
        await client.disconnect()
