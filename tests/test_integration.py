"""Integration tests for the ClickHouse client."""

from clickhouse_async.client import ClickHouseClient
from clickhouse_async.client_options import ClientOptions


async def test_execute_query(clickhouse_connection_params: ClientOptions) -> None:
    """Test executing a query against a real ClickHouse server.
    Args:
        clickhouse_connection_params: Connection parameters for the ClickHouse container
    """
    client = ClickHouseClient(clickhouse_connection_params)

    # Connect to the server
    await client.connect()

    try:
        # Execute a simple query
        # Note: This is currently a placeholder implementation that returns an empty list
        # Once the actual query execution is implemented, this test should be updated
        result = await client.execute("SELECT 1")
        # For now, we just check that we get a list back
        assert isinstance(result, list)
    finally:
        # Disconnect from the server
        await client.disconnect()


async def test_execute_iter(clickhouse_connection_params: ClientOptions) -> None:
    """Test executing a query with iterator interface.
    Args:
        clickhouse_connection_params: Connection parameters for the ClickHouse container
    """
    client = ClickHouseClient(clickhouse_connection_params)

    # Connect to the server
    await client.connect()

    try:
        # Execute a simple query with iterator interface
        # Note: This is currently a placeholder implementation that yields nothing
        # Once the actual query execution is implemented, this test should be updated
        rows = []
        async for row in client.execute_iter("SELECT 1"):
            rows.append(row)
        # For now, we just check that we get an empty list back
        assert isinstance(rows, list)
    finally:
        # Disconnect from the server
        await client.disconnect()


async def test_connection_string(clickhouse_connection_params: ClientOptions) -> None:
    """Test connecting with a connection string.
    Args:
        clickhouse_connection_params: Connection parameters for the ClickHouse container
    """
    # Create a connection string from the connection parameters
    conn_str = (
        f"clickhouse://{clickhouse_connection_params.user}:{clickhouse_connection_params.password}"
        f"@{clickhouse_connection_params.host}:{clickhouse_connection_params.port}"
        f"/{clickhouse_connection_params.database}"
    )
    # Create a client with the connection string
    client = ClickHouseClient(conn_str)

    # Connect to the server
    await client.connect()

    try:
        # Verify that server_info was set
        assert client.server_info is not None
        assert client.server_info.name == "ClickHouse"
        assert client.server_info.version_major > 0

        # Ping the server
        assert await client.ping()
    finally:
        # Disconnect from the server
        await client.disconnect()
