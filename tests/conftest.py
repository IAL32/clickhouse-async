"""Pytest configuration for clickhouse-async tests."""

from collections.abc import Generator

import pytest
from testcontainers.clickhouse import ClickHouseContainer  # type: ignore

from clickhouse_async.client_options import ClientOptions

# Configure pytest plugins
pytest_plugins = ["pytest_asyncio"]


@pytest.fixture(scope="session")
def clickhouse_container() -> Generator[ClickHouseContainer, None, None]:
    """Provide a ClickHouse container for testing.
    Returns:
        Generator yielding a ClickHouseContainer instance
    """
    with ClickHouseContainer(
        image="clickhouse/clickhouse-server:21.8",
        username="default",
        password="test",
        dbname="test",
    ) as container:
        # Wait for container to be ready
        yield container


@pytest.fixture
def clickhouse_connection_params(
    clickhouse_container: ClickHouseContainer,
) -> ClientOptions:
    """Provide connection parameters for the ClickHouse container.
    Args:
        clickhouse_container: The ClickHouse container instance
    Returns:
        ClientOptions instance with connection parameters
    """
    # Get the host and port directly from the container
    host = clickhouse_container.get_container_host_ip()
    # Get the exposed port for the native protocol port (9000)
    port = clickhouse_container.get_exposed_port(9000)

    # Create and return a ClientOptions object
    options = ClientOptions(
        host=host,
        port=int(port),
        user="default",
        password="test",
        database="test",
    )
    return options
