"""Pytest configuration for clickhouse-async tests."""

from collections.abc import Generator

import pytest
from testcontainers.clickhouse import ClickHouseContainer  # type: ignore

from clickhouse_async.client_options import ClientOptions

# Configure pytest plugins
pytest_plugins = ["pytest_asyncio", "tests.pytest_plugins"]


@pytest.fixture(scope="session")
def clickhouse_container() -> Generator[ClickHouseContainer, None, None]:
    """Provide a ClickHouse container for testing.
    Returns:
        Generator yielding a ClickHouseContainer instance
    """
    import logging

    logger = logging.getLogger(__name__)

    with ClickHouseContainer(
        image="clickhouse/clickhouse-server:24.12",
        username="default",
        password="test",
        dbname="test",
    ) as container:
        # Wait for container to be ready
        logger.info(
            f"ClickHouse container started: {container.get_wrapped_container().id}"
        )
        logger.info(f"Container host: {container.get_container_host_ip()}")
        logger.info(f"Container port: {container.get_exposed_port(9000)}")

        yield container

        # Log container logs before cleanup
        logger.info("Container logs before cleanup:")
        try:
            logs = container.get_wrapped_container().logs(tail=50).decode("utf-8")
            for line in logs.split("\n"):
                if line.strip():
                    logger.info(f"CLICKHOUSE: {line}")
        except Exception as e:
            logger.error(f"Failed to get container logs: {e}")


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
