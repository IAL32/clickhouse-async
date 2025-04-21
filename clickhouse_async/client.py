"""ClickHouse async client implementation."""

import asyncio
from collections.abc import AsyncGenerator
from typing import Any


class ClickHouseClient:
    """Async client for ClickHouse database."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        user: str = "default",
        password: str = "",
        database: str = "default",
    ) -> None:
        """Initialize ClickHouse client.

        Args:
            host: ClickHouse server host
            port: ClickHouse server HTTP port
            user: Username for authentication
            password: Password for authentication
            database: Default database to use
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    async def execute(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a query and return the result.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            List of dictionaries representing the query results
        """
        # This is a placeholder implementation
        await asyncio.sleep(0.1)  # Simulate network delay
        return []

    async def execute_iter(
        self, query: str, params: dict[str, Any] | None = None
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Execute a query and return an async iterator over the results.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            Async iterator over the query results
        """
        # This is a placeholder implementation
        await asyncio.sleep(0.1)  # Simulate network delay
        # In a real implementation, we would yield rows from the query result
        # For now, this is just a placeholder that yields nothing
        for _ in range(
            0
        ):  # This is a valid empty loop that satisfies the AsyncGenerator type
            yield {}
