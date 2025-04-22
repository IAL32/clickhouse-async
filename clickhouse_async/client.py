"""ClickHouse async client implementation."""

import types
from collections.abc import AsyncGenerator
from typing import Any

from .client_options import ClientOptions
from .connection_string import ConnectionString
from .protocol import Connection, ServerInfo


class ClickHouseClient:
    """Async client for ClickHouse database."""

    def __init__(
        self,
        connection_string_or_options: str
        | ConnectionString
        | ClientOptions
        | None = None,
        host: str = "localhost",
        port: int = 9000,
        user: str = "default",
        password: str = "",
        database: str = "default",
        **kwargs: Any,  # noqa: ANN401
    ) -> None:
        """Initialize ClickHouse client.

        Args:
            connection_string_or_options: Connection string, ConnectionString object, or ClientOptions object
            host: ClickHouse server host (used if connection_string_or_options is None)
            port: ClickHouse server port (used if connection_string_or_options is None)
            user: Username for authentication (used if connection_string_or_options is None)
            password: Password for authentication (used if connection_string_or_options is None)
            database: Default database to use (used if connection_string_or_options is None)
            **kwargs: Additional options for ClientOptions
        """
        if connection_string_or_options is None:
            self.options = ClientOptions(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                **kwargs,
            )
        elif isinstance(connection_string_or_options, str):
            self.options = ClientOptions.from_connection_string(
                connection_string_or_options
            )
        elif isinstance(connection_string_or_options, ConnectionString):
            self.options = ClientOptions.from_connection_string(
                connection_string_or_options
            )
        elif isinstance(connection_string_or_options, ClientOptions):
            self.options = connection_string_or_options
        else:
            raise TypeError(
                "connection_string_or_options must be a string, ConnectionString, or ClientOptions"
            )

        self.connection: Connection | None = None
        self.server_info: ServerInfo | None = None

    async def connect(self) -> None:
        """Connect to the ClickHouse server.

        Raises:
            ConnectionError: If connection fails
        """
        if self.connection is not None:
            return

        self.connection = Connection(
            host=self.options.host,
            port=self.options.port,
            database=self.options.database,
            user=self.options.user,
            password=self.options.password,
            compression=self.options.compression,
            connect_timeout=self.options.connect_timeout,
            send_timeout=self.options.send_receive_timeout,
            receive_timeout=self.options.send_receive_timeout,
        )

        await self.connection.connect()
        self.server_info = self.connection.server_info

    async def disconnect(self) -> None:
        """Disconnect from the ClickHouse server."""
        if self.connection is not None:
            await self.connection.close()
            self.connection = None
            self.server_info = None

    async def ping(self) -> bool:
        """Ping the ClickHouse server.

        Returns:
            True if ping was successful, False otherwise
        """
        if self.connection is None:
            try:
                await self.connect()
            except Exception:
                return False

        if self.connection is None:
            return False

        try:
            return await self.connection.ping()
        except Exception:
            return False

    async def execute(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a query and return the result.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            List of dictionaries representing the query results

        Raises:
            ConnectionError: If connection fails
            Exception: If query execution fails
        """
        # Connect to the server if not connected
        await self.connect()

        if self.connection is None:
            raise ConnectionError("Failed to connect to the server")

        # Execute the query
        return await self.connection.execute_query(query, params)

    async def execute_iter(
        self, query: str, params: dict[str, Any] | None = None
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Execute a query and return an async iterator over the results.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            Async iterator over the query results

        Raises:
            ConnectionError: If connection fails
            Exception: If query execution fails
        """
        # Get all results at once for now
        # In the future, we'll implement true streaming
        results = await self.execute(query, params)

        # Yield each row
        for row in results:
            yield row

    async def __aenter__(self) -> "ClickHouseClient":
        """Enter async context manager."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: BaseException | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        """Exit async context manager."""
        await self.disconnect()
