"""Connection implementation for ClickHouse client."""

import logging
import types
from typing import Any

from .constants import (
    ClickHouseProtocol,
    ClientCodes,
    CompressionMethod,
    CompressionState,
    ServerCodes,
)
from .query import (
    read_block,
    read_exception,
    send_query,
)
from .socket import AsyncSocket
from .stream import InputStream, OutputStream

logger = logging.getLogger(__name__)


class ServerInfo:
    """Server information."""

    def __init__(self) -> None:
        """Initialize server information."""
        self.name = ""
        self.version_major = 0
        self.version_minor = 0
        self.version_patch = 0
        self.revision = 0
        self.timezone = ""
        self.display_name = ""


class Connection:
    """Connection to ClickHouse server."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        compression: bool = False,
        connect_timeout: float = 5.0,
        send_timeout: float = 5.0,
        receive_timeout: float = 5.0,
    ) -> None:
        """Initialize connection.

        Args:
            host: Host to connect to
            port: Port to connect to
            database: Database to use
            user: Username for authentication
            password: Password for authentication
            compression: Whether to use compression
            connect_timeout: Connection timeout in seconds
            send_timeout: Send timeout in seconds
            receive_timeout: Receive timeout in seconds
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.compression = (
            CompressionState.ENABLE if compression else CompressionState.DISABLE
        )
        self.compression_method = (
            CompressionMethod.LZ4 if compression else CompressionMethod.NONE
        )

        self.socket = AsyncSocket(
            host=host,
            port=port,
            connect_timeout=connect_timeout,
            send_timeout=send_timeout,
            receive_timeout=receive_timeout,
        )

        self.input_stream: InputStream | None = None
        self.output_stream: OutputStream | None = None

        self.server_info = ServerInfo()

    @property
    def protocol_version(self) -> int:
        """Get the protocol version."""
        # Use the server's revision if available, otherwise use our default
        if hasattr(self.server_info, "revision") and self.server_info.revision > 0:
            logger.debug(f"Using server revision: {self.server_info.revision}")
            return self.server_info.revision
        logger.debug(
            f"Using default protocol version: {ClickHouseProtocol.DBMS_PROTOCOL_VERSION}"
        )
        return ClickHouseProtocol.DBMS_PROTOCOL_VERSION

    async def connect(self) -> None:
        """Connect to the server and perform handshake.

        Raises:
            ConnectionError: If connection fails
            ProtocolError: If handshake fails
        """
        await self.socket.connect()

        self.input_stream = InputStream(self.socket)
        self.output_stream = OutputStream(self.socket)

        await self.send_hello()
        await self.receive_hello()

    async def send_hello(self) -> None:
        """Send hello packet to the server.

        Raises:
            ConnectionError: If send fails
        """
        if not self.output_stream:
            raise ConnectionError("Not connected")

        logger.debug(
            f"Sending hello to {self.host}:{self.port} with user={self.user}, password={self.password}, database={self.database}"
        )

        # Client Hello
        await self.output_stream.write_varint(ClientCodes.HELLO)

        # Client name
        await self.output_stream.write_string(f"{ClickHouseProtocol.DBMS_NAME} client")

        # Client version
        await self.output_stream.write_varint(ClickHouseProtocol.DBMS_VERSION_MAJOR)
        await self.output_stream.write_varint(ClickHouseProtocol.DBMS_VERSION_MINOR)
        await self.output_stream.write_varint(ClickHouseProtocol.DBMS_PROTOCOL_VERSION)

        # Database
        await self.output_stream.write_string(self.database)

        # User
        await self.output_stream.write_string(self.user)

        # Password
        await self.output_stream.write_string(self.password)

        await self.output_stream.flush()

    async def receive_hello(self) -> None:
        """Receive hello packet from the server.

        Raises:
            ConnectionError: If receive fails
            ProtocolError: If handshake fails
        """
        if not self.input_stream:
            raise ConnectionError("Not connected")

        packet_type = await self.input_stream.read_varint()

        if packet_type == ServerCodes.HELLO:
            # Server name
            self.server_info.name = await self.input_stream.read_string()
            logger.debug(f"Server name: {self.server_info.name}")

            # Server version
            self.server_info.version_major = await self.input_stream.read_varint()
            self.server_info.version_minor = await self.input_stream.read_varint()
            self.server_info.revision = await self.input_stream.read_varint()
            logger.debug(
                f"Server version: {self.server_info.version_major}.{self.server_info.version_minor}, revision: {self.server_info.revision}"
            )

            # Server timezone (if supported)
            if (
                self.server_info.revision
                >= ClickHouseProtocol.MIN_REVISION_WITH_SERVER_TIMEZONE
            ):
                self.server_info.timezone = await self.input_stream.read_string()

            # Server display name (if supported)
            if (
                self.server_info.revision
                >= ClickHouseProtocol.MIN_REVISION_WITH_SERVER_DISPLAY_NAME
            ):
                self.server_info.display_name = await self.input_stream.read_string()

            # Server version patch (if supported)
            if (
                self.server_info.revision
                >= ClickHouseProtocol.MIN_REVISION_WITH_VERSION_PATCH
            ):
                self.server_info.version_patch = await self.input_stream.read_varint()

        elif packet_type == ServerCodes.EXCEPTION:
            # Read the exception
            exception = await read_exception(self.input_stream)
            raise ConnectionError(
                f"Server returned exception during handshake: {exception}"
            )

        else:
            raise ConnectionError(
                f"Unexpected packet type during handshake: {packet_type}"
            )

    async def close(self) -> None:
        """Close the connection."""
        await self.socket.close()
        self.input_stream = None
        self.output_stream = None

    async def ping(self) -> bool:
        """Ping the server.

        Returns:
            True if ping was successful, False otherwise
        """
        if not self.output_stream or not self.input_stream:
            return False

        try:
            # Send ping
            await self.output_stream.write_varint(ClientCodes.PING)
            await self.output_stream.flush()

            # Receive pong
            packet_type = await self.input_stream.read_varint()
            return packet_type == ServerCodes.PONG

        except Exception:
            return False

    async def receive_packet(self) -> bool:
        """Receive a packet from the server.

        Returns:
            True if more packets should be processed, False if end of stream

        Raises:
            ConnectionError: If connection fails
            ProtocolError: If protocol error occurs
        """
        if not self.input_stream:
            raise ConnectionError("Not connected")

        # Create query result if not exists
        if not hasattr(self, "query_result"):
            from .query import QueryResult

            self.query_result = QueryResult()

        try:
            # Read packet type
            packet_type = await self.input_stream.read_varint()
            logger.debug(
                f"Received packet type: {packet_type} ({ServerCodes(packet_type).name if packet_type in ServerCodes.__members__.values() else 'UNKNOWN'})"
            )

            if packet_type == ServerCodes.DATA:
                # Skip temporary table name
                await self.input_stream.read_string()

                # Read block
                block = await read_block(self.input_stream)
                self.query_result.blocks.append(block)
                return True

            elif packet_type == ServerCodes.EXCEPTION:
                # Read exception
                self.query_result.exception = await read_exception(self.input_stream)
                return False

            elif packet_type == ServerCodes.PROGRESS:
                # Read progress
                self.query_result.progress_rows = await self.input_stream.read_varint()
                self.query_result.progress_bytes = await self.input_stream.read_varint()
                self.query_result.progress_total_rows = (
                    await self.input_stream.read_varint()
                )
                return True

            elif packet_type == ServerCodes.PROFILE_INFO:
                # Read profile info
                self.query_result.rows_read = await self.input_stream.read_varint()
                self.query_result.bytes_read = await self.input_stream.read_varint()
                self.query_result.elapsed_seconds = (
                    await self.input_stream.read_varint() / 1000.0
                )
                return True

            elif packet_type == ServerCodes.TOTALS:
                # Read totals block
                await self.input_stream.read_string()  # Skip temporary table name
                self.query_result.totals = await read_block(self.input_stream)
                return True

            elif packet_type == ServerCodes.EXTREMES:
                # Read extremes block
                await self.input_stream.read_string()  # Skip temporary table name
                self.query_result.extremes = await read_block(self.input_stream)
                return True

            elif packet_type == ServerCodes.END_OF_STREAM:
                # End of stream
                return False

            elif packet_type == ServerCodes.PONG:
                # Pong response
                return True

            else:
                logger.debug(f"Unknown packet type: {packet_type}")
                # Skip unknown packet
                return True
        except EOFError as e:
            # If we get an EOFError, the server closed the connection
            logger.debug(f"Server closed connection: {e}")
            # Return False to stop processing packets
            return False

    async def execute_query(
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
            ServerException: If query execution fails
        """
        if not self.output_stream or not self.input_stream:
            raise ConnectionError("Not connected")

        # Create a new query result
        from .query import QueryResult

        self.query_result = QueryResult()

        # Send query
        await send_query(
            self.output_stream,
            self.user,
            self.protocol_version,
            self.compression,
            query,
            query_id="",
            settings=params,
        )

        # Process packets until end of stream
        while await self.receive_packet():
            pass

        # Check for exception
        if self.query_result.has_exception and self.query_result.exception is not None:
            raise self.query_result.exception

        # Return rows
        return self.query_result.rows

    async def __aenter__(self) -> "Connection":
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
        await self.close()
