"""Connection implementation for ClickHouse client."""

import types

from .constants import (
    ClientCodes,
    CompressionMethod,
    CompressionState,
    Protocol,
    ServerCodes,
)
from .socket import AsyncSocket
from .stream import InputStream, OutputStream


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

        # Client Hello
        await self.output_stream.write_varint(ClientCodes.HELLO)

        # Client name
        await self.output_stream.write_string(f"{Protocol.DBMS_NAME} client")

        # Client version
        await self.output_stream.write_varint(Protocol.DBMS_VERSION_MAJOR)
        await self.output_stream.write_varint(Protocol.DBMS_VERSION_MINOR)
        await self.output_stream.write_varint(Protocol.DBMS_PROTOCOL_VERSION)

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

            # Server version
            self.server_info.version_major = await self.input_stream.read_varint()
            self.server_info.version_minor = await self.input_stream.read_varint()
            self.server_info.revision = await self.input_stream.read_varint()

            # Server timezone (if supported)
            if self.server_info.revision >= Protocol.MIN_REVISION_WITH_SERVER_TIMEZONE:
                self.server_info.timezone = await self.input_stream.read_string()

            # Server display name (if supported)
            if (
                self.server_info.revision
                >= Protocol.MIN_REVISION_WITH_SERVER_DISPLAY_NAME
            ):
                self.server_info.display_name = await self.input_stream.read_string()

            # Server version patch (if supported)
            if self.server_info.revision >= Protocol.MIN_REVISION_WITH_VERSION_PATCH:
                self.server_info.version_patch = await self.input_stream.read_varint()

        elif packet_type == ServerCodes.EXCEPTION:
            # TODO: Implement exception handling
            raise ConnectionError("Server returned exception during handshake")

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
