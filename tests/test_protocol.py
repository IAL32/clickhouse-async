"""Tests for the ClickHouse protocol implementation."""

import io

from clickhouse_async.client_options import ClientOptions
from clickhouse_async.protocol import (
    AsyncSocket,
    Connection,
    WireFormat,
)


class TestWireFormat:
    """Tests for the WireFormat class."""

    def test_varint(self) -> None:
        """Test reading and writing variable-length integers."""
        # Test values
        test_values = [
            0,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384,
            2097151,
            2097152,
            268435455,
            268435456,
        ]

        for value in test_values:
            # Write value
            buffer = io.BytesIO()
            WireFormat.write_varint(buffer, value)

            # Read value
            buffer.seek(0)
            read_value = WireFormat.read_varint(buffer)

            assert read_value == value

    def test_string(self) -> None:
        """Test reading and writing strings."""
        # Test values
        test_values = ["", "hello", "привет", "你好", "😀"]

        for value in test_values:
            # Write value
            buffer = io.BytesIO()
            WireFormat.write_string(buffer, value)

            # Read value
            buffer.seek(0)
            read_value = WireFormat.read_string(buffer)

            assert read_value == value

    def test_binary_string(self) -> None:
        """Test reading and writing binary strings."""
        # Test values
        test_values = [b"", b"hello", b"\x00\x01\x02\x03\x04"]

        for value in test_values:
            # Write value
            buffer = io.BytesIO()
            WireFormat.write_binary_string(buffer, value)

            # Read value
            buffer.seek(0)
            read_value = WireFormat.read_binary_string(buffer)

            assert read_value == value


class MockSocket:
    """Mock socket for testing."""

    def __init__(self) -> None:
        """Initialize mock socket."""
        self.sent_data = bytearray()
        self.receive_data = bytearray()

    async def connect(self) -> None:
        """Connect to the server."""
        pass

    async def send(self, data: bytes) -> None:
        """Send data to the server."""
        self.sent_data.extend(data)

    async def receive(self, n: int) -> bytes:
        """Receive data from the server."""
        if not self.receive_data:
            return b""

        data = bytes(self.receive_data[:n])
        self.receive_data = self.receive_data[n:]
        return data

    async def receive_exactly(self, n: int) -> bytes:
        """Receive exactly n bytes from the server."""
        if len(self.receive_data) < n:
            raise EOFError(f"End of stream reached before {n} bytes could be read")

        data = bytes(self.receive_data[:n])
        self.receive_data = self.receive_data[n:]
        return data

    async def close(self) -> None:
        """Close the connection."""
        pass


class TestAsyncSocket:
    """Tests for the AsyncSocket class."""

    async def test_connect(self) -> None:
        """Test connecting to the server."""
        # This test is a placeholder since we can't easily test actual socket connections
        socket = AsyncSocket("localhost", 9000)

        # Just make sure the class can be instantiated
        assert socket.host == "localhost"
        assert socket.port == 9000

    async def test_context_manager(
        self, clickhouse_connection_params: ClientOptions
    ) -> None:
        """Test using the socket as a context manager.
        Args:
            clickhouse_connection_params: Connection parameters for the ClickHouse container
        """
        # Use the real ClickHouse container
        async with AsyncSocket(
            clickhouse_connection_params.host,
            clickhouse_connection_params.port,
        ) as socket:
            assert socket.host == clickhouse_connection_params.host
            assert socket.port == clickhouse_connection_params.port


class TestConnection:
    """Tests for the Connection class."""

    async def test_connect(self, clickhouse_connection_params: ClientOptions) -> None:
        """Test connecting to the server.
        Args:
            clickhouse_connection_params: Connection parameters for the ClickHouse container
        """
        connection = Connection(
            host=clickhouse_connection_params.host,
            port=clickhouse_connection_params.port,
            database=clickhouse_connection_params.database,
            user=clickhouse_connection_params.user,
            password=clickhouse_connection_params.password,
        )

        await connection.connect()

        # Check server info
        assert connection.server_info.name == "ClickHouse"
        assert connection.server_info.version_major > 0

        await connection.close()

    async def test_ping(self, clickhouse_connection_params: ClientOptions) -> None:
        """Test pinging the server.
        Args:
            clickhouse_connection_params: Connection parameters for the ClickHouse container
        """
        connection = Connection(
            host=clickhouse_connection_params.host,
            port=clickhouse_connection_params.port,
            database=clickhouse_connection_params.database,
            user=clickhouse_connection_params.user,
            password=clickhouse_connection_params.password,
        )

        await connection.connect()

        # Ping server
        assert await connection.ping()

        await connection.close()

    async def test_context_manager(
        self, clickhouse_connection_params: ClientOptions
    ) -> None:
        """Test using the connection as a context manager.
        Args:
            clickhouse_connection_params: Connection parameters for the ClickHouse container
        """
        async with Connection(
            host=clickhouse_connection_params.host,
            port=clickhouse_connection_params.port,
            database=clickhouse_connection_params.database,
            user=clickhouse_connection_params.user,
            password=clickhouse_connection_params.password,
        ) as connection:
            # Check server info
            assert connection.server_info.name == "ClickHouse"
            assert connection.server_info.version_major > 0

            # Ping server
            assert await connection.ping()
