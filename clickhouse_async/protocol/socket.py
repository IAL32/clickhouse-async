"""Async socket implementation for ClickHouse client."""

import asyncio
import types


class AsyncSocket:
    """Async socket implementation for ClickHouse client."""

    def __init__(
        self,
        host: str,
        port: int,
        connect_timeout: float = 10.0,
        send_timeout: float = 10.0,
        receive_timeout: float = 30.0,
    ) -> None:
        """Initialize async socket.

        Args:
            host: Host to connect to
            port: Port to connect to
            connect_timeout: Connection timeout in seconds
            send_timeout: Send timeout in seconds
            receive_timeout: Receive timeout in seconds
        """
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.send_timeout = send_timeout
        self.receive_timeout = receive_timeout
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        """Connect to the server.

        Raises:
            ConnectionError: If connection fails
            asyncio.TimeoutError: If connection times out
        """
        try:
            future = asyncio.open_connection(self.host, self.port)
            self.reader, self.writer = await asyncio.wait_for(
                future, timeout=self.connect_timeout
            )
        except (OSError, TimeoutError) as e:
            raise ConnectionError(
                f"Failed to connect to {self.host}:{self.port}: {e}"
            ) from e

    async def send(self, data: bytes) -> None:
        """Send data to the server.

        Args:
            data: Data to send

        Raises:
            ConnectionError: If not connected or send fails
            asyncio.TimeoutError: If send times out
        """
        if not self.writer:
            raise ConnectionError("Not connected")

        try:
            self.writer.write(data)
            await asyncio.wait_for(self.writer.drain(), timeout=self.send_timeout)
        except (TimeoutError, OSError) as e:
            raise ConnectionError(f"Failed to send data: {e}") from e

    async def receive(self, n: int) -> bytes:
        """Receive data from the server.

        Args:
            n: Number of bytes to receive

        Returns:
            Received data

        Raises:
            ConnectionError: If not connected or receive fails
            asyncio.TimeoutError: If receive times out
        """
        if not self.reader:
            raise ConnectionError("Not connected")

        try:
            return await asyncio.wait_for(
                self.reader.read(n), timeout=self.receive_timeout
            )
        except (TimeoutError, OSError) as e:
            raise ConnectionError(f"Failed to receive data: {e}") from e

    async def receive_exactly(self, n: int) -> bytes:
        """Receive exactly n bytes from the server.

        Args:
            n: Number of bytes to receive

        Returns:
            Received data

        Raises:
            ConnectionError: If not connected or receive fails
            asyncio.TimeoutError: If receive times out
            EOFError: If end of stream is reached before n bytes are received
        """
        if not self.reader:
            raise ConnectionError("Not connected")

        try:
            data = await asyncio.wait_for(
                self.reader.readexactly(n), timeout=self.receive_timeout
            )
            if len(data) < n:
                raise EOFError(f"End of stream reached before {n} bytes could be read")
            return data
        except (TimeoutError, OSError) as e:
            raise ConnectionError(f"Failed to receive data: {e}") from e

    async def close(self) -> None:
        """Close the connection."""
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            finally:
                self.writer = None
                self.reader = None

    async def __aenter__(self) -> "AsyncSocket":
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
