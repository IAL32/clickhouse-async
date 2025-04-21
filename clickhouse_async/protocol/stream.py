"""Stream implementations for ClickHouse client."""

import io

from .socket import AsyncSocket
from .wire_format import WireFormat


class InputStream:
    """Input stream for reading data from the server."""

    def __init__(self, socket: AsyncSocket) -> None:
        """Initialize input stream.

        Args:
            socket: Socket to read from
        """
        self.socket = socket
        self.buffer = bytearray()

    async def read(self, n: int) -> bytes:
        """Read n bytes from the stream.

        Args:
            n: Number of bytes to read

        Returns:
            Read data
        """
        if len(self.buffer) >= n:
            data = bytes(self.buffer[:n])
            self.buffer = self.buffer[n:]
            return data

        # Read more data from the socket
        data = await self.socket.receive(max(n, 4096))
        if not data:
            if len(self.buffer) > 0:
                result = bytes(self.buffer)
                self.buffer.clear()
                return result
            return b""

        self.buffer.extend(data)
        return await self.read(n)

    async def read_exactly(self, n: int) -> bytes:
        """Read exactly n bytes from the stream.

        Args:
            n: Number of bytes to read

        Returns:
            Read data

        Raises:
            EOFError: If end of stream is reached before n bytes are read
        """
        if len(self.buffer) >= n:
            data = bytes(self.buffer[:n])
            self.buffer = self.buffer[n:]
            return data

        # Read more data from the socket
        remaining = n - len(self.buffer)
        data = await self.socket.receive_exactly(remaining)

        if len(data) < remaining:
            self.buffer.extend(data)
            raise EOFError(f"End of stream reached before {n} bytes could be read")

        result = bytes(self.buffer) + data
        self.buffer.clear()
        return result

    async def read_varint(self) -> int:
        """Read a variable-length integer from the stream.

        Returns:
            Integer value
        """
        result = 0
        shift = 0

        while True:
            byte_data = await self.read(1)
            if not byte_data:
                raise EOFError("Unexpected end of stream while reading varint")

            b = byte_data[0]
            result |= (b & 0x7F) << shift
            shift += 7

            if not (b & 0x80):
                break

        return result

    async def read_string(self) -> str:
        """Read a string from the stream.

        Returns:
            String value
        """
        length = await self.read_varint()
        data = await self.read_exactly(length)
        return data.decode("utf-8")

    async def read_binary_string(self) -> bytes:
        """Read a binary string from the stream.

        Returns:
            Binary string
        """
        length = await self.read_varint()
        return await self.read_exactly(length)


class OutputStream:
    """Output stream for writing data to the server."""

    def __init__(self, socket: AsyncSocket) -> None:
        """Initialize output stream.

        Args:
            socket: Socket to write to
        """
        self.socket = socket
        self.buffer = io.BytesIO()

    async def write(self, data: bytes) -> None:
        """Write data to the stream.

        Args:
            data: Data to write
        """
        self.buffer.write(data)

    async def write_varint(self, value: int) -> None:
        """Write a variable-length integer to the stream.

        Args:
            value: Integer value to write
        """
        data = io.BytesIO()
        WireFormat.write_varint(data, value)
        await self.write(data.getvalue())

    async def write_string(self, value: str) -> None:
        """Write a string to the stream.

        Args:
            value: String to write
        """
        data = io.BytesIO()
        WireFormat.write_string(data, value)
        await self.write(data.getvalue())

    async def write_binary_string(self, value: bytes) -> None:
        """Write a binary string to the stream.

        Args:
            value: Binary string to write
        """
        data = io.BytesIO()
        WireFormat.write_binary_string(data, value)
        await self.write(data.getvalue())

    async def flush(self) -> None:
        """Flush the buffer to the socket."""
        data = self.buffer.getvalue()
        if data:
            await self.socket.send(data)
            self.buffer = io.BytesIO()
