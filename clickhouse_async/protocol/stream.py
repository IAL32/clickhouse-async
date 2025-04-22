"""Stream implementations for ClickHouse client."""

import io
import struct

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

        Raises:
            EOFError: If end of stream is reached before n bytes are read
        """
        # If we already have enough data in the buffer, return it
        if len(self.buffer) >= n:
            data = bytes(self.buffer[:n])
            self.buffer = self.buffer[n:]
            return data

        # Use a loop instead of recursion to avoid stack overflow
        while len(self.buffer) < n:
            # Read more data from the socket
            try:
                data = await self.socket.receive(max(n - len(self.buffer), 4096))
                if not data:
                    # If no more data is available and we haven't read enough,
                    # raise an EOFError
                    if n > 0:
                        raise EOFError(
                            f"End of stream reached before {n} bytes could be read"
                        )
                    return b""
            except ConnectionError as e:
                # If the connection is closed, raise an EOFError
                raise EOFError(f"Connection closed: {e}") from e

            self.buffer.extend(data)

        # We have enough data in the buffer
        data = bytes(self.buffer[:n])
        self.buffer = self.buffer[n:]
        return data

    async def read_exactly(self, n: int) -> bytes:
        """Read exactly n bytes from the stream.

        Args:
            n: Number of bytes to read

        Returns:
            Read data

        Raises:
            EOFError: If end of stream is reached before n bytes are read
        """
        # Use the improved read method to read exactly n bytes
        data = await self.read(n)
        if len(data) < n:
            raise EOFError(f"End of stream reached before {n} bytes could be read")
        return data

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

    async def read_float32(self) -> float:
        """Read a 32-bit floating-point value from the stream.

        Returns:
            Float32 value
        """
        data = await self.read_exactly(4)
        return struct.unpack("<f", data)[0]  # type: ignore[no-any-return]

    async def read_float64(self) -> float:
        """Read a 64-bit floating-point value from the stream.

        Returns:
            Float64 value
        """
        data = await self.read_exactly(8)
        return struct.unpack("<d", data)[0]  # type: ignore[no-any-return]


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

    async def write_float32(self, value: float) -> None:
        """Write a 32-bit floating-point value to the stream.

        Args:
            value: Float32 value to write
        """
        data = struct.pack("<f", value)
        await self.write(data)

    async def write_float64(self, value: float) -> None:
        """Write a 64-bit floating-point value to the stream.

        Args:
            value: Float64 value to write
        """
        data = struct.pack("<d", value)
        await self.write(data)

    async def flush(self) -> None:
        """Flush the buffer to the socket."""
        data = self.buffer.getvalue()
        if data:
            await self.socket.send(data)
            self.buffer = io.BytesIO()
