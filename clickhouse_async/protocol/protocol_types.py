"""Protocol types for ClickHouse client."""

from typing import Protocol

from clickhouse_async.protocol.constants import CompressionState


class SupportsRead(Protocol):
    """Protocol for reading from a stream."""

    async def read_varint(self) -> int:
        """Read a variable-length integer from the stream."""
        ...

    async def read_string(self) -> str:
        """Read a string from the stream."""
        ...

    async def read_exactly(self, n: int) -> bytes:
        """Read exactly n bytes from the stream."""
        ...

    async def read_float32(self) -> float:
        """Read a 32-bit floating-point value from the stream."""
        ...

    async def read_float64(self) -> float:
        """Read a 64-bit floating-point value from the stream."""
        ...


class SupportsWrite(Protocol):
    """Protocol for writing to a stream."""

    async def write(self, data: bytes) -> None:
        """Write raw bytes to the stream."""
        ...

    async def write_varint(self, value: int) -> None:
        """Write a variable-length integer."""
        ...

    async def write_string(self, value: str) -> None:
        """Write a string."""
        ...

    async def write_binary_string(self, value: bytes) -> None:
        """Write a binary string."""
        ...

    async def write_float32(self, value: float) -> None:
        """Write a 32-bit floating-point value."""
        ...

    async def write_float64(self, value: float) -> None:
        """Write a 64-bit floating-point value."""
        ...

    async def write_uint8(self, value: int) -> None:
        """Write an 8-bit unsigned integer."""
        ...

    async def write_int32(self, value: int) -> None:
        """Write a 32-bit signed integer."""
        ...

    async def write_int64(self, value: int) -> None:
        """Write a 64-bit signed integer."""
        ...

    async def flush(self) -> None:
        """Flush the output stream."""
        ...


class ConnectionProtocol(Protocol):
    """Protocol for Connection class."""

    user: str
    compression: CompressionState
    input_stream: SupportsRead | None
    output_stream: SupportsWrite | None

    @property
    def protocol_version(self) -> int:
        """Get the protocol version."""
        ...
