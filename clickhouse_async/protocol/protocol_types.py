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

    async def write_varint(self, value: int) -> None:
        """Write a variable-length integer to the stream."""
        ...

    async def write_string(self, value: str) -> None:
        """Write a string to the stream."""
        ...

    async def write(self, data: bytes) -> None:
        """Write data to the stream."""
        ...

    async def write_float32(self, value: float) -> None:
        """Write a 32-bit floating-point value to the stream."""
        ...

    async def write_float64(self, value: float) -> None:
        """Write a 64-bit floating-point value to the stream."""
        ...

    async def flush(self) -> None:
        """Flush the buffer to the socket."""
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
