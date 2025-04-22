"""ClickHouse wire format implementation."""

import struct
from typing import BinaryIO


class WireFormat:
    """ClickHouse wire format implementation.

    This class provides methods for reading and writing data in the ClickHouse wire format.
    """

    @staticmethod
    def read_varint(stream: BinaryIO) -> int:
        """Read a variable-length integer from the stream.

        Args:
            stream: Binary stream to read from

        Returns:
            Integer value
        """
        result = 0
        shift = 0

        while True:
            byte = stream.read(1)
            if not byte:
                raise EOFError("Unexpected end of stream while reading varint")

            b = byte[0]
            result |= (b & 0x7F) << shift
            shift += 7

            if not (b & 0x80):
                break

        return result

    @staticmethod
    def write_varint(stream: BinaryIO, value: int) -> None:
        """Write a variable-length integer to the stream.

        Args:
            stream: Binary stream to write to
            value: Integer value to write
        """
        while True:
            byte = value & 0x7F
            value >>= 7

            if value:
                byte |= 0x80

            stream.write(bytes([byte]))

            if not value:
                break

    @staticmethod
    def read_binary_uint8(stream: BinaryIO) -> int:
        """Read an unsigned 8-bit integer from the stream.

        Args:
            stream: Binary stream to read from

        Returns:
            Unsigned 8-bit integer
        """
        data = stream.read(1)
        if len(data) != 1:
            raise EOFError("Unexpected end of stream while reading uint8")

        return struct.unpack("<B", data)[0]  # type: ignore[no-any-return]

    @staticmethod
    def write_binary_uint8(stream: BinaryIO, value: int) -> None:
        """Write an unsigned 8-bit integer to the stream.

        Args:
            stream: Binary stream to write to
            value: Unsigned 8-bit integer to write
        """
        stream.write(struct.pack("<B", value))

    @staticmethod
    def read_binary_int32(stream: BinaryIO) -> int:
        """Read a signed 32-bit integer from the stream.

        Args:
            stream: Binary stream to read from

        Returns:
            Signed 32-bit integer
        """
        data = stream.read(4)
        if len(data) != 4:
            raise EOFError("Unexpected end of stream while reading int32")

        return struct.unpack("<i", data)[0]  # type: ignore[no-any-return]

    @staticmethod
    def write_binary_int32(stream: BinaryIO, value: int) -> None:
        """Write a signed 32-bit integer to the stream.

        Args:
            stream: Binary stream to write to
            value: Signed 32-bit integer to write
        """
        stream.write(struct.pack("<i", value))

    @staticmethod
    def read_binary_uint64(stream: BinaryIO) -> int:
        """Read an unsigned 64-bit integer from the stream.

        Args:
            stream: Binary stream to read from

        Returns:
            Unsigned 64-bit integer
        """
        data = stream.read(8)
        if len(data) != 8:
            raise EOFError("Unexpected end of stream while reading uint64")

        return struct.unpack("<Q", data)[0]  # type: ignore[no-any-return]

    @staticmethod
    def write_binary_uint64(stream: BinaryIO, value: int) -> None:
        """Write an unsigned 64-bit integer to the stream.

        Args:
            stream: Binary stream to write to
            value: Unsigned 64-bit integer to write
        """
        stream.write(struct.pack("<Q", value))

    @staticmethod
    def read_binary_string(stream: BinaryIO) -> bytes:
        """Read a binary string from the stream.

        Args:
            stream: Binary stream to read from

        Returns:
            Binary string
        """
        length = WireFormat.read_varint(stream)
        data = stream.read(length)

        if len(data) != length:
            raise EOFError("Unexpected end of stream while reading string")

        return data

    @staticmethod
    def write_binary_string(stream: BinaryIO, value: str | bytes) -> None:
        """Write a binary string to the stream.

        Args:
            stream: Binary stream to write to
            value: String or bytes to write
        """
        if isinstance(value, str):
            value = value.encode("utf-8")

        WireFormat.write_varint(stream, len(value))
        stream.write(value)

    @staticmethod
    def read_string(stream: BinaryIO) -> str:
        """Read a UTF-8 string from the stream.

        Args:
            stream: Binary stream to read from

        Returns:
            UTF-8 string
        """
        return WireFormat.read_binary_string(stream).decode("utf-8")

    @staticmethod
    def read_binary_float32(stream: BinaryIO) -> float:
        """Read a 32-bit floating-point value from the stream.

        Args:
            stream: Binary stream to read from

        Returns:
            Float32 value
        """
        data = stream.read(4)
        if len(data) != 4:
            raise EOFError("Unexpected end of stream while reading float32")

        return struct.unpack("<f", data)[0]  # type: ignore[no-any-return]

    @staticmethod
    def write_binary_float32(stream: BinaryIO, value: float) -> None:
        """Write a 32-bit floating-point value to the stream.

        Args:
            stream: Binary stream to write to
            value: Float32 value to write
        """
        stream.write(struct.pack("<f", value))

    @staticmethod
    def read_binary_float64(stream: BinaryIO) -> float:
        """Read a 64-bit floating-point value from the stream.

        Args:
            stream: Binary stream to read from

        Returns:
            Float64 value
        """
        data = stream.read(8)
        if len(data) != 8:
            raise EOFError("Unexpected end of stream while reading float64")

        return struct.unpack("<d", data)[0]  # type: ignore[no-any-return]

    @staticmethod
    def write_binary_float64(stream: BinaryIO, value: float) -> None:
        """Write a 64-bit floating-point value to the stream.

        Args:
            stream: Binary stream to write to
            value: Float64 value to write
        """
        stream.write(struct.pack("<d", value))

    @staticmethod
    def write_string(stream: BinaryIO, value: str) -> None:
        """Write a UTF-8 string to the stream.

        Args:
            stream: Binary stream to write to
            value: String to write
        """
        WireFormat.write_binary_string(stream, value)
