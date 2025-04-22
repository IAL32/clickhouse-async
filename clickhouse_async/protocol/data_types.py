"""Data type implementations for ClickHouse client."""

import struct
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any, TypeVar

from .protocol_types import SupportsRead, SupportsWrite

T = TypeVar("T")


class DataType(ABC):
    """Base class for all data types."""

    @abstractmethod
    async def read_value(self, input_stream: SupportsRead) -> Any:  # noqa: ANN401
        """Read a value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            The read value
        """
        pass

    @abstractmethod
    async def write_value(
        self,
        output_stream: SupportsWrite,
        value: Any,  # noqa: ANN401
    ) -> None:
        """Write a value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: Value to write
        """
        pass

    @classmethod
    def create(cls, type_string: str) -> "DataType":
        """Create a data type from a type string.

        Args:
            type_string: Type string (e.g., "UInt8", "Array(String)")

        Returns:
            Data type instance
        """
        # Basic types
        if type_string == "String":
            return StringType()
        elif type_string == "UInt8":
            return UInt8Type()
        elif type_string == "UInt16":
            return UInt16Type()
        elif type_string == "UInt32":
            return UInt32Type()
        elif type_string == "UInt64":
            return UInt64Type()
        elif type_string == "Int8":
            return Int8Type()
        elif type_string == "Int16":
            return Int16Type()
        elif type_string == "Int32":
            return Int32Type()
        elif type_string == "Int64":
            return Int64Type()
        elif type_string == "Float32":
            return Float32Type()
        elif type_string == "Float64":
            return Float64Type()
        elif type_string == "Date":
            return DateType()
        elif type_string == "DateTime":
            return DateTimeType()

        # Complex types
        elif type_string.startswith("Array(") and type_string.endswith(")"):
            inner_type = type_string[6:-1]  # Remove "Array(" and ")"
            return ArrayType(DataType.create(inner_type))
        elif type_string.startswith("Tuple(") and type_string.endswith(")"):
            inner_types_str = type_string[6:-1]  # Remove "Tuple(" and ")"
            inner_types = parse_tuple_types(inner_types_str)
            return TupleType([DataType.create(t) for t in inner_types])
        elif type_string.startswith("Map(") and type_string.endswith(")"):
            inner_types_str = type_string[4:-1]  # Remove "Map(" and ")"
            key_type, value_type = parse_map_types(inner_types_str)
            return MapType(DataType.create(key_type), DataType.create(value_type))

        # Special types
        elif type_string.startswith("Nullable(") and type_string.endswith(")"):
            inner_type = type_string[9:-1]  # Remove "Nullable(" and ")"
            return NullableType(DataType.create(inner_type))
        elif type_string.startswith("LowCardinality(") and type_string.endswith(")"):
            inner_type = type_string[15:-1]  # Remove "LowCardinality(" and ")"
            return LowCardinalityType(DataType.create(inner_type))

        # Default
        raise ValueError(f"Unsupported data type: {type_string}")


class StringType(DataType):
    """String data type."""

    async def read_value(self, input_stream: SupportsRead) -> str:
        """Read a string value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            String value
        """
        return await input_stream.read_string()

    async def write_value(self, output_stream: SupportsWrite, value: str) -> None:
        """Write a string value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: String value to write
        """
        await output_stream.write_string(value)


class NumericType(DataType, ABC):
    """Base class for numeric data types."""

    pass


class IntegerType(NumericType, ABC):
    """Base class for integer data types."""

    pass


class UInt8Type(IntegerType):
    """UInt8 data type."""

    async def read_value(self, input_stream: SupportsRead) -> int:
        """Read a UInt8 value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            UInt8 value
        """
        return await input_stream.read_varint()

    async def write_value(self, output_stream: SupportsWrite, value: int) -> None:
        """Write a UInt8 value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: UInt8 value to write
        """
        await output_stream.write_varint(value)


class UInt16Type(IntegerType):
    """UInt16 data type."""

    async def read_value(self, input_stream: SupportsRead) -> int:
        """Read a UInt16 value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            UInt16 value
        """
        return await input_stream.read_varint()

    async def write_value(self, output_stream: SupportsWrite, value: int) -> None:
        """Write a UInt16 value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: UInt16 value to write
        """
        await output_stream.write_varint(value)


class UInt32Type(IntegerType):
    """UInt32 data type."""

    async def read_value(self, input_stream: SupportsRead) -> int:
        """Read a UInt32 value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            UInt32 value
        """
        return await input_stream.read_varint()

    async def write_value(self, output_stream: SupportsWrite, value: int) -> None:
        """Write a UInt32 value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: UInt32 value to write
        """
        await output_stream.write_varint(value)


class UInt64Type(IntegerType):
    """UInt64 data type."""

    async def read_value(self, input_stream: SupportsRead) -> int:
        """Read a UInt64 value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            UInt64 value
        """
        return await input_stream.read_varint()

    async def write_value(self, output_stream: SupportsWrite, value: int) -> None:
        """Write a UInt64 value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: UInt64 value to write
        """
        await output_stream.write_varint(value)


class Int8Type(IntegerType):
    """Int8 data type."""

    async def read_value(self, input_stream: SupportsRead) -> int:
        """Read an Int8 value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            Int8 value
        """
        value = await input_stream.read_varint()
        return value if value < 128 else value - 256

    async def write_value(self, output_stream: SupportsWrite, value: int) -> None:
        """Write an Int8 value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: Int8 value to write
        """
        if value < 0:
            value += 256
        await output_stream.write_varint(value)


class Int16Type(IntegerType):
    """Int16 data type."""

    async def read_value(self, input_stream: SupportsRead) -> int:
        """Read an Int16 value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            Int16 value
        """
        value = await input_stream.read_varint()
        return value if value < 32768 else value - 65536

    async def write_value(self, output_stream: SupportsWrite, value: int) -> None:
        """Write an Int16 value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: Int16 value to write
        """
        if value < 0:
            value += 65536
        await output_stream.write_varint(value)


class Int32Type(IntegerType):
    """Int32 data type."""

    async def read_value(self, input_stream: SupportsRead) -> int:
        """Read an Int32 value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            Int32 value
        """
        value = await input_stream.read_varint()
        return value if value < 2147483648 else value - 4294967296

    async def write_value(self, output_stream: SupportsWrite, value: int) -> None:
        """Write an Int32 value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: Int32 value to write
        """
        if value < 0:
            value += 4294967296
        await output_stream.write_varint(value)


class Int64Type(IntegerType):
    """Int64 data type."""

    async def read_value(self, input_stream: SupportsRead) -> int:
        """Read an Int64 value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            Int64 value
        """
        value = await input_stream.read_varint()
        return value if value < 9223372036854775808 else value - 18446744073709551616

    async def write_value(self, output_stream: SupportsWrite, value: int) -> None:
        """Write an Int64 value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: Int64 value to write
        """
        if value < 0:
            value += 18446744073709551616
        await output_stream.write_varint(value)


class FloatType(NumericType, ABC):
    """Base class for floating-point data types."""

    pass


class Float32Type(FloatType):
    """Float32 data type."""

    async def read_value(self, input_stream: SupportsRead) -> float:
        """Read a Float32 value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            Float32 value
        """
        data = await input_stream.read_exactly(4)
        return struct.unpack("<f", data)[0]  # type: ignore[no-any-return]

    async def write_value(self, output_stream: SupportsWrite, value: float) -> None:
        """Write a Float32 value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: Float32 value to write
        """
        data = struct.pack("<f", value)
        await output_stream.write(data)


class Float64Type(FloatType):
    """Float64 data type."""

    async def read_value(self, input_stream: SupportsRead) -> float:
        """Read a Float64 value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            Float64 value
        """
        data = await input_stream.read_exactly(8)
        return struct.unpack("<d", data)[0]  # type: ignore[no-any-return]

    async def write_value(self, output_stream: SupportsWrite, value: float) -> None:
        """Write a Float64 value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: Float64 value to write
        """
        data = struct.pack("<d", value)
        await output_stream.write(data)


class DateType(DataType):
    """Date data type."""

    async def read_value(self, input_stream: SupportsRead) -> date:
        """Read a Date value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            Date value
        """
        days = await input_stream.read_varint()
        return date.fromordinal(days + 719163)  # 1970-01-01 is day 719163

    async def write_value(self, output_stream: SupportsWrite, value: date) -> None:
        """Write a Date value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: Date value to write
        """
        days = value.toordinal() - 719163
        await output_stream.write_varint(days)


class DateTimeType(DataType):
    """DateTime data type."""

    async def read_value(self, input_stream: SupportsRead) -> datetime:
        """Read a DateTime value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            DateTime value
        """
        timestamp = await input_stream.read_varint()
        return datetime.fromtimestamp(timestamp)

    async def write_value(self, output_stream: SupportsWrite, value: datetime) -> None:
        """Write a DateTime value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: DateTime value to write
        """
        timestamp = int(value.timestamp())
        await output_stream.write_varint(timestamp)


class ArrayType(DataType):
    """Array data type."""

    def __init__(self, item_type: DataType) -> None:
        """Initialize array data type.

        Args:
            item_type: Type of array items
        """
        self.item_type = item_type

    async def read_value(self, input_stream: SupportsRead) -> list[Any]:
        """Read an array value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            Array value
        """
        length = await input_stream.read_varint()
        result = []

        for _ in range(length):
            item = await self.item_type.read_value(input_stream)
            result.append(item)

        return result

    async def write_value(self, output_stream: SupportsWrite, value: list[Any]) -> None:
        """Write an array value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: Array value to write
        """
        await output_stream.write_varint(len(value))

        for item in value:
            await self.item_type.write_value(output_stream, item)


class TupleType(DataType):
    """Tuple data type."""

    def __init__(self, item_types: list[DataType]) -> None:
        """Initialize tuple data type.

        Args:
            item_types: Types of tuple items
        """
        self.item_types = item_types

    async def read_value(self, input_stream: SupportsRead) -> tuple[Any, ...]:
        """Read a tuple value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            Tuple value
        """
        result = []

        for item_type in self.item_types:
            item = await item_type.read_value(input_stream)
            result.append(item)

        return tuple(result)

    async def write_value(
        self, output_stream: SupportsWrite, value: tuple[Any, ...]
    ) -> None:
        """Write a tuple value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: Tuple value to write
        """
        if len(value) != len(self.item_types):
            raise ValueError(
                f"Tuple length mismatch: expected {len(self.item_types)}, got {len(value)}"
            )

        for i, item in enumerate(value):
            await self.item_types[i].write_value(output_stream, item)


class MapType(DataType):
    """Map data type."""

    def __init__(self, key_type: DataType, value_type: DataType) -> None:
        """Initialize map data type.

        Args:
            key_type: Type of map keys
            value_type: Type of map values
        """
        self.key_type = key_type
        self.value_type = value_type

    async def read_value(self, input_stream: SupportsRead) -> dict[Any, Any]:
        """Read a map value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            Map value
        """
        size = await input_stream.read_varint()
        result = {}

        for _ in range(size):
            key = await self.key_type.read_value(input_stream)
            value = await self.value_type.read_value(input_stream)
            result[key] = value

        return result

    async def write_value(
        self, output_stream: SupportsWrite, value: dict[Any, Any]
    ) -> None:
        """Write a map value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: Map value to write
        """
        await output_stream.write_varint(len(value))

        for key, val in value.items():
            await self.key_type.write_value(output_stream, key)
            await self.value_type.write_value(output_stream, val)


class NullableType(DataType):
    """Nullable data type."""

    def __init__(self, inner_type: DataType) -> None:
        """Initialize nullable data type.

        Args:
            inner_type: Inner type
        """
        self.inner_type = inner_type

    async def read_value(self, input_stream: SupportsRead) -> Any | None:  # noqa: ANN401
        """Read a nullable value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            Nullable value
        """
        is_null = await input_stream.read_varint()

        if is_null:
            return None

        return await self.inner_type.read_value(input_stream)

    async def write_value(
        self,
        output_stream: SupportsWrite,
        value: Any | None,  # noqa: ANN401
    ) -> None:
        """Write a nullable value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: Nullable value to write
        """
        if value is None:
            await output_stream.write_varint(1)
        else:
            await output_stream.write_varint(0)
            await self.inner_type.write_value(output_stream, value)


class LowCardinalityType(DataType):
    """LowCardinality data type."""

    def __init__(self, inner_type: DataType) -> None:
        """Initialize LowCardinality data type.

        Args:
            inner_type: Inner type
        """
        self.inner_type = inner_type

    async def read_value(self, input_stream: SupportsRead) -> Any:  # noqa: ANN401
        """Read a LowCardinality value from the input stream.

        Args:
            input_stream: Input stream to read from

        Returns:
            LowCardinality value
        """
        # Read dictionary size
        dict_size = await input_stream.read_varint()

        # Read dictionary values
        dictionary = []
        for _ in range(dict_size):
            value = await self.inner_type.read_value(input_stream)
            dictionary.append(value)

        # Read index
        index = await input_stream.read_varint()

        # Return the value from the dictionary
        return dictionary[index]

    async def write_value(self, output_stream: SupportsWrite, value: Any) -> None:  # noqa: ANN401
        """Write a LowCardinality value to the output stream.

        Args:
            output_stream: Output stream to write to
            value: LowCardinality value to write
        """
        # For simplicity, we'll just write a dictionary with one value
        # and an index of 0
        await output_stream.write_varint(1)  # Dictionary size
        await self.inner_type.write_value(output_stream, value)
        await output_stream.write_varint(0)  # Index


def parse_tuple_types(inner_types_str: str) -> list[str]:
    """Parse tuple types from a string.

    Args:
        inner_types_str: String containing tuple types (e.g., "UInt8, String")

    Returns:
        List of type strings
    """
    result = []
    current_type = ""
    nesting_level = 0

    for char in inner_types_str:
        if char == "," and nesting_level == 0:
            result.append(current_type.strip())
            current_type = ""
        else:
            current_type += char
            if char == "(":
                nesting_level += 1
            elif char == ")":
                nesting_level -= 1

    if current_type:
        result.append(current_type.strip())

    return result


def parse_map_types(inner_types_str: str) -> tuple[str, str]:
    """Parse map types from a string.

    Args:
        inner_types_str: String containing map types (e.g., "String, UInt8")

    Returns:
        Tuple of key type and value type
    """
    types = parse_tuple_types(inner_types_str)
    if len(types) != 2:
        raise ValueError(f"Invalid map type: {inner_types_str}")
    return types[0], types[1]
