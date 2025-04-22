"""Tests for data types."""

import pytest

from clickhouse_async.protocol.data_types import (
    ArrayType,
    DataType,
    DateTimeType,
    DateType,
    Float32Type,
    Float64Type,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    LowCardinalityType,
    MapType,
    NullableType,
    StringType,
    TupleType,
    UInt8Type,
    UInt16Type,
    UInt32Type,
    UInt64Type,
    parse_map_types,
    parse_tuple_types,
)


class TestDataTypeCreation:
    """Test data type creation."""

    def test_create_basic_types(self) -> None:
        """Test creating basic data types."""
        assert isinstance(DataType.create("String"), StringType)
        assert isinstance(DataType.create("UInt8"), UInt8Type)
        assert isinstance(DataType.create("UInt16"), UInt16Type)
        assert isinstance(DataType.create("UInt32"), UInt32Type)
        assert isinstance(DataType.create("UInt64"), UInt64Type)
        assert isinstance(DataType.create("Int8"), Int8Type)
        assert isinstance(DataType.create("Int16"), Int16Type)
        assert isinstance(DataType.create("Int32"), Int32Type)
        assert isinstance(DataType.create("Int64"), Int64Type)
        assert isinstance(DataType.create("Float32"), Float32Type)
        assert isinstance(DataType.create("Float64"), Float64Type)
        assert isinstance(DataType.create("Date"), DateType)
        assert isinstance(DataType.create("DateTime"), DateTimeType)

    def test_create_array_type(self) -> None:
        """Test creating array data type."""
        data_type = DataType.create("Array(UInt8)")
        assert isinstance(data_type, ArrayType)
        assert isinstance(data_type.item_type, UInt8Type)

    def test_create_nested_array_type(self) -> None:
        """Test creating nested array data type."""
        data_type = DataType.create("Array(Array(UInt8))")
        assert isinstance(data_type, ArrayType)
        assert isinstance(data_type.item_type, ArrayType)
        assert isinstance(data_type.item_type.item_type, UInt8Type)

    def test_create_tuple_type(self) -> None:
        """Test creating tuple data type."""
        data_type = DataType.create("Tuple(UInt8, String)")
        assert isinstance(data_type, TupleType)
        assert len(data_type.item_types) == 2
        assert isinstance(data_type.item_types[0], UInt8Type)
        assert isinstance(data_type.item_types[1], StringType)

    def test_create_map_type(self) -> None:
        """Test creating map data type."""
        data_type = DataType.create("Map(String, UInt8)")
        assert isinstance(data_type, MapType)
        assert isinstance(data_type.key_type, StringType)
        assert isinstance(data_type.value_type, UInt8Type)

    def test_create_nullable_type(self) -> None:
        """Test creating nullable data type."""
        data_type = DataType.create("Nullable(UInt8)")
        assert isinstance(data_type, NullableType)
        assert isinstance(data_type.inner_type, UInt8Type)

    def test_create_low_cardinality_type(self) -> None:
        """Test creating low cardinality data type."""
        data_type = DataType.create("LowCardinality(String)")
        assert isinstance(data_type, LowCardinalityType)
        assert isinstance(data_type.inner_type, StringType)

    def test_create_complex_type(self) -> None:
        """Test creating complex data type."""
        data_type = DataType.create(
            "Array(Nullable(Map(String, Tuple(UInt8, Float32))))"
        )
        assert isinstance(data_type, ArrayType)
        assert isinstance(data_type.item_type, NullableType)
        assert isinstance(data_type.item_type.inner_type, MapType)
        assert isinstance(data_type.item_type.inner_type.key_type, StringType)
        assert isinstance(data_type.item_type.inner_type.value_type, TupleType)
        assert len(data_type.item_type.inner_type.value_type.item_types) == 2
        assert isinstance(
            data_type.item_type.inner_type.value_type.item_types[0], UInt8Type
        )
        assert isinstance(
            data_type.item_type.inner_type.value_type.item_types[1], Float32Type
        )

    def test_create_invalid_type(self) -> None:
        """Test creating invalid data type."""
        with pytest.raises(ValueError):
            DataType.create("InvalidType")


class TestTypeParsing:
    """Test type parsing."""

    def test_parse_tuple_types(self) -> None:
        """Test parsing tuple types."""
        assert parse_tuple_types("UInt8, String") == ["UInt8", "String"]
        assert parse_tuple_types("UInt8, Array(String)") == ["UInt8", "Array(String)"]
        assert parse_tuple_types("UInt8, Tuple(Int8, Int16)") == [
            "UInt8",
            "Tuple(Int8, Int16)",
        ]

    def test_parse_map_types(self) -> None:
        """Test parsing map types."""
        assert parse_map_types("String, UInt8") == ("String", "UInt8")
        assert parse_map_types("String, Array(UInt8)") == ("String", "Array(UInt8)")

    def test_parse_map_types_invalid(self) -> None:
        """Test parsing invalid map types."""
        with pytest.raises(ValueError):
            parse_map_types("String")

        with pytest.raises(ValueError):
            parse_map_types("String, UInt8, Int8")
