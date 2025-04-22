"""Integration tests for data type serialization/deserialization."""

from collections.abc import AsyncGenerator
from datetime import date, datetime

import pytest

from clickhouse_async.client import ClickHouseClient
from clickhouse_async.client_options import ClientOptions


@pytest.fixture
async def test_tables(
    clickhouse_connection_params: ClientOptions,
) -> AsyncGenerator[ClickHouseClient, None]:
    """Set up test tables with various data types.

    Args:
        clickhouse_connection_params: Connection parameters for the ClickHouse container

    Yields:
        ClickHouseClient connected to the test database
    """
    client = ClickHouseClient(
        clickhouse_connection_params,
        connect_timeout=10.0,
        send_receive_timeout=10.0,
    )
    await client.connect()

    try:
        # Create tables for basic types
        await client.execute("""
            CREATE TABLE IF NOT EXISTS test_basic_types (
                str_col String,
                uint8_col UInt8,
                int32_col Int32,
                float32_col Float32,
                float64_col Float64,
                date_col Date,
                datetime_col DateTime
            ) ENGINE = Memory
        """)

        # Create tables for complex types
        await client.execute("""
            CREATE TABLE IF NOT EXISTS test_complex_types (
                array_col Array(String),
                tuple_col Tuple(Int32, String),
                map_col Map(String, UInt64)
            ) ENGINE = Memory
        """)

        # Create tables for special types
        await client.execute("""
            CREATE TABLE IF NOT EXISTS test_special_types (
                nullable_col Nullable(String),
                low_cardinality_col LowCardinality(String)
            ) ENGINE = Memory
        """)

        # Insert test data
        await client.execute(
            "INSERT INTO test_basic_types VALUES ('test', 123, -456, 3.14, 2.71828, '2023-01-01', '2023-01-01 12:34:56')"
        )
        await client.execute(
            "INSERT INTO test_complex_types VALUES (['a', 'b', 'c'], (42, 'tuple'), map('key1', 1, 'key2', 2))"
        )
        await client.execute(
            "INSERT INTO test_special_types VALUES ('not null', 'low cardinality')"
        )
        await client.execute(
            "INSERT INTO test_special_types VALUES (NULL, 'another value')"
        )

        yield client
    finally:
        # Clean up
        await client.execute("DROP TABLE IF EXISTS test_basic_types")
        await client.execute("DROP TABLE IF EXISTS test_complex_types")
        await client.execute("DROP TABLE IF EXISTS test_special_types")
        await client.disconnect()


async def test_basic_types(test_tables: ClickHouseClient) -> None:
    """Test fetching basic data types."""
    result = await test_tables.execute("SELECT * FROM test_basic_types")

    assert len(result) == 1
    row = result[0]

    assert row["str_col"] == "test"
    assert row["uint8_col"] == 123
    assert row["int32_col"] == -456
    assert isinstance(row["float32_col"], float)
    assert abs(row["float32_col"] - 3.14) < 0.0001
    assert isinstance(row["float64_col"], float)
    assert abs(row["float64_col"] - 2.71828) < 0.0001
    assert isinstance(row["date_col"], date)
    assert row["date_col"].isoformat() == "2023-01-01"
    assert isinstance(row["datetime_col"], datetime)
    assert row["datetime_col"].strftime("%Y-%m-%d %H:%M:%S") == "2023-01-01 12:34:56"


async def test_complex_types(test_tables: ClickHouseClient) -> None:
    """Test fetching complex data types."""
    result = await test_tables.execute("SELECT * FROM test_complex_types")

    assert len(result) == 1
    row = result[0]

    # Array type
    assert isinstance(row["array_col"], list)
    assert row["array_col"] == ["a", "b", "c"]

    # Tuple type
    assert isinstance(row["tuple_col"], tuple)
    assert row["tuple_col"] == (42, "tuple")

    # Map type
    assert isinstance(row["map_col"], dict)
    assert row["map_col"] == {"key1": 1, "key2": 2}


async def test_special_types(test_tables: ClickHouseClient) -> None:
    """Test fetching special data types."""
    result = await test_tables.execute(
        "SELECT * FROM test_special_types ORDER BY nullable_col NULLS FIRST"
    )

    assert len(result) == 2

    # First row has NULL value
    assert result[0]["nullable_col"] is None
    assert result[0]["low_cardinality_col"] == "another value"

    # Second row has non-NULL value
    assert result[1]["nullable_col"] == "not null"
    assert result[1]["low_cardinality_col"] == "low cardinality"


async def test_empty_result(test_tables: ClickHouseClient) -> None:
    """Test fetching an empty result set."""
    # Create an empty table
    await test_tables.execute("""
        CREATE TABLE IF NOT EXISTS test_empty (
            id UInt32,
            name String
        ) ENGINE = Memory
    """)

    try:
        # Query the empty table
        result = await test_tables.execute("SELECT * FROM test_empty")

        # Verify the result is an empty list
        assert isinstance(result, list)
        assert len(result) == 0
    finally:
        # Clean up
        await test_tables.execute("DROP TABLE IF EXISTS test_empty")


async def test_large_array(test_tables: ClickHouseClient) -> None:
    """Test fetching a large array."""
    # Create a table with a large array
    await test_tables.execute("""
        CREATE TABLE IF NOT EXISTS test_large_array (
            id UInt32,
            large_array Array(UInt32)
        ) ENGINE = Memory
    """)

    try:
        # Generate a large array of 1000 elements
        large_array = list(range(1000))
        large_array_str = str(large_array).replace("[", "[").replace("]", "]")

        # Insert the large array
        await test_tables.execute(
            f"INSERT INTO test_large_array VALUES (1, {large_array_str})"
        )

        # Query the large array
        result = await test_tables.execute("SELECT * FROM test_large_array")

        # Verify the result
        assert len(result) == 1
        assert len(result[0]["large_array"]) == 1000
        assert result[0]["large_array"] == large_array
    finally:
        # Clean up
        await test_tables.execute("DROP TABLE IF EXISTS test_large_array")


async def test_nested_complex_types(test_tables: ClickHouseClient) -> None:
    """Test fetching nested complex types."""
    # Create a table with nested complex types
    await test_tables.execute("""
        CREATE TABLE IF NOT EXISTS test_nested_types (
            nested_array Array(Array(UInt32)),
            nested_tuple Tuple(String, Tuple(UInt32, Float64)),
            array_of_tuples Array(Tuple(String, UInt32))
        ) ENGINE = Memory
    """)

    try:
        # Insert test data
        await test_tables.execute("""
            INSERT INTO test_nested_types VALUES (
                [[1, 2, 3], [4, 5, 6]],
                ('outer', (42, 3.14)),
                [('item1', 1), ('item2', 2)]
            )
        """)

        # Query the nested types
        result = await test_tables.execute("SELECT * FROM test_nested_types")

        # Verify the result
        assert len(result) == 1
        row = result[0]

        # Nested array
        assert isinstance(row["nested_array"], list)
        assert len(row["nested_array"]) == 2
        assert row["nested_array"][0] == [1, 2, 3]
        assert row["nested_array"][1] == [4, 5, 6]

        # Nested tuple
        assert isinstance(row["nested_tuple"], tuple)
        assert row["nested_tuple"][0] == "outer"
        assert isinstance(row["nested_tuple"][1], tuple)
        assert row["nested_tuple"][1][0] == 42
        assert abs(row["nested_tuple"][1][1] - 3.14) < 0.0001

        # Array of tuples
        assert isinstance(row["array_of_tuples"], list)
        assert len(row["array_of_tuples"]) == 2
        assert row["array_of_tuples"][0] == ("item1", 1)
        assert row["array_of_tuples"][1] == ("item2", 2)
    finally:
        # Clean up
        await test_tables.execute("DROP TABLE IF EXISTS test_nested_types")


async def test_extreme_values(test_tables: ClickHouseClient) -> None:
    """Test fetching extreme values for numeric types."""
    # Create a table with extreme values
    await test_tables.execute("""
        CREATE TABLE IF NOT EXISTS test_extreme_values (
            min_int8 Int8,
            max_int8 Int8,
            min_uint8 UInt8,
            max_uint8 UInt8,
            min_int32 Int32,
            max_int32 Int32,
            min_uint32 UInt32,
            max_uint32 UInt32,
            min_int64 Int64,
            max_int64 Int64,
            min_uint64 UInt64,
            max_uint64 UInt64,
            min_float32 Float32,
            max_float32 Float32,
            min_float64 Float64,
            max_float64 Float64
        ) ENGINE = Memory
    """)

    try:
        # Insert extreme values
        await test_tables.execute("""
            INSERT INTO test_extreme_values VALUES (
                -128, 127,                  -- Int8
                0, 255,                     -- UInt8
                -2147483648, 2147483647,    -- Int32
                0, 4294967295,              -- UInt32
                -9223372036854775808, 9223372036854775807,  -- Int64
                0, 18446744073709551615,    -- UInt64
                -3.4028235e38, 3.4028235e38,  -- Float32
                -1.7976931348623157e308, 1.7976931348623157e308  -- Float64
            )
        """)

        # Query the extreme values
        result = await test_tables.execute("SELECT * FROM test_extreme_values")

        # Verify the result
        assert len(result) == 1
        row = result[0]

        # Int8
        assert row["min_int8"] == -128
        assert row["max_int8"] == 127

        # UInt8
        assert row["min_uint8"] == 0
        assert row["max_uint8"] == 255

        # Int32
        assert row["min_int32"] == -2147483648
        assert row["max_int32"] == 2147483647

        # UInt32
        assert row["min_uint32"] == 0
        assert row["max_uint32"] == 4294967295

        # Int64
        assert row["min_int64"] == -9223372036854775808
        assert row["max_int64"] == 9223372036854775807

        # UInt64
        assert row["min_uint64"] == 0
        assert row["max_uint64"] == 18446744073709551615

        # Float32 (approximate comparison due to floating-point precision)
        assert abs(row["min_float32"] + 3.4028235e38) < 1e32
        assert abs(row["max_float32"] - 3.4028235e38) < 1e32

        # Float64 (approximate comparison due to floating-point precision)
        assert abs(row["min_float64"] + 1.7976931348623157e308) < 1e300
        assert abs(row["max_float64"] - 1.7976931348623157e308) < 1e300
    finally:
        # Clean up
        await test_tables.execute("DROP TABLE IF EXISTS test_extreme_values")
