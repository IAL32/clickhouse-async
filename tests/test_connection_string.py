"""Tests for the connection string parsing functionality."""

import pytest

from clickhouse_async.connection_string import ConnectionString


def test_connection_string_basic() -> None:
    """Test basic connection string parsing."""
    conn_str = "clickhouse://user:password@localhost:9000/database"
    conn = ConnectionString(conn_str)

    assert conn.host == "localhost"
    assert conn.port == 9000
    assert conn.user == "user"
    assert conn.password == "password"
    assert conn.database == "database"


def test_connection_string_default_values() -> None:
    """Test connection string with default values."""
    conn_str = "clickhouse://localhost"
    conn = ConnectionString(conn_str)

    assert conn.host == "localhost"
    assert conn.port == 9000  # Default port
    assert conn.user == "default"  # Default user
    assert conn.password == ""  # Default password
    assert conn.database == "default"  # Default database


def test_connection_string_multiple_hosts() -> None:
    """Test connection string with multiple hosts for failover."""
    conn_str = "clickhouse://user:password@host1:9000,host2:9001/database"
    conn = ConnectionString(conn_str)

    assert len(conn.hosts) == 2
    assert conn.hosts[0] == {"host": "host1", "port": 9000}
    assert conn.hosts[1] == {"host": "host2", "port": 9001}
    assert conn.user == "user"
    assert conn.password == "password"
    assert conn.database == "database"


def test_connection_string_with_options() -> None:
    """Test connection string with additional options."""
    conn_str = "clickhouse://localhost/database?compression=lz4&connect_timeout=5"
    conn = ConnectionString(conn_str)

    assert conn.host == "localhost"
    assert conn.database == "database"
    assert conn.options["compression"] == "lz4"
    assert conn.options["connect_timeout"] == "5"


def test_connection_string_invalid() -> None:
    """Test invalid connection string."""
    with pytest.raises(ValueError):
        ConnectionString("invalid_connection_string")

    with pytest.raises(ValueError):
        ConnectionString("mysql://localhost")  # Invalid scheme


def test_connection_string_from_dict() -> None:
    """Test creating connection string from dictionary."""
    params = {
        "host": "localhost",
        "port": 9000,
        "user": "user",
        "password": "password",
        "database": "database",
        "compression": "lz4",
    }
    conn = ConnectionString.from_dict(params)

    assert conn.host == "localhost"
    assert conn.port == 9000
    assert conn.user == "user"
    assert conn.password == "password"
    assert conn.database == "database"
    assert conn.options["compression"] == "lz4"


def test_connection_string_to_dict() -> None:
    """Test converting connection string to dictionary."""
    conn_str = "clickhouse://user:password@localhost:9000/database?compression=lz4"
    conn = ConnectionString(conn_str)
    params = conn.to_dict()

    assert params["host"] == "localhost"
    assert params["port"] == 9000
    assert params["user"] == "user"
    assert params["password"] == "password"
    assert params["database"] == "database"
    assert params["compression"] == "lz4"


def test_connection_string_str_representation() -> None:
    """Test string representation of connection string."""
    conn_str = "clickhouse://user:password@localhost:9000/database"
    conn = ConnectionString(conn_str)

    assert str(conn) == conn_str
