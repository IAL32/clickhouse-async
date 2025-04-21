"""Tests for the ClientOptions class."""

from clickhouse_async.client_options import ClientOptions
from clickhouse_async.connection_string import ConnectionString


def test_client_options_defaults() -> None:
    """Test default client options."""
    options = ClientOptions()

    assert options.host == "localhost"
    assert options.port == 9000
    assert options.user == "default"
    assert options.password == ""
    assert options.database == "default"
    assert options.compression is False
    assert options.connect_timeout == 5.0
    assert options.send_receive_timeout == 5.0
    assert options.retry_timeout == 5.0
    assert options.retry_count == 3


def test_client_options_custom_values() -> None:
    """Test client options with custom values."""
    options = ClientOptions(
        host="clickhouse.example.com",
        port=9001,
        user="user",
        password="password",
        database="db",
        compression=True,
        connect_timeout=10.0,
        send_receive_timeout=15.0,
        retry_timeout=3.0,
        retry_count=5,
    )

    assert options.host == "clickhouse.example.com"
    assert options.port == 9001
    assert options.user == "user"
    assert options.password == "password"
    assert options.database == "db"
    assert options.compression is True
    assert options.connect_timeout == 10.0
    assert options.send_receive_timeout == 15.0
    assert options.retry_timeout == 3.0
    assert options.retry_count == 5


def test_client_options_from_connection_string() -> None:
    """Test creating client options from connection string."""
    conn_str = (
        "clickhouse://user:password@host:9001/db?compression=true&connect_timeout=10"
    )
    options = ClientOptions.from_connection_string(conn_str)

    assert options.host == "host"
    assert options.port == 9001
    assert options.user == "user"
    assert options.password == "password"
    assert options.database == "db"
    assert options.compression is True
    assert options.connect_timeout == 10.0


def test_client_options_from_connection_string_object() -> None:
    """Test creating client options from ConnectionString object."""
    conn = ConnectionString(
        "clickhouse://user:password@host:9001/db?compression=true&connect_timeout=10"
    )
    options = ClientOptions.from_connection_string(conn)

    assert options.host == "host"
    assert options.port == 9001
    assert options.user == "user"
    assert options.password == "password"
    assert options.database == "db"
    assert options.compression is True
    assert options.connect_timeout == 10.0


def test_client_options_with_multiple_hosts() -> None:
    """Test client options with multiple hosts."""
    conn_str = "clickhouse://user:password@host1:9000,host2:9001/db"
    options = ClientOptions.from_connection_string(conn_str)

    assert options.host == "host1"
    assert options.port == 9000
    assert len(options.hosts) == 2
    assert options.hosts[0] == {"host": "host1", "port": 9000}
    assert options.hosts[1] == {"host": "host2", "port": 9001}


def test_client_options_to_dict() -> None:
    """Test converting client options to dictionary."""
    options = ClientOptions(
        host="clickhouse.example.com",
        port=9001,
        user="user",
        password="password",
        database="db",
        compression=True,
    )

    options_dict = options.to_dict()

    assert options_dict["host"] == "clickhouse.example.com"
    assert options_dict["port"] == 9001
    assert options_dict["user"] == "user"
    assert options_dict["password"] == "password"
    assert options_dict["database"] == "db"
    assert options_dict["compression"] is True


def test_client_options_str_representation() -> None:
    """Test string representation of client options."""
    options = ClientOptions(
        host="clickhouse.example.com",
        port=9001,
        user="user",
        password="password",
        database="db",
    )

    options_str = str(options)

    assert "host='clickhouse.example.com'" in options_str
    assert "port=9001" in options_str
    assert "user='user'" in options_str
    assert "database='db'" in options_str
    assert (
        "password" not in options_str
    )  # Password should not be included in string representation
