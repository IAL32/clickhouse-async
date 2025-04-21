"""Tests for the ClickHouse client with connection string."""

import pytest

from clickhouse_async.client import ClickHouseClient
from clickhouse_async.client_options import ClientOptions
from clickhouse_async.connection_string import ConnectionString


@pytest.mark.asyncio
async def test_client_with_connection_string() -> None:
    """Test client initialization with connection string."""
    client = ClickHouseClient("clickhouse://user:password@localhost:9000/db")

    assert client.options.host == "localhost"
    assert client.options.port == 9000
    assert client.options.user == "user"
    assert client.options.password == "password"
    assert client.options.database == "db"


@pytest.mark.asyncio
async def test_client_with_connection_string_object() -> None:
    """Test client initialization with ConnectionString object."""
    conn = ConnectionString("clickhouse://user:password@localhost:9000/db")
    client = ClickHouseClient(conn)

    assert client.options.host == "localhost"
    assert client.options.port == 9000
    assert client.options.user == "user"
    assert client.options.password == "password"
    assert client.options.database == "db"


@pytest.mark.asyncio
async def test_client_with_client_options() -> None:
    """Test client initialization with ClientOptions object."""
    options = ClientOptions(
        host="localhost", port=9000, user="user", password="password", database="db"
    )
    client = ClickHouseClient(options)

    assert client.options.host == "localhost"
    assert client.options.port == 9000
    assert client.options.user == "user"
    assert client.options.password == "password"
    assert client.options.database == "db"


@pytest.mark.asyncio
async def test_client_with_parameters() -> None:
    """Test client initialization with parameters."""
    client = ClickHouseClient(
        host="localhost", port=9000, user="user", password="password", database="db"
    )

    assert client.options.host == "localhost"
    assert client.options.port == 9000
    assert client.options.user == "user"
    assert client.options.password == "password"
    assert client.options.database == "db"


@pytest.mark.asyncio
async def test_client_with_multiple_hosts() -> None:
    """Test client initialization with multiple hosts."""
    client = ClickHouseClient("clickhouse://user:password@host1:9000,host2:9001/db")

    assert client.options.host == "host1"
    assert client.options.port == 9000
    assert len(client.options.hosts) == 2
    assert client.options.hosts[0] == {"host": "host1", "port": 9000}
    assert client.options.hosts[1] == {"host": "host2", "port": 9001}
