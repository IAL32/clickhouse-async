"""Client options for ClickHouse client."""

from typing import Any

from .connection_string import ConnectionString


class ClientOptions:
    """Client options for ClickHouse client.

    This class stores connection parameters and other options for the ClickHouse client.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9000,
        user: str = "default",
        password: str = "",
        database: str = "default",
        compression: bool = False,
        connect_timeout: float = 5.0,
        send_receive_timeout: float = 5.0,
        retry_timeout: float = 5.0,
        retry_count: int = 3,
    ) -> None:
        """Initialize client options.

        Args:
            host: ClickHouse server host
            port: ClickHouse server port
            user: Username for authentication
            password: Password for authentication
            database: Default database to use
            compression: Whether to use compression
            connect_timeout: Connection timeout in seconds
            send_receive_timeout: Send/receive timeout in seconds
            retry_timeout: Retry timeout in seconds
            retry_count: Number of retries
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.compression = compression
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.retry_timeout = retry_timeout
        self.retry_count = retry_count
        self.hosts: list[dict[str, Any]] = [{"host": host, "port": port}]

    @classmethod
    def from_connection_string(
        cls, connection_string: str | ConnectionString
    ) -> "ClientOptions":
        """Create client options from connection string.

        Args:
            connection_string: Connection string or ConnectionString object

        Returns:
            ClientOptions instance
        """
        if isinstance(connection_string, str):
            conn = ConnectionString(connection_string)
        else:
            conn = connection_string

        options = cls(
            host=conn.host,
            port=conn.port,
            user=conn.user,
            password=conn.password,
            database=conn.database,
        )

        options.hosts = conn.hosts

        if "compression" in conn.options:
            options.compression = conn.options["compression"].lower() == "true"

        if "connect_timeout" in conn.options:
            options.connect_timeout = float(conn.options["connect_timeout"])

        if "send_receive_timeout" in conn.options:
            options.send_receive_timeout = float(conn.options["send_receive_timeout"])

        if "retry_timeout" in conn.options:
            options.retry_timeout = float(conn.options["retry_timeout"])

        if "retry_count" in conn.options:
            options.retry_count = int(conn.options["retry_count"])

        return options

    def to_dict(self) -> dict[str, Any]:
        """Convert client options to dictionary.

        Returns:
            Dictionary with client options
        """
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "compression": self.compression,
            "connect_timeout": self.connect_timeout,
            "send_receive_timeout": self.send_receive_timeout,
            "retry_timeout": self.retry_timeout,
            "retry_count": self.retry_count,
        }

    def __str__(self) -> str:
        """Return string representation of client options."""
        return (
            f"ClientOptions(host='{self.host}', port={self.port}, "
            f"user='{self.user}', database='{self.database}', "
            f"compression={self.compression}, connect_timeout={self.connect_timeout}, "
            f"send_receive_timeout={self.send_receive_timeout}, "
            f"retry_timeout={self.retry_timeout}, retry_count={self.retry_count})"
        )
