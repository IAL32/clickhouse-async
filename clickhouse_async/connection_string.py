"""Connection string parsing for ClickHouse client."""

from typing import Any
from urllib.parse import parse_qs, urlparse


class ConnectionString:
    """Connection string parser for ClickHouse client.

    Supports the following formats:
    - clickhouse://user:password@host:port/database
    - clickhouse://user:password@host1:port1,host2:port2/database
    - clickhouse://host/database?param1=value1&param2=value2
    """

    DEFAULT_PORT = 9000
    DEFAULT_USER = "default"
    DEFAULT_PASSWORD = ""
    DEFAULT_DATABASE = "default"
    VALID_SCHEME = "clickhouse"

    def __init__(self, connection_string: str) -> None:
        """Initialize connection string parser.

        Args:
            connection_string: Connection string in the format
                clickhouse://user:password@host:port/database

        Raises:
            ValueError: If the connection string is invalid
        """
        self.connection_string = connection_string
        self.hosts: list[dict[str, Any]] = []
        self.options: dict[str, str] = {}

        self._parse_connection_string()

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> "ConnectionString":
        """Create connection string from dictionary.

        Args:
            params: Dictionary with connection parameters

        Returns:
            ConnectionString instance
        """
        host = params.get("host", "localhost")
        port = params.get("port", cls.DEFAULT_PORT)
        user = params.get("user", cls.DEFAULT_USER)
        password = params.get("password", cls.DEFAULT_PASSWORD)
        database = params.get("database", cls.DEFAULT_DATABASE)

        conn_str = f"{cls.VALID_SCHEME}://"

        if user != cls.DEFAULT_USER or password != cls.DEFAULT_PASSWORD:
            conn_str += f"{user}:{password}@"

        conn_str += f"{host}:{port}"

        if database != cls.DEFAULT_DATABASE:
            conn_str += f"/{database}"

        options = {
            k: v
            for k, v in params.items()
            if k not in ["host", "port", "user", "password", "database"]
        }
        if options:
            query_params = "&".join(f"{k}={v}" for k, v in options.items())
            conn_str += f"?{query_params}"

        return cls(conn_str)

    def to_dict(self) -> dict[str, Any]:
        """Convert connection string to dictionary.

        Returns:
            Dictionary with connection parameters
        """
        result = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }

        result.update(self.options)

        return result

    def _parse_connection_string(self) -> None:
        """Parse connection string into components."""
        try:
            parsed = urlparse(self.connection_string)

            if parsed.scheme != self.VALID_SCHEME:
                raise ValueError(
                    f"Invalid scheme: {parsed.scheme}, expected: {self.VALID_SCHEME}"
                )

            self.user = self.DEFAULT_USER
            self.password = self.DEFAULT_PASSWORD
            if parsed.username:
                self.user = parsed.username
            if parsed.password:
                self.password = parsed.password

            hosts_str = parsed.netloc.split("@")[-1]
            hosts_list = hosts_str.split(",")

            for host_str in hosts_list:
                host_parts = host_str.split(":")
                host = host_parts[0]
                port = int(host_parts[1]) if len(host_parts) > 1 else self.DEFAULT_PORT

                self.hosts.append({"host": host, "port": port})

            if self.hosts:
                self.host = self.hosts[0]["host"]
                self.port = self.hosts[0]["port"]
            else:
                raise ValueError("No host specified in connection string")

            self.database = (
                parsed.path.lstrip("/") if parsed.path else self.DEFAULT_DATABASE
            )

            if parsed.query:
                self.options = {k: v[0] for k, v in parse_qs(parsed.query).items()}

        except Exception as e:
            raise ValueError(f"Invalid connection string: {e}") from e

    def __str__(self) -> str:
        """Return string representation of connection string."""
        result = f"{self.VALID_SCHEME}://"

        if self.user != self.DEFAULT_USER or self.password != self.DEFAULT_PASSWORD:
            result += f"{self.user}:{self.password}@"

        result += f"{self.host}:{self.port}"

        if self.database != self.DEFAULT_DATABASE:
            result += f"/{self.database}"

        if self.options:
            query_params = "&".join(f"{k}={v}" for k, v in self.options.items())
            result += f"?{query_params}"

        return result
