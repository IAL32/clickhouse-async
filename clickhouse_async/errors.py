"""Error hierarchy. See DESIGN.md §8."""


class ClickHouseError(Exception):
    """Base class for all clickhouse-async errors."""


class ProtocolError(ClickHouseError):
    """Wire data violated the ClickHouse protocol."""
