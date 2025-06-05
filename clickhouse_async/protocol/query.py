"""Query execution protocol implementation."""

import logging
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

from ..exceptions import RemoteServerError
from .constants import (
    ClickHouseProtocol,
    ClientCodes,
    CompressionState,
    ServerCodes,
    Stages,
)
from .protocol_types import SupportsRead, SupportsWrite

logger = logging.getLogger(__name__)


class QueryKind(IntEnum):
    """Query kind."""

    INITIAL_QUERY = 1
    SECONDARY_QUERY = 2


@dataclass
class BlockInfo:
    """Block information."""

    is_overflows: bool = False
    bucket_num: int = 0


@dataclass
class Block:
    """Data block."""

    column_names: list[str]
    column_types: list[str]
    rows: list[dict[str, Any]]
    info: BlockInfo = field(default_factory=BlockInfo)

    @property
    def row_count(self) -> int:
        """Get the number of rows in the block."""
        return len(self.rows)

    @property
    def column_count(self) -> int:
        """Get the number of columns in the block."""
        return len(self.column_names)


@dataclass
class QueryInfo:
    """Query information."""

    query_id: str
    query: str
    settings: dict[str, str]


class QueryResult:
    """Query result."""

    def __init__(self) -> None:
        """Initialize query result."""
        self.blocks: list[Block] = []
        self.totals: Block | None = None
        self.extremes: Block | None = None
        self.exception: RemoteServerError | None = None
        self.progress_rows: int = 0
        self.progress_bytes: int = 0
        self.progress_total_rows: int = 0
        self.elapsed_seconds: float = 0.0
        self.rows_read: int = 0
        self.bytes_read: int = 0

    @property
    def rows(self) -> list[dict[str, Any]]:
        """Get all rows from all blocks."""
        result = []
        for block in self.blocks:
            result.extend(block.rows)
        return result

    @property
    def has_exception(self) -> bool:
        """Check if the query result has an exception."""
        return self.exception is not None


async def send_query(
    output_stream: SupportsWrite,
    user: str,
    protocol_version: int,
    compression: CompressionState,
    query: str,
    query_id: str = "",
    settings: dict[str, str] | None = None,
) -> None:
    """Send a query to the server.

    Args:
        connection: ConnectionProtocol to use
        query: SQL query to execute
        query_id: Query ID
        settings: Query settings
    """

    # Send query packet
    logger.debug(f"Sending query: {query}")
    logger.debug(f"Protocol version: {protocol_version}")
    logger.debug(f"User: {user}")
    logger.debug(f"Compression: {compression}")
    await output_stream.write_varint(ClientCodes.QUERY)

    # Query ID
    await output_stream.write_string(query_id)

    # Client info (only if supported)
    if protocol_version >= ClickHouseProtocol.MIN_REVISION_WITH_CLIENT_INFO:
        await output_stream.write_uint8(
            1
        )  # Query kind: INITIAL_QUERY (must be fixed size)
        await output_stream.write_string("")  # Initial user (empty)
        await output_stream.write_string("")  # Initial query ID (empty)
        await output_stream.write_string("[::ffff:127.0.0.1]:0")  # Initial address

        # Initial query start time (if supported)
        if (
            protocol_version
            >= ClickHouseProtocol.MIN_REVISION_WITH_INITIAL_QUERY_START_TIME
        ):
            await output_stream.write_int64(0)  # Initial query start time

        await output_stream.write_uint8(1)  # Interface type (1 = TCP)

        # OS user, client hostname, client name
        await output_stream.write_string("")  # OS user (empty)
        await output_stream.write_string("")  # Client hostname (empty)
        await output_stream.write_string(
            "ClickHouse client"
        )  # Client name (match C++ client)

        # Client version (match C++ client)
        await output_stream.write_varint(24)  # Major (CLICKHOUSE_CPP_VERSION_MAJOR)
        await output_stream.write_varint(12)  # Minor (CLICKHOUSE_CPP_VERSION_MINOR)
        await output_stream.write_varint(
            ClickHouseProtocol.DBMS_PROTOCOL_VERSION
        )  # Client revision (use our protocol version, not server's)

        # Quota key (if supported)
        if (
            protocol_version
            >= ClickHouseProtocol.MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO
        ):
            await output_stream.write_string("")  # Quota key

        # Distributed depth (if supported)
        if protocol_version >= ClickHouseProtocol.MIN_REVISION_WITH_DISTRIBUTED_DEPTH:
            await output_stream.write_varint(0)  # Distributed depth

        # Version patch (if supported)
        if protocol_version >= ClickHouseProtocol.MIN_REVISION_WITH_VERSION_PATCH:
            await output_stream.write_varint(0)  # Patch

        # OpenTelemetry (if supported)
        if protocol_version >= ClickHouseProtocol.MIN_REVISION_WITH_OPENTELEMETRY:
            await output_stream.write_uint8(0)  # No OpenTelemetry

        # Parallel replicas (if supported)
        if protocol_version >= ClickHouseProtocol.MIN_REVISION_WITH_PARALLEL_REPLICAS:
            await output_stream.write_varint(0)  # Replica number
            await output_stream.write_varint(0)  # Replica count
            await output_stream.write_varint(0)  # Coordinator address

    # Settings
    logger.debug(f"Settings: {settings}")
    if (
        protocol_version
        >= ClickHouseProtocol.MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
    ):
        logger.debug(
            f"Using new settings format for protocol version {protocol_version}"
        )
        # For newer protocol versions, settings are serialized differently
        # Send each setting followed by flags and value
        if settings:
            logger.debug(f"Sending {len(settings)} settings")
            for key, value in settings.items():
                logger.debug(f"Sending setting: {key}={value}")
                await output_stream.write_string(key)
                await output_stream.write_varint(0)  # flags (0 for now)
                await output_stream.write_string(value)
        # Empty string signals end of serialized settings
        await output_stream.write_string("")
    else:
        logger.debug(
            f"Using old settings format for protocol version {protocol_version}"
        )
        # For older protocol versions
        if settings:
            await output_stream.write_varint(len(settings))
            for key, value in settings.items():
                await output_stream.write_string(key)
                await output_stream.write_string(value)
        else:
            await output_stream.write_varint(0)
        # Empty string signals end of serialized settings
        await output_stream.write_string("")

    # Add interserver secret for newer protocol versions
    if protocol_version >= ClickHouseProtocol.MIN_REVISION_WITH_INTERSERVER_SECRET:
        logger.debug(
            f"Sending interserver secret (protocol version {protocol_version} >= {ClickHouseProtocol.MIN_REVISION_WITH_INTERSERVER_SECRET})"
        )
        await output_stream.write_string("")  # Empty interserver secret
    else:
        logger.debug(
            f"Skipping interserver secret (protocol version {protocol_version} < {ClickHouseProtocol.MIN_REVISION_WITH_INTERSERVER_SECRET})"
        )

    # Add query processing stage
    await output_stream.write_varint(Stages.COMPLETE)

    # Compression
    await output_stream.write_varint(compression)

    # Query
    await output_stream.write_string(query)

    # Add query parameters support
    if protocol_version >= ClickHouseProtocol.MIN_PROTOCOL_VERSION_WITH_PARAMETERS:
        # No parameters for now, just send empty string to signal end
        logger.debug(
            f"Sending parameters (protocol version {protocol_version} >= {ClickHouseProtocol.MIN_PROTOCOL_VERSION_WITH_PARAMETERS})"
        )
        await output_stream.write_string("")
    else:
        logger.debug(
            f"Skipping parameters (protocol version {protocol_version} < {ClickHouseProtocol.MIN_PROTOCOL_VERSION_WITH_PARAMETERS})"
        )

    # The C++ client always sends an empty block after the query
    # Send DATA packet with empty block (don't flush yet)
    await output_stream.write_varint(ClientCodes.DATA)

    # Temporary table name (only if supported)
    if protocol_version >= ClickHouseProtocol.MIN_REVISION_WITH_TEMPORARY_TABLES:
        await output_stream.write_string("")

    # Empty block
    await write_empty_block(output_stream, protocol_version)

    # Now flush everything together
    await output_stream.flush()


async def write_empty_block(
    output_stream: SupportsWrite,
    protocol_version: int = 0,
) -> None:
    """Write an empty block to the server.

    Args:
        output_stream: Output stream to write to
        protocol_version: Protocol version
    """
    # Block info (only if supported)
    if protocol_version >= ClickHouseProtocol.MIN_REVISION_WITH_BLOCK_INFO:
        # Write block info fields (matching C++ client exactly)
        await output_stream.write_varint(1)  # field number 1
        await output_stream.write_uint8(0)  # is_overflows = false (fixed uint8)
        await output_stream.write_varint(2)  # field number 2
        await output_stream.write_int32(0)  # bucket_num = 0 (fixed int32)
        await output_stream.write_varint(0)  # end of fields marker

    # No columns
    await output_stream.write_varint(0)
    # No rows
    await output_stream.write_varint(0)


async def read_exception(
    input_stream: SupportsRead,
) -> RemoteServerError:
    """Read an exception from the server.

    Args:
        input_stream: Input stream to read from

    Returns:
        Server exception
    """
    try:
        code = await input_stream.read_varint()
        name = await input_stream.read_string()
        message = await input_stream.read_string()
        stack_trace = await input_stream.read_string()
        has_nested = await input_stream.read_varint()

        nested = None
        if has_nested:
            nested = await read_exception(input_stream)

        logger.debug(f"Read exception: code={code}, name={name}, message={message}")

        from ..exceptions import RemoteServerError as ClientServerException

        return ClientServerException(code, name, message, stack_trace, nested)
    except EOFError:
        # If we can't read the full exception, return a generic one
        from ..exceptions import RemoteServerError as ClientServerException

        return ClientServerException(
            0, "UnknownException", "Failed to read exception from server", "", None
        )


async def read_block(input_stream: SupportsRead, protocol_version: int = 0) -> Block:
    """Read a block from the server.

    Args:
        input_stream: Input stream to read from
        protocol_version: Protocol version

    Returns:
        Data block
    """
    try:
        # Create block info
        info = BlockInfo()

        # Read block info only if supported
        if protocol_version >= ClickHouseProtocol.MIN_REVISION_WITH_BLOCK_INFO:
            # Read block info fields until we get 0
            while True:
                field_num = await input_stream.read_varint()
                if field_num == 0:
                    break
                elif field_num == 1:
                    # is_overflows (uint8) - fixed size, not varint
                    is_overflows_byte = await input_stream.read_exactly(1)
                    info.is_overflows = is_overflows_byte[0] != 0
                elif field_num == 2:
                    # bucket_num (int32) - fixed size, not varint
                    bucket_bytes = await input_stream.read_exactly(4)
                    import struct

                    info.bucket_num = struct.unpack("<i", bucket_bytes)[0]
                else:
                    # Unknown field, skip it based on field number
                    # For now, we'll just log it
                    logger.debug(f"Unknown block info field: {field_num}")

        # Column names and types
        num_columns = await input_stream.read_varint()
        num_rows = await input_stream.read_varint()
        logger.debug(f"Block info: {num_columns} columns, {num_rows} rows")
        column_names = []
        column_types = []

        for _ in range(num_columns):
            column_name = await input_stream.read_string()
            column_type = await input_stream.read_string()
            column_names.append(column_name)
            column_types.append(column_type)
            logger.debug(f"Column: {column_name} ({column_type})")

        # Create block with info
        block = Block(column_names=column_names, column_types=column_types, rows=[])
        block.info = info

        # Read rows
        rows: list[Any] = []
        if num_rows > 0:
            # For each column, read all values for that column
            for _, (col_name, col_type) in enumerate(
                zip(column_names, column_types, strict=False)
            ):
                try:
                    # Read custom serialization flag if supported
                    if (
                        protocol_version
                        >= ClickHouseProtocol.MIN_REVISION_WITH_CUSTOM_SERIALIZATION
                    ):
                        custom_serialization = await input_stream.read_exactly(1)
                        if custom_serialization[0] != 0:
                            raise ValueError(
                                f"Custom serialization not supported for column {col_name}"
                            )

                    # Read all values for this column
                    values = []
                    for row_idx in range(num_rows):
                        value = await read_column_data(input_stream, col_type)
                        values.append(value)
                        logger.debug(
                            f"Read value for column {col_name} row {row_idx}: {value}"
                        )

                    # Add values to rows
                    for row_idx, value in enumerate(values):
                        if row_idx >= len(rows):
                            rows.append({})
                        rows[row_idx][col_name] = value

                except Exception as e:
                    from ..exceptions import ProtocolError

                    raise ProtocolError(
                        f"Error reading column '{col_name}' of type '{col_type}': {e}"
                    ) from e

        block.rows = rows
        return block
    except EOFError as e:
        # If we get an EOFError, the server closed the connection
        logger.debug(f"Server closed connection while reading block: {e}")
        # Return an empty block
        return Block(column_names=[], column_types=[], rows=[])


async def read_column_data(input_stream: SupportsRead, column_type: str) -> Any:  # noqa: ANN401
    """Read column data from the server.

    Args:
        input_stream: Input stream to read from
        column_type: Column type

    Returns:
        Column data
    """
    from .data_types import DataType

    # Create a data type instance and read the value
    data_type = DataType.create(column_type)
    return await data_type.read_value(input_stream)


async def process_query_response(
    input_stream: SupportsRead,
) -> QueryResult:
    """Process a query response from the server.

    Args:
        connection: ConnectionProtocol to use

    Returns:
        Query result
    """
    result = QueryResult()

    while True:
        packet_type = await input_stream.read_varint()

        if packet_type == ServerCodes.DATA:
            # Skip temporary table name
            await input_stream.read_string()

            # Read block
            block = await read_block(input_stream)
            result.blocks.append(block)

        elif packet_type == ServerCodes.EXCEPTION:
            # Read exception
            result.exception = await read_exception(input_stream)
            break

        elif packet_type == ServerCodes.PROGRESS:
            # Read progress
            result.progress_rows = await input_stream.read_varint()
            result.progress_bytes = await input_stream.read_varint()
            result.progress_total_rows = await input_stream.read_varint()

        elif packet_type == ServerCodes.PROFILE_INFO:
            # Read profile info
            result.rows_read = await input_stream.read_varint()
            result.bytes_read = await input_stream.read_varint()
            result.elapsed_seconds = await input_stream.read_varint() / 1000.0

        elif packet_type == ServerCodes.TOTALS:
            # Read totals block
            await input_stream.read_string()  # Skip temporary table name
            result.totals = await read_block(input_stream)

        elif packet_type == ServerCodes.EXTREMES:
            # Read extremes block
            await input_stream.read_string()  # Skip temporary table name
            result.extremes = await read_block(input_stream)

        elif packet_type == ServerCodes.END_OF_STREAM:
            # End of stream
            break

        else:
            # Skip unknown packet
            pass

    return result
