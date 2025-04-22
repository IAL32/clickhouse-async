# clickhouse-async

An asyncio ClickHouse Python Driver with native (TCP) interface support.

## Status

This project is currently in development and not ready for production use. The following features are implemented:

- Connection string parsing
- Client options
- Basic protocol implementation (handshake, ping)
- Connection management
- Data type serialization/deserialization (basic, complex, and special types)

The following features are still in development:

- Query execution protocol
- Data insertion
- Connection pooling

## Installation

```bash
pip install clickhouse-async
```

Or with Poetry:

```bash
poetry add clickhouse-async
```

## Basic Usage

```python
import asyncio
from clickhouse_async.client import ClickHouseClient

async def main():
    # Initialize the client
    client = ClickHouseClient(
        host="localhost",
        port=9000,  # Native protocol port
        user="default",
        password="",
        database="default"
    )
    
    # Connect to the server
    await client.connect()
    
    try:
        # Check server info
        print(f"Connected to {client.server_info.name} {client.server_info.version_major}.{client.server_info.version_minor}")
        
        # Ping the server
        if await client.ping():
            print("Ping successful")
        
        # Execute a query (not fully implemented yet)
        # result = await client.execute("SELECT 1")
        # print(result)
    finally:
        # Disconnect from the server
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```

## Features

- Asynchronous API using Python's asyncio
- Support for ClickHouse native protocol
- Connection string support
- Simple, intuitive interface
- Type annotations for better IDE support
- Integration testing with testcontainers

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/clickhouse-async.git
cd clickhouse-async

# Install dependencies with Poetry
poetry install

# Run tests
poetry run pytest
```

## License

See the [LICENSE](LICENSE) file for details.
