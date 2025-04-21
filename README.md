# clickhouse-async

An asyncio ClickHouse Python Driver with native (TCP) interface support.

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
        port=8123,
        user="default",
        password="",
        database="default"
    )
    
    # Execute a query
    result = await client.execute("SELECT 1")
    print(result)
    
    # Use the iterator interface for large result sets
    async for row in client.execute_iter("SELECT number FROM system.numbers LIMIT 10"):
        print(row)

if __name__ == "__main__":
    asyncio.run(main())
```

## Features

- Asynchronous API using Python's asyncio
- Support for ClickHouse HTTP interface
- Simple, intuitive interface
- Type annotations for better IDE support

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
