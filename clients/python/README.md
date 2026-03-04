# VeridicalDB Python Driver

A Python client library for [VeridicalDB](https://github.com/JayabrataBasu/VeridicalDB) that implements the PostgreSQL wire protocol.

## Features

- **Full PostgreSQL Wire Protocol Support**: Compatible with VeridicalDB's PostgreSQL-compatible protocol
- **Synchronous and Asynchronous APIs**: Both blocking and async/await interfaces
- **Connection Pooling**: Efficient connection reuse with configurable pool sizes
- **Type Safety**: Comprehensive type mapping between Python and VeridicalDB types
- **PEP 249 Compliant**: Follows Python Database API Specification v2.0
- **No External Dependencies**: Pure Python implementation

## Installation

### User Installation

```bash
cd clients/python
pip install .
```

### Development Installation

For development, including test dependencies:

```bash
cd clients/python
pip install -e ".[dev]"
```

This installs pytest and related testing tools required for running the test suite.

### Resolving IDE Type Checking Issues

If you see "Import 'pytest' could not be resolved" in your IDE:

## Option 1: Install dev dependencies

```bash
pip install -e ".[dev]"
```

## Option 2: Configure your IDE**

The repository includes a `pyrightconfig.json` which disables import resolution warnings for dev-only imports. The `py.typed` marker indicates the package supports type checking.

## Quick Start

### Synchronous Usage

```python
import veridicaldb

# Connect to VeridicalDB
conn = veridicaldb.connect(
    host='localhost',
    port=5432,
    database='mydb',
    user='admin',
    password=''
)

# Execute queries
cursor = conn.cursor()
cursor.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
cursor.execute("INSERT INTO users VALUES (?, ?, ?)", (1, 'Alice', 30))
cursor.execute("SELECT * FROM users WHERE age > ?", (25,))

# Fetch results
rows = cursor.fetchall()
for row in rows:
    print(row)

# Commit and close
conn.commit()
conn.close()
```

### Context Manager Usage

```python
import veridicaldb

with veridicaldb.connect(host='localhost', database='mydb') as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users")
        rows = cursor.fetchall()
        # Automatic commit and close
```

### Asynchronous Usage

```python
import asyncio
import veridicaldb

async def main():
    async with veridicaldb.connect_async(host='localhost') as conn:
        cursor = await conn.cursor()
        await cursor.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
        print(rows)

asyncio.run(main())
```

### Connection Pooling

```python
from veridicaldb import ConnectionPool

# Create connection pool
pool = ConnectionPool(
    min_size=2,
    max_size=10,
    host='localhost',
    database='mydb'
)

# Acquire connection from pool
with pool.acquire() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()

# Close pool when done
pool.close()
```

### TLS / SSL

```python
import ssl
import veridicaldb

# Opportunistic TLS: use TLS if server supports it, else fallback to plaintext
conn = veridicaldb.connect(host='localhost', sslmode='prefer')

# Strict TLS: fail if server does not support TLS
conn = veridicaldb.connect(host='localhost', sslmode='require')

# Custom SSL context
ctx = ssl.create_default_context()
conn = veridicaldb.connect(host='localhost', sslmode='require', ssl_context=ctx)
```

## API Reference

### Connection

#### `veridicaldb.connect(**params)`

Create a connection to VeridicalDB.

**Parameters:**

- `host` (str): Server hostname or IP (default: 'localhost')
- `port` (int): Server port (default: 5432)
- `database` (str): Database name (default: 'default')
- `user` (str): Username (default: 'admin')
- `password` (str): Password (default: '')
- `connect_timeout` (int): Connection timeout in seconds (default: 10)
- `sslmode` (str): TLS mode (`disable`, `prefer`, `require`) (default: `disable`)
- `ssl_context` (`ssl.SSLContext`): Optional custom TLS context

**Returns:** `Connection` object

#### Connection Methods

- `cursor()`: Create a new cursor
- `commit()`: Commit current transaction
- `rollback()`: Rollback current transaction
- `close()`: Close the connection
- `get_server_version()`: Get server version string

### Cursor

#### Cursor Methods

- `execute(query, parameters=None)`: Execute a query
- `executemany(query, seq_of_parameters)`: Execute query multiple times
- `fetchone()`: Fetch next row
- `fetchmany(size=None)`: Fetch multiple rows
- `fetchall()`: Fetch all remaining rows
- `close()`: Close the cursor

#### Cursor Properties

- `description`: Column descriptions (name, type, etc.)
- `rowcount`: Number of rows affected
- `arraysize`: Default fetchmany() size

### Connection Pool

#### `ConnectionPool(**params)`

Create a connection pool.

**Parameters:**

- `min_size` (int): Minimum connections (default: 2)
- `max_size` (int): Maximum connections (default: 10)
- `timeout` (float): Acquire timeout in seconds (default: 30.0)
- `max_idle_time` (float): Max idle time in seconds (default: 300.0)
- `**connection_params`: Parameters for Connection

#### ConnectionPool Methods

- `acquire(timeout=None)`: Acquire connection from pool
- `close()`: Close all connections

### Async API

#### `veridicaldb.connect_async(**params)`

Create an async connection.

**Returns:** `AsyncConnection` object

#### AsyncConnection Methods

- `cursor()`: Create async cursor (awaitable)
- `commit()`: Commit transaction (awaitable)
- `rollback()`: Rollback transaction (awaitable)
- `close()`: Close connection (awaitable)

#### AsyncCursor Methods

- `execute(query, parameters)`: Execute query (awaitable)
- `executemany(query, seq_of_parameters)`: Execute multiple (awaitable)
- `fetchone()`: Fetch one row (awaitable)
- `fetchmany(size)`: Fetch many rows (awaitable)
- `fetchall()`: Fetch all rows (awaitable)

## Type Mapping

| Python Type | VeridicalDB Type |
| ------------ | ------------------ |
| `None` | NULL |
| `bool` | BOOL |
| `int` | INT64 |
| `float` | FLOAT64 |
| `str` | TEXT |
| `bytes` | BYTEA |
| `datetime` | TIMESTAMP |
| `Decimal` | NUMERIC |

### Explicit Types

```python
from veridicaldb import Int32, Int64, Float64, Text

cursor.execute("INSERT INTO data VALUES (?, ?)", (Int32(42), Text("hello")))
```

## Exception Hierarchy

```python
Error (base exception)
├── Warning
├── InterfaceError
│   └── ProtocolError
└── DatabaseError
    ├── DataError
    ├── OperationalError
    │   └── ConnectionError
    ├── IntegrityError
    ├── InternalError
    ├── ProgrammingError
    │   └── QueryError
    └── NotSupportedError
```

## Examples

See the `examples/` directory for more examples:

- `basic_connection.py`: Basic connection and queries
- `connection_pool.py`: Using connection pooling
- `async_example.py`: Async/await usage
- `transactions.py`: Transaction management
- `type_mapping.py`: Type conversion examples

## Testing

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/

# Run tests with coverage
pytest --cov=veridicaldb tests/
```

## License

MIT License - See main repository for details

## Contributing

Contributions are welcome! Please see the main repository's CONTRIBUTING.md for guidelines.
