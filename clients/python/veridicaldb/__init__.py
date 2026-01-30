"""
VeridicalDB Python Driver

A Python client library for VeridicalDB that implements the PostgreSQL wire protocol.
Supports both synchronous and asynchronous operations, connection pooling, and
comprehensive type mapping.

Usage:
    import veridicaldb
    
    # Synchronous connection
    conn = veridicaldb.connect(host='localhost', port=5432, database='mydb')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE age > ?", (25,))
    rows = cursor.fetchall()
    
    # Async connection
    async with veridicaldb.connect_async(host='localhost', port=5432) as conn:
        cursor = await conn.cursor()
        await cursor.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
"""

__version__ = "0.1.0"
__author__ = "VeridicalDB Contributors"

from .connection import Connection, connect
from .cursor import Cursor
from .pool import ConnectionPool
from .types import (
    DataType,
    Int32,
    Int64,
    Float64,
    Text,
    Boolean,
    Timestamp,
    Null,
)
from .exceptions import (
    Error,
    DatabaseError,
    OperationalError,
    ProgrammingError,
    IntegrityError,
    DataError,
    NotSupportedError,
    ConnectionError as VeridicalConnectionError,
)

# Async support
try:
    from .asyncio_support import AsyncConnection, connect_async
    __all__ = [
        "connect",
        "connect_async",
        "Connection",
        "AsyncConnection",
        "Cursor",
        "ConnectionPool",
        # Types
        "DataType",
        "Int32",
        "Int64",
        "Float64",
        "Text",
        "Boolean",
        "Timestamp",
        "Null",
        # Exceptions
        "Error",
        "DatabaseError",
        "OperationalError",
        "ProgrammingError",
        "IntegrityError",
        "DataError",
        "NotSupportedError",
        "VeridicalConnectionError",
    ]
except ImportError:
    __all__ = [
        "connect",
        "Connection",
        "Cursor",
        "ConnectionPool",
        # Types
        "DataType",
        "Int32",
        "Int64",
        "Float64",
        "Text",
        "Boolean",
        "Timestamp",
        "Null",
        # Exceptions
        "Error",
        "DatabaseError",
        "OperationalError",
        "ProgrammingError",
        "IntegrityError",
        "DataError",
        "NotSupportedError",
        "VeridicalConnectionError",
    ]
