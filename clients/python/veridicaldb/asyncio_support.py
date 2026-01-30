"""
Async/await support for VeridicalDB Python driver.

Provides async connection and cursor for use with asyncio.
"""

import asyncio
from typing import Optional, List, Tuple, Any
from .connection import Connection
from .cursor import Cursor
from .exceptions import OperationalError, ProgrammingError


class AsyncCursor:
    """
    Async wrapper for Cursor.
    
    Executes database operations in thread pool to avoid blocking event loop.
    """
    
    def __init__(self, cursor: Cursor, loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        Initialize async cursor.
        
        Args:
            cursor: Underlying Cursor object
            loop: Event loop (defaults to current loop)
        """
        self._cursor = cursor
        self._loop = loop or asyncio.get_event_loop()
    
    async def execute(self, query: str, parameters: Optional[Tuple[Any, ...]] = None):
        """
        Execute query asynchronously.
        
        Args:
            query: SQL query string
            parameters: Optional query parameters
        """
        await self._loop.run_in_executor(
            None,
            self._cursor.execute,
            query,
            parameters
        )
    
    async def executemany(self, query: str, seq_of_parameters: List[Tuple[Any, ...]]):
        """
        Execute query multiple times asynchronously.
        
        Args:
            query: SQL query string
            seq_of_parameters: Sequence of parameter tuples
        """
        await self._loop.run_in_executor(
            None,
            self._cursor.executemany,
            query,
            seq_of_parameters
        )
    
    async def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """Fetch next row asynchronously."""
        return await self._loop.run_in_executor(None, self._cursor.fetchone)
    
    async def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        """Fetch multiple rows asynchronously."""
        return await self._loop.run_in_executor(None, self._cursor.fetchmany, size)
    
    async def fetchall(self) -> List[Tuple[Any, ...]]:
        """Fetch all rows asynchronously."""
        return await self._loop.run_in_executor(None, self._cursor.fetchall)
    
    def close(self):
        """Close cursor."""
        self._cursor.close()
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.close()
        return False
    
    @property
    def description(self):
        """Column description."""
        return self._cursor.description
    
    @property
    def rowcount(self) -> int:
        """Row count."""
        return self._cursor.rowcount
    
    @property
    def arraysize(self) -> int:
        """Array size."""
        return self._cursor.arraysize
    
    @arraysize.setter
    def arraysize(self, size: int):
        """Set array size."""
        self._cursor.arraysize = size


class AsyncConnection:
    """
    Async wrapper for Connection.
    
    Executes database operations in thread pool to avoid blocking event loop.
    """
    
    def __init__(self, connection: Connection, loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        Initialize async connection.
        
        Args:
            connection: Underlying Connection object
            loop: Event loop (defaults to current loop)
        """
        self._connection = connection
        self._loop = loop or asyncio.get_event_loop()
    
    async def cursor(self) -> AsyncCursor:
        """
        Create async cursor.
        
        Returns:
            AsyncCursor object
        """
        cursor = await self._loop.run_in_executor(None, self._connection.cursor)
        return AsyncCursor(cursor, self._loop)
    
    async def commit(self):
        """Commit transaction asynchronously."""
        await self._loop.run_in_executor(None, self._connection.commit)
    
    async def rollback(self):
        """Rollback transaction asynchronously."""
        await self._loop.run_in_executor(None, self._connection.rollback)
    
    async def close(self):
        """Close connection asynchronously."""
        await self._loop.run_in_executor(None, self._connection.close)
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()
        
        await self.close()
        return False
    
    @property
    def closed(self) -> bool:
        """Check if connection is closed."""
        return self._connection.closed
    
    @property
    def in_transaction(self) -> bool:
        """Check if in transaction."""
        return self._connection.in_transaction
    
    async def get_server_version(self) -> Optional[str]:
        """Get server version asynchronously."""
        return await self._loop.run_in_executor(None, self._connection.get_server_version)


async def connect_async(
    host: str = 'localhost',
    port: int = 5432,
    database: str = 'default',
    user: str = 'admin',
    password: str = '',
    loop: Optional[asyncio.AbstractEventLoop] = None,
    **kwargs
) -> AsyncConnection:
    """
    Create an async connection to VeridicalDB.
    
    Args:
        host: Server hostname or IP address
        port: Server port number
        database: Database name
        user: Username
        password: Password
        loop: Event loop (defaults to current loop)
        **kwargs: Additional connection parameters
        
    Returns:
        AsyncConnection object
        
    Example:
        >>> async with connect_async(host='localhost') as conn:
        ...     cursor = await conn.cursor()
        ...     await cursor.execute("SELECT * FROM users")
        ...     rows = await cursor.fetchall()
    """
    if loop is None:
        loop = asyncio.get_event_loop()
    
    # Create connection in thread pool
    connection = await loop.run_in_executor(
        None,
        Connection,
        host,
        port,
        database,
        user,
        password,
        10,  # connect_timeout
        **kwargs
    )
    
    return AsyncConnection(connection, loop)
