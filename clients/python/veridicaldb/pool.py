"""
Connection pooling for VeridicalDB Python driver.

Provides efficient connection reuse with configurable pool size and timeouts.
"""

import threading
import time
from typing import Optional, Dict, Any
from queue import Queue, Empty, Full
from .connection import Connection
from .exceptions import OperationalError


class PooledConnection:
    """
    Wrapper for a pooled connection.
    
    Automatically returns connection to pool when closed.
    """
    
    def __init__(self, connection: Connection, pool: 'ConnectionPool'):
        """
        Initialize pooled connection.
        
        Args:
            connection: Underlying Connection object
            pool: Parent ConnectionPool
        """
        self._connection = connection
        self._pool = pool
        self._returned = False
    
    def cursor(self):
        """Create cursor."""
        return self._connection.cursor()
    
    def commit(self):
        """Commit transaction."""
        self._connection.commit()
    
    def rollback(self):
        """Rollback transaction."""
        self._connection.rollback()
    
    def close(self):
        """Return connection to pool."""
        if not self._returned:
            self._pool._return_connection(self._connection)
            self._returned = True
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type is not None:
            try:
                self._connection.rollback()
            except Exception:
                pass
        else:
            try:
                self._connection.commit()
            except Exception:
                pass
        
        self.close()
        return False
    
    @property
    def closed(self) -> bool:
        """Check if connection is closed."""
        return self._connection.closed
    
    @property
    def in_transaction(self) -> bool:
        """Check if in transaction."""
        return self._connection.in_transaction
    
    def get_server_version(self) -> Optional[str]:
        """Get server version."""
        return self._connection.get_server_version()


class ConnectionPool:
    """
    Connection pool for VeridicalDB.
    
    Maintains a pool of reusable connections with configurable min/max size.
    """
    
    def __init__(
        self,
        min_size: int = 2,
        max_size: int = 10,
        timeout: float = 30.0,
        max_idle_time: float = 300.0,
        **connection_params
    ):
        """
        Initialize connection pool.
        
        Args:
            min_size: Minimum number of connections to maintain
            max_size: Maximum number of connections allowed
            timeout: Timeout for acquiring connection (seconds)
            max_idle_time: Maximum time a connection can remain idle (seconds)
            **connection_params: Parameters passed to Connection constructor
        """
        if min_size < 0:
            raise ValueError("min_size must be >= 0")
        if max_size < min_size:
            raise ValueError("max_size must be >= min_size")
        
        self.min_size = min_size
        self.max_size = max_size
        self.timeout = timeout
        self.max_idle_time = max_idle_time
        self.connection_params = connection_params
        
        self._pool: Queue[Connection] = Queue(maxsize=max_size)
        self._total_connections = 0
        self._lock = threading.Lock()
        self._closed = False
        
        # Initialize minimum connections
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Create initial connections for the pool."""
        for _ in range(self.min_size):
            try:
                conn = self._create_connection()
                self._pool.put_nowait(conn)
            except Exception as e:
                # Log error but continue
                print(f"Warning: Failed to create initial connection: {e}")
    
    def _create_connection(self) -> Connection:
        """
        Create a new connection.
        
        Returns:
            New Connection object
            
        Raises:
            OperationalError: If max connections reached
        """
        with self._lock:
            if self._total_connections >= self.max_size:
                raise OperationalError(f"Maximum connections ({self.max_size}) reached")
            
            conn = Connection(**self.connection_params)
            self._total_connections += 1
            return conn
    
    def _return_connection(self, connection: Connection):
        """
        Return a connection to the pool.
        
        Args:
            connection: Connection to return
        """
        if self._closed:
            # Pool is closed, close the connection
            connection.close()
            with self._lock:
                self._total_connections -= 1
            return
        
        # Check if connection is still valid
        if connection.closed:
            # Connection is closed, create a new one to maintain min_size
            with self._lock:
                self._total_connections -= 1
                if self._total_connections < self.min_size:
                    try:
                        new_conn = self._create_connection()
                        self._pool.put_nowait(new_conn)
                    except Exception:
                        pass
            return
        
        # Rollback any pending transaction
        try:
            if connection.in_transaction:
                connection.rollback()
        except Exception:
            # Connection is broken, close it
            connection.close()
            with self._lock:
                self._total_connections -= 1
            return
        
        # Return to pool
        try:
            self._pool.put_nowait(connection)
        except Full:
            # Pool is full, close the connection
            connection.close()
            with self._lock:
                self._total_connections -= 1
    
    def acquire(self, timeout: Optional[float] = None) -> PooledConnection:
        """
        Acquire a connection from the pool.
        
        Args:
            timeout: Optional timeout override
            
        Returns:
            PooledConnection object
            
        Raises:
            OperationalError: If pool is closed or timeout occurs
        """
        if self._closed:
            raise OperationalError("Connection pool is closed")
        
        if timeout is None:
            timeout = self.timeout
        
        start_time = time.time()
        
        while True:
            try:
                # Try to get connection from pool
                conn = self._pool.get(timeout=0.1)
                
                # Verify connection is still valid
                if conn.closed:
                    with self._lock:
                        self._total_connections -= 1
                    # Try again
                    if time.time() - start_time > timeout:
                        raise OperationalError("Timeout acquiring connection from pool")
                    continue
                
                return PooledConnection(conn, self)
                
            except Empty:
                # Pool is empty, try to create new connection
                if self._total_connections < self.max_size:
                    try:
                        conn = self._create_connection()
                        return PooledConnection(conn, self)
                    except Exception as e:
                        if time.time() - start_time > timeout:
                            raise OperationalError(f"Timeout acquiring connection: {e}")
                else:
                    # Wait for a connection to be returned
                    if time.time() - start_time > timeout:
                        raise OperationalError("Timeout acquiring connection from pool")
                
                time.sleep(0.1)
    
    def close(self):
        """Close all connections in the pool."""
        if self._closed:
            return
        
        self._closed = True
        
        # Close all connections in pool
        while True:
            try:
                conn = self._pool.get_nowait()
                conn.close()
                with self._lock:
                    self._total_connections -= 1
            except Empty:
                break
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
    
    @property
    def size(self) -> int:
        """Current number of connections in pool."""
        return self._pool.qsize()
    
    @property
    def total_connections(self) -> int:
        """Total number of active connections."""
        return self._total_connections
    
    @property
    def available_connections(self) -> int:
        """Number of available connections in pool."""
        return self._pool.qsize()
    
    @property
    def closed(self) -> bool:
        """Check if pool is closed."""
        return self._closed
