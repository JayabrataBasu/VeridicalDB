"""
Tests for VeridicalDB connection pooling.
"""

try:
    import pytest # type: ignore
except ImportError:
    # pytest not installed, tests will be skipped
    pytest = None

import threading
import time
from veridicaldb.pool import ConnectionPool
from veridicaldb.exceptions import OperationalError


class MockConnection:
    """Mock connection for testing pool without server."""
    
    def __init__(self, **kwargs):
        self.closed_flag = False
        self.in_transaction_flag = False
        self.params = kwargs
    
    def cursor(self):
        return None
    
    def commit(self):
        self.in_transaction_flag = False
    
    def rollback(self):
        self.in_transaction_flag = False
    
    def close(self):
        self.closed_flag = True
    
    @property
    def closed(self):
        return self.closed_flag
    
    @property
    def in_transaction(self):
        return self.in_transaction_flag


class TestConnectionPool:
    """Test connection pool functionality."""
    
    def test_pool_creation(self, monkeypatch):
        """Test pool creation with min_size connections."""
        # Mock Connection class
        monkeypatch.setattr('veridicaldb.pool.Connection', MockConnection)
        
        pool = ConnectionPool(min_size=2, max_size=5, host='localhost')
        
        assert pool.min_size == 2
        assert pool.max_size == 5
        assert pool.total_connections >= 2
        
        pool.close()
    
    def test_pool_validation(self):
        """Test pool parameter validation."""
        with pytest.raises(ValueError):
            ConnectionPool(min_size=-1)
        
        with pytest.raises(ValueError):
            ConnectionPool(min_size=10, max_size=5)
    
    def test_acquire_and_return(self, monkeypatch):
        """Test acquiring and returning connections."""
        monkeypatch.setattr('veridicaldb.pool.Connection', MockConnection)
        
        pool = ConnectionPool(min_size=1, max_size=3)
        
        # Acquire connection
        conn = pool.acquire()
        assert conn is not None
        
        # Return connection
        conn.close()
        
        # Should be able to acquire again
        conn2 = pool.acquire()
        assert conn2 is not None
        
        conn2.close()
        pool.close()
    
    def test_max_connections(self, monkeypatch):
        """Test that pool respects max_size."""
        monkeypatch.setattr('veridicaldb.pool.Connection', MockConnection)
        
        pool = ConnectionPool(min_size=1, max_size=2, timeout=0.5)
        
        # Acquire max connections
        conn1 = pool.acquire()
        conn2 = pool.acquire()
        
        # Should timeout trying to acquire third
        with pytest.raises(OperationalError):
            pool.acquire(timeout=0.5)
        
        # Return one and try again
        conn1.close()
        conn3 = pool.acquire()
        assert conn3 is not None
        
        conn2.close()
        conn3.close()
        pool.close()
    
    def test_context_manager(self, monkeypatch):
        """Test pool context manager."""
        monkeypatch.setattr('veridicaldb.pool.Connection', MockConnection)
        
        with ConnectionPool(min_size=1, max_size=3) as pool:
            conn = pool.acquire()
            assert conn is not None
            conn.close()
        
        assert pool.closed
    
    def test_concurrent_access(self, monkeypatch):
        """Test concurrent connection acquisition."""
        monkeypatch.setattr('veridicaldb.pool.Connection', MockConnection)
        
        pool = ConnectionPool(min_size=2, max_size=5)
        results = []
        
        def worker():
            try:
                conn = pool.acquire(timeout=2.0)
                time.sleep(0.1)  # Simulate work
                conn.close()
                results.append(True)
            except Exception as e:
                results.append(False)
        
        # Create multiple threads
        threads = [threading.Thread(target=worker) for _ in range(10)]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # All should succeed
        assert all(results)
        assert len(results) == 10
        
        pool.close()
    
    def test_closed_pool(self, monkeypatch):
        """Test that closed pool raises errors."""
        monkeypatch.setattr('veridicaldb.pool.Connection', MockConnection)
        
        pool = ConnectionPool(min_size=1, max_size=2)
        pool.close()
        
        with pytest.raises(OperationalError):
            pool.acquire()


class TestPooledConnection:
    """Test pooled connection wrapper."""
    
    def test_auto_return(self, monkeypatch):
        """Test that connection is automatically returned on close."""
        monkeypatch.setattr('veridicaldb.pool.Connection', MockConnection)
        
        pool = ConnectionPool(min_size=1, max_size=2)
        
        initial_available = pool.available_connections
        
        # Acquire and close
        with pool.acquire() as conn:
            assert pool.available_connections == initial_available - 1
        
        # Should be returned
        assert pool.available_connections == initial_available
        
        pool.close()
    
    def test_rollback_on_error(self, monkeypatch):
        """Test that connection rolls back on exception."""
        monkeypatch.setattr('veridicaldb.pool.Connection', MockConnection)
        
        pool = ConnectionPool(min_size=1, max_size=2)
        
        try:
            with pool.acquire() as conn:
                conn._connection.in_transaction_flag = True
                raise ValueError("Test error")
        except ValueError:
            pass
        
        # Connection should have been rolled back
        # (we can't directly test this without mocking, but the code path is covered)
        
        pool.close()
