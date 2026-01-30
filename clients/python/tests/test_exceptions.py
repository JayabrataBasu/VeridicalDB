"""
Tests for VeridicalDB exceptions.
"""

import pytest
from veridicaldb.exceptions import (
    Error,
    Warning,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
    ConnectionError,
    QueryError,
    ProtocolError,
)


class TestExceptionHierarchy:
    """Test exception class hierarchy."""
    
    def test_base_error(self):
        """Test base Error exception."""
        err = Error("Test error")
        assert isinstance(err, Exception)
        assert str(err) == "Test error"
    
    def test_warning(self):
        """Test Warning exception."""
        warn = Warning("Test warning")
        assert isinstance(warn, Exception)
    
    def test_interface_error(self):
        """Test InterfaceError hierarchy."""
        err = InterfaceError("Interface error")
        assert isinstance(err, Error)
        
        # ProtocolError should be subclass of InterfaceError
        proto_err = ProtocolError("Protocol error")
        assert isinstance(proto_err, InterfaceError)
        assert isinstance(proto_err, Error)
    
    def test_database_error(self):
        """Test DatabaseError hierarchy."""
        err = DatabaseError("Database error")
        assert isinstance(err, Error)
        
        # All database-related errors should be subclasses
        assert isinstance(DataError("data"), DatabaseError)
        assert isinstance(OperationalError("op"), DatabaseError)
        assert isinstance(IntegrityError("integrity"), DatabaseError)
        assert isinstance(InternalError("internal"), DatabaseError)
        assert isinstance(ProgrammingError("prog"), DatabaseError)
        assert isinstance(NotSupportedError("not supported"), DatabaseError)
    
    def test_operational_error(self):
        """Test OperationalError hierarchy."""
        err = OperationalError("Operational error")
        assert isinstance(err, DatabaseError)
        
        # ConnectionError should be subclass of OperationalError
        conn_err = ConnectionError("Connection failed")
        assert isinstance(conn_err, OperationalError)
        assert isinstance(conn_err, DatabaseError)
    
    def test_programming_error(self):
        """Test ProgrammingError hierarchy."""
        err = ProgrammingError("Programming error")
        assert isinstance(err, DatabaseError)
        
        # QueryError should be subclass of ProgrammingError
        query_err = QueryError("Query error")
        assert isinstance(query_err, ProgrammingError)
        assert isinstance(query_err, DatabaseError)


class TestExceptionUsage:
    """Test exception usage patterns."""
    
    def test_raise_and_catch_specific(self):
        """Test raising and catching specific exceptions."""
        with pytest.raises(ConnectionError):
            raise ConnectionError("Connection failed")
        
        with pytest.raises(QueryError):
            raise QueryError("Invalid SQL")
    
    def test_catch_base_class(self):
        """Test catching exceptions by base class."""
        # Catch DatabaseError for any database-related error
        with pytest.raises(DatabaseError):
            raise IntegrityError("Constraint violation")
        
        with pytest.raises(DatabaseError):
            raise QueryError("SQL syntax error")
    
    def test_exception_messages(self):
        """Test that exception messages are preserved."""
        msg = "Detailed error message"
        
        try:
            raise OperationalError(msg)
        except OperationalError as e:
            assert str(e) == msg
    
    def test_reraise_as_different_type(self):
        """Test re-raising as different exception type."""
        try:
            try:
                raise ValueError("Original error")
            except ValueError as e:
                raise DatabaseError(f"Database error: {e}")
        except DatabaseError as e:
            assert "Database error: Original error" in str(e)
