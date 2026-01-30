"""
Exception hierarchy for VeridicalDB Python driver.

Follows PEP 249 (Python Database API Specification v2.0).
"""


class Error(Exception):
    """Base class for all VeridicalDB exceptions."""
    pass


class Warning(Exception):
    """Exception raised for important warnings."""
    pass


class InterfaceError(Error):
    """Exception raised for errors related to the database interface."""
    pass


class DatabaseError(Error):
    """Exception raised for errors related to the database."""
    pass


class DataError(DatabaseError):
    """Exception raised for errors due to problems with processed data."""
    pass


class OperationalError(DatabaseError):
    """Exception raised for errors related to database operation."""
    pass


class IntegrityError(DatabaseError):
    """Exception raised when database integrity is affected."""
    pass


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error."""
    pass


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors (SQL syntax, etc.)."""
    pass


class NotSupportedError(DatabaseError):
    """Exception raised when a feature is not supported."""
    pass


class ConnectionError(OperationalError):
    """Exception raised when connection to database fails."""
    pass


class QueryError(ProgrammingError):
    """Exception raised when query execution fails."""
    pass


class ProtocolError(InterfaceError):
    """Exception raised when wire protocol communication fails."""
    pass
