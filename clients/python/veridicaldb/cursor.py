"""
Cursor implementation for VeridicalDB Python driver.

Implements the Cursor class following PEP 249 (Python Database API).
"""

from typing import Optional, List, Tuple, Any, Iterator, TYPE_CHECKING
from .protocol import (
    MSG_ROW_DESCRIPTION,
    MSG_DATA_ROW,
    MSG_COMMAND_COMPLETE,
    MSG_READY_FOR_QUERY,
    MSG_ERROR_RESPONSE,
    MSG_EMPTY_QUERY,
)
from .types import DataType, decode_value
from .exceptions import (
    ProgrammingError,
    DatabaseError,
    OperationalError,
)

if TYPE_CHECKING:
    from .connection import Connection


class Cursor:
    """
    Database cursor for executing queries and fetching results.
    
    Implements PEP 249 Database API Cursor interface.
    """
    
    def __init__(self, connection: 'Connection'):
        """
        Initialize cursor.
        
        Args:
            connection: Parent Connection object
        """
        self.connection = connection
        self._closed = False
        self._description: Optional[List[Tuple]] = None
        self._rowcount = -1
        self._arraysize = 1
        self._rows: List[Tuple[Any, ...]] = []
        self._row_index = 0
        self._last_command: Optional[str] = None
    
    def execute(self, query: str, parameters: Optional[Tuple[Any, ...]] = None):
        """
        Execute a query.
        
        Args:
            query: SQL query string
            parameters: Optional query parameters
            
        Raises:
            ProgrammingError: If cursor is closed or query is invalid
            DatabaseError: If query execution fails
        """
        if self._closed:
            raise ProgrammingError("Cursor is closed")
        
        if self.connection.closed:
            raise OperationalError("Connection is closed")
        
        # Reset state
        self._description = None
        self._rowcount = -1
        self._rows = []
        self._row_index = 0
        self._last_command = None
        
        # Handle parameterized queries
        if parameters:
            query = self._bind_parameters(query, parameters)
        
        try:
            # Send query to server
            self.connection.protocol.send_query(query)
            
            # Process response
            self._process_query_response()
            
        except Exception as e:
            raise DatabaseError(f"Query execution failed: {e}")
    
    def _bind_parameters(self, query: str, parameters: Tuple[Any, ...]) -> str:
        """
        Bind parameters to query (simple placeholder replacement).
        
        Args:
            query: Query with ? placeholders
            parameters: Parameter values
            
        Returns:
            Query with parameters substituted
        """
        # Simple implementation: replace ? with escaped values
        # In production, should use proper parameter binding
        result = query
        for param in parameters:
            if param is None:
                value_str = 'NULL'
            elif isinstance(param, str):
                # Escape single quotes
                value_str = "'" + param.replace("'", "''") + "'"
            elif isinstance(param, bool):
                value_str = 'TRUE' if param else 'FALSE'
            elif isinstance(param, (int, float)):
                value_str = str(param)
            else:
                value_str = "'" + str(param).replace("'", "''") + "'"
            
            result = result.replace('?', value_str, 1)
        
        return result
    
    def _process_query_response(self):
        """
        Process query response from server.
        
        Raises:
            DatabaseError: If server returns error
        """
        columns = None
        rows = []
        
        while True:
            msg_type, data = self.connection.protocol.receive_message()
            
            if msg_type == MSG_ROW_DESCRIPTION:
                # Column metadata
                column_info = self.connection.protocol.parse_row_description(data)
                columns = column_info
                
                # Build description tuple (name, type_code, display_size, internal_size, precision, scale, null_ok)
                self._description = []
                for col in column_info:
                    self._description.append((
                        col['name'],
                        col['type_oid'],
                        None,  # display_size
                        col['type_size'],
                        None,  # precision
                        None,  # scale
                        None,  # null_ok
                    ))
            
            elif msg_type == MSG_DATA_ROW:
                # Data row
                raw_values = self.connection.protocol.parse_data_row(data)
                
                # Decode values based on column types
                if columns:
                    decoded_values = []
                    for i, raw_value in enumerate(raw_values):
                        if raw_value is None:
                            decoded_values.append(None)
                        else:
                            type_oid = columns[i]['type_oid']
                            decoded_value = decode_value(raw_value, DataType(type_oid))
                            decoded_values.append(decoded_value)
                    rows.append(tuple(decoded_values))
                else:
                    # No column info, store as bytes
                    rows.append(tuple(raw_values))
            
            elif msg_type == MSG_COMMAND_COMPLETE:
                # Command completed
                tag = self.connection.protocol.parse_command_complete(data)
                self._last_command = tag
                
                # Parse row count from tag (e.g., "SELECT 10", "INSERT 0 1")
                parts = tag.split()
                if len(parts) >= 2:
                    try:
                        self._rowcount = int(parts[-1])
                    except ValueError:
                        pass
            
            elif msg_type == MSG_READY_FOR_QUERY:
                # Ready for next query
                status = self.connection.protocol.parse_ready_for_query(data)
                self.connection._in_transaction = (status == 'T')
                break
            
            elif msg_type == MSG_ERROR_RESPONSE:
                # Error occurred
                error_fields = self.connection.protocol.parse_error_response(data)
                error_msg = error_fields.get('M', 'Unknown query error')
                severity = error_fields.get('S', 'ERROR')
                
                raise DatabaseError(f"{severity}: {error_msg}")
            
            elif msg_type == MSG_EMPTY_QUERY:
                # Empty query (no-op)
                pass
        
        # Store rows
        self._rows = rows
        if self._rowcount == -1 and rows:
            self._rowcount = len(rows)
    
    def executemany(self, query: str, seq_of_parameters: List[Tuple[Any, ...]]):
        """
        Execute a query multiple times with different parameters.
        
        Args:
            query: SQL query string
            seq_of_parameters: Sequence of parameter tuples
        """
        for parameters in seq_of_parameters:
            self.execute(query, parameters)
    
    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """
        Fetch the next row from query results.
        
        Returns:
            Next row as tuple, or None if no more rows
        """
        if self._row_index < len(self._rows):
            row = self._rows[self._row_index]
            self._row_index += 1
            return row
        return None
    
    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        """
        Fetch multiple rows from query results.
        
        Args:
            size: Number of rows to fetch (default: arraysize)
            
        Returns:
            List of rows
        """
        if size is None:
            size = self._arraysize
        
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        
        return rows
    
    def fetchall(self) -> List[Tuple[Any, ...]]:
        """
        Fetch all remaining rows from query results.
        
        Returns:
            List of all remaining rows
        """
        rows = self._rows[self._row_index:]
        self._row_index = len(self._rows)
        return rows
    
    def close(self):
        """Close the cursor."""
        self._closed = True
        self._rows = []
        self._row_index = 0
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
    
    def __iter__(self) -> Iterator[Tuple[Any, ...]]:
        """Iterate over query results."""
        return iter(self._rows[self._row_index:])
    
    @property
    def description(self) -> Optional[List[Tuple]]:
        """
        Column description for last query.
        
        Returns list of tuples: (name, type_code, display_size, internal_size, precision, scale, null_ok)
        """
        return self._description
    
    @property
    def rowcount(self) -> int:
        """Number of rows affected by last query."""
        return self._rowcount
    
    @property
    def arraysize(self) -> int:
        """Default number of rows to fetch with fetchmany()."""
        return self._arraysize
    
    @arraysize.setter
    def arraysize(self, size: int):
        """Set default fetchmany() size."""
        self._arraysize = size
    
    @property
    def closed(self) -> bool:
        """Check if cursor is closed."""
        return self._closed
    
    @property
    def lastrowid(self) -> Optional[int]:
        """
        Last row ID (not implemented).
        
        Returns None as VeridicalDB doesn't expose this.
        """
        return None
