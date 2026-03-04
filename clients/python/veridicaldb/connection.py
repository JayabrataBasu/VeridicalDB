"""
Connection management for VeridicalDB Python driver.

Implements the Connection class following PEP 249 (Python Database API).
"""

import socket
import ssl
import threading
from typing import Optional, Dict, Any
from .protocol import (
    WireProtocol,
    MSG_AUTHENTICATION,
    MSG_BACKEND_KEY_DATA,
    MSG_PARAMETER_STATUS,
    MSG_READY_FOR_QUERY,
    MSG_ERROR_RESPONSE,
)
from .cursor import Cursor
from .exceptions import (
    ConnectionError as VeridicalConnectionError,
    DatabaseError,
    ProtocolError,
)


class Connection:
    """
    Connection to VeridicalDB server.
    
    Implements PEP 249 Database API Connection interface.
    """
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5432,
        database: str = 'default',
        user: str = 'admin',
        password: str = '',
        connect_timeout: int = 10,
        **kwargs
    ):
        """
        Initialize connection to VeridicalDB.
        
        Args:
            host: Server hostname or IP address
            port: Server port number
            database: Database name
            user: Username
            password: Password
            connect_timeout: Connection timeout in seconds
            **kwargs: Additional connection parameters
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connect_timeout = connect_timeout
        self.sslmode = str(kwargs.pop('sslmode', 'disable')).lower()
        self.ssl_context = kwargs.pop('ssl_context', None)
        self.server_hostname = kwargs.pop('server_hostname', self.host)
        
        self.sock: Optional[socket.socket] = None
        self.protocol: Optional[WireProtocol] = None
        self._closed = True
        self._lock = threading.Lock()
        self._in_transaction = False
        
        # Connect to server
        self._connect(**kwargs)
    
    def _connect(self, **kwargs):
        """
        Establish connection to VeridicalDB server.
        
        Raises:
            VeridicalConnectionError: If connection fails
        """
        try:
            # Create TCP socket
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.connect_timeout)
            self.sock.connect((self.host, self.port))
            
            # Initialize wire protocol and optionally negotiate TLS
            self.protocol = WireProtocol(self.sock)
            self._negotiate_tls_if_requested()
            
            # Send startup message
            self.protocol.send_startup_message(
                user=self.user,
                database=self.database,
                **kwargs
            )
            
            # Handle authentication and initialization
            self._handle_authentication()
            
            self._closed = False
            
        except socket.error as e:
            self._cleanup()
            raise VeridicalConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")
        except Exception as e:
            self._cleanup()
            raise VeridicalConnectionError(f"Connection initialization failed: {e}")

    def _negotiate_tls_if_requested(self):
        """Negotiate PostgreSQL SSLRequest/TLS handshake based on sslmode."""
        if self.sslmode not in ('disable', 'prefer', 'require'):
            raise VeridicalConnectionError(
                f"Invalid sslmode {self.sslmode!r}; expected disable|prefer|require"
            )

        if self.sslmode == 'disable':
            return

        self.protocol.send_ssl_request()
        response = self.sock.recv(1)
        if response == b'S':
            context = self.ssl_context or ssl.create_default_context()
            # For require mode without a custom context, don't require CA validation by default.
            if self.ssl_context is None and self.sslmode == 'require':
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE

            self.sock = context.wrap_socket(self.sock, server_hostname=self.server_hostname)
            self.protocol = WireProtocol(self.sock)
            return

        if response == b'N':
            if self.sslmode == 'require':
                raise VeridicalConnectionError("Server does not support TLS but sslmode=require")
            # sslmode=prefer: continue in plaintext
            return

        raise VeridicalConnectionError("Invalid response to SSLRequest during startup")
    
    def _handle_authentication(self):
        """
        Handle authentication handshake with server.
        
        Raises:
            DatabaseError: If authentication fails
        """
        authenticated = False

        while True:
            msg_type, data = self.protocol.receive_message()
            
            if msg_type == MSG_AUTHENTICATION:
                auth_type = self.protocol.parse_authentication(data)
                
                if auth_type == 0:
                    # AuthenticationOk
                    authenticated = True
                elif auth_type == 3:
                    # AuthenticationCleartextPassword
                    self.protocol.send_password_message(self.password)
                else:
                    raise DatabaseError(f"Unsupported authentication method: {auth_type}")
            
            elif msg_type == MSG_BACKEND_KEY_DATA:
                pid, secret = self.protocol.parse_backend_key_data(data)
                self.protocol.backend_pid = pid
                self.protocol.backend_secret = secret
            
            elif msg_type == MSG_PARAMETER_STATUS:
                name, value = self.protocol.parse_parameter_status(data)
                self.protocol.parameters[name] = value
            
            elif msg_type == MSG_READY_FOR_QUERY:
                # Connection is ready
                if not authenticated:
                    raise DatabaseError("Server became ready before authentication completed")
                status = self.protocol.parse_ready_for_query(data)
                self._in_transaction = (status == 'T')
                break
            
            elif msg_type == MSG_ERROR_RESPONSE:
                error_fields = self.protocol.parse_error_response(data)
                error_msg = error_fields.get('M', 'Unknown authentication error')
                raise DatabaseError(f"Authentication failed: {error_msg}")
    
    def cursor(self) -> Cursor:
        """
        Create a new cursor for executing queries.
        
        Returns:
            New Cursor object
            
        Raises:
            VeridicalConnectionError: If connection is closed
        """
        if self._closed:
            raise VeridicalConnectionError("Connection is closed")
        
        return Cursor(self)
    
    def commit(self):
        """
        Commit the current transaction.
        
        Raises:
            DatabaseError: If commit fails
        """
        if self._closed:
            raise VeridicalConnectionError("Connection is closed")
        
        if not self._in_transaction:
            return  # No transaction to commit
        
        cursor = self.cursor()
        try:
            cursor.execute("COMMIT")
            self._in_transaction = False
        finally:
            cursor.close()
    
    def rollback(self):
        """
        Rollback the current transaction.
        
        Raises:
            DatabaseError: If rollback fails
        """
        if self._closed:
            raise VeridicalConnectionError("Connection is closed")
        
        if not self._in_transaction:
            return  # No transaction to rollback
        
        cursor = self.cursor()
        try:
            cursor.execute("ROLLBACK")
            self._in_transaction = False
        finally:
            cursor.close()
    
    def close(self):
        """Close the connection."""
        if self._closed:
            return
        
        with self._lock:
            if self._closed:
                return
            
            try:
                # Try to rollback any pending transaction
                if self._in_transaction:
                    try:
                        self.rollback()
                    except Exception:
                        pass  # Ignore errors during cleanup
                
                # Send terminate message
                if self.protocol:
                    try:
                        self.protocol.send_terminate()
                    except Exception:
                        pass  # Ignore errors during cleanup
            finally:
                self._cleanup()
    
    def _cleanup(self):
        """Clean up connection resources."""
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
        
        self.protocol = None
        self._closed = True
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type is not None:
            # Exception occurred, rollback
            try:
                self.rollback()
            except Exception:
                pass
        else:
            # No exception, commit
            try:
                self.commit()
            except Exception:
                pass
        
        self.close()
        return False
    
    @property
    def closed(self) -> bool:
        """Check if connection is closed."""
        return self._closed
    
    @property
    def in_transaction(self) -> bool:
        """Check if currently in a transaction."""
        return self._in_transaction
    
    def get_server_version(self) -> Optional[str]:
        """
        Get server version.
        
        Returns:
            Server version string or None
        """
        if self.protocol:
            return self.protocol.parameters.get('server_version')
        return None


def connect(
    host: str = 'localhost',
    port: int = 5432,
    database: str = 'default',
    user: str = 'admin',
    password: str = '',
    **kwargs
) -> Connection:
    """
    Create a connection to VeridicalDB.
    
    Args:
        host: Server hostname or IP address
        port: Server port number
        database: Database name
        user: Username
        password: Password
        **kwargs: Additional connection parameters
        
    Returns:
        Connection object
        
    Example:
        >>> conn = connect(host='localhost', port=5432, database='mydb')
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT * FROM users")
        >>> rows = cursor.fetchall()
        >>> conn.close()
    """
    return Connection(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        **kwargs
    )
