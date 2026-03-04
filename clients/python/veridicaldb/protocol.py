"""
PostgreSQL wire protocol implementation for VeridicalDB.

Implements the PostgreSQL frontend/backend protocol for communication
with VeridicalDB server over TCP sockets.

Reference: https://www.postgresql.org/docs/current/protocol.html
"""

import struct
import socket
from typing import Dict, List, Tuple, Any, Optional
from .exceptions import ProtocolError


# Message type codes (backend -> frontend)
MSG_AUTHENTICATION = ord('R')
MSG_BACKEND_KEY_DATA = ord('K')
MSG_BIND_COMPLETE = ord('2')
MSG_CLOSE_COMPLETE = ord('3')
MSG_COMMAND_COMPLETE = ord('C')
MSG_DATA_ROW = ord('D')
MSG_EMPTY_QUERY = ord('I')
MSG_ERROR_RESPONSE = ord('E')
MSG_NO_DATA = ord('n')
MSG_NOTICE_RESPONSE = ord('N')
MSG_PARAMETER_DESCRIPTION = ord('t')
MSG_PARAMETER_STATUS = ord('S')
MSG_PARSE_COMPLETE = ord('1')
MSG_READY_FOR_QUERY = ord('Z')
MSG_ROW_DESCRIPTION = ord('T')

# Message type codes (frontend -> backend)
MSG_BIND = ord('B')
MSG_CLOSE = ord('C')
MSG_DESCRIBE = ord('D')
MSG_EXECUTE = ord('E')
MSG_FLUSH = ord('H')
MSG_PARSE = ord('P')
MSG_PASSWORD = ord('p')
MSG_QUERY = ord('Q')
MSG_SYNC = ord('S')
MSG_TERMINATE = ord('X')

# Startup request codes
SSL_REQUEST_CODE = 80877103


class WireProtocol:
    """
    PostgreSQL wire protocol handler for VeridicalDB.
    
    Handles encoding/decoding of messages according to PostgreSQL protocol.
    """
    
    def __init__(self, sock: socket.socket):
        """
        Initialize wire protocol handler.
        
        Args:
            sock: Connected socket to VeridicalDB server
        """
        self.sock = sock
        self.buffer = b''
        self.backend_pid = None
        self.backend_secret = None
        self.parameters = {}  # Server parameters (e.g., server_version)
    
    def send_startup_message(self, user: str, database: str, **kwargs):
        """
        Send startup message to initiate connection.
        
        Args:
            user: Username for authentication
            database: Database name
            **kwargs: Additional connection parameters
        """
        # Protocol version 3.0
        protocol_version = 196608  # 3.0 in binary (0x00030000)
        
        # Build parameters
        params = {
            'user': user,
            'database': database,
        }
        params.update(kwargs)
        
        # Encode parameters as null-terminated strings
        param_bytes = b''
        for key, value in params.items():
            param_bytes += key.encode('utf-8') + b'\x00'
            param_bytes += str(value).encode('utf-8') + b'\x00'
        param_bytes += b'\x00'  # Final null terminator
        
        # Message length (includes itself)
        length = 4 + 4 + len(param_bytes)
        
        # Send startup message (no message type byte for startup)
        message = struct.pack('>I', length) + struct.pack('>I', protocol_version) + param_bytes
        self.sock.sendall(message)

    def send_ssl_request(self):
        """Send SSLRequest startup packet."""
        self.sock.sendall(struct.pack('>II', 8, SSL_REQUEST_CODE))
    
    def send_password_message(self, password: str):
        """
        Send password for authentication.
        
        Args:
            password: User password
        """
        password_bytes = password.encode('utf-8') + b'\x00'
        length = 4 + len(password_bytes)
        
        message = struct.pack('>cI', MSG_PASSWORD.to_bytes(1, 'big'), length) + password_bytes
        self.sock.sendall(message)
    
    def send_query(self, query: str):
        """
        Send a simple query to the server.
        
        Args:
            query: SQL query string
        """
        query_bytes = query.encode('utf-8') + b'\x00'
        length = 4 + len(query_bytes)
        
        message = struct.pack('>cI', MSG_QUERY.to_bytes(1, 'big'), length) + query_bytes
        self.sock.sendall(message)
    
    def send_parse(self, statement_name: str, query: str, param_types: List[int] = None):
        """
        Send Parse message for prepared statement.
        
        Args:
            statement_name: Name for prepared statement
            query: SQL query string
            param_types: List of parameter type OIDs
        """
        if param_types is None:
            param_types = []
        
        name_bytes = statement_name.encode('utf-8') + b'\x00'
        query_bytes = query.encode('utf-8') + b'\x00'
        
        # Parameter type count
        type_data = struct.pack('>H', len(param_types))
        for param_type in param_types:
            type_data += struct.pack('>I', param_type)
        
        data = name_bytes + query_bytes + type_data
        length = 4 + len(data)
        
        message = struct.pack('>cI', MSG_PARSE.to_bytes(1, 'big'), length) + data
        self.sock.sendall(message)
    
    def send_bind(self, portal_name: str, statement_name: str, param_values: List[bytes], param_formats: List[int] = None):
        """
        Send Bind message for prepared statement execution.
        
        Args:
            portal_name: Portal name
            statement_name: Prepared statement name
            param_values: List of parameter values (encoded)
            param_formats: List of format codes (0=text, 1=binary)
        """
        if param_formats is None:
            param_formats = [0] * len(param_values)
        
        portal_bytes = portal_name.encode('utf-8') + b'\x00'
        statement_bytes = statement_name.encode('utf-8') + b'\x00'
        
        # Parameter format codes
        format_data = struct.pack('>H', len(param_formats))
        for fmt in param_formats:
            format_data += struct.pack('>H', fmt)
        
        # Parameter values
        value_data = struct.pack('>H', len(param_values))
        for value in param_values:
            if value is None:
                value_data += struct.pack('>i', -1)  # NULL
            else:
                value_data += struct.pack('>I', len(value)) + value
        
        # Result format codes (all text for now)
        result_format_data = struct.pack('>H', 0)  # 0 means all text
        
        data = portal_bytes + statement_bytes + format_data + value_data + result_format_data
        length = 4 + len(data)
        
        message = struct.pack('>cI', MSG_BIND.to_bytes(1, 'big'), length) + data
        self.sock.sendall(message)
    
    def send_describe(self, target_type: str, target_name: str):
        """
        Send Describe message (for statement or portal).
        
        Args:
            target_type: 'S' for statement, 'P' for portal
            target_name: Name of target
        """
        type_byte = target_type.encode('ascii')
        name_bytes = target_name.encode('utf-8') + b'\x00'
        
        data = type_byte + name_bytes
        length = 4 + len(data)
        
        message = struct.pack('>cI', MSG_DESCRIBE.to_bytes(1, 'big'), length) + data
        self.sock.sendall(message)
    
    def send_execute(self, portal_name: str, max_rows: int = 0):
        """
        Send Execute message for portal.
        
        Args:
            portal_name: Portal name
            max_rows: Maximum rows to return (0 = unlimited)
        """
        portal_bytes = portal_name.encode('utf-8') + b'\x00'
        max_rows_data = struct.pack('>I', max_rows)
        
        data = portal_bytes + max_rows_data
        length = 4 + len(data)
        
        message = struct.pack('>cI', MSG_EXECUTE.to_bytes(1, 'big'), length) + data
        self.sock.sendall(message)
    
    def send_sync(self):
        """Send Sync message."""
        message = struct.pack('>cI', MSG_SYNC.to_bytes(1, 'big'), 4)
        self.sock.sendall(message)
    
    def send_terminate(self):
        """Send Terminate message to close connection."""
        message = struct.pack('>cI', MSG_TERMINATE.to_bytes(1, 'big'), 4)
        self.sock.sendall(message)
    
    def receive_message(self) -> Tuple[int, bytes]:
        """
        Receive a single message from server.
        
        Returns:
            Tuple of (message_type, message_data)
            
        Raises:
            ProtocolError: If protocol error occurs
        """
        # Read message type (1 byte)
        msg_type_byte = self._recv_exact(1)
        if not msg_type_byte:
            raise ProtocolError("Connection closed by server")
        
        msg_type = msg_type_byte[0]
        
        # Read message length (4 bytes, includes itself)
        length_bytes = self._recv_exact(4)
        length = struct.unpack('>I', length_bytes)[0]
        
        # Read message data (length - 4 bytes)
        data_length = length - 4
        if data_length > 0:
            data = self._recv_exact(data_length)
        else:
            data = b''
        
        return msg_type, data
    
    def _recv_exact(self, n: int) -> bytes:
        """
        Receive exactly n bytes from socket.
        
        Args:
            n: Number of bytes to receive
            
        Returns:
            Received bytes
            
        Raises:
            ProtocolError: If connection closed before receiving n bytes
        """
        data = b''
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise ProtocolError(f"Connection closed while expecting {n} bytes, got {len(data)}")
            data += chunk
        return data
    
    def parse_authentication(self, data: bytes) -> int:
        """
        Parse AuthenticationOk/AuthenticationCleartextPassword message.
        
        Args:
            data: Message data
            
        Returns:
            Authentication type (0=OK, 3=cleartext password, etc.)
        """
        auth_type = struct.unpack('>I', data[:4])[0]
        return auth_type
    
    def parse_backend_key_data(self, data: bytes) -> Tuple[int, int]:
        """
        Parse BackendKeyData message.
        
        Args:
            data: Message data
            
        Returns:
            Tuple of (process_id, secret_key)
        """
        pid, secret = struct.unpack('>II', data)
        return pid, secret
    
    def parse_parameter_status(self, data: bytes) -> Tuple[str, str]:
        """
        Parse ParameterStatus message.
        
        Args:
            data: Message data
            
        Returns:
            Tuple of (parameter_name, parameter_value)
        """
        parts = data.split(b'\x00', 2)
        name = parts[0].decode('utf-8')
        value = parts[1].decode('utf-8') if len(parts) > 1 else ''
        return name, value
    
    def parse_error_response(self, data: bytes) -> Dict[str, str]:
        """
        Parse ErrorResponse message.
        
        Args:
            data: Message data
            
        Returns:
            Dictionary of error fields
        """
        fields = {}
        i = 0
        while i < len(data):
            field_type = chr(data[i])
            if field_type == '\x00':
                break
            
            i += 1
            end = data.find(b'\x00', i)
            if end == -1:
                break
            
            value = data[i:end].decode('utf-8', errors='replace')
            fields[field_type] = value
            i = end + 1
        
        return fields
    
    def parse_row_description(self, data: bytes) -> List[Dict[str, Any]]:
        """
        Parse RowDescription message (column metadata).
        
        Args:
            data: Message data
            
        Returns:
            List of column descriptions
        """
        columns = []
        num_fields = struct.unpack('>H', data[:2])[0]
        
        offset = 2
        for _ in range(num_fields):
            # Column name (null-terminated)
            name_end = data.find(b'\x00', offset)
            name = data[offset:name_end].decode('utf-8')
            offset = name_end + 1
            
            # Parse column metadata
            table_oid, col_num, type_oid, type_size, type_mod, format_code = struct.unpack('>IHIhiH', data[offset:offset+18])
            offset += 18
            
            columns.append({
                'name': name,
                'table_oid': table_oid,
                'column_number': col_num,
                'type_oid': type_oid,
                'type_size': type_size,
                'type_modifier': type_mod,
                'format_code': format_code,
            })
        
        return columns
    
    def parse_data_row(self, data: bytes) -> List[Optional[bytes]]:
        """
        Parse DataRow message.
        
        Args:
            data: Message data
            
        Returns:
            List of column values (as bytes, or None for NULL)
        """
        num_columns = struct.unpack('>H', data[:2])[0]
        values = []
        
        offset = 2
        for _ in range(num_columns):
            length = struct.unpack('>i', data[offset:offset+4])[0]
            offset += 4
            
            if length == -1:
                # NULL value
                values.append(None)
            else:
                value = data[offset:offset+length]
                offset += length
                values.append(value)
        
        return values
    
    def parse_command_complete(self, data: bytes) -> str:
        """
        Parse CommandComplete message.
        
        Args:
            data: Message data
            
        Returns:
            Command tag string
        """
        # Remove null terminator
        tag = data.rstrip(b'\x00').decode('utf-8')
        return tag
    
    def parse_ready_for_query(self, data: bytes) -> str:
        """
        Parse ReadyForQuery message.
        
        Args:
            data: Message data
            
        Returns:
            Transaction status ('I'=idle, 'T'=in transaction, 'E'=failed transaction)
        """
        return chr(data[0])
