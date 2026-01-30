"""
Type system for VeridicalDB Python driver.

Provides type mapping between Python objects and VeridicalDB types,
following the PostgreSQL wire protocol OID system.
"""

from datetime import datetime, date, time
from decimal import Decimal
from enum import IntEnum
from typing import Any, Optional, Union


class DataType(IntEnum):
    """VeridicalDB data types (PostgreSQL OIDs)."""
    
    # Primitive types
    NULL = 0
    BOOL = 16
    INT16 = 21
    INT32 = 23
    INT64 = 20
    FLOAT32 = 700
    FLOAT64 = 701
    
    # String types
    TEXT = 25
    VARCHAR = 1043
    CHAR = 18
    
    # Binary types
    BYTEA = 17
    
    # Date/time types
    DATE = 1082
    TIME = 1083
    TIMESTAMP = 1114
    TIMESTAMPTZ = 1184
    
    # Numeric types
    NUMERIC = 1700
    DECIMAL = 1700
    
    # JSON types
    JSON = 114
    JSONB = 3802
    
    # Array type
    ARRAY = 2277


class Int32:
    """32-bit signed integer."""
    __slots__ = ('value',)
    
    def __init__(self, value: int):
        if not (-2**31 <= value < 2**31):
            raise ValueError(f"Value {value} out of range for Int32")
        self.value = value
    
    def __repr__(self):
        return f"Int32({self.value})"
    
    def __int__(self):
        return self.value


class Int64:
    """64-bit signed integer."""
    __slots__ = ('value',)
    
    def __init__(self, value: int):
        if not (-2**63 <= value < 2**63):
            raise ValueError(f"Value {value} out of range for Int64")
        self.value = value
    
    def __repr__(self):
        return f"Int64({self.value})"
    
    def __int__(self):
        return self.value


class Float64:
    """64-bit floating point."""
    __slots__ = ('value',)
    
    def __init__(self, value: float):
        self.value = float(value)
    
    def __repr__(self):
        return f"Float64({self.value})"
    
    def __float__(self):
        return self.value


class Text:
    """UTF-8 text string."""
    __slots__ = ('value',)
    
    def __init__(self, value: str):
        self.value = str(value)
    
    def __repr__(self):
        return f"Text({self.value!r})"
    
    def __str__(self):
        return self.value


class Boolean:
    """Boolean value."""
    __slots__ = ('value',)
    
    def __init__(self, value: bool):
        self.value = bool(value)
    
    def __repr__(self):
        return f"Boolean({self.value})"
    
    def __bool__(self):
        return self.value


class Timestamp:
    """Timestamp without timezone."""
    __slots__ = ('value',)
    
    def __init__(self, value: Union[datetime, str]):
        if isinstance(value, str):
            self.value = datetime.fromisoformat(value)
        elif isinstance(value, datetime):
            self.value = value
        else:
            raise TypeError(f"Expected datetime or str, got {type(value)}")
    
    def __repr__(self):
        return f"Timestamp({self.value.isoformat()})"
    
    def isoformat(self):
        return self.value.isoformat()


class Null:
    """NULL value."""
    
    def __repr__(self):
        return "NULL"
    
    def __bool__(self):
        return False


# Type mapping: Python type -> VeridicalDB DataType
PYTHON_TO_VERIDICAL = {
    type(None): DataType.NULL,
    bool: DataType.BOOL,
    int: DataType.INT64,  # Python int is unbounded, use INT64 by default
    float: DataType.FLOAT64,
    str: DataType.TEXT,
    bytes: DataType.BYTEA,
    datetime: DataType.TIMESTAMP,
    date: DataType.DATE,
    time: DataType.TIME,
    Decimal: DataType.NUMERIC,
    
    # Explicit typed values
    Int32: DataType.INT32,
    Int64: DataType.INT64,
    Float64: DataType.FLOAT64,
    Text: DataType.TEXT,
    Boolean: DataType.BOOL,
    Timestamp: DataType.TIMESTAMP,
    Null: DataType.NULL,
}


def python_to_veridical_type(value: Any) -> DataType:
    """
    Convert a Python value to its corresponding VeridicalDB type.
    
    Args:
        value: Python value to convert
        
    Returns:
        VeridicalDB DataType
        
    Raises:
        TypeError: If value type is not supported
    """
    if value is None:
        return DataType.NULL
    
    value_type = type(value)
    veridical_type = PYTHON_TO_VERIDICAL.get(value_type)
    
    if veridical_type is None:
        raise TypeError(f"Unsupported Python type: {value_type}")
    
    return veridical_type


def encode_value(value: Any, data_type: Optional[DataType] = None) -> bytes:
    """
    Encode a Python value to bytes for wire protocol.
    
    Args:
        value: Python value to encode
        data_type: Optional explicit data type
        
    Returns:
        Encoded bytes
    """
    if value is None or isinstance(value, Null):
        return b''
    
    if data_type is None:
        data_type = python_to_veridical_type(value)
    
    # Handle wrapped types
    if isinstance(value, (Int32, Int64, Float64, Text, Boolean, Timestamp)):
        value = value.value
    
    # Encode based on type
    if data_type == DataType.NULL:
        return b''
    elif data_type == DataType.BOOL:
        return b'\x01' if value else b'\x00'
    elif data_type == DataType.INT32:
        return int(value).to_bytes(4, byteorder='big', signed=True)
    elif data_type == DataType.INT64:
        return int(value).to_bytes(8, byteorder='big', signed=True)
    elif data_type == DataType.FLOAT64:
        import struct
        return struct.pack('>d', float(value))
    elif data_type in (DataType.TEXT, DataType.VARCHAR):
        return str(value).encode('utf-8')
    elif data_type == DataType.BYTEA:
        return bytes(value)
    elif data_type == DataType.TIMESTAMP:
        if isinstance(value, datetime):
            return value.isoformat().encode('utf-8')
        return str(value).encode('utf-8')
    else:
        # Fallback: convert to string
        return str(value).encode('utf-8')


def decode_value(data: bytes, data_type: DataType) -> Any:
    """
    Decode bytes from wire protocol to Python value.
    
    Args:
        data: Encoded bytes
        data_type: VeridicalDB data type
        
    Returns:
        Decoded Python value
    """
    if not data or data_type == DataType.NULL:
        return None
    
    if data_type == DataType.BOOL:
        return data[0] != 0
    elif data_type == DataType.INT32:
        return int.from_bytes(data, byteorder='big', signed=True)
    elif data_type == DataType.INT64:
        return int.from_bytes(data, byteorder='big', signed=True)
    elif data_type == DataType.FLOAT64:
        import struct
        return struct.unpack('>d', data)[0]
    elif data_type in (DataType.TEXT, DataType.VARCHAR, DataType.CHAR):
        return data.decode('utf-8')
    elif data_type == DataType.BYTEA:
        return data
    elif data_type == DataType.TIMESTAMP:
        timestamp_str = data.decode('utf-8')
        try:
            return datetime.fromisoformat(timestamp_str)
        except ValueError:
            return timestamp_str
    else:
        # Fallback: decode as text
        try:
            return data.decode('utf-8')
        except UnicodeDecodeError:
            return data
