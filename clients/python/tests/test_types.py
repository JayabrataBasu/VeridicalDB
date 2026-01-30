"""
Tests for VeridicalDB type system.
"""

import pytest
from datetime import datetime
from veridicaldb.types import (
    DataType,
    Int32,
    Int64,
    Float64,
    Text,
    Boolean,
    Timestamp,
    Null,
    python_to_veridical_type,
    encode_value,
    decode_value,
)


class TestTypes:
    """Test type wrappers and conversions."""
    
    def test_int32(self):
        """Test Int32 type."""
        val = Int32(42)
        assert val.value == 42
        assert int(val) == 42
        assert repr(val) == "Int32(42)"
        
        # Test bounds
        Int32(-2**31)  # Min value
        Int32(2**31 - 1)  # Max value
        
        with pytest.raises(ValueError):
            Int32(2**31)  # Overflow
    
    def test_int64(self):
        """Test Int64 type."""
        val = Int64(9223372036854775807)
        assert val.value == 9223372036854775807
        assert int(val) == 9223372036854775807
        
        with pytest.raises(ValueError):
            Int64(2**63)  # Overflow
    
    def test_float64(self):
        """Test Float64 type."""
        val = Float64(3.14159)
        assert abs(val.value - 3.14159) < 0.0001
        assert float(val) == val.value
    
    def test_text(self):
        """Test Text type."""
        val = Text("Hello, World!")
        assert val.value == "Hello, World!"
        assert str(val) == "Hello, World!"
    
    def test_boolean(self):
        """Test Boolean type."""
        val_true = Boolean(True)
        val_false = Boolean(False)
        
        assert bool(val_true) is True
        assert bool(val_false) is False
    
    def test_timestamp(self):
        """Test Timestamp type."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        val = Timestamp(dt)
        assert val.value == dt
        
        # Test from ISO string
        val2 = Timestamp("2024-01-15T10:30:00")
        assert val2.value.year == 2024
        assert val2.value.month == 1
    
    def test_null(self):
        """Test Null type."""
        val = Null()
        assert bool(val) is False
        assert repr(val) == "NULL"


class TestTypeMapping:
    """Test type mapping between Python and VeridicalDB."""
    
    def test_python_to_veridical_primitives(self):
        """Test mapping of Python primitive types."""
        assert python_to_veridical_type(None) == DataType.NULL
        assert python_to_veridical_type(True) == DataType.BOOL
        assert python_to_veridical_type(42) == DataType.INT64
        assert python_to_veridical_type(3.14) == DataType.FLOAT64
        assert python_to_veridical_type("text") == DataType.TEXT
        assert python_to_veridical_type(b"bytes") == DataType.BYTEA
    
    def test_python_to_veridical_typed(self):
        """Test mapping of explicit typed values."""
        assert python_to_veridical_type(Int32(42)) == DataType.INT32
        assert python_to_veridical_type(Int64(100)) == DataType.INT64
        assert python_to_veridical_type(Float64(1.5)) == DataType.FLOAT64
        assert python_to_veridical_type(Text("hi")) == DataType.TEXT
        assert python_to_veridical_type(Boolean(True)) == DataType.BOOL
    
    def test_unsupported_type(self):
        """Test error for unsupported types."""
        with pytest.raises(TypeError):
            python_to_veridical_type(object())


class TestEncoding:
    """Test value encoding for wire protocol."""
    
    def test_encode_null(self):
        """Test encoding NULL."""
        assert encode_value(None) == b''
        assert encode_value(Null()) == b''
    
    def test_encode_boolean(self):
        """Test encoding boolean."""
        assert encode_value(True, DataType.BOOL) == b'\x01'
        assert encode_value(False, DataType.BOOL) == b'\x00'
    
    def test_encode_int32(self):
        """Test encoding Int32."""
        data = encode_value(42, DataType.INT32)
        assert len(data) == 4
        assert int.from_bytes(data, byteorder='big', signed=True) == 42
    
    def test_encode_int64(self):
        """Test encoding Int64."""
        data = encode_value(1000000, DataType.INT64)
        assert len(data) == 8
        assert int.from_bytes(data, byteorder='big', signed=True) == 1000000
    
    def test_encode_text(self):
        """Test encoding text."""
        data = encode_value("Hello", DataType.TEXT)
        assert data == b'Hello'


class TestDecoding:
    """Test value decoding from wire protocol."""
    
    def test_decode_null(self):
        """Test decoding NULL."""
        assert decode_value(b'', DataType.NULL) is None
        assert decode_value(None, DataType.NULL) is None
    
    def test_decode_boolean(self):
        """Test decoding boolean."""
        assert decode_value(b'\x01', DataType.BOOL) is True
        assert decode_value(b'\x00', DataType.BOOL) is False
    
    def test_decode_int32(self):
        """Test decoding Int32."""
        data = (42).to_bytes(4, byteorder='big', signed=True)
        assert decode_value(data, DataType.INT32) == 42
    
    def test_decode_int64(self):
        """Test decoding Int64."""
        data = (1000000).to_bytes(8, byteorder='big', signed=True)
        assert decode_value(data, DataType.INT64) == 1000000
    
    def test_decode_text(self):
        """Test decoding text."""
        assert decode_value(b'Hello', DataType.TEXT) == "Hello"
    
    def test_decode_float64(self):
        """Test decoding Float64."""
        import struct
        data = struct.pack('>d', 3.14159)
        result = decode_value(data, DataType.FLOAT64)
        assert abs(result - 3.14159) < 0.0001


class TestRoundTrip:
    """Test encoding/decoding round trips."""
    
    def test_roundtrip_int32(self):
        """Test Int32 round trip."""
        original = 42
        encoded = encode_value(original, DataType.INT32)
        decoded = decode_value(encoded, DataType.INT32)
        assert decoded == original
    
    def test_roundtrip_text(self):
        """Test Text round trip."""
        original = "Hello, VeridicalDB!"
        encoded = encode_value(original, DataType.TEXT)
        decoded = decode_value(encoded, DataType.TEXT)
        assert decoded == original
    
    def test_roundtrip_float64(self):
        """Test Float64 round trip."""
        original = 3.14159265359
        encoded = encode_value(original, DataType.FLOAT64)
        decoded = decode_value(encoded, DataType.FLOAT64)
        assert abs(decoded - original) < 1e-10
