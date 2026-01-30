"""
Pytest configuration and fixtures for VeridicalDB tests.

This module provides shared fixtures and configuration for all tests.
"""

import pytest
from unittest.mock import Mock


@pytest.fixture
def mock_connection():
    """Fixture providing a mock connection."""
    conn = Mock()
    conn.closed = False
    conn.in_transaction = False
    conn.protocol = Mock()
    return conn


@pytest.fixture
def mock_cursor():
    """Fixture providing a mock cursor."""
    cursor = Mock()
    cursor.description = None
    cursor.rowcount = -1
    cursor._rows = []
    cursor._row_index = 0
    return cursor
