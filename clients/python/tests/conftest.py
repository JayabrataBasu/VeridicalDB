"""
Pytest configuration and fixtures for VeridicalDB tests.

This module provides shared fixtures and configuration for all tests.

Note: This module is only loaded when running tests with pytest. The pytest
import warnings in Pylance are expected since pytest is a dev-only dependency.
To suppress these warnings, install pytest in your development environment:
    pip install pytest pytest-asyncio
"""

import pytest  # noqa: F401 - Used by pytest plugin system
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
