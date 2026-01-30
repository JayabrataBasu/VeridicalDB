# Python Driver Test Suite

This directory contains tests for the VeridicalDB Python driver.

## Running Tests

### Run all tests

```bash
pytest
```

### Run specific test file

```bash
pytest tests/test_types.py
```

### Run with coverage

```bash
pytest --cov=veridicaldb --cov-report=html
```

### Run only unit tests (no server required)

```bash
pytest -m unit
```

### Run integration tests (requires server)

```bash
pytest -m integration
```

## Test Categories

- **test_types.py**: Type system tests (encoding, decoding, conversions)
- **test_pool.py**: Connection pool tests
- **test_exceptions.py**: Exception hierarchy tests
- **test_connection.py**: Connection tests (requires server)
- **test_cursor.py**: Cursor tests (requires server)

## Notes

- Unit tests (types, pool, exceptions) use mocking and don't require a running server
- Integration tests require a VeridicalDB server running on localhost:5432
- Use pytest markers to selectively run tests: `@pytest.mark.unit`, `@pytest.mark.integration`
