# Tests for WS Preprocessing

This directory contains tests for the ws_preprocessing service.

## Running Tests

### Install test dependencies

```bash
pip install -r requirements-dev.txt
```

### Run all tests

```bash
# From the ws_preprocessing directory
pytest

# With coverage report
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_db_client.py

# Run specific test
pytest tests/test_db_client.py::TestDatabaseClient::test_read_sql_success

# Run with verbose output
pytest -v

# Run with print statements visible
pytest -s
```

## Test Structure

- `test_db_client.py` - Tests for the DatabaseClient class
  - Tests all CRUD methods (read_sql, write_df, execute_query, fetch_one, fetch_all, execute_scalar)
  - Tests error handling
  - Tests connection lifecycle management

## Test Coverage

The tests use mocking to avoid requiring a real database connection. This allows:
- Fast test execution
- No external dependencies
- Consistent test results
- Easy CI/CD integration

## Adding New Tests

When adding new tests:
1. Create test files with the `test_` prefix
2. Create test classes with the `Test` prefix
3. Create test methods with the `test_` prefix
4. Use fixtures for common setup
5. Use mocks for external dependencies
6. Add docstrings to explain what each test validates

## Example Test

```python
def test_my_feature(db_client):
    """Test description explaining what is being validated"""
    client, mock_engine = db_client
    
    # Setup
    expected_result = "value"
    
    # Execute
    result = client.my_method()
    
    # Assert
    assert result == expected_result
```

