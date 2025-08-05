# NWSL Analytics Test Suite

Comprehensive testing framework for the NWSL Advanced Analytics Intelligence platform.

## Test Organization

### 🔬 Unit Tests (`unit/`)
- **`test_analytics_engine.py`**: Core analytics engine functionality
- **`test_database_context.py`**: Database connectivity and query logic
- **`test_visualization_agents.py`**: Chart generation and AI visualization
- **`test_response_helpers.py`**: Utility functions and response formatting

### 🔗 Integration Tests (`integration/`)
- **`test_mcp_server.py`**: MCP server tool integration
- **`test_database_integration.py`**: End-to-end database workflows
- **`test_analytics_pipeline.py`**: Complete analytics processing pipeline

### 🌐 End-to-End Tests (`e2e/`)
- **`test_server_deployment.py`**: Complete server deployment scenarios
- **`test_production_scenarios.py`**: Production-like load and performance tests

### 🎭 Test Fixtures (`fixtures/`)
- **`sample_data.py`**: Reusable test data (matches, players, teams)
- **`mock_responses.py`**: Mock API responses and external dependencies

### 🛠️ Test Utilities (`utils/`)
- **`test_helpers.py`**: Common testing utilities and validation functions
- **`database_helpers.py`**: Database setup and teardown utilities

## Running Tests

### Install Test Dependencies
```bash
pip install -r requirements/dev.txt
```

### Run All Tests
```bash
pytest
```

### Run Specific Test Categories
```bash
# Unit tests only
pytest tests/unit/

# Integration tests
pytest tests/integration/

# End-to-end tests (requires environment setup)
pytest tests/e2e/ --slow

# Skip slow tests
pytest -m "not slow"
```

### Run with Coverage
```bash
pytest --cov=src --cov-report=html
```

### Run Specific Tests
```bash
# Test specific component
pytest tests/unit/test_analytics_engine.py

# Test specific function
pytest tests/unit/test_analytics_engine.py::TestNWSLAnalyticsEngine::test_calculate_nir_score

# Run with verbose output
pytest -v tests/unit/
```

## Test Configuration

### Pytest Configuration (`pytest.ini`)
```ini
[tool:pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
markers =
    slow: marks tests as slow (deselect with '-m "not slow"')
    integration: marks tests as integration tests
    e2e: marks tests as end-to-end tests
```

### Test Environment Variables
```bash
# Enable end-to-end tests
export RUN_E2E_TESTS=1

# Test database path
export TEST_DATABASE_PATH=/tmp/test_nwsl.db

# Mock external services
export MOCK_EXTERNAL_APIS=true
```

## Test Data Management

### Sample Database
Tests use an isolated SQLite database with sample data:
- 2 sample matches
- 4 sample players with statistics
- Representative team and season data

### Fixtures Usage
```python
def test_analytics_calculation(sample_analytics_context, mock_team_data):
    # Use provided fixtures for consistent testing
    engine = NWSLAnalyticsEngine(test_database)
    result = engine.calculate_metrics(EntityType.TEAM, mock_team_data)
    assert result['nir_score'] > 0
```

## Testing Best Practices

### 1. Isolation
- Each test should be independent
- Use fixtures for shared setup
- Clean up resources after tests

### 2. Mocking
- Mock external dependencies (APIs, file system)
- Use dependency injection for testability
- Mock at the boundary of your system

### 3. Data Validation
- Use test helpers for common validations
- Test both success and error scenarios
- Validate data types and ranges

### 4. Performance
- Mark slow tests with `@pytest.mark.slow`
- Use appropriate test database size
- Monitor test execution time

## Continuous Integration

### GitHub Actions Integration
```yaml
- name: Run Tests
  run: |
    pytest tests/unit/ tests/integration/
    pytest --cov=src --cov-report=xml
```

### Test Coverage Goals
- **Unit Tests**: >90% coverage for core components
- **Integration Tests**: Cover all MCP tool interactions
- **E2E Tests**: Validate deployment scenarios

## Troubleshooting

### Common Issues

#### Database Connection Errors
```bash
# Check test database exists
ls -la /tmp/test_nwsl.db

# Recreate test database
python -c "from tests.conftest import test_database; next(test_database())"
```

#### Import Errors
```bash
# Ensure project root is in Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Install in development mode
pip install -e .
```

#### Slow Test Performance
```bash
# Skip slow tests during development
pytest -m "not slow"

# Run only fast unit tests
pytest tests/unit/ -x --tb=short
```

## Writing New Tests

### Test Structure Template
```python
class TestComponentName:
    """Test description."""
    
    @pytest.fixture
    def setup_component(self):
        """Setup test component."""
        return ComponentName()
    
    def test_basic_functionality(self, setup_component):
        """Test basic functionality."""
        result = setup_component.method()
        assert result is not None
    
    def test_error_handling(self, setup_component):
        """Test error scenarios."""
        with pytest.raises(ValueError):
            setup_component.method_with_invalid_input()
```

### Adding Test Data
1. Add sample data to `fixtures/sample_data.py`
2. Create fixture in `conftest.py`
3. Use fixture in tests for consistency

### Mocking External Dependencies
```python
@patch('src.module.external_service')
def test_with_mock(mock_service):
    mock_service.return_value = expected_response
    # Test logic here
```