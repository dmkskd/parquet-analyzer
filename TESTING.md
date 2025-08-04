# Testing Guide

This document explains how testing is organized and executed in the Parquet Analyzer project.

## Test Structure

```text
tests/
├── __init__.py
├── test_parquet_analyzer.py      # Core analyzer tests
├── run_tests.py                  # Test runner and integration tests
├── test_data/                    # Test data generators
│   ├── create_multi_rowgroup_test.py
│   ├── create_orderbook_test.py
│   └── ...
├── test_*.py                     # Individual test modules
├── debug_*.py                    # Debug and development scripts
└── ...
```

## Running Tests

### Using uv (Recommended)

```bash
# Run all tests with pytest
uv run pytest

# Run with coverage
uv run pytest --cov=parquet_analyzer

# Run specific test file
uv run pytest tests/test_parquet_analyzer.py

# Run with verbose output
uv run pytest -v
```

### Using the Custom Test Runner

```bash
# Run comprehensive test suite
uv run python tests/run_tests.py

# This will:
# 1. Create various test data files
# 2. Run unit tests
# 3. Validate TUI components
# 4. Compare with parquet-tools (if available)
```

## Test Categories

### 1. Unit Tests (`test_parquet_analyzer.py`)

Comprehensive tests for the core analysis engine:

- **Schema Analysis**: Recursive schema parsing, field extraction, type mapping
- **Compression Analysis**: Column-level compression calculations, algorithm detection
- **Statistics Extraction**: Min/max values, null counts, distinct counts
- **Page Analysis**: Page count estimation, size calculations
- **Data Type Handling**: Complex nested structures, arrays, dictionaries
- **Error Handling**: Invalid files, missing data, edge cases

```python
class TestParquetAnalyzer(unittest.TestCase):
    def test_analyze_complex_file(self):
        # Tests analysis of files with nested structures
        
    def test_compression_calculations(self):
        # Tests compression ratio accuracy
        
    def test_schema_recursion(self):
        # Tests deep nested schema parsing
```

### 2. Integration Tests (`run_tests.py`)

End-to-end testing of the complete system:

- **File Creation**: Generate test files with various structures
- **Analysis Validation**: Compare results with known expectations
- **TUI Integration**: Test user interface components
- **Cross-validation**: Compare with parquet-tools when available

### 3. TUI Tests (`test_*_display.py`, `test_*_panel.py`)

User interface testing:

- **Panel Rendering**: Test individual TUI panels
- **Data Display**: Test complex data formatting
- **Navigation**: Test keyboard navigation and file browsing
- **Responsive Layout**: Test terminal size adaptation

### 4. Debug Scripts (`debug_*.py`)

Development and debugging tools:

- **Content Display**: Debug data formatting logic
- **Formatting**: Test specific formatting scenarios
- **Performance**: Profile analysis performance

## Test Data Generation

The project includes scripts to generate test data with various characteristics:

### Complex Order Book Data (`create_orderbook_test.py`)

```python
# Creates test_complex_orderbook.parquet with:
# - Nested arrays of dictionaries
# - Price/quantity data structures
# - Market data with various data types
```

### Multi-Row Group Data (`create_multi_rowgroup_test.py`)

```python
# Creates files with multiple row groups for testing:
# - Page-level analysis
# - Compression across row groups
# - Large file handling
```

## Testing Best Practices

### 1. Test Data Management

- Test data files are created dynamically
- Files are small but representative
- Complex structures mirror real-world scenarios
- Test data is cleaned up automatically

### 2. Assertion Patterns

```python
# Test specific analysis results
def test_compression_analysis(self):
    analysis = self.analyzer.analyze_file('test_file.parquet')
    self.assertGreater(len(analysis.columns), 0)
    self.assertIsInstance(analysis.total_compressed, int)
    self.assertLess(analysis.total_compressed, analysis.total_uncompressed)

# Test error handling
def test_invalid_file(self):
    with self.assertRaises(Exception):
        self.analyzer.analyze_file('nonexistent.parquet')
```

### 3. Cross-Validation

When available, tests compare results with `parquet-tools`:

```python
def test_against_parquet_tools(self):
    # Compare our analysis with parquet-tools output
    # Validates accuracy of metadata extraction
```

## Adding New Tests

### For Core Functionality

1. Add test methods to `test_parquet_analyzer.py`
2. Follow the naming convention: `test_<functionality>`
3. Include both positive and negative test cases
4. Test edge cases and error conditions

### For TUI Components

1. Create focused test scripts in `tests/`
2. Use descriptive names: `test_<component>_<scenario>.py`
3. Test both functionality and display formatting
4. Include terminal size variations

### For New Data Types

1. Add data generation to `tests/test_data/`
2. Create representative test files
3. Add specific tests for the new data type
4. Update integration tests to include the new type

## Continuous Integration

The test suite is designed to work in CI environments:

- No external dependencies beyond Python packages
- Deterministic test data generation
- Clear pass/fail criteria
- Comprehensive coverage reporting

## Performance Testing

Performance tests are included for:

- Large file analysis (simulated)
- Complex nested structure parsing
- Memory usage patterns
- TUI rendering performance

## Debugging Failed Tests

### Common Issues

1. **Import Errors**: Ensure package structure is correct
2. **Missing Test Data**: Run data generation scripts first
3. **Platform Differences**: Terminal behavior varies by OS
4. **Dependencies**: Ensure all required packages are installed

### Debug Commands

```bash
# Run with maximum verbosity
uv run pytest -vvv --tb=long

# Run specific failing test
uv run pytest tests/test_parquet_analyzer.py::TestParquetAnalyzer::test_specific_method -v

# Debug TUI components
uv run python tests/debug_content_display.py
```

## Test Coverage

Target coverage areas:

- **Core Analysis**: >95% coverage of analyzer.py
- **TUI Components**: >80% coverage of interactive elements
- **Error Handling**: 100% coverage of error paths
- **Data Types**: Comprehensive coverage of all supported types

Run coverage reports:

```bash
uv run pytest --cov=parquet_analyzer --cov-report=html
# View: htmlcov/index.html
```
