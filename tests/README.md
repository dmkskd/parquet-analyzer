# Tests Directory

This directory contains all tests and debugging scripts for the Parquet Analyzer project.

## Test Organization

### Unit Tests

- `test_parquet_analyzer.py` - Core analyzer functionality tests
- `test_data_panel.py` - Data panel component tests
- `test_data_panel_detailed.py` - Detailed data panel tests
- `test_all_data_panels.py` - Comprehensive data panel tests
- `test_complex_display.py` - Complex display logic tests
- `test_compression_fix.py` - Compression handling tests
- `test_compression_minimal.py` - Minimal compression tests
- `test_compression_navigation.py` - Compression navigation tests

### Debug Scripts

- `debug_formatting.py` - Debug data panel formatting logic
- `debug_content_display.py` - Debug content display issues
- `debug_data_error.py` - Debug data error handling
- `debug_data_view.py` - Debug data view components
- `debug_edge_cases.py` - Debug edge case scenarios
- `debug_full_flow.py` - Debug full application flow
- `debug_terminal_sizes.py` - Debug terminal size handling

### Test Data

- `test_data/` - Directory containing test parquet files and data creation scripts
  - `test_complex_orderbook.parquet` - Complex orderbook data for testing
  - `test_timestamp.parquet` - Timestamp data for testing
  - `create_orderbook_test.py` - Script to create test orderbook data
  - `create_multi_rowgroup_test.py` - Script to create multi-rowgroup test data

## Running Tests

### Running All Tests

```bash
# From the project root using pytest (recommended)
.venv/bin/python -m pytest tests/ -v

# Or using the test runner script (basic functionality test)
.venv/bin/python tests/run_tests.py
```

### Running Individual Tests

```bash
# Run a specific test file
.venv/bin/python -m pytest tests/test_parquet_analyzer.py

# Run with verbose output
.venv/bin/python -m pytest tests/test_parquet_analyzer.py -v

# Run a specific test method
.venv/bin/python -m pytest tests/test_parquet_analyzer.py::TestParquetAnalyzer::test_simple_file_analysis
```

### Running Debug Scripts

Debug scripts are standalone Python scripts that can be run directly to investigate specific issues:

#### Debug Formatting Logic

```bash
# From the project root (using virtual environment)
.venv/bin/python tests/debug_formatting.py

# Or if you have the environment activated
python tests/debug_formatting.py
```

This script tests the data panel formatting logic by:

1. Loading test data with complex orderbook structures
2. Testing all formatting conditions (NA values, numeric, string, list/dict, arrays)
3. Analyzing which formatting path should be taken
4. Providing detailed output about type detection and formatting decisions

#### Debug Content Display

```bash
# From the project root
.venv/bin/python tests/debug_content_display.py
```

### Test Requirements

Make sure you have the required dependencies installed:

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -r requirements.txt
```

### Test Data Setup

Some tests require specific test data files. These are typically created by the scripts in `test_data/`:

```bash
# Create test orderbook data
.venv/bin/python tests/test_data/create_orderbook_test.py

# Create multi-rowgroup test data
.venv/bin/python tests/test_data/create_multi_rowgroup_test.py
```

## Writing New Tests

### Test File Naming

- Unit tests: `test_*.py`
- Debug scripts: `debug_*.py`
- Test data creation: `create_*.py` (in `test_data/` directory)

### Test Structure

```python
#!/usr/bin/env python3
"""
Description of what this test does
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import parquet_analyzer

def test_something():
    """Test description"""
    # Test implementation
    pass

if __name__ == "__main__":
    # Allow running the test directly
    test_something()
```

### Debug Script Structure

```python
#!/usr/bin/env python3
"""
Debug description
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import parquet_analyzer

def debug_something():
    """Debug function with detailed output"""
    # Debug implementation with print statements
    pass

if __name__ == "__main__":
    debug_something()
```

## Common Issues

### Import Errors

If you encounter import errors, make sure you're running tests from the project root directory and that the `parquet_analyzer` package is properly installed. All test scripts now include the necessary path setup to find the package.

### Missing Test Data

If tests fail due to missing test data files, run the appropriate creation scripts in the `test_data/` directory.

### Environment Issues

Ensure you're using the correct Python environment. The project uses `uv` for dependency management:

```bash
# Using uv (recommended)
uv run python tests/test_file.py

# Or activate the virtual environment
source .venv/bin/activate
python tests/test_file.py
```
