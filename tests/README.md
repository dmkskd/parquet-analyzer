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
- `test_arrow_key_fix.py` - Legacy arrow key navigation test (now using j/k keys instead)
- `test_navigation.py` - Test navigation keys (j/k keys for reliable navigation)
- `test_data_pagination.py` - Test data view pagination functionality

### Test Data

- `test_data/` - Directory containing test parquet files and data creation scripts
  - `test_complex_orderbook.parquet` - Complex orderbook data for testing
  - `test_timestamp.parquet` - Timestamp data for testing
  - `large_test_data.parquet` - Large dataset (100 rows) for pagination testing
  - `create_orderbook_test.py` - Script to create test orderbook data
  - `create_multi_rowgroup_test.py` - Script to create multi-rowgroup test data
  - `create_large_test.py` - Script to create large test data for pagination

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

# Test j/k navigation (replaces arrow key navigation)
.venv/bin/python tests/test_navigation.py

# Test data pagination
.venv/bin/python tests/test_data_pagination.py

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

## Data Pagination Feature

The TUI now supports pagination in the data preview view, allowing you to browse through large datasets efficiently.

### How to Use Data Pagination

1. **Load a Parquet file**: `uv run parquet-analyzer your_file.parquet`
2. **Switch to data view**: Press `2`
3. **Navigate through pages**:
   - Press `j` to go to the next page (page down)
   - Press `k` to go to the previous page (page up)
4. **View pagination info**: The title shows current page and row range

### Testing Data Pagination

```bash
# Create a large test file (100 rows)
uv run python tests/test_data/create_large_test.py

# Test the pagination functionality
uv run python tests/test_data_pagination.py

# Interactive test with the large file
uv run parquet-analyzer tests/test_data/large_test_data.parquet
# Press '2' to enter data view, then use 'j'/'k' to page through data
```

### Pagination Details

- **Page size**: 20 rows per page
- **Navigation**: `j` (next page) / `k` (previous page)
- **Status display**: Shows current page number and row range
- **Bounds checking**: Cannot go below page 1 or above the last page
- **Automatic reset**: Returns to page 1 when switching to data view
