# Parquet Analyzer

Interactive TUI for analyzing Parquet file metadata, compression, and optimization.

![PyPI - Python Version](https://img.shields.io/pypi/pyversions/parquet-analyzer)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

## Features

- **Interactive Analysis**: Rich TUI for exploring Parquet file structure
- **Compression Analysis**: Column-by-column compression ratios and recommendations
- **Schema Exploration**: Recursive schema tree with logical and physical types
- **Page-Level Insights**: Page-level statistics and optimization tips
- **Optimization Recommendations**: Actionable suggestions for improving compression
- **Data Preview**: Smart preview of actual data with nested structure support
- **File Browser**: Built-in file browser for easy navigation

## Quick Start

### Using uv (Recommended)

```bash
# Clone the repository
git clone https://github.com/yourusername/parquet-analyzer.git
cd parquet-analyzer

# Install dependencies and run
uv run parquet-analyzer [file.parquet]

# Or install in development mode
uv sync
uv run parquet-analyzer
```

### Using pip

```bash
# Install from PyPI (when published)
pip install parquet-analyzer

# Or install from source
pip install -e .

# Run the analyzer
parquet-analyzer [file.parquet]
```

## Usage

### Interactive TUI Mode

```bash
# Start with file browser
uv run parquet-analyzer

# Analyze specific file
uv run parquet-analyzer your-file.parquet
```

**Navigation:**
- `1-6`: Switch between views (Overview, Data, Schema, Compression, Pages, Optimization)
- `f`: File browser to load different files
- `ESC`: Go back to overview (or exit if already in overview)
- `↑/↓`: Navigate columns in compression view
- `h`: Toggle help
- `q`: Quit

### Command Line Analysis

```bash
# Quick analysis without TUI
uv run parquet-analyzer --analyze-only your-file.parquet
```

## Views

### 1. Overview

File summary with basic statistics, size info, and compression ratios.

### 2. Data Preview  

Smart preview of actual data with:

- Dynamic column sizing
- Nested structure visualization
- Complex data type support (arrays, dictionaries)
- Order book and financial data support

### 3. Schema

Interactive tree view of the Parquet schema with logical and physical types, supporting deeply nested structures.

### 4. Compression

Column-by-column analysis showing:

- Compression algorithms used
- Uncompressed vs compressed sizes
- Compression ratios
- Space savings

### 5. Pages

Page-level analysis with:

- Page count and size distribution
- Fragmentation insights
- Page efficiency metrics

### 6. Optimization

Actionable recommendations including:

- Algorithm changes (ZSTD, LZ4)
- Data type optimizations
- Structural improvements
- Estimated savings

## Development

### Setup Development Environment

```bash
# Clone and setup
git clone https://github.com/yourusername/parquet-analyzer.git
cd parquet-analyzer

# Install with development dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Format code
uv run black parquet_analyzer tests
uv run isort parquet_analyzer tests

# Type checking
uv run mypy parquet_analyzer
```

### Project Structure

```text
parquet-analyzer/
├── parquet_analyzer/          # Main package
│   ├── __init__.py
│   ├── analyzer.py           # Core analysis engine
│   ├── cli.py               # Command-line interface
│   └── tui.py               # Terminal user interface
├── tests/                   # Test suite
│   ├── test_parquet_analyzer.py
│   ├── test_data/          # Test data generators
│   └── ...
├── examples/               # Example files and demos
│   ├── demo_files/
│   └── ...
├── pyproject.toml         # Project configuration
└── README.md
```

See [TESTING.md](TESTING.md) for detailed testing information.

## Requirements

- Python 3.8+
- pandas >= 1.5.0
- pyarrow >= 10.0.0
- rich >= 12.0.0
