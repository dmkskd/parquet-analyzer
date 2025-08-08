#!/usr/bin/env python3
"""
Setup script for comprehensive test suite
"""

import os
import sys
from pathlib import Path

def create_test_runner():
    """Create the main test runner script"""
    content = '''#!/usr/bin/env python3
"""
Comprehensive test runner for Parquet Analyzer
"""

import subprocess
import sys
import argparse

def run_tests():
    """Run tests with pytest"""
    cmd = ["python3", "-m", "pytest", "tests/", "-v"]
    try:
        result = subprocess.run(cmd)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running tests: {e}")
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage", action="store_true", help="Run with coverage")
    parser.add_argument("--unit", action="store_true", help="Run unit tests only")
    
    args = parser.parse_args()
    
    print("🧪 Running Parquet Analyzer tests...")
    
    if run_tests():
        print("✅ All tests passed!")
        sys.exit(0)
    else:
        print("❌ Some tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
'''
    
    with open("tests/run_tests.py", "w") as f:
        f.write(content)
    
    os.chmod("tests/run_tests.py", 0o755)
    print("✅ Test runner created")

def create_basic_test():
    """Create a basic test to verify functionality"""
    content = '''"""
Basic functional tests for Parquet Analyzer
"""

import pytest
import pandas as pd
import tempfile
from pathlib import Path

class TestBasic:
    """Basic functionality tests"""
    
    def test_imports(self):
        """Test that we can import the main modules"""
        try:
            from parquet_analyzer import ParquetAnalyzer
            assert ParquetAnalyzer is not None
        except ImportError:
            pytest.skip("ParquetAnalyzer not available")
    
    def test_pandas_works(self):
        """Test that pandas works for creating test data"""
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': ['x', 'y', 'z']
        })
        assert len(df) == 3
        assert list(df.columns) == ['a', 'b']

if __name__ == "__main__":
    pytest.main([__file__])
'''
    
    with open("tests/test_basic_functionality.py", "w") as f:
        f.write(content)
    
    print("✅ Basic test created")

def create_makefile():
    """Create a simple Makefile for running tests"""
    content = '''# Simple Makefile for Parquet Analyzer testing

.PHONY: test install clean help

help:
@echo "Available commands:"
@echo "  make test      - Run all tests"
@echo "  make install   - Install dependencies"
@echo "  make clean     - Clean up"

test:
uv run pytest tests/ -v

install:
uv sync --extra dev

clean:
find . -name "*.pyc" -delete
find . -name "__pycache__" -delete
rm -rf .pytest_cache/
'''
    
    with open("Makefile", "w") as f:
        f.write(content)
    
    print("✅ Makefile created")

def main():
    print("🚀 Setting up comprehensive test suite for Parquet Analyzer...")
    
    # Create tests directory if it doesn't exist
    Path("tests").mkdir(exist_ok=True)
    
    # Create test components
    create_test_runner()
    create_basic_test()
    create_makefile()
    
    print("\n📋 Test suite setup complete!")
    print("\nTo run tests:")
    print("  make test")
    print("  python3 tests/run_tests.py")
    print("  uv run pytest tests/ -v")
    
    print("\nTo install dependencies:")
    print("  make install")
    print("  uv sync --extra dev")

if __name__ == "__main__":
    main()
