#!/usr/bin/env python3
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
