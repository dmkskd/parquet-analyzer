#!/usr/bin/env python3
"""
Minimal compression navigation test.
1. Run this script
2. Press 'c' to go to compression view
3. Try pressing up/down arrows
4. Press ESC to go back
5. Press ESC again to quit
"""

import sys
sys.path.insert(0, '.')

from parquet_analyzer.analyzer import ParquetAnalyzer
from parquet_analyzer.tui import ParquetTUI

def main():
    # Use the financial data file for testing
    file_path = "examples/demo_files/financial_data.parquet"
    
    print(f"Testing compression navigation with: {file_path}")
    print("Instructions:")
    print("1. Press 'c' to go to compression view")
    print("2. Try pressing up/down arrows to navigate")
    print("3. Press ESC to go back to overview")
    print("4. Press ESC again to quit")
    print()
    
    analyzer = ParquetAnalyzer(file_path)
    analysis = analyzer.analyze()
    
    tui = ParquetTUI(file_path, analysis)
    tui.run()

if __name__ == "__main__":
    main()
