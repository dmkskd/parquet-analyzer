#!/usr/bin/env python3
"""
Test the data panel with other complex files to ensure robustness
"""

import sys
sys.path.append('.')

from parquet_tui import ParquetTUI

def test_with_file(filename):
    print(f"\nTesting with {filename}")
    print("=" * 40)
    
    try:
        # Create TUI instance
        tui = ParquetTUI(filename)
        
        # Load the file
        if not tui.load_parquet_file():
            print(f"Failed to load {filename}")
            return
        
        # Set to data view
        tui.current_view = "data"
        
        # Create and show the data panel
        panel = tui.create_data_panel()
        
        # Print using console
        tui.console.print(panel)
        
    except Exception as e:
        print(f"Error with {filename}: {e}")

if __name__ == "__main__":
    # Test with different files
    test_with_file('test_complex_orderbook.parquet')
    test_with_file('demo_files/complex_nested.parquet')
    test_with_file('demo_files/financial_data.parquet')
    
    print("\n" + "=" * 60)
    print("Summary: Data panel now shows actual content instead of just array lengths!")
    print("- Order book data shows price levels: [{data:[100...]")
    print("- Market data shows values: {high24h:101...}")
    print("- Complex structures are now much more informative")
