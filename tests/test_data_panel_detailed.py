#!/usr/bin/env python3
"""
Test the data panel with order book data in more detail
"""

import sys
sys.path.append('.')

from parquet_tui import ParquetTUI

def test_data_panel():
    print("Testing Data Panel with Order Book Data")
    print("=" * 50)
    
    # Create TUI instance
    tui = ParquetTUI('test_complex_orderbook.parquet')
    
    # Load the file
    if not tui.load_parquet_file():
        print("Failed to load file")
        return
    
    # Set to data view
    tui.current_view = "data"
    
    # Create and show the data panel
    panel = tui.create_data_panel()
    
    # Print using console
    tui.console.print(panel)
    
    print("\n" + "=" * 50)
    print("Data panel now shows actual price/quantity content!")
    print("Instead of just '[2]', you can see '[{data:[100...]' with real prices.")

if __name__ == "__main__":
    test_data_panel()
