#!/usr/bin/env python3

import sys
sys.path.insert(0, '/Users/filippo/dev/data-analysis/parquet-analyzer')

from parquet_analyzer.tui import ParquetTUI
from rich.console import Console

def test_with_small_terminal():
    file_path = "/Users/filippo/dev/data-analysis/parquet-analyzer/examples/test_files_multirow/medium_multi_rowgroup.parquet"
    
    print(f"Testing with small terminal size: {file_path}")
    
    try:
        # Create console with very small size to trigger edge cases
        console = Console(width=20, height=10)
        tui = ParquetTUI(file_path)
        tui.console = console  # Override console
        
        if not tui.load_parquet_file():
            print("Failed to load parquet file")
            return
        
        print("Creating data panel with small terminal...")
        tui.current_view = "data"
        data_panel = tui.create_data_panel()
        
        console.print(data_panel)
        print("Small terminal test successful!")
        
    except Exception as e:
        import traceback
        print(f"Error with small terminal: {e}")
        print("Full traceback:")
        traceback.print_exc()

def test_with_various_sizes():
    file_path = "/Users/filippo/dev/data-analysis/parquet-analyzer/examples/test_files_multirow/medium_multi_rowgroup.parquet"
    
    sizes = [(10, 5), (20, 10), (40, 15), (80, 20), (120, 30)]
    
    for width, height in sizes:
        print(f"\nTesting with terminal size: {width}x{height}")
        try:
            console = Console(width=width, height=height)
            tui = ParquetTUI(file_path)
            tui.console = console
            
            if not tui.load_parquet_file():
                print("Failed to load parquet file")
                continue
            
            tui.current_view = "data"
            data_panel = tui.create_data_panel()
            print(f"Success with {width}x{height}")
            
        except Exception as e:
            print(f"Error with {width}x{height}: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    test_with_small_terminal()
    test_with_various_sizes()
