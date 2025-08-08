#!/usr/bin/env python3

import sys
sys.path.insert(0, '/Users/filippo/dev/data-analysis/parquet-analyzer')

from parquet_analyzer.tui import ParquetTUI
from rich.console import Console

def test_data_view_rendering():
    file_path = "/Users/filippo/dev/data-analysis/parquet-analyzer/examples/test_files_multirow/medium_multi_rowgroup.parquet"
    
    print(f"Testing data view rendering with: {file_path}")
    
    try:
        console = Console()
        tui = ParquetTUI(file_path)
        
        if not tui.load_parquet_file():
            print("Failed to load parquet file")
            return
        
        print("File loaded successfully, switching to data view...")
        tui.current_view = "data"
        
        print("Creating data panel...")
        data_panel = tui.create_data_panel()
        
        print("Rendering data panel...")
        console.print(data_panel)
        
        print("Data view rendering successful!")
        
    except Exception as e:
        import traceback
        print(f"Error: {e}")
        print("Full traceback:")
        traceback.print_exc()

if __name__ == "__main__":
    test_data_view_rendering()
