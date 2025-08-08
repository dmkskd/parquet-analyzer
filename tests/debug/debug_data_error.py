#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parquet_analyzer.tui import ParquetTUI

def test_data_panel():
    file_path = "examples/test_files_multirow/medium_multi_rowgroup.parquet"
    
    print(f"Testing data panel with: {file_path}")
    
    try:
        tui = ParquetTUI(file_path)
        if not tui.load_parquet_file():
            print("Failed to load parquet file")
            return
        
        print("File loaded successfully, creating data panel...")
        data_panel = tui.create_data_panel()
        print("Data panel created successfully!")
        print(data_panel)
        
    except Exception as e:
        import traceback
        print(f"Error: {e}")
        print("Full traceback:")
        traceback.print_exc()

if __name__ == "__main__":
    test_data_panel()
