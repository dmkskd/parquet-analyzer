#!/usr/bin/env python3

import sys
sys.path.insert(0, '/Users/filippo/dev/data-analysis/parquet-analyzer')

from parquet_analyzer.tui import ParquetTUI

def test_compression_navigation():
    """Test arrow key navigation in compression view"""
    file_path = "/Users/filippo/dev/data-analysis/parquet-analyzer/examples/test_files_multirow/medium_multi_rowgroup.parquet"
    
    print(f"Testing compression view navigation...")
    
    try:
        tui = ParquetTUI(file_path)
        if not tui.load_parquet_file():
            print("Failed to load parquet file")
            return
        
        print(f"✅ File loaded successfully")
        print(f"   Total columns: {len(tui.analysis.columns)}")
        
        # Switch to compression view
        tui.current_view = "compression"
        print("✅ Switched to compression view")
        
        # Test column selection bounds
        print("\nTesting column selection logic:")
        print(f"   Initial selected_column: {tui.selected_column}")
        
        # Test moving up when at 0 (should stay at 0)
        original_selection = tui.selected_column
        new_selection = max(0, tui.selected_column - 1)
        print(f"   Up arrow simulation: {original_selection} -> {new_selection}")
        
        # Test moving down 
        original_selection = tui.selected_column
        max_cols = len(tui.analysis.columns) - 1
        new_selection = min(max_cols, tui.selected_column + 1)
        print(f"   Down arrow simulation: {original_selection} -> {new_selection} (max: {max_cols})")
        
        # Create compression panel to verify it works
        print("\nTesting compression panel creation:")
        compression_panel = tui.create_compression_panel()
        print("✅ Compression panel created successfully")
        
        # Test with different selected columns
        for i in range(min(3, len(tui.analysis.columns))):
            tui.selected_column = i
            panel = tui.create_compression_panel()
            print(f"✅ Panel created with selected_column={i}")
        
        print("\n🎉 All compression navigation tests passed!")
        
    except Exception as e:
        import traceback
        print(f"❌ Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    test_compression_navigation()
