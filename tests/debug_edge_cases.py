#!/usr/bin/env python3

import sys
sys.path.insert(0, '/Users/filippo/dev/data-analysis/parquet-analyzer')

from parquet_analyzer.tui import ParquetTUI
import traceback

def test_edge_cases():
    """Test edge cases that might trigger the multiplication error"""
    file_path = "/Users/filippo/dev/data-analysis/parquet-analyzer/examples/test_files_multirow/medium_multi_rowgroup.parquet"
    
    print("Testing edge cases for multiplication error...")
    
    try:
        tui = ParquetTUI(file_path)
        if not tui.load_parquet_file():
            print("Failed to load file")
            return
        
        # Test with manually corrupted column widths
        print("1. Testing with corrupted column widths...")
        tui.current_view = "data"
        
        # Try to trigger the error by manipulating internal state
        original_create_data_panel = tui.create_data_panel
        
        def test_with_bad_widths():
            try:
                # This will call the original method and see if we can catch any issues
                panel = original_create_data_panel()
                print("   ✅ No error with normal execution")
                return panel
            except Exception as e:
                print(f"   ❌ Found error: {e}")
                print(f"   Error type: {type(e)}")
                if "multiply" in str(e).lower():
                    print("   🎯 This is the multiplication error!")
                    traceback.print_exc()
                raise
        
        test_with_bad_widths()
        
        print("2. Testing with extreme terminal sizes...")
        from rich.console import Console
        
        # Test with terminal size that might cause issues
        for width, height in [(1, 1), (5, 5), (200, 50)]:
            try:
                console = Console(width=width, height=height)
                tui.console = console
                panel = tui.create_data_panel()
                print(f"   ✅ Success with {width}x{height}")
            except Exception as e:
                print(f"   ❌ Error with {width}x{height}: {e}")
                if "multiply" in str(e).lower():
                    print("   🎯 Found the multiplication error!")
                    traceback.print_exc()
        
        print("3. All edge case tests completed!")
        
    except Exception as e:
        print(f"Fatal error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    test_edge_cases()
