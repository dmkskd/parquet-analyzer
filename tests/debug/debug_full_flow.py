#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, '/Users/filippo/dev/data-analysis/parquet-analyzer')

from parquet_analyzer.tui import ParquetTUI

def test_interactive_flow():
    """Test the exact same flow as the interactive TUI"""
    file_path = "/Users/filippo/dev/data-analysis/parquet-analyzer/examples/test_files_multirow/medium_multi_rowgroup.parquet"
    
    print(f"Testing interactive TUI flow with: {os.path.basename(file_path)}")
    
    try:
        # Create TUI exactly as in the CLI
        tui = ParquetTUI(file_path)
        
        # Simulate the run() method logic
        print("1. Loading parquet file...")
        if not tui.load_parquet_file():
            print("ERROR: Failed to load parquet file")
            return
        print("   ✅ File loaded successfully")
        
        # Test each view individually
        views = ["overview", "data", "schema", "compression", "pages", "optimization"]
        
        for view_name in views:
            print(f"2. Testing {view_name} view...")
            tui.current_view = view_name
            
            try:
                if view_name == "overview":
                    panel = tui.create_overview_panel()
                elif view_name == "data":
                    panel = tui.create_data_panel()
                elif view_name == "schema":
                    panel = tui.create_schema_panel()
                elif view_name == "compression":
                    panel = tui.create_compression_panel()
                elif view_name == "pages":
                    panel = tui.create_pages_panel()
                elif view_name == "optimization":
                    panel = tui.create_optimization_panel()
                
                print(f"   ✅ {view_name} panel created successfully")
                
            except Exception as e:
                print(f"   ❌ ERROR in {view_name} view: {e}")
                import traceback
                traceback.print_exc()
                return
        
        print("\n🎉 All views tested successfully!")
        
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_interactive_flow()
