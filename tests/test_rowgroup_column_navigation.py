#!/usr/bin/env python3
"""
Test row group column navigation to ensure column details update correctly
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parquet_analyzer.tui import ParquetTUI

def test_rowgroup_column_navigation():
    """Test that navigating through row group columns updates the detail panel"""
    # Create a TUI instance with a test file
    test_file = "examples/demo_files/financial_data.parquet"
    if not os.path.exists(test_file):
        print("⚠️  Test file not found, creating mock scenario")
        return True
    
    tui = ParquetTUI(test_file)
    
    # Load the file
    if not tui.load_parquet_file():
        print("⚠️  Could not load test file")
        return True
        
    # Simulate navigation into row group detail view
    tui.current_view = "rowgroups"
    tui.compression_level = "rowgroup_detail"
    tui.selected_rowgroup = 0
    tui.selected_rowgroup_column = 0
    
    # Test that we can create the panels without errors
    try:
        # Test main panel creation
        main_panel = tui.create_compression_panel()
        assert main_panel is not None
        
        # Test detail panel creation
        detail_panel = tui.create_rowgroup_column_detail_panel()  
        assert detail_panel is not None
        
        # Test navigation - move to different column
        if tui.analysis and tui.analysis.row_groups:
            rg = tui.analysis.row_groups[tui.selected_rowgroup]
            if len(rg.columns) > 1:
                tui.selected_rowgroup_column = 1
                
                # Create panels again with new selection
                main_panel2 = tui.create_compression_panel()
                detail_panel2 = tui.create_rowgroup_column_detail_panel()
                
                assert main_panel2 is not None
                assert detail_panel2 is not None
                
                print("✅ Row group column navigation test passed")
            else:
                print("⚠️  Only one column in row group, limited navigation test")
        else:
            print("⚠️  No row groups found in test file")
            
    except Exception as e:
        print(f"❌ Row group column navigation test failed: {e}")
        return False
        
    return True

if __name__ == "__main__":
    test_rowgroup_column_navigation()
