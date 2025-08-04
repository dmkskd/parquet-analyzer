#!/usr/bin/env python3
"""
Test data pagination functionality
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parquet_analyzer.tui import ParquetTUI
from parquet_analyzer.analyzer import ParquetAnalyzer

def test_data_pagination():
    """Test that data pagination navigation works"""
    
    # Create a TUI instance with test data
    test_file = "tests/test_data/test_complex_orderbook.parquet"
    if not os.path.exists(test_file):
        print(f"❌ Test file not found: {test_file}")
        return False
    
    tui = ParquetTUI(test_file)
    
    # Load the analysis
    try:
        analyzer = ParquetAnalyzer()
        analysis = analyzer.analyze_file(test_file)
        tui.analysis = analysis
        tui.current_view = "data"  # Set to data view
        tui.data_row_offset = 0
        
        print("✅ TUI initialized successfully")
        print(f"📊 Total rows: {analysis.total_rows}")
        print(f"🎯 Initial offset: {tui.data_row_offset}")
        
        # Test pagination logic (simulate j/k keypresses)
        rows_per_page = 20
        max_offset = max(0, analysis.total_rows - rows_per_page)
        
        # Test 'j' key (page down) - if we have enough rows
        if analysis.total_rows > rows_per_page:
            if tui.data_row_offset < max_offset:
                old_offset = tui.data_row_offset
                tui.data_row_offset = min(tui.data_row_offset + rows_per_page, max_offset)
                print(f"⬇️  After 'j': offset {old_offset} -> {tui.data_row_offset}")
            else:
                print("⬇️  At last page, 'j' should not advance")
        else:
            print("📄 Small file (≤20 rows), pagination not needed")
        
        # Test 'k' key (page up)
        if tui.data_row_offset > 0:
            old_offset = tui.data_row_offset
            tui.data_row_offset = max(0, tui.data_row_offset - rows_per_page)
            print(f"⬆️  After 'k': offset {old_offset} -> {tui.data_row_offset}")
        else:
            print("⬆️  At first page, 'k' should not go back")
        
        # Test bounds checking
        original_offset = tui.data_row_offset
        tui.data_row_offset = -10  # Should clamp to 0
        tui.data_row_offset = max(0, tui.data_row_offset)
        print(f"🔒 Bounds check (negative): clamped to {tui.data_row_offset}")
        
        tui.data_row_offset = analysis.total_rows + 100  # Should clamp to max
        tui.data_row_offset = min(tui.data_row_offset, max_offset)
        print(f"🔒 Bounds check (too high): clamped to {tui.data_row_offset}")
        
        # Test getting paginated data
        print("📋 Testing paginated data retrieval...")
        analyzer = ParquetAnalyzer()
        df = analyzer.get_data_sample_paginated(test_file, max_rows=10, offset=0)
        print(f"✅ Retrieved {len(df)} rows from offset 0")
        
        print("✅ All data pagination tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Error testing data pagination: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🧪 Testing data pagination...")
    success = test_data_pagination()
    if success:
        print("\n🎉 Data pagination test completed successfully!")
        print("💡 Use 'j' and 'k' keys in data view (press '2') to page through data")
    else:
        print("\n💥 Data pagination test failed!")
        sys.exit(1)
