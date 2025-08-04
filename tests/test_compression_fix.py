#!/usr/bin/env python3

import sys
sys.path.insert(0, '/Users/filippo/dev/data-analysis/parquet-analyzer')

from parquet_analyzer.tui import ParquetTUI

def test_compression_view():
    """Test that compression view doesn't show duplicate columns"""
    file_path = "/Users/filippo/dev/data-analysis/parquet-analyzer/examples/test_files_multirow/medium_multi_rowgroup.parquet"
    
    print(f"Testing compression view with multi-row group file...")
    print(f"File: {file_path}")
    
    try:
        tui = ParquetTUI(file_path)
        if not tui.load_parquet_file():
            print("Failed to load parquet file")
            return
        
        print(f"✅ File loaded successfully")
        print(f"   Logical Columns: {tui.analysis.num_logical_columns}")
        print(f"   Physical Columns: {tui.analysis.num_physical_columns}")
        print(f"   Row Groups: {tui.analysis.num_row_groups}")
        print(f"   Total Columns in analysis: {len(tui.analysis.columns)}")
        
        # Check for duplicates
        column_names = [col.name for col in tui.analysis.columns]
        unique_names = set(column_names)
        
        print(f"   Unique column names: {len(unique_names)}")
        print(f"   Column names: {sorted(unique_names)}")
        
        if len(column_names) == len(unique_names):
            print("✅ No duplicate columns found!")
        else:
            print("❌ Duplicate columns detected!")
            from collections import Counter
            duplicates = Counter(column_names)
            for name, count in duplicates.items():
                if count > 1:
                    print(f"   '{name}' appears {count} times")
        
        # Test compression view creation
        print("\nTesting compression view creation...")
        tui.current_view = "compression"
        compression_panel = tui.create_compression_panel()
        print("✅ Compression panel created successfully")
        
    except Exception as e:
        import traceback
        print(f"❌ Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    test_compression_view()
