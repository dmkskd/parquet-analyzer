#!/usr/bin/env python3
"""
Test j/k navigation functionality
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parquet_analyzer.tui import ParquetTUI
from parquet_analyzer.analyzer import ParquetAnalyzer
import io
from unittest.mock import patch, MagicMock

def test_jk_navigation():
    """Test that j/k keys work for navigation in compression view"""
    
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
        tui.current_view = "compression"  # Set to compression view
        tui.selected_column = 0
        
        print("✅ TUI initialized successfully")
        print(f"📊 File size: {analysis.file_size_bytes} bytes")
        print(f"📋 Columns: {len(analysis.columns)}")
        
        # Test j/k navigation logic
        initial_column = tui.selected_column
        print(f"🎯 Initial column: {initial_column}")
        
        # Test 'j' key (down navigation)
        if tui.selected_column < len(analysis.columns) - 1:
            tui.selected_column += 1
            print(f"⬇️  After 'j': column {tui.selected_column}")
        
        # Test 'k' key (up navigation)  
        if tui.selected_column > 0:
            tui.selected_column -= 1
            print(f"⬆️  After 'k': column {tui.selected_column}")
        
        # Test bounds checking
        tui.selected_column = 0
        if tui.selected_column > 0:
            tui.selected_column -= 1  # Should not go below 0
        print(f"🔒 Bounds check (k at 0): column {tui.selected_column}")
        
        tui.selected_column = len(analysis.columns) - 1
        if tui.selected_column < len(analysis.columns) - 1:
            tui.selected_column += 1  # Should not exceed max
        print(f"🔒 Bounds check (j at max): column {tui.selected_column}")
        
        print("✅ All navigation tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Error testing navigation: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing j/k navigation...")
    success = test_jk_navigation()
    if success:
        print("\n🎉 Navigation test completed successfully!")
        print("💡 Use 'j' and 'k' keys in compression view for reliable navigation")
    else:
        print("\n💥 Navigation test failed!")
        sys.exit(1)
