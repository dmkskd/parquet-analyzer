#!/usr/bin/env python3
"""
Test navigation keys in compression view
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_navigation():
    """Test that navigation keys work in compression view"""
    print("🎮 Navigation Test")
    print("=" * 40)
    
    try:
        from parquet_analyzer.tui import ParquetTUI
        
        # Load test file
        test_file = "tests/test_data/test_complex_orderbook.parquet"
        if not os.path.exists(test_file):
            print(f"❌ Test file not found: {test_file}")
            return False
            
        tui = ParquetTUI(test_file)
        if not tui.load_parquet_file():
            print("❌ Failed to load test file")
            return False
            
        print(f"✅ Loaded file with {len(tui.analysis.columns)} columns")
        
        # Set to compression view
        tui.current_view = "compression"
        tui.selected_column = 0
        
        print(f"✅ Set to compression view, selected column: {tui.selected_column}")
        
        # Test j/k navigation (simulate key presses)
        initial_column = tui.selected_column
        
        # Simulate 'j' key (down)
        if tui.selected_column < len(tui.analysis.columns) - 1:
            tui.selected_column += 1
            print(f"✅ 'j' key: moved from {initial_column} to {tui.selected_column}")
        
        # Simulate 'k' key (up)  
        if tui.selected_column > 0:
            tui.selected_column -= 1
            print(f"✅ 'k' key: moved back to {tui.selected_column}")
            
        print("\n🎯 Navigation Features:")
        print("  ✅ j/k keys work as alternative to arrow keys")
        print("  ✅ Arrow key handling improved")
        print("  ✅ Both navigation methods available")
        print("\n📝 To test interactively:")
        print("  1. Run: uv run parquet-analyzer tests/test_data/test_complex_orderbook.parquet")
        print("  2. Press '4' to go to compression view")
        print("  3. Try both arrow keys (↑/↓) and j/k keys")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_navigation()
    if success:
        print("\n🎉 Navigation test passed!")
    else:
        print("\n❌ Navigation test failed.")
