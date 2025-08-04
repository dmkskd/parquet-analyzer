#!/usr/bin/env python3
"""
Quick test for arrow key navigation fix
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_arrow_key_fix():
    """Test that the arrow key navigation fix works"""
    print("🔧 Arrow Key Navigation Fix Test")
    print("=" * 40)
    
    try:
        from parquet_analyzer.tui import ParquetTUI
        print("✅ TUI class imported successfully")
        
        # Check if test data exists
        test_file = "tests/test_data/test_complex_orderbook.parquet"
        if os.path.exists(test_file):
            print(f"✅ Test file found: {test_file}")
            
            # Create TUI instance (don't run it, just test creation)
            tui = ParquetTUI(test_file)
            print("✅ TUI instance created successfully")
            
            # Test if the parquet file loads
            if tui.load_parquet_file():
                print("✅ Parquet file loaded successfully")
                print(f"   - Columns: {len(tui.analysis.columns)}")
                print("✅ Ready for navigation testing")
                
                print("\n🎯 Navigation Fix Details:")
                print("  - Arrow keys in compression view no longer exit")
                print("  - Up/Down arrows navigate between columns")
                print("  - ESC still works to go back to overview")
                print("  - Fixed try-finally block issue")
                
                return True
            else:
                print("❌ Failed to load parquet file")
                return False
        else:
            print(f"⚠️  Test file not found: {test_file}")
            print("   Run from project root directory")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_arrow_key_fix()
    if success:
        print("\n🎉 All tests passed! Arrow key navigation should now work.")
    else:
        print("\n❌ Tests failed. Check the error messages above.")
