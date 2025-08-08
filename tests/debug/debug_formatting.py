#!/usr/bin/env python3
"""
Debug the data panel formatting logic
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import parquet_analyzer
import pandas as pd

def debug_formatting():
    analyzer = parquet_analyzer.ParquetAnalyzer()
    df = analyzer.get_data_sample('tests/test_data/test_complex_orderbook.parquet', 1)
    
    orderbook_val = df['orderBookSides'].iloc[0]
    print(f"Testing value: {type(orderbook_val)}")
    
    # Test all conditions in order
    print("\n=== Testing conditions ===")
    
    # 1. pandas NA values
    try:
        is_na = pd.isna(orderbook_val)
        # Handle array case
        if hasattr(is_na, '__len__') and len(is_na) > 1:
            is_na = is_na.any()
        print(f"1. pd.isna(value): {is_na}")
    except Exception as e:
        print(f"1. pd.isna(value): Exception - {e}")
        is_na = False
    
    # 2. numeric types
    is_numeric = isinstance(orderbook_val, (int, float, complex)) and not pd.isna(orderbook_val)
    print(f"2. isinstance(value, (int, float, complex)): {is_numeric}")
    
    # 3. string types
    is_string = isinstance(orderbook_val, str)
    print(f"3. isinstance(value, str): {is_string}")
    
    # 4. list/dict types
    is_list_dict = isinstance(orderbook_val, (list, dict))
    print(f"4. isinstance(value, (list, dict)): {is_list_dict}")
    
    # 5. numpy array check
    has_array = hasattr(orderbook_val, '__array__')
    has_shape = hasattr(orderbook_val, 'shape')
    shape_len = len(getattr(orderbook_val, 'shape', [])) if has_shape else 0
    is_array = has_array and has_shape and shape_len > 0
    print(f"5. array condition: __array__={has_array}, shape={has_shape}, shape_len={shape_len}, result={is_array}")
    
    # 6. numpy scalar check
    is_numpy_str = str(type(orderbook_val)).startswith("<class 'numpy.")
    has_shape_attr = hasattr(orderbook_val, 'shape')
    is_numpy_scalar = is_numpy_str and not has_shape_attr
    print(f"6. numpy scalar: numpy_str={is_numpy_str}, has_shape={has_shape_attr}, result={is_numpy_scalar}")
    
    print(f"\n=== Which condition should trigger? ===")
    if not is_na and not is_numeric and not is_string and not is_list_dict and is_array:
        print("✅ Array condition should trigger!")
        
        # Test array logic
        shape = orderbook_val.shape
        if len(shape) == 1:
            print(f"✅ 1D array, length: {len(orderbook_val)}")
            if len(orderbook_val) <= 3 and len(orderbook_val) > 0:
                print("✅ Small array with items")
                first_item = orderbook_val[0]
                print(f"✅ First item type: {type(first_item)}")
                if isinstance(first_item, dict):
                    print("✅ First item is dict!")
                    keys = list(first_item.keys())[:2]
                    result = f"[data,exchange,...][2]"  # Simplified
                    print(f"✅ Should show: {result}")
    else:
        print("❌ Array condition won't trigger")
        print("Reasons:")
        if is_na: print("  - Value is NA")
        if is_numeric: print("  - Value is numeric") 
        if is_string: print("  - Value is string")
        if is_list_dict: print("  - Value is list/dict")
        if not is_array: print("  - Value is not array")

if __name__ == "__main__":
    debug_formatting()
