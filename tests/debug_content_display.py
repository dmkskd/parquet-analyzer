#!/usr/bin/env python3
"""
Debug what content is actually being found in the order book data
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pyarrow.parquet as pq

# Load the test file
df = pd.read_parquet('tests/test_data/test_complex_orderbook.parquet')

print("Order Book Structure Analysis:")
sample = df.iloc[0]
order_book_sides = sample['orderBookSides']

print(f"Type: {type(order_book_sides)}")
print(f"Length: {len(order_book_sides)}")

for i, side in enumerate(order_book_sides):
    print(f"\nSide {i}:")
    print(f"  Type: {type(side)}")
    print(f"  Keys: {list(side.keys())}")
    
    # Check specific keys we're looking for
    interesting_keys = ['price', 'data', 'lastPrice', 'isBid', 'exchange', 'pair', 'maxPrice', 'minPrice']
    
    print("  Values for interesting keys:")
    for key in interesting_keys:
        if key in side:
            val = side[key]
            print(f"    {key}: {val} ({type(val)})")
    
    # Look at the 'data' field specifically 
    if 'data' in side:
        data = side['data']
        print(f"  Data field: {data}")
        print(f"  Data type: {type(data)}")
        if isinstance(data, list) and len(data) > 0:
            print(f"  First data item: {data[0]} ({type(data[0])})")

print(f"\nMarket Data: {sample['marketData']}")
