#!/usr/bin/env python3
"""
Create a test file with complex nested structures similar to order book data
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

def create_complex_orderbook_test():
    """Create a Parquet file with complex nested order book-like structures"""
    
    # Create sample order book data
    data = []
    
    for i in range(5):
        # Create complex nested structure similar to your example
        order_book_sides = [
            {
                'exchange': f'exchange_{i%3}',
                'pair': 'BTC/USD',
                'exchangeTimestamp': 1640995200000 + i*1000,
                'exchangeTimestampNanoseconds': i * 1000000,
                'isBid': i % 2 == 0,
                'timestamp': 1640995200000 + i*1000,
                'receivedTimestamp': 1640995200000 + i*1000 + 100,
                'receivedTimestampNanoseconds': (i+1) * 1000000,
                'metadata': {
                    'firstUpdateId': i * 100,
                    'version': 1,
                    'lastId': i * 100 + 50
                },
                'sequence': i * 10,
                'data': [
                    [100.5 + i, 0.1 + i*0.01],  # [price, quantity] pairs
                    [100.6 + i, 0.2 + i*0.01],
                    [100.7 + i, 0.15 + i*0.01]
                ],
                'maxPrice': 100.7 + i,
                'minPrice': 100.5 + i,
                'maxPriceNumOrders': 10 + i,
                'minPriceNumOrders': 5 + i,
                'maxPriceVolume': 1.5 + i*0.1,
                'minPriceVolume': 0.8 + i*0.1
            }
            for _ in range(2)  # Two sides per order book
        ]
        
        data.append({
            'id': i,
            'symbol': 'BTC/USD',
            'timestamp': pd.Timestamp(2023, 1, 1) + pd.Timedelta(minutes=i),
            'orderBookSides': order_book_sides,
            'marketData': {
                'lastPrice': 100.0 + i,
                'volume24h': 1000000 + i*1000,
                'high24h': 101.0 + i,
                'low24h': 99.0 + i
            }
        })
    
    df = pd.DataFrame(data)
    
    # Write to parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 'test_complex_orderbook.parquet')
    
    print("Created test_complex_orderbook.parquet with complex nested structures")
    print(f"Columns: {list(df.columns)}")
    print(f"Shape: {df.shape}")
    
    # Show structure
    print("\nColumn types:")
    for col in df.columns:
        print(f"  {col}: {df[col].dtype}")
    
    print("\nSample of complex data:")
    sample = df.iloc[0]
    print(f"orderBookSides type: {type(sample['orderBookSides'])}")
    if isinstance(sample['orderBookSides'], list) and len(sample['orderBookSides']) > 0:
        first_side = sample['orderBookSides'][0]
        print(f"First order book side keys: {list(first_side.keys())}")
        print(f"Metadata keys: {list(first_side['metadata'].keys())}")
        print(f"Data structure: {type(first_side['data'])}, length: {len(first_side['data'])}")

if __name__ == "__main__":
    create_complex_orderbook_test()
