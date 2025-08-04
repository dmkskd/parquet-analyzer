#!/usr/bin/env python3
"""
Create a test file with many rows to test data pagination
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime, timedelta

def create_large_test_file():
    """Create a test parquet file with many rows for pagination testing"""
    
    # Generate data with 100 rows
    num_rows = 100
    
    # Create synthetic time series data
    base_time = datetime(2023, 1, 1)
    timestamps = [base_time + timedelta(minutes=i) for i in range(num_rows)]
    
    data = {
        'timestamp': timestamps,
        'price': np.random.uniform(50000, 60000, num_rows),
        'volume': np.random.uniform(0.1, 10.0, num_rows),
        'side': np.random.choice(['buy', 'sell'], num_rows),
        'exchange': np.random.choice(['binance', 'coinbase', 'kraken'], num_rows),
        'symbol': ['BTC/USD'] * num_rows,
        'trade_id': range(1000, 1000 + num_rows),
        'fee': np.random.uniform(0.001, 0.01, num_rows),
        'order_type': np.random.choice(['market', 'limit'], num_rows),
        'user_id': np.random.randint(1, 1000, num_rows)
    }
    
    df = pd.DataFrame(data)
    
    # Save to parquet
    output_file = "tests/test_data/large_test_data.parquet"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file, compression='snappy')
    
    print(f"✅ Created test file: {output_file}")
    print(f"📊 Rows: {len(df)}")
    print(f"📋 Columns: {len(df.columns)}")
    print(f"🗂️  File size: {os.path.getsize(output_file)} bytes")
    
    return output_file

if __name__ == "__main__":
    print("🔧 Creating large test file for pagination...")
    output_file = create_large_test_file()
    print(f"\n🎯 Test with: uv run parquet-analyzer {output_file}")
    print("💡 Press '2' to go to data view, then use 'j/k' to page through data")
