#!/usr/bin/env python3
"""
Create test Parquet files with multiple row groups for testing
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import numpy as np

def create_multi_rowgroup_files():
    """Create Parquet files with different row group configurations"""
    
    test_dir = Path("test_files_multirow")
    test_dir.mkdir(exist_ok=True)
    
    print(f"Creating test files in: {test_dir.absolute()}")
    
    # 1. Small file with forced multiple row groups (small row group size)
    print("\n1. Creating small file with multiple small row groups...")
    
    small_data = pd.DataFrame({
        'id': range(1000),
        'category': np.random.choice(['A', 'B', 'C', 'D'], 1000),
        'value': np.random.randn(1000) * 100,
        'timestamp': pd.date_range('2023-01-01', periods=1000, freq='1h'),
        'description': [f"Item {i}" for i in range(1000)]
    })
    
    small_file = test_dir / "small_multi_rowgroup.parquet"
    
    # Write with very small row group size to force multiple row groups
    table = pa.Table.from_pandas(small_data)
    with pq.ParquetWriter(small_file, table.schema) as writer:
        # Write in chunks to create multiple row groups
        for i in range(0, len(small_data), 100):
            chunk = small_data.iloc[i:i+100]
            chunk_table = pa.Table.from_pandas(chunk)
            writer.write_table(chunk_table)
    
    # Verify row groups
    parquet_file = pq.ParquetFile(small_file)
    print(f"   ✅ Created {small_file}")
    print(f"   📊 Rows: {parquet_file.metadata.num_rows}")
    print(f"   📦 Row groups: {parquet_file.metadata.num_row_groups}")
    for i in range(parquet_file.metadata.num_row_groups):
        rg = parquet_file.metadata.row_group(i)
        print(f"      Row group {i}: {rg.num_rows} rows, {rg.total_byte_size} bytes")
    
    # 2. Medium file with moderate row groups
    print("\n2. Creating medium file with moderate row groups...")
    
    medium_data = pd.DataFrame({
        'user_id': range(50000),
        'session_id': np.random.randint(1, 10000, 50000),
        'event_type': np.random.choice(['click', 'view', 'purchase', 'search'], 50000),
        'timestamp': pd.date_range('2023-01-01', periods=50000, freq='30s'),
        'value': np.random.exponential(scale=25, size=50000),
        'country': np.random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR'], 50000),
        'device': np.random.choice(['mobile', 'desktop', 'tablet'], 50000)
    })
    
    medium_file = test_dir / "medium_multi_rowgroup.parquet"
    
    # Write with moderate row group size
    table = pa.Table.from_pandas(medium_data)
    with pq.ParquetWriter(medium_file, table.schema) as writer:
        for i in range(0, len(medium_data), 5000):
            chunk = medium_data.iloc[i:i+5000]
            chunk_table = pa.Table.from_pandas(chunk)
            writer.write_table(chunk_table)
    
    # Verify row groups
    parquet_file = pq.ParquetFile(medium_file)
    print(f"   ✅ Created {medium_file}")
    print(f"   📊 Rows: {parquet_file.metadata.num_rows}")
    print(f"   📦 Row groups: {parquet_file.metadata.num_row_groups}")
    for i in range(parquet_file.metadata.num_row_groups):
        rg = parquet_file.metadata.row_group(i)
        print(f"      Row group {i}: {rg.num_rows} rows, {rg.total_byte_size} bytes")
    
    # 3. Large file with many row groups
    print("\n3. Creating large file with many row groups...")
    
    # Generate data in chunks to avoid memory issues
    large_file = test_dir / "large_multi_rowgroup.parquet"
    
    # Create schema first
    schema = pa.schema([
        ('record_id', pa.int64()),
        ('batch_id', pa.int64()),  # Changed from int32 to int64
        ('metric_name', pa.string()),
        ('metric_value', pa.float64()),
        ('created_at', pa.timestamp('ns')),
        ('region', pa.string()),
        ('status', pa.string())
    ])
    
    total_rows = 200000
    chunk_size = 10000
    
    with pq.ParquetWriter(large_file, schema) as writer:
        for batch_id in range(0, total_rows, chunk_size):
            chunk_data = pd.DataFrame({
                'record_id': range(batch_id, min(batch_id + chunk_size, total_rows)),
                'batch_id': [batch_id // chunk_size] * min(chunk_size, total_rows - batch_id),
                'metric_name': np.random.choice(['cpu_usage', 'memory_usage', 'disk_io', 'network_io'], 
                                              min(chunk_size, total_rows - batch_id)),
                'metric_value': np.random.uniform(0, 100, min(chunk_size, total_rows - batch_id)),
                'created_at': pd.date_range('2023-01-01', periods=min(chunk_size, total_rows - batch_id), freq='10s'),
                'region': np.random.choice(['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1'], 
                                         min(chunk_size, total_rows - batch_id)),
                'status': np.random.choice(['active', 'inactive', 'pending'], 
                                         min(chunk_size, total_rows - batch_id))
            })
            
            chunk_table = pa.Table.from_pandas(chunk_data)
            writer.write_table(chunk_table)
            
            print(f"   📝 Written batch {batch_id // chunk_size + 1}/{(total_rows + chunk_size - 1) // chunk_size}")
    
    # Verify row groups
    parquet_file = pq.ParquetFile(large_file)
    print(f"   ✅ Created {large_file}")
    print(f"   📊 Rows: {parquet_file.metadata.num_rows}")
    print(f"   📦 Row groups: {parquet_file.metadata.num_row_groups}")
    print(f"   💾 File size: {large_file.stat().st_size / (1024*1024):.1f} MB")
    
    # Show first few and last few row groups
    num_rg = parquet_file.metadata.num_row_groups
    for i in range(min(3, num_rg)):
        rg = parquet_file.metadata.row_group(i)
        print(f"      Row group {i}: {rg.num_rows} rows, {rg.total_byte_size} bytes")
    
    if num_rg > 6:
        print("      ...")
        for i in range(max(3, num_rg-3), num_rg):
            rg = parquet_file.metadata.row_group(i)
            print(f"      Row group {i}: {rg.num_rows} rows, {rg.total_byte_size} bytes")
    elif num_rg > 3:
        for i in range(3, num_rg):
            rg = parquet_file.metadata.row_group(i)
            print(f"      Row group {i}: {rg.num_rows} rows, {rg.total_byte_size} bytes")
    
    # 4. File with different compression per row group (if possible)
    print("\n4. Creating file with mixed data patterns per row group...")
    
    mixed_file = test_dir / "mixed_pattern_rowgroups.parquet"
    
    schema = pa.schema([
        ('id', pa.int64()),
        ('data_type', pa.string()),
        ('numeric_value', pa.float64()),
        ('text_value', pa.string()),
        ('timestamp', pa.timestamp('ns'))
    ])
    
    with pq.ParquetWriter(mixed_file, schema, compression='snappy') as writer:
        # Row group 1: Highly compressible data (repeated values)
        rg1_data = pd.DataFrame({
            'id': range(2000),
            'data_type': ['repeated'] * 2000,
            'numeric_value': [42.0] * 2000,
            'text_value': ['same_value'] * 2000,
            'timestamp': pd.date_range('2023-01-01', periods=2000, freq='1min')
        })
        writer.write_table(pa.Table.from_pandas(rg1_data))
        
        # Row group 2: Random data (less compressible)
        rg2_data = pd.DataFrame({
            'id': range(2000, 4000),
            'data_type': ['random'] * 2000,
            'numeric_value': np.random.randn(2000) * 1000,
            'text_value': [f"random_text_{np.random.randint(0, 10000)}" for _ in range(2000)],
            'timestamp': pd.date_range('2023-01-01', periods=2000, freq='1min')
        })
        writer.write_table(pa.Table.from_pandas(rg2_data))
        
        # Row group 3: Sequential data (medium compressibility)
        rg3_data = pd.DataFrame({
            'id': range(4000, 6000),
            'data_type': ['sequential'] * 2000,
            'numeric_value': range(4000, 6000),
            'text_value': [f"item_{i}" for i in range(4000, 6000)],
            'timestamp': pd.date_range('2023-01-01', periods=2000, freq='1min')
        })
        writer.write_table(pa.Table.from_pandas(rg3_data))
    
    # Verify row groups and their compression
    parquet_file = pq.ParquetFile(mixed_file)
    print(f"   ✅ Created {mixed_file}")
    print(f"   📊 Rows: {parquet_file.metadata.num_rows}")
    print(f"   📦 Row groups: {parquet_file.metadata.num_row_groups}")
    
    for i in range(parquet_file.metadata.num_row_groups):
        rg = parquet_file.metadata.row_group(i)
        compression_ratio = rg.total_compressed_size / rg.total_byte_size if rg.total_byte_size > 0 else 0
        print(f"      Row group {i}: {rg.num_rows} rows, {rg.total_byte_size} bytes, compression: {compression_ratio:.1%}")
    
    print(f"\n🎉 All test files created in: {test_dir.absolute()}")
    print("\nYou can analyze these with:")
    print(f"   python parquet_tui.py {small_file}")
    print(f"   python parquet_tui.py {medium_file}")  
    print(f"   python parquet_tui.py {large_file}")
    print(f"   python parquet_tui.py {mixed_file}")
    
    return {
        'test_dir': str(test_dir),
        'small': str(small_file),
        'medium': str(medium_file),
        'large': str(large_file),
        'mixed': str(mixed_file)
    }

if __name__ == "__main__":
    create_multi_rowgroup_files()
