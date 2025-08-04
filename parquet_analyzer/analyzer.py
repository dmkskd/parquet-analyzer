#!/usr/bin/env python3
"""
Core Parquet Analysis Engine
Separated from UI for better testability and reusability
"""

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import json

try:
    import pandas as pd
    import pyarrow.parquet as pq
    import pyarrow as pa
except ImportError as e:
    raise ImportError(f"Missing required packages. Please install with: uv sync") from e


@dataclass
class PageInfo:
    """Information about a single page in a column"""
    page_type: str
    uncompressed_size: int
    compressed_size: int
    num_values: int
    encoding: str
    compression_ratio: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ColumnInfo:
    """Comprehensive information about a column"""
    name: str
    physical_type: str
    logical_type: str
    compression: str
    uncompressed_size: int
    compressed_size: int
    compression_ratio: float
    values: int
    null_count: Optional[int] = None
    distinct_count: Optional[int] = None
    min_value: Any = None
    max_value: Any = None
    encodings: List[str] = None
    num_pages: int = 0
    pages: List[PageInfo] = None
    path_in_schema: str = ""
    repetition_type: str = ""
    converted_type: str = ""

    def __post_init__(self):
        if self.encodings is None:
            self.encodings = []
        if self.pages is None:
            self.pages = []

    def to_dict(self) -> dict:
        """Convert to dictionary, handling special values"""
        result = asdict(self)
        # Handle values that might not be JSON serializable
        if self.min_value is not None:
            result['min_value'] = str(self.min_value)
        if self.max_value is not None:
            result['max_value'] = str(self.max_value)
        return result


@dataclass
class SchemaField:
    """Represents a field in the schema with full type information"""
    name: str
    type_str: str
    logical_type: Optional[str]
    nullable: bool
    repetition: str
    children: List['SchemaField'] = None
    physical_type: Optional[str] = None

    def __post_init__(self):
        if self.children is None:
            self.children = []

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        result = {
            'name': self.name,
            'type_str': self.type_str,
            'logical_type': self.logical_type,
            'nullable': self.nullable,
            'repetition': self.repetition,
            'physical_type': self.physical_type,
            'children': [child.to_dict() for child in self.children]
        }
        return result


@dataclass
class ParquetAnalysis:
    """Complete analysis of a Parquet file"""
    file_path: str
    file_size_bytes: int
    schema_fields: List[SchemaField]
    columns: List[ColumnInfo]
    total_uncompressed: int
    total_compressed: int
    total_rows: int
    num_row_groups: int
    num_logical_columns: int
    num_physical_columns: int
    created_by: Optional[str] = None
    version: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'file_path': self.file_path,
            'file_size_bytes': self.file_size_bytes,
            'schema_fields': [field.to_dict() for field in self.schema_fields],
            'columns': [col.to_dict() for col in self.columns],
            'total_uncompressed': self.total_uncompressed,
            'total_compressed': self.total_compressed,
            'total_rows': self.total_rows,
            'num_row_groups': self.num_row_groups,
            'num_logical_columns': self.num_logical_columns,
            'num_physical_columns': self.num_physical_columns,
            'created_by': self.created_by,
            'version': self.version
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=indent, default=str)


class ParquetAnalyzer:
    """Core analysis engine for Parquet files"""

    def __init__(self):
        self.debug = False

    def analyze_file(self, file_path: str) -> ParquetAnalysis:
        """
        Analyze a Parquet file and return comprehensive information
        
        Args:
            file_path: Path to the Parquet file
            
        Returns:
            ParquetAnalysis object with all extracted information
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file is not a valid Parquet file
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            # Get file size
            file_size = os.path.getsize(file_path)
            
            # Load Parquet file
            table = pq.read_table(file_path)
            parquet_file = pq.ParquetFile(file_path)
            metadata = parquet_file.metadata
            
            # Extract schema information
            schema_fields = self._extract_schema_fields(table.schema)
            
            # Analyze columns
            columns = self._analyze_columns(metadata)
            
            # Calculate totals
            total_uncompressed = sum(col.uncompressed_size for col in columns)
            total_compressed = sum(col.compressed_size for col in columns)
            
            # Extract metadata
            created_by = getattr(metadata, 'created_by', None)
            version = getattr(metadata, 'version', None)
            
            return ParquetAnalysis(
                file_path=file_path,
                file_size_bytes=file_size,
                schema_fields=schema_fields,
                columns=columns,
                total_uncompressed=total_uncompressed,
                total_compressed=total_compressed,
                total_rows=metadata.num_rows,
                num_row_groups=metadata.num_row_groups,
                num_logical_columns=len(schema_fields),
                num_physical_columns=len(columns),
                created_by=str(created_by) if created_by else None,
                version=str(version) if version else None
            )
            
        except Exception as e:
            raise ValueError(f"Error analyzing Parquet file: {e}") from e

    def get_data_sample(self, file_path: str, max_rows: int = 1000) -> pd.DataFrame:
        """Get a sample of the actual data from the Parquet file"""
        try:
            import pyarrow.parquet as pq
            
            # Read a sample of the data
            parquet_file = pq.ParquetFile(file_path)
            
            # If the file is small, read all data
            if parquet_file.metadata.num_rows <= max_rows:
                table = pq.read_table(file_path)
            else:
                # Read only the first max_rows rows
                table = pq.read_table(file_path, use_threads=True)
                table = table.slice(0, max_rows)
            
            # Convert to pandas DataFrame
            df = table.to_pandas()
            
            return df
            
        except Exception as e:
            raise ValueError(f"Error reading data from Parquet file: {e}") from e

    def _extract_schema_fields(self, schema: pa.Schema) -> List[SchemaField]:
        """Extract schema fields with full type information"""
        fields = []
        
        for field in schema:
            schema_field = self._convert_arrow_field(field)
            fields.append(schema_field)
        
        return fields

    def _convert_arrow_field(self, field: pa.Field, depth: int = 0) -> SchemaField:
        """Convert Arrow field to our SchemaField representation"""
        field_type = field.type
        type_str = str(field_type)
        logical_type = None
        physical_type = None
        children = []
        
        # Extract logical type information
        if hasattr(field_type, 'logical_type') and field_type.logical_type:
            logical_type = str(field_type.logical_type)
        
        # Handle different field types
        if pa.types.is_list(field_type):
            value_type = field_type.value_type
            if pa.types.is_struct(value_type):
                for struct_field in value_type:
                    children.append(self._convert_arrow_field(struct_field, depth + 1))
            elif pa.types.is_list(value_type):
                # Nested list
                inner_field = pa.field("element", value_type)
                children.append(self._convert_arrow_field(inner_field, depth + 1))
            else:
                # Simple list element
                physical_type = self._get_physical_type(value_type)
                
        elif pa.types.is_struct(field_type):
            for struct_field in field_type:
                children.append(self._convert_arrow_field(struct_field, depth + 1))
        else:
            physical_type = self._get_physical_type(field_type)
        
        return SchemaField(
            name=field.name,
            type_str=type_str,
            logical_type=logical_type,
            nullable=field.nullable,
            repetition="optional" if field.nullable else "required",
            children=children,
            physical_type=physical_type
        )

    def _get_physical_type(self, arrow_type) -> str:
        """Get the physical type name for an Arrow type"""
        type_mapping = {
            pa.bool_(): "BOOLEAN",
            pa.int32(): "INT32",
            pa.int64(): "INT64",
            pa.float32(): "FLOAT",
            pa.float64(): "DOUBLE",
            pa.string(): "BYTE_ARRAY",
            pa.binary(): "BYTE_ARRAY",
        }
        
        for arrow_t, physical_t in type_mapping.items():
            if arrow_type == arrow_t:
                return physical_t
        
        # Handle specific type categories
        if pa.types.is_timestamp(arrow_type):
            return "INT64"  # Timestamps are stored as INT64 in Parquet
        elif pa.types.is_date(arrow_type):
            return "INT32"  # Dates are typically stored as INT32
        elif pa.types.is_time(arrow_type):
            return "INT64" if arrow_type.bit_width > 32 else "INT32"
        elif pa.types.is_integer(arrow_type):
            return "INT64" if arrow_type.bit_width > 32 else "INT32"
        elif pa.types.is_floating(arrow_type):
            return "DOUBLE" if arrow_type.bit_width > 32 else "FLOAT"
        elif pa.types.is_string(arrow_type) or pa.types.is_binary(arrow_type):
            return "BYTE_ARRAY"
        elif pa.types.is_boolean(arrow_type):
            return "BOOLEAN"
        elif pa.types.is_decimal(arrow_type):
            return "FIXED_LEN_BYTE_ARRAY"
        
        return "UNKNOWN"

    def _analyze_columns(self, metadata) -> List[ColumnInfo]:
        """Analyze all columns in the Parquet file"""
        # Aggregate column statistics across all row groups
        column_stats = {}
        
        for i in range(metadata.num_row_groups):
            rg = metadata.row_group(i)
            for j in range(rg.num_columns):
                col = rg.column(j)
                col_path = col.path_in_schema
                
                # Extract page information
                pages, num_pages = self._extract_page_info(col)
                
                # Extract statistics
                stats = self._extract_column_statistics(col)
                
                # Get type information
                logical_type = self._get_logical_type_name(col)
                
                if col_path not in column_stats:
                    # First time seeing this column
                    column_stats[col_path] = {
                        'name': col_path,
                        'physical_type': col.physical_type,
                        'logical_type': logical_type,
                        'compression': col.compression,
                        'uncompressed_size': col.total_uncompressed_size,
                        'compressed_size': col.total_compressed_size,
                        'values': col.num_values,
                        'null_count': stats.get('null_count', 0) or 0,
                        'distinct_count': stats.get('distinct_count'),
                        'min_value': stats.get('min_value'),
                        'max_value': stats.get('max_value'),
                        'encodings': set(col.encodings),
                        'num_pages': num_pages,
                        'pages': pages,
                        'path_in_schema': col_path,
                        'repetition_type': getattr(col, 'repetition_type', 'UNKNOWN'),
                        'converted_type': getattr(col, 'converted_type', 'UNKNOWN')
                    }
                else:
                    # Aggregate stats across row groups
                    existing = column_stats[col_path]
                    existing['uncompressed_size'] += col.total_uncompressed_size
                    existing['compressed_size'] += col.total_compressed_size
                    existing['values'] += col.num_values
                    existing['null_count'] += stats.get('null_count', 0) or 0
                    existing['num_pages'] += num_pages
                    existing['pages'].extend(pages)
                    existing['encodings'].update(col.encodings)
                    
                    # Update min/max values across row groups
                    if stats.get('min_value') is not None:
                        if existing['min_value'] is None:
                            existing['min_value'] = stats['min_value']
                        else:
                            try:
                                existing['min_value'] = min(existing['min_value'], stats['min_value'])
                            except (TypeError, ValueError):
                                pass  # Skip if values can't be compared
                    
                    if stats.get('max_value') is not None:
                        if existing['max_value'] is None:
                            existing['max_value'] = stats['max_value']
                        else:
                            try:
                                existing['max_value'] = max(existing['max_value'], stats['max_value'])
                            except (TypeError, ValueError):
                                pass  # Skip if values can't be compared
        
        # Convert aggregated stats to ColumnInfo objects
        columns = []
        for col_path, stats in column_stats.items():
            # Calculate final compression ratio
            ratio = stats['compressed_size'] / stats['uncompressed_size'] if stats['uncompressed_size'] > 0 else 0
            
            col_info = ColumnInfo(
                name=stats['name'],
                physical_type=stats['physical_type'],
                logical_type=stats['logical_type'],
                compression=stats['compression'],
                uncompressed_size=stats['uncompressed_size'],
                compressed_size=stats['compressed_size'],
                compression_ratio=ratio,
                values=stats['values'],
                null_count=stats['null_count'],
                distinct_count=stats['distinct_count'],
                min_value=stats['min_value'],
                max_value=stats['max_value'],
                encodings=list(stats['encodings']),
                num_pages=stats['num_pages'],
                pages=stats['pages'],
                path_in_schema=stats['path_in_schema'],
                repetition_type=stats['repetition_type'],
                converted_type=stats['converted_type']
            )
            columns.append(col_info)
        
        return columns

    def _extract_page_info(self, col) -> Tuple[List[PageInfo], int]:
        """Extract page-level information from a column"""
        pages = []
        num_pages = 0
        
        # PyArrow doesn't expose detailed page statistics directly
        # We estimate based on metadata and typical page sizes
        if hasattr(col, 'statistics') and col.statistics:
            # Estimate page count based on data size
            estimated_page_size = 1024 * 1024  # 1MB typical page size
            num_pages = max(1, col.total_uncompressed_size // estimated_page_size)
            
            # Create estimated page info
            avg_uncompressed_per_page = col.total_uncompressed_size // num_pages
            avg_compressed_per_page = col.total_compressed_size // num_pages
            avg_values_per_page = col.num_values // num_pages
            
            for page_idx in range(num_pages):
                page_ratio = avg_compressed_per_page / avg_uncompressed_per_page if avg_uncompressed_per_page > 0 else 0
                page_info = PageInfo(
                    page_type="DATA_PAGE",
                    uncompressed_size=avg_uncompressed_per_page,
                    compressed_size=avg_compressed_per_page,
                    num_values=avg_values_per_page,
                    encoding=col.encodings[0] if col.encodings else "UNKNOWN",
                    compression_ratio=page_ratio
                )
                pages.append(page_info)
        
        return pages, num_pages

    def _extract_column_statistics(self, col) -> Dict[str, Any]:
        """Extract statistics from a column"""
        stats = {}
        
        if col.statistics:
            column_stats = col.statistics
            
            if column_stats.has_null_count:
                stats['null_count'] = column_stats.null_count
            
            if column_stats.has_distinct_count:
                stats['distinct_count'] = column_stats.distinct_count
            
            if column_stats.has_min_max:
                try:
                    min_val = column_stats.min
                    max_val = column_stats.max
                    
                    # Format based on type
                    if col.physical_type == 'BYTE_ARRAY':
                        try:
                            min_val = min_val.decode('utf-8') if min_val else None
                            max_val = max_val.decode('utf-8') if max_val else None
                        except:
                            pass
                    elif col.physical_type in ['INT64'] and 'timestamp' in col.path_in_schema.lower():
                        try:
                            min_val = datetime.fromtimestamp(min_val / 1000).strftime('%Y-%m-%d %H:%M:%S')
                            max_val = datetime.fromtimestamp(max_val / 1000).strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            pass
                    
                    stats['min_value'] = min_val
                    stats['max_value'] = max_val
                    
                except Exception:
                    pass
        
        return stats

    def _get_logical_type_name(self, col) -> str:
        """Get the logical type name for a column"""
        if hasattr(col, 'logical_type') and col.logical_type:
            return str(col.logical_type)
        elif hasattr(col, 'converted_type') and col.converted_type:
            return str(col.converted_type)
        else:
            return col.physical_type

    def compare_with_parquet_tools(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Compare our analysis with parquet-tools output if available
        
        Returns:
            Dictionary with comparison results, or None if parquet-tools not available
        """
        try:
            import subprocess
            import tempfile
            
            # Try to run parquet-tools
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
                temp_file = f.name
            
            try:
                # Run parquet-tools meta command
                result = subprocess.run(
                    ['parquet-tools', 'meta', file_path],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    return {
                        'available': True,
                        'output': result.stdout,
                        'error': result.stderr if result.stderr else None
                    }
                else:
                    return {
                        'available': False,
                        'error': f"parquet-tools failed: {result.stderr}"
                    }
            
            except subprocess.TimeoutExpired:
                return {
                    'available': False,
                    'error': "parquet-tools timeout"
                }
            except FileNotFoundError:
                return {
                    'available': False,
                    'error': "parquet-tools not found"
                }
            finally:
                try:
                    os.unlink(temp_file)
                except:
                    pass
                    
        except ImportError:
            return {
                'available': False,
                'error': "subprocess module not available"
            }


def create_test_parquet_files():
    """Create various test Parquet files with different structures"""
    import tempfile
    
    test_dir = Path(tempfile.mkdtemp(prefix="parquet_test_"))
    
    # Simple flat structure
    simple_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'salary': [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
        'active': [True, True, False, True, False]
    }
    simple_df = pd.DataFrame(simple_data)
    simple_file = test_dir / "simple.parquet"
    simple_df.to_parquet(simple_file, index=False)
    
    # Nested structure
    nested_data = {
        'user_id': [1, 2, 3],
        'profile': [
            {'name': 'Alice', 'age': 25, 'address': {'city': 'NYC', 'zip': '10001'}},
            {'name': 'Bob', 'age': 30, 'address': {'city': 'LA', 'zip': '90210'}},
            {'name': 'Charlie', 'age': 35, 'address': {'city': 'Chicago', 'zip': '60601'}}
        ],
        'orders': [
            [{'id': 1, 'amount': 100.0}, {'id': 2, 'amount': 200.0}],
            [{'id': 3, 'amount': 150.0}],
            []
        ]
    }
    nested_df = pd.DataFrame(nested_data)
    nested_file = test_dir / "nested.parquet"
    nested_df.to_parquet(nested_file, index=False)
    
    # Large file with multiple row groups
    large_data = {
        'id': list(range(100000)),
        'value': [i * 1.5 for i in range(100000)],
        'category': (['A', 'B', 'C'] * 33334)[:100000],  # Ensure exact length
        'timestamp': pd.date_range('2023-01-01', periods=100000, freq='1min')
    }
    large_df = pd.DataFrame(large_data)
    large_file = test_dir / "large.parquet"
    large_df.to_parquet(large_file, index=False, row_group_size=10000)
    
    return {
        'simple': str(simple_file),
        'nested': str(nested_file),
        'large': str(large_file),
        'test_dir': str(test_dir)
    }


if __name__ == "__main__":
    # Demo usage
    analyzer = ParquetAnalyzer()
    
    # Create test files
    test_files = create_test_parquet_files()
    print(f"Created test files in: {test_files['test_dir']}")
    
    # Analyze each test file
    for name, file_path in test_files.items():
        if name == 'test_dir':
            continue
            
        print(f"\n=== Analyzing {name} ===")
        try:
            analysis = analyzer.analyze_file(file_path)
            print(f"Rows: {analysis.total_rows}")
            print(f"Logical columns: {analysis.num_logical_columns}")
            print(f"Physical columns: {analysis.num_physical_columns}")
            print(f"Compression ratio: {analysis.total_compressed/analysis.total_uncompressed:.1%}")
            
            # Show schema structure
            print("Schema fields:")
            for field in analysis.schema_fields:
                print(f"  - {field.name}: {field.type_str}")
                for child in field.children:
                    print(f"    - {child.name}: {child.type_str}")
                    
        except Exception as e:
            print(f"Error: {e}")
