#!/usr/bin/env python3
"""
Comprehensive unit tests for Parquet Analyzer
Tests the core analysis engine with various file structures
"""

import unittest
import tempfile
import shutil
import json
import os
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from parquet_analyzer import (
    ParquetAnalyzer, 
    ParquetAnalysis, 
    ColumnInfo, 
    SchemaField
)


class TestParquetAnalyzer(unittest.TestCase):
    """Test the core ParquetAnalyzer functionality"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures"""
        cls.analyzer = ParquetAnalyzer()
        cls.test_files = create_test_parquet_files()
        
    @classmethod
    def tearDownClass(cls):
        """Clean up test files"""
        if 'test_dir' in cls.test_files:
            shutil.rmtree(cls.test_files['test_dir'], ignore_errors=True)
    
    def test_simple_file_analysis(self):
        """Test analysis of a simple flat Parquet file"""
        analysis = self.analyzer.analyze_file(self.test_files['simple'])
        
        # Basic structure checks
        self.assertEqual(analysis.total_rows, 5)
        self.assertEqual(analysis.num_logical_columns, 5)
        self.assertEqual(analysis.num_physical_columns, 5)
        self.assertEqual(analysis.num_row_groups, 1)
        
        # Check schema fields
        field_names = [field.name for field in analysis.schema_fields]
        expected_fields = ['id', 'name', 'age', 'salary', 'active']
        self.assertEqual(set(field_names), set(expected_fields))
        
        # Check column information
        self.assertEqual(len(analysis.columns), 5)
        
        # Check specific column details
        name_column = next((col for col in analysis.columns if col.name == 'name'), None)
        self.assertIsNotNone(name_column)
        self.assertEqual(name_column.physical_type, 'BYTE_ARRAY')
        self.assertEqual(name_column.values, 5)
        
        # Check compression ratio is reasonable
        self.assertGreater(analysis.total_compressed, 0)
        self.assertGreater(analysis.total_uncompressed, analysis.total_compressed)
    
    def test_nested_file_analysis(self):
        """Test analysis of a nested Parquet file"""
        analysis = self.analyzer.analyze_file(self.test_files['nested'])
        
        # Basic structure checks
        self.assertEqual(analysis.total_rows, 3)
        self.assertGreaterEqual(analysis.num_physical_columns, 3)  # Nested fields expand
        
        # Check that nested fields are properly extracted
        field_names = [field.name for field in analysis.schema_fields]
        self.assertIn('user_id', field_names)
        self.assertIn('profile', field_names)
        self.assertIn('orders', field_names)
        
        # Check nested structure
        profile_field = next((f for f in analysis.schema_fields if f.name == 'profile'), None)
        self.assertIsNotNone(profile_field)
        self.assertGreater(len(profile_field.children), 0)
        
        # Check that child fields are present
        child_names = [child.name for child in profile_field.children]
        expected_children = ['name', 'age', 'address']
        for expected in expected_children:
            self.assertIn(expected, child_names)
    
    def test_large_file_analysis(self):
        """Test analysis of a large file with multiple row groups"""
        analysis = self.analyzer.analyze_file(self.test_files['large'])
        
        # Basic structure checks
        self.assertEqual(analysis.total_rows, 100000)
        self.assertEqual(analysis.num_logical_columns, 4)
        self.assertGreaterEqual(analysis.num_row_groups, 2)  # Should have multiple row groups
        
        # Check that all expected columns are present
        field_names = [field.name for field in analysis.schema_fields]
        expected_fields = ['id', 'value', 'category', 'timestamp']
        self.assertEqual(set(field_names), set(expected_fields))
        
        # Check compression is working
        compression_ratio = analysis.total_compressed / analysis.total_uncompressed
        self.assertLess(compression_ratio, 1.0)
        self.assertGreater(compression_ratio, 0.1)  # Should have reasonable compression
    
    def test_column_statistics(self):
        """Test extraction of column statistics"""
        analysis = self.analyzer.analyze_file(self.test_files['simple'])
        
        # Find integer column
        id_column = next((col for col in analysis.columns if col.name == 'id'), None)
        self.assertIsNotNone(id_column)
        
        # Check that statistics are extracted when available
        # Note: Statistics availability depends on how the file was written
        if id_column.min_value is not None:
            self.assertIsInstance(id_column.min_value, (int, str))
        if id_column.max_value is not None:
            self.assertIsInstance(id_column.max_value, (int, str))
    
    def test_page_information(self):
        """Test extraction of page-level information"""
        analysis = self.analyzer.analyze_file(self.test_files['large'])
        
        # Check that page information is estimated
        for column in analysis.columns:
            if column.pages:
                self.assertGreater(column.num_pages, 0)
                self.assertEqual(len(column.pages), column.num_pages)
                
                # Check page info structure
                for page in column.pages:
                    self.assertGreater(page.uncompressed_size, 0)
                    self.assertGreaterEqual(page.compressed_size, 0)
                    self.assertGreaterEqual(page.num_values, 0)
                    self.assertIsNotNone(page.encoding)
    
    def test_schema_field_structure(self):
        """Test that schema fields are properly structured"""
        analysis = self.analyzer.analyze_file(self.test_files['nested'])
        
        # Check root level fields
        self.assertGreater(len(analysis.schema_fields), 0)
        
        for field in analysis.schema_fields:
            self.assertIsInstance(field, SchemaField)
            self.assertIsNotNone(field.name)
            self.assertIsNotNone(field.type_str)
            self.assertIsInstance(field.nullable, bool)
            self.assertIsInstance(field.children, list)
            
            # Check nested fields
            for child in field.children:
                self.assertIsInstance(child, SchemaField)
                self.assertIsNotNone(child.name)
                self.assertIsNotNone(child.type_str)
    
    def test_serialization(self):
        """Test that analysis results can be serialized to JSON"""
        analysis = self.analyzer.analyze_file(self.test_files['simple'])
        
        # Test dictionary conversion
        analysis_dict = analysis.to_dict()
        self.assertIsInstance(analysis_dict, dict)
        self.assertIn('file_path', analysis_dict)
        self.assertIn('total_rows', analysis_dict)
        self.assertIn('schema_fields', analysis_dict)
        self.assertIn('columns', analysis_dict)
        
        # Test JSON serialization
        json_str = analysis.to_json()
        self.assertIsInstance(json_str, str)
        
        # Test that JSON can be parsed back
        parsed = json.loads(json_str)
        self.assertEqual(parsed['total_rows'], analysis.total_rows)
        self.assertEqual(parsed['num_logical_columns'], analysis.num_logical_columns)
    
    def test_error_handling(self):
        """Test error handling for invalid files"""
        # Test non-existent file
        with self.assertRaises(FileNotFoundError):
            self.analyzer.analyze_file("/path/that/does/not/exist.parquet")
        
        # Test invalid file (create a text file with .parquet extension)
        with tempfile.NamedTemporaryFile(suffix='.parquet', mode='w', delete=False) as f:
            f.write("This is not a parquet file")
            invalid_file = f.name
        
        try:
            with self.assertRaises(ValueError):
                self.analyzer.analyze_file(invalid_file)
        finally:
            os.unlink(invalid_file)
    
    def test_compression_ratio_calculation(self):
        """Test that compression ratios are calculated correctly"""
        analysis = self.analyzer.analyze_file(self.test_files['simple'])
        
        # Check overall compression ratio
        expected_ratio = analysis.total_compressed / analysis.total_uncompressed
        calculated_ratio = analysis.total_compressed / analysis.total_uncompressed
        self.assertAlmostEqual(expected_ratio, calculated_ratio, places=6)
        
        # Check column-level compression ratios
        for column in analysis.columns:
            if column.uncompressed_size > 0:
                expected_col_ratio = column.compressed_size / column.uncompressed_size
                self.assertAlmostEqual(column.compression_ratio, expected_col_ratio, places=6)
    
    def test_column_encodings(self):
        """Test that column encodings are properly extracted"""
        analysis = self.analyzer.analyze_file(self.test_files['simple'])
        
        for column in analysis.columns:
            self.assertIsInstance(column.encodings, list)
            # Should have at least one encoding
            if column.encodings:
                for encoding in column.encodings:
                    self.assertIsInstance(encoding, str)
                    self.assertGreater(len(encoding), 0)


class TestSchemaComparison(unittest.TestCase):
    """Test schema comparison and validation"""
    
    def setUp(self):
        self.analyzer = ParquetAnalyzer()
        self.test_files = create_test_parquet_files()
    
    def tearDown(self):
        if hasattr(self, 'test_files') and 'test_dir' in self.test_files:
            shutil.rmtree(self.test_files['test_dir'], ignore_errors=True)
    
    def test_schema_consistency_with_pyarrow(self):
        """Test that our schema extraction is consistent with PyArrow's schema"""
        file_path = self.test_files['simple']
        
        # Get our analysis
        analysis = self.analyzer.analyze_file(file_path)
        
        # Get PyArrow's schema directly
        table = pq.read_table(file_path)
        arrow_schema = table.schema
        
        # Compare field counts
        self.assertEqual(len(analysis.schema_fields), len(arrow_schema))
        
        # Compare field names
        our_field_names = [field.name for field in analysis.schema_fields]
        arrow_field_names = [field.name for field in arrow_schema]
        self.assertEqual(set(our_field_names), set(arrow_field_names))
        
        # Compare field types (basic check)
        for our_field in analysis.schema_fields:
            arrow_field = arrow_schema.field(our_field.name)
            self.assertEqual(our_field.name, arrow_field.name)
            self.assertEqual(our_field.nullable, arrow_field.nullable)
    
    def test_column_count_consistency(self):
        """Test that physical column counts match PyArrow metadata"""
        file_path = self.test_files['large']
        
        analysis = self.analyzer.analyze_file(file_path)
        
        # Get PyArrow metadata
        parquet_file = pq.ParquetFile(file_path)
        metadata = parquet_file.metadata
        
        # Count physical columns across all row groups
        total_physical_cols = 0
        for i in range(metadata.num_row_groups):
            rg = metadata.row_group(i)
            total_physical_cols += rg.num_columns
        
        # Our analysis should match
        self.assertEqual(len(analysis.columns), total_physical_cols)
    
    def test_row_count_consistency(self):
        """Test that row counts match across different access methods"""
        for file_key in ['simple', 'nested', 'large']:
            file_path = self.test_files[file_key]
            
            # Our analysis
            analysis = self.analyzer.analyze_file(file_path)
            
            # PyArrow table
            table = pq.read_table(file_path)
            
            # PyArrow metadata
            parquet_file = pq.ParquetFile(file_path)
            metadata = parquet_file.metadata
            
            # All should agree
            self.assertEqual(analysis.total_rows, len(table))
            self.assertEqual(analysis.total_rows, metadata.num_rows)


class TestFileStructureValidation(unittest.TestCase):
    """Test validation of different Parquet file structures"""
    
    def test_create_and_validate_complex_structure(self):
        """Create and validate a complex nested structure"""
        # Create a complex test case
        complex_data = {
            'transaction_id': [1, 2, 3],
            'user': [
                {
                    'id': 101,
                    'profile': {
                        'name': 'Alice',
                        'demographics': {
                            'age': 25,
                            'location': {'city': 'NYC', 'country': 'USA'}
                        }
                    },
                    'preferences': ['sports', 'tech']
                },
                {
                    'id': 102,
                    'profile': {
                        'name': 'Bob',
                        'demographics': {
                            'age': 30,
                            'location': {'city': 'LA', 'country': 'USA'}
                        }
                    },
                    'preferences': ['music', 'art', 'travel']
                },
                {
                    'id': 103,
                    'profile': {
                        'name': 'Charlie',
                        'demographics': {
                            'age': 35,
                            'location': {'city': 'London', 'country': 'UK'}
                        }
                    },
                    'preferences': ['books']
                }
            ],
            'items': [
                [
                    {'product_id': 'P1', 'quantity': 2, 'price': 19.99},
                    {'product_id': 'P2', 'quantity': 1, 'price': 29.99}
                ],
                [
                    {'product_id': 'P3', 'quantity': 3, 'price': 9.99}
                ],
                []
            ]
        }
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            temp_file = f.name
        
        try:
            # Write to parquet
            df = pd.DataFrame(complex_data)
            df.to_parquet(temp_file, index=False)
            
            # Analyze with our tool
            analyzer = ParquetAnalyzer()
            analysis = analyzer.analyze_file(temp_file)
            
            # Validate structure
            self.assertEqual(analysis.total_rows, 3)
            self.assertGreaterEqual(analysis.num_physical_columns, 3)
            
            # Check that nested structures are detected
            field_names = [field.name for field in analysis.schema_fields]
            self.assertIn('transaction_id', field_names)
            self.assertIn('user', field_names)
            self.assertIn('items', field_names)
            
            # Check nested field extraction
            user_field = next((f for f in analysis.schema_fields if f.name == 'user'), None)
            self.assertIsNotNone(user_field)
            self.assertGreater(len(user_field.children), 0)
            
            # Validate that we can serialize the complex structure
            json_output = analysis.to_json()
            self.assertIsInstance(json_output, str)
            
            # Validate that JSON is parseable
            parsed = json.loads(json_output)
            self.assertEqual(parsed['total_rows'], 3)
            
        finally:
            os.unlink(temp_file)


class TestComparisonTools(unittest.TestCase):
    """Test comparison with external tools"""
    
    def setUp(self):
        self.analyzer = ParquetAnalyzer()
        self.test_files = create_test_parquet_files()
    
    def tearDown(self):
        if hasattr(self, 'test_files') and 'test_dir' in self.test_files:
            shutil.rmtree(self.test_files['test_dir'], ignore_errors=True)
    
    def test_parquet_tools_comparison(self):
        """Test comparison with parquet-tools if available"""
        file_path = self.test_files['simple']
        
        # Try to compare with parquet-tools
        comparison = self.analyzer.compare_with_parquet_tools(file_path)
        
        if comparison and comparison.get('available'):
            # If parquet-tools is available, validate the output
            self.assertIn('output', comparison)
            self.assertIsInstance(comparison['output'], str)
            
            # Basic sanity checks on parquet-tools output
            output = comparison['output']
            # Check for either standard parquet-tools format or JSON format
            has_standard_format = 'file:' in output.lower() or 'schema:' in output.lower()
            has_json_format = 'numrowgroups' in output.lower() or 'rowgroups' in output.lower()
            
            self.assertTrue(has_standard_format or has_json_format, 
                          f"Unexpected parquet-tools output format: {output[:200]}...")
            
            print(f"parquet-tools output:\n{output}")
        else:
            # If not available, just ensure we handle it gracefully
            self.assertIsInstance(comparison, dict)
            self.assertIn('available', comparison)
            self.assertFalse(comparison['available'])
            self.assertIn('error', comparison)


def create_benchmark_suite():
    """Create a suite of benchmark files with known characteristics"""
    benchmark_dir = Path(tempfile.mkdtemp(prefix="parquet_benchmark_"))
    
    benchmarks = {}
    
    # 1. Different compression algorithms
    data = pd.DataFrame({
        'id': range(10000),
        'value': [f"value_{i}" for i in range(10000)],
        'number': [i * 1.5 for i in range(10000)]
    })
    
    for compression in ['snappy', 'gzip', 'lz4']:
        try:
            file_path = benchmark_dir / f"compression_{compression}.parquet"
            data.to_parquet(file_path, compression=compression, index=False)
            benchmarks[f'compression_{compression}'] = str(file_path)
        except:
            # Skip if compression not available
            pass
    
    # 2. Different data types
    types_data = pd.DataFrame({
        'int8_col': pd.array([1, 2, 3, 4, 5], dtype='int8'),
        'int32_col': pd.array([100, 200, 300, 400, 500], dtype='int32'),
        'int64_col': pd.array([1000, 2000, 3000, 4000, 5000], dtype='int64'),
        'float32_col': pd.array([1.1, 2.2, 3.3, 4.4, 5.5], dtype='float32'),
        'float64_col': pd.array([10.1, 20.2, 30.3, 40.4, 50.5], dtype='float64'),
        'string_col': ['a', 'b', 'c', 'd', 'e'],
        'bool_col': [True, False, True, False, True],
        'datetime_col': pd.date_range('2023-01-01', periods=5)
    })
    
    types_file = benchmark_dir / "data_types.parquet"
    types_data.to_parquet(types_file, index=False)
    benchmarks['data_types'] = str(types_file)
    
    benchmarks['benchmark_dir'] = str(benchmark_dir)
    return benchmarks


class TestBenchmarkSuite(unittest.TestCase):
    """Test with benchmark suite"""
    
    @classmethod
    def setUpClass(cls):
        cls.benchmarks = create_benchmark_suite()
        cls.analyzer = ParquetAnalyzer()
    
    @classmethod
    def tearDownClass(cls):
        if 'benchmark_dir' in cls.benchmarks:
            shutil.rmtree(cls.benchmarks['benchmark_dir'], ignore_errors=True)
    
    def test_different_compressions(self):
        """Test analysis of files with different compression algorithms"""
        compression_files = {k: v for k, v in self.benchmarks.items() 
                           if k.startswith('compression_')}
        
        results = {}
        for comp_type, file_path in compression_files.items():
            analysis = self.analyzer.analyze_file(file_path)
            results[comp_type] = {
                'compression_ratio': analysis.total_compressed / analysis.total_uncompressed,
                'file_size': analysis.file_size_bytes,
                'columns': [(col.name, col.compression) for col in analysis.columns]
            }
        
        # Validate that different compressions produce different results
        if len(results) > 1:
            compression_ratios = [r['compression_ratio'] for r in results.values()]
            # They should not all be identical
            self.assertGreater(max(compression_ratios) - min(compression_ratios), 0)
    
    def test_data_types_analysis(self):
        """Test analysis of various data types"""
        if 'data_types' in self.benchmarks:
            analysis = self.analyzer.analyze_file(self.benchmarks['data_types'])
            
            # Should have detected all the different types
            self.assertEqual(analysis.num_logical_columns, 8)
            
            # Check that different physical types are detected
            physical_types = set(col.physical_type for col in analysis.columns)
            expected_types = {'INT32', 'INT64', 'DOUBLE', 'BYTE_ARRAY', 'BOOLEAN'}
            # Should have at least some of these types
            self.assertGreater(len(physical_types.intersection(expected_types)), 0)


if __name__ == '__main__':
    # Run all tests
    unittest.main(verbosity=2)
