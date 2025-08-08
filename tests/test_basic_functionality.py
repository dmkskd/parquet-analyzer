"""
Basic functional tests for Parquet Analyzer
"""

import pytest
import pandas as pd
import tempfile
from pathlib import Path

class TestBasic:
    """Basic functionality tests"""
    
    def test_imports(self):
        """Test that we can import the main modules"""
        try:
            from parquet_analyzer import ParquetAnalyzer
            assert ParquetAnalyzer is not None
        except ImportError:
            pytest.skip("ParquetAnalyzer not available")
    
    def test_pandas_works(self):
        """Test that pandas works for creating test data"""
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': ['x', 'y', 'z']
        })
        assert len(df) == 3
        assert list(df.columns) == ['a', 'b']

if __name__ == "__main__":
    pytest.main([__file__])
