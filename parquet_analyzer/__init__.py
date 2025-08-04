"""
Parquet Analyzer - Interactive TUI for analyzing Parquet file metadata, compression, and optimization.

This package provides tools for analyzing Parquet files with a focus on metadata,
compression ratios, schema analysis, and optimization recommendations.
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from .analyzer import ParquetAnalyzer, ParquetAnalysis, ColumnInfo, SchemaField, PageInfo
from .tui import ParquetTUI

__all__ = ["ParquetAnalyzer", "ParquetAnalysis", "ColumnInfo", "SchemaField", "PageInfo", "ParquetTUI"]
