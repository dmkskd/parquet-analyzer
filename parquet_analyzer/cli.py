#!/usr/bin/env python3
"""
Command-line interface for Parquet Analyzer.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

from .tui import ParquetTUI
from .analyzer import ParquetAnalyzer


def main(args: Optional[list] = None) -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Interactive TUI for analyzing Parquet file metadata, compression, and optimization",
        prog="parquet-analyzer"
    )
    
    parser.add_argument(
        "file",
        nargs="?",
        help="Path to Parquet file to analyze (optional - you can browse files in the TUI)"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0"
    )
    
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="Just analyze the file and print results (no TUI)"
    )
    
    parsed_args = parser.parse_args(args)
    
    # If no file specified, start with file browser
    file_path = parsed_args.file or ""
    
    if parsed_args.analyze_only:
        if not file_path:
            print("Error: --analyze-only requires a file path", file=sys.stderr)
            return 1
        
        if not Path(file_path).exists():
            print(f"Error: File '{file_path}' not found", file=sys.stderr)
            return 1
        
        # Just analyze and print results
        try:
            analyzer = ParquetAnalyzer()
            analysis = analyzer.analyze_file(file_path)
            
            print(f"File: {analysis.file_path}")
            print(f"Size: {analysis.file_size_bytes / (1024*1024):.2f} MB")
            print(f"Rows: {analysis.total_rows:,}")
            print(f"Columns: {analysis.num_physical_columns}")
            print(f"Compression: {analysis.total_compressed / analysis.total_uncompressed:.1%}")
            
            return 0
        except Exception as e:
            print(f"Error analyzing file: {e}", file=sys.stderr)
            return 1
    else:
        # Start the TUI
        try:
            tui = ParquetTUI(file_path)
            tui.run()
            return 0
        except KeyboardInterrupt:
            print("\nExiting...")
            return 0
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1


if __name__ == "__main__":
    sys.exit(main())
