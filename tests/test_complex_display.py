#!/usr/bin/env python3
"""
Test the data panel with complex nested structures
"""

from parquet_tui import ParquetTUI
from rich.console import Console
import parquet_analyzer

def test_complex_display():
    console = Console()
    
    console.print("[bold green]Testing Complex Order Book Structure[/bold green]")
    
    # Load the data manually to see what we're working with
    analyzer = parquet_analyzer.ParquetAnalyzer()
    df = analyzer.get_data_sample('test_complex_orderbook.parquet', 5)
    
    console.print(f"[cyan]Loaded data with columns: {list(df.columns)}[/cyan]")
    
    # Show sample values for complex columns
    for col in ['orderBookSides', 'marketData']:
        if col in df.columns:
            sample = df[col].iloc[0]
            console.print(f"[yellow]{col}[/yellow] sample type: {type(sample)}")
            if isinstance(sample, dict):
                console.print(f"  Keys: {list(sample.keys())}")
            elif hasattr(sample, '__len__') and len(sample) > 0:
                console.print(f"  Length: {len(sample)}")
                if hasattr(sample[0], 'keys'):
                    console.print(f"  First item keys: {list(sample[0].keys())[:10]}...")
    
    console.print()
    
    # Now test the TUI data panel
    tui = ParquetTUI('test_complex_orderbook.parquet')
    tui.load_parquet_file()
    
    # Create the panel
    panel = tui.create_data_panel()
    console.print(panel)

if __name__ == "__main__":
    test_complex_display()
