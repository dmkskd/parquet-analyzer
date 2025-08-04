#!/usr/bin/env python3
"""
Simple test to verify data panel works
"""

from parquet_tui import ParquetTUI
from rich.console import Console

def test_data_panel():
    """Test the data panel functionality"""
    console = Console()
    
    try:
        # Test with financial data (simpler structure)
        console.print("[bold green]Testing Data Panel with Financial Data[/bold green]")
        tui = ParquetTUI('demo_files/financial_data.parquet')
        tui.load_parquet_file()  # Load the analysis
        panel = tui.create_data_panel()
        console.print(panel)
        console.print()
        
        # Test with complex nested data
        console.print("[bold green]Testing Data Panel with Complex Nested Data[/bold green]")
        tui2 = ParquetTUI('demo_files/complex_nested.parquet')
        tui2.load_parquet_file()  # Load the analysis
        panel2 = tui2.create_data_panel()
        console.print(panel2)
        
        console.print("\n[bold green]✅ Data panel tests completed successfully![/bold green]")
        
    except Exception as e:
        console.print(f"[bold red]❌ Error: {e}[/bold red]")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_data_panel()
