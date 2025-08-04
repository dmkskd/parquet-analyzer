#!/usr/bin/env python3
"""
Demo script showing what the Parquet TUI looks like
"""

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree
from rich import box
from rich.layout import Layout

def demo_overview():
    """Show what the overview panel looks like"""
    console = Console()
    
    table = Table(show_header=False, box=box.SIMPLE)
    table.add_column("Property", style="bold cyan")
    table.add_column("Value", style="white")
    
    table.add_row("📁 File", "2023-05-01.gdax.btc_usd.00.parquet")
    table.add_row("📊 Rows", "180")
    table.add_row("🗂️  Logical Columns", "1")
    table.add_row("📋 Physical Columns", "19")
    table.add_row("📦 Row Groups", "1")
    table.add_row("💾 Uncompressed", "34.77 MB")
    table.add_row("🗜️  Compressed", "31.16 MB")
    table.add_row("📈 Compression Ratio", "89.6%")
    table.add_row("💰 Space Saved", "3.61 MB")
    table.add_row("🛠️  Created By", "parquet-mr version 1.12.2")
    
    panel = Panel(table, title="📊 Parquet File Overview", border_style="blue")
    console.print(panel)

def demo_schema():
    """Show what the schema panel looks like"""
    console = Console()
    
    tree = Tree("🗂️ Schema")
    
    field_node = tree.add("[bold]orderBookSides[/bold] (list<element: struct<...>>) [dim]nullable[/dim]")
    field_node.add("List of: struct<exchange: string, pair: string, ...>")
    
    struct_node = field_node.add("Struct (17 fields)")
    struct_node.add("exchange (string) [dim]nullable[/dim]")
    struct_node.add("pair (string) [dim]nullable[/dim]")
    struct_node.add("exchangeTimestamp (int64) [dim]nullable[/dim]")
    struct_node.add("isBid (bool) [dim]nullable[/dim]")
    struct_node.add("timestamp (int64) [dim]nullable[/dim]")
    
    metadata_node = struct_node.add("metadata (struct<firstUpdateId: int64, version: int64, lastId: int64>) [dim]nullable[/dim]")
    metadata_node.add("firstUpdateId (int64) [dim]nullable[/dim]")
    metadata_node.add("version (int64) [dim]nullable[/dim]")
    metadata_node.add("lastId (int64) [dim]nullable[/dim]")
    
    data_node = struct_node.add("data (list<element: list<element: double>>) [dim]nullable[/dim]")
    data_node.add("List of: list<element: double>")
    data_node.add("└─ Nested List of: double")
    
    struct_node.add("maxPrice (double) [dim]nullable[/dim]")
    struct_node.add("minPrice (double) [dim]nullable[/dim]")
    struct_node.add("... (more fields)")
    
    panel = Panel(tree, title="🏗️ Schema Structure", border_style="green")
    console.print(panel)

def demo_compression():
    """Show what the compression panel looks like"""
    console = Console()
    
    # Main compression table
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Column", style="cyan", width=30)
    table.add_column("Type", style="white", width=12)
    table.add_column("Compression", style="yellow", width=12)
    table.add_column("Uncompressed", style="blue", width=12)
    table.add_column("Compressed", style="green", width=12)
    table.add_column("Ratio", style="red", width=8)
    table.add_column("Saved", style="magenta", width=10)
    
    # Worst compressed columns first
    table.add_row(
        "orderBookSides...data.list...",
        "DOUBLE",
        "SNAPPY", 
        "34.7MB",
        "31.1MB",
        "89.6%",
        "3.6MB",
        style="bold white on blue"  # Selected
    )
    table.add_row(
        "orderBookSides...minPriceVolume",
        "DOUBLE",
        "SNAPPY",
        "1.8KB",
        "1.8KB", 
        "98.3%",
        "32B"
    )
    table.add_row(
        "orderBookSides...maxPriceVolume",  
        "DOUBLE",
        "SNAPPY",
        "1.8KB",
        "1.8KB",
        "98.2%",
        "33B"
    )
    table.add_row(
        "orderBookSides...maxPriceNumOrders",
        "INT32", 
        "SNAPPY",
        "257B",
        "222B",
        "86.4%",
        "35B"
    )
    
    compression_panel = Panel(table, title="🗜️ Compression Analysis (Worst First)", border_style="red")
    
    # Detail panel for selected column
    detail_table = Table(show_header=False, box=box.SIMPLE)
    detail_table.add_column("Property", style="bold cyan")
    detail_table.add_column("Value", style="white")
    
    detail_table.add_row("📝 Name", "orderBookSides...data.list.element.list.element")
    detail_table.add_row("🔤 Physical Type", "DOUBLE")
    detail_table.add_row("🗜️ Compression", "SNAPPY")
    detail_table.add_row("📎 Encodings", "RLE, PLAIN_DICTIONARY")
    detail_table.add_row("📊 Values", "14,901,264")
    detail_table.add_row("❌ Null Count", "0")
    detail_table.add_row("📏 Uncompressed", "36,440,396 bytes")
    detail_table.add_row("📦 Compressed", "32,658,128 bytes")
    detail_table.add_row("📈 Ratio", "89.6%")
    detail_table.add_row("💰 Saved", "3,782,268 bytes")
    detail_table.add_row("📉 Min Value", "0.000005")
    detail_table.add_row("📈 Max Value", "48322747.410000")
    
    detail_panel = Panel(detail_table, title="🔍 Column Details: orderBookSides...data...", border_style="yellow")
    
    # Create layout
    layout = Layout()
    layout.split_row(
        Layout(compression_panel, ratio=2),
        Layout(detail_panel, ratio=1)
    )
    
    console.print(layout)

def demo_optimization():
    """Show what the optimization panel looks like"""
    console = Console()
    
    content = """[bold yellow]🎯 OPTIMIZATION RECOMMENDATIONS[/bold yellow]

[bold red]📊 Worst Compressed Column:[/bold red] orderBookSides.list.element.data.list.element.list.element
Current: 89.6% ratio (31.1MB)

[bold green]⚡ QUICK WINS:[/bold green]
├─ ZSTD compression: Save ~6.5MB
├─ Use FLOAT32: Save ~16.3MB

[bold blue]🔧 MAJOR OPTIMIZATIONS:[/bold blue]
├─ Integer basis points (price * 10000)
│  └─ Potential: Save ~21.7MB
├─ Delta encoding (for sequential data)
├─ Separate price/volume columns
└─ Sort data before compression

[bold magenta]💎 MAXIMUM POTENTIAL:[/bold magenta]
└─ Up to 24.2MB savings (77% reduction)"""
    
    panel = Panel(content, title="🚀 Optimization Guide", border_style="magenta")
    console.print(panel)

if __name__ == "__main__":
    console = Console()
    
    console.print("\n[bold green]Parquet TUI Demo - What Each View Looks Like[/bold green]\n")
    
    console.print("[bold blue]View 1: Overview (Press '1')[/bold blue]")
    demo_overview()
    
    console.print("\n[bold blue]View 2: Schema (Press '2')[/bold blue]")
    demo_schema()
    
    console.print("\n[bold blue]View 3: Compression (Press '3') - Interactive with ↑/↓[/bold blue]")
    demo_compression()
    
    console.print("\n[bold blue]View 4: Optimization (Press '4')[/bold blue]")
    demo_optimization()
    
    console.print("\n[bold green]To run the actual interactive TUI:[/bold green]")
    console.print("[bold cyan]uv run parquet-analyzer /path/to/your/file.parquet[/bold cyan]")
    console.print("[dim]# or: .venv/bin/python -m parquet_analyzer.cli /path/to/your/file.parquet[/dim]")
    console.print("\nControls: 1-4 (switch views), ↑/↓ (navigate), h (help), q (quit)")
