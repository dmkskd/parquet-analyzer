#!/usr/bin/env python3
"""
Interactive TUI for Parquet File Analysis
Navigate through schema, compression, and optimization details
Uses the refactored ParquetAnalyzer for better testability
"""

import sys
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
import glob

try:
    import pandas as pd
    import pyarrow.parquet as pq
    import pyarrow as pa
    from rich.console import Console
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.table import Table
    from rich.tree import Tree
    from rich.text import Text
    from rich.columns import Columns
    from rich.align import Align
    from rich import box
    from rich.live import Live
    from rich.prompt import Prompt
    import termios
    import tty
except ImportError as e:
    print(f"Missing required packages. Please install with:")
    print(f"Missing required packages. Please install with:")
    print(f"uv sync")
    print(f"# or using pip: pip install pandas pyarrow rich")
    sys.exit(1)

# Import our refactored analyzer
from .analyzer import ParquetAnalyzer, ParquetAnalysis


class FileSelector:
    """Interactive file selector for parquet files"""
    
    def __init__(self):
        self.console = Console()
        self.current_path = Path.cwd()
        self.selected_index = 0
        self.files_and_dirs = []
        self.show_hidden = False
        
    def scan_directory(self):
        """Scan current directory for files and subdirectories"""
        self.files_and_dirs = []
        
        # Add parent directory option (except for root)
        if self.current_path.parent != self.current_path:
            self.files_and_dirs.append(("📁", "..", self.current_path.parent, True))
        
        try:
            # Get all items in current directory
            items = list(self.current_path.iterdir())
            
            # Sort: directories first, then files
            directories = [item for item in items if item.is_dir() and (not item.name.startswith('.') or self.show_hidden)]
            parquet_files = [item for item in items if item.is_file() and item.suffix.lower() in ['.parquet', '.pq']]
            other_files = [item for item in items if item.is_file() and item.suffix.lower() not in ['.parquet', '.pq'] and (not item.name.startswith('.') or self.show_hidden)]
            
            # Add directories
            for dir_path in sorted(directories, key=lambda x: x.name.lower()):
                self.files_and_dirs.append(("📁", dir_path.name, dir_path, True))
            
            # Add parquet files (highlighted)
            for file_path in sorted(parquet_files, key=lambda x: x.name.lower()):
                size_mb = file_path.stat().st_size / (1024 * 1024) if file_path.exists() else 0
                display_name = f"{file_path.name} ({size_mb:.1f}MB)"
                self.files_and_dirs.append(("📊", display_name, file_path, False))
            
            # Add other files (dimmed)
            for file_path in sorted(other_files, key=lambda x: x.name.lower()):
                if not file_path.name.startswith('.') or self.show_hidden:
                    self.files_and_dirs.append(("📄", file_path.name, file_path, False))
                    
        except PermissionError:
            self.files_and_dirs.append(("❌", "Permission denied", None, False))
        
        # Reset selection if out of bounds
        if self.selected_index >= len(self.files_and_dirs):
            self.selected_index = 0
    
    def create_file_panel(self) -> Panel:
        """Create the file browser panel"""
        if not self.files_and_dirs:
            return Panel("No files found", title="File Browser")
        
        table = Table(show_header=True, header_style="bold cyan", box=box.ROUNDED)
        table.add_column("", width=3, no_wrap=True)  # Icon
        table.add_column("Name", style="white", no_wrap=False)
        table.add_column("Type", width=8, style="dim")
        
        for i, (icon, name, path, is_dir) in enumerate(self.files_and_dirs):
            # Highlight selected item
            style = "bold white on blue" if i == self.selected_index else None
            
            # Determine type and style
            if path is None:
                type_str = "Error"
                name_style = "red"
            elif name == "..":
                type_str = "Parent"
                name_style = "yellow"
            elif is_dir:
                type_str = "Dir"
                name_style = "cyan"
            elif path and path.suffix.lower() in ['.parquet', '.pq']:
                type_str = "Parquet"
                name_style = "bold green"
            else:
                type_str = "File"
                name_style = "dim white"
            
            # Apply selection style if this row is selected
            if style:
                name_style = style
            elif not style and name_style != "red":
                # Apply type-specific styling only if not selected
                pass
            
            table.add_row(icon, name, type_str, style=style)
        
        title = f"📂 File Browser - {self.current_path}"
        if len(str(self.current_path)) > 50:
            title = f"📂 ...{str(self.current_path)[-47:]}"
        
            # Help if requested
            if show_help:
                self.console.print(self.create_help_panel())
            
            # Status
            parquet_count = sum(1 for _, _, path, is_dir in self.files_and_dirs 
                              if path and not is_dir and path.suffix.lower() in ['.parquet', '.pq'])
            
            status = f"[bold green]Found {parquet_count} parquet file(s)[/bold green]"
            if self.selected_index < len(self.files_and_dirs):
                _, name, path, is_dir = self.files_and_dirs[self.selected_index]
                if path and not is_dir and path.suffix.lower() in ['.parquet', '.pq']:
                    status += f" | [bold cyan]Selected: {name}[/bold cyan]"
            
            controls = "[bold green]Controls:[/bold green] [cyan]↑/↓ or j/k[/cyan] (navigate) [cyan]Enter[/cyan] (select) [cyan].[/cyan] (hidden) [cyan]q[/cyan] (quit) [cyan]h[/cyan] (help)"
            
            self.console.print(f"\n{status}")
            self.console.print(controls)
        
        try:
            render_selector()
            
            while True:
                # Get input
                fd = sys.stdin.fileno()
                old_settings = termios.tcgetattr(fd)
                try:
                    tty.setraw(sys.stdin.fileno())
                    key = sys.stdin.read(1)
                finally:
                    termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                
                needs_update = False
                
                if key == 'q' or key == 'Q':
                    return None
                elif key == 'h' or key == 'H':
                    show_help = not show_help
                    needs_update = True
                elif key == '.' :
                    self.show_hidden = not self.show_hidden
                    self.scan_directory()
                    needs_update = True
                elif key == 'r' or key == 'R':
                    self.scan_directory()
                    needs_update = True
                elif key == '\r' or key == '\n':  # Enter
                    if self.selected_index < len(self.files_and_dirs):
                        _, name, path, is_dir = self.files_and_dirs[self.selected_index]
                        
                        if path is None:
                            continue
                        elif name == ".." or is_dir:
                            # Navigate to directory
                            self.current_path = path
                            self.selected_index = 0
                            self.scan_directory()
                            needs_update = True
                        elif path.suffix.lower() in ['.parquet', '.pq']:
                            # Select parquet file
                            return str(path)
                elif key == '\x7f' or key == '\b':  # Backspace
                    if self.current_path.parent != self.current_path:
                        self.current_path = self.current_path.parent
                        self.selected_index = 0
                        self.scan_directory()
                        needs_update = True
                elif key == 'k' or key == 'K':  # Up navigation (fallback)
                    if self.selected_index > 0:
                        self.selected_index -= 1
                        needs_update = True
                elif key == 'j' or key == 'J':  # Down navigation (fallback)
                    if self.selected_index < len(self.files_and_dirs) - 1:
                        self.selected_index += 1
                        needs_update = True
                elif key == '\x1b':  # Escape or arrow key sequence
                    # More robust arrow key detection
                    sequence = [key]
                    try:
                        # Try to read a complete 3-character arrow sequence
                        char2 = sys.stdin.read(1)
                        sequence.append(char2)
                        if char2 == '[':
                            char3 = sys.stdin.read(1) 
                            sequence.append(char3)
                            
                            # Check for standard arrow keys
                            if char3 == 'A':  # Up arrow
                                if self.selected_index > 0:
                                    self.selected_index -= 1
                                    needs_update = True
                            elif char3 == 'B':  # Down arrow  
                                if self.selected_index < len(self.files_and_dirs) - 1:
                                    self.selected_index += 1
                                    needs_update = True
                            # Other arrow keys (C=right, D=left) are ignored in file browser
                        # If not a complete arrow sequence, treat as ESC (ignored)
                    except (OSError, ValueError):
                        # If we can't read the sequence, just ignore
                        pass
                elif ord(key) == 3:  # Ctrl+C
                    return None
                
                if needs_update:
                    render_selector()
                    
        except KeyboardInterrupt:
            return None
        finally:
            self.console.show_cursor(True)


class ParquetTUI:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.console = Console()
        self.current_view = "overview"  # overview, data, schema, rowgroups, pages, optimization
        self.selected_column = 0
        self.data_row_offset = 0  # For data view pagination
        
        # Row groups view navigation state
        self.compression_level = "file"  # "file", "rowgroups", "rowgroup_detail"
        self.selected_rowgroup = 0
        self.selected_rowgroup_column = 0
        
        self.analysis: Optional[ParquetAnalysis] = None
        self.analyzer = ParquetAnalyzer()
        
    def load_parquet_file(self) -> bool:
        """Load and analyze the parquet file using the refactored analyzer"""
        try:
            self.console.print(f"[bold blue]Loading {self.file_path}...[/bold blue]")
            
            # Use the refactored analyzer
            self.analysis = self.analyzer.analyze_file(self.file_path)
            return True
            
        except Exception as e:
            self.console.print(f"[bold red]Error loading file: {e}[/bold red]")
            return False
    
    def create_overview_panel(self) -> Panel:
        """Create the overview panel"""
        if not self.analysis:
            return Panel("No data loaded", title="Overview")
        
        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Property", style="bold cyan")
        table.add_column("Value", style="white")
        
        # File info
        table.add_row("File", Path(self.analysis.file_path).name)
        table.add_row("File Size", f"{self.analysis.file_size_bytes / (1024*1024):.2f} MB")
        table.add_row("Rows", f"{self.analysis.total_rows:,}")
        table.add_row("Logical Columns", f"{self.analysis.num_logical_columns}")
        table.add_row("Physical Columns", f"{self.analysis.num_physical_columns}")
        table.add_row("Row Groups", f"{self.analysis.num_row_groups}")
        
        # Size info
        uncompressed_mb = self.analysis.total_uncompressed / (1024 * 1024)
        compressed_mb = self.analysis.total_compressed / (1024 * 1024)
        
        # Safe division for compression ratio
        if self.analysis.total_uncompressed > 0:
            overall_ratio = self.analysis.total_compressed / self.analysis.total_uncompressed
            ratio_text = f"{overall_ratio:.1%}"
        else:
            ratio_text = "N/A"
        
        table.add_row("Uncompressed", f"{uncompressed_mb:.2f} MB")
        table.add_row("Compressed", f"{compressed_mb:.2f} MB")
        table.add_row("Compression Ratio", ratio_text)
        
        # Creator info
        if self.analysis.created_by:
            table.add_row("Created By", self.analysis.created_by)
        
        return Panel(table, title="Parquet File Overview", border_style="blue")
    
    def create_schema_panel(self) -> Panel:
        """Create the schema tree panel using the new schema structure"""
        if not self.analysis:
            return Panel("No data loaded", title="Schema")
        
        tree = Tree("🗂️ Schema")
        
        def add_schema_field_to_tree(parent_node, field, depth=0):
            """Recursively add schema fields to the tree structure"""
            # Create field display text
            field_text = f"[bold]{field.name}[/bold] ({field.type_str})"
            if field.nullable:
                field_text += " [dim]nullable[/dim]"
            
            field_node = parent_node.add(field_text)
            
            # Add logical type if different from physical
            if field.logical_type and field.logical_type != field.physical_type:
                field_node.add(f"Logical: [bold cyan]{field.logical_type}[/bold cyan]")
            
            # Add physical type if available
            if field.physical_type:
                field_node.add(f"Physical: [bold yellow]{field.physical_type}[/bold yellow]")
            
            # Recursively add children
            for child in field.children:
                add_schema_field_to_tree(field_node, child, depth + 1)
        
        # Process each top-level field
        for field in self.analysis.schema_fields:
            add_schema_field_to_tree(tree, field)
        
        return Panel(tree, title="🏗️ Schema Structure", border_style="green")
    
    def create_compression_panel(self) -> Panel:
        """Create the row groups analysis panel with hierarchical navigation"""
        if not self.analysis:
            return Panel("No data loaded", title="Row Groups")
        
        if self.compression_level == "file":
            return Panel(f"File: {Path(self.analysis.file_path).name}\nRows: {self.analysis.total_rows:,}\nColumns: {self.analysis.num_logical_columns}\nRow Groups: {self.analysis.num_row_groups}", title="PARQUET FILE OVERVIEW", border_style="green")
        elif self.compression_level == "rowgroups":
            return self._create_rowgroups_browser_panel()
        elif self.compression_level == "rowgroup_detail":
            return self._create_rowgroup_detail_panel()
        else:
            return Panel(f"Invalid view state: {self.compression_level}", title=f"DEBUG: {self.compression_level}")

    def _create_rowgroups_browser_panel(self) -> Panel:
        """Create the row groups browser panel"""
        if not self.analysis or not self.analysis.row_groups:
            return Panel("No row group data available", title="Row Groups")
        
        # Get terminal size for responsive layout
        terminal_width = self.console.size.width
        terminal_height = self.console.size.height
        
        # Reserve space for UI
        reserved_space = 12
        available_height = max(3, terminal_height - reserved_space)
        max_rows = max(1, available_height - 4)
        
        # Create table for row groups
        table = Table(show_header=True, header_style="bold blue", box=box.ROUNDED)
        table.add_column("RG#", style="cyan", width=4)
        table.add_column("Rows", style="white", width=8)
        table.add_column("Uncompressed", style="blue", width=12)
        table.add_column("Compressed", style="green", width=12)
        table.add_column("Ratio", style="red", width=7)
        table.add_column("Min/Max Hint", style="yellow", no_wrap=False)
        
        # Ensure selected_rowgroup is within bounds
        self.selected_rowgroup = max(0, min(self.selected_rowgroup, len(self.analysis.row_groups) - 1))
        
        # Show row groups (potentially paginated)
        displayed_rgs = self.analysis.row_groups[:max_rows] if len(self.analysis.row_groups) > max_rows else self.analysis.row_groups
        
        for i, rg in enumerate(displayed_rgs):
            is_selected = i == self.selected_rowgroup
            style = "bold white on blue" if is_selected else None
            
            def format_size(size_bytes):
                mb = size_bytes / (1024 * 1024)
                if mb >= 1:
                    return f"{mb:.1f}MB"
                else:
                    kb = size_bytes / 1024
                    return f"{kb:.0f}KB" if kb >= 1 else f"{size_bytes}B"
            
            table.add_row(
                str(rg.index),
                f"{rg.num_rows:,}",
                format_size(rg.total_uncompressed_size),
                format_size(rg.total_compressed_size),
                f"{rg.compression_ratio:.1%}",
                rg.get_min_max_hint(),
                style=style
            )
        
        title = f"🗜️ Row Group Browser - MAIN PANEL ({len(self.analysis.row_groups)} row groups)"
        if len(self.analysis.row_groups) > max_rows:
            title += f" - Showing {len(displayed_rgs)}/{len(self.analysis.row_groups)}"
        
        footer_text = "[bold cyan]Navigation:[/bold cyan] ↑/↓ (row groups) → (drill into columns) ← (back to file)"
        table.caption = footer_text
        
        return Panel(table, title=title, border_style="blue")

    def _create_rowgroup_detail_panel(self) -> Panel:
        """Create the detailed view of a specific row group"""
        if not self.analysis or not self.analysis.row_groups:
            return Panel("No row group data available", title="Row Group Detail")
        
        if self.selected_rowgroup >= len(self.analysis.row_groups):
            return Panel("Invalid row group selected", title="Row Group Detail")
        
        rg = self.analysis.row_groups[self.selected_rowgroup]
        
        # Get terminal size
        terminal_width = self.console.size.width
        terminal_height = self.console.size.height
        col_width = max(15, (terminal_width - 90) // 2)
        
        reserved_space = 12
        available_height = max(3, terminal_height - reserved_space)
        max_rows = max(1, available_height - 3)
        
        # Create table for row group columns
        table = Table(show_header=True, header_style="bold magenta", box=box.ROUNDED)
        table.add_column("Column", style="cyan", width=col_width, no_wrap=False)
        table.add_column("Type", style="white", width=6)
        table.add_column("Uncompressed", style="blue", width=11)
        table.add_column("Compressed", style="green", width=11)
        table.add_column("Ratio", style="red", width=6)
        table.add_column("Min → Max", style="yellow", no_wrap=False)
        
        # Ensure selected_rowgroup_column is within bounds
        self.selected_rowgroup_column = max(0, min(self.selected_rowgroup_column, len(rg.columns) - 1))
        
        # Sort columns by compression ratio (worst first)
        sorted_columns = sorted(rg.columns, key=lambda x: x.compression_ratio, reverse=True)
        # Ensure selected column is visible - adjust display window if needed
        start_index = 0
        if self.selected_rowgroup_column >= max_rows:
            start_index = max(0, self.selected_rowgroup_column - max_rows + 1)
        
        displayed_columns = sorted_columns[start_index:start_index + max_rows]
        
        for i, col in enumerate(displayed_columns):
            # Highlight selected column - check if this column index matches selection
            actual_index = start_index + i
            is_selected = actual_index == self.selected_rowgroup_column
            style = "bold white on blue" if is_selected else None
            
            # Truncate column name
            if len(col.name) > col_width - 3:
                col_name = "..." + col.name[-(col_width-6):]
            else:
                col_name = col.name
            
            # Format min/max values
            min_max_str = "No range data"
            if col.min_value is not None and col.max_value is not None:
                min_str = str(col.min_value)[:15]
                max_str = str(col.max_value)[:15]
                min_max_str = f"{min_str} → {max_str}"
            
            def format_size(size_bytes):
                mb = size_bytes / (1024 * 1024)
                kb = size_bytes / 1024
                if mb >= 1:
                    return f"{mb:.1f}MB"
                elif kb >= 1:
                    return f"{kb:.0f}KB"
                else:
                    return f"{size_bytes}B"
            
            table.add_row(
                col_name,
                col.physical_type[:6],
                format_size(col.uncompressed_size),
                format_size(col.compressed_size),
                f"{col.compression_ratio:.1%}",
                min_max_str,
                style=style
            )
        
        title = f"🗜️ Row Group {rg.index} - Column Details ({rg.num_rows:,} rows)"
        footer_text = "[bold cyan]Navigation:[/bold cyan] ↑/↓ (columns) ← (back to row groups) ESC (file level)"
        table.caption = footer_text
        
        return Panel(table, title=title, border_style="magenta")
    
    def create_pages_panel(self) -> Panel:
        """Create the page-level analysis panel"""
        if not self.analysis:
            return Panel("No data loaded", title="Page Analysis")
        
        # Get terminal size for responsive layout
        terminal_width = self.console.size.width
        
        # Create summary statistics
        total_pages = sum(col.num_pages for col in self.analysis.columns if col.pages)
        avg_pages_per_col = total_pages / len(self.analysis.columns) if self.analysis.columns else 0
        
        # Find columns with most/least pages
        columns_with_pages = [col for col in self.analysis.columns if col.pages and col.num_pages > 0]
        
        if not columns_with_pages:
            return Panel(
                "[yellow]⚠️ Page-level data not available[/yellow]\n\n"
                "PyArrow's Python API has limited access to page-level statistics.\n"
                "This analysis shows estimated page information based on column metadata.\n\n"
                "[dim]For detailed page analysis, consider using:\n"
                "• parquet-tools (Java-based)\n"
                "• Apache Arrow C++ tools\n"
                "• Custom Parquet readers[/dim]",
                title="📄 Page Analysis", border_style="yellow"
            )
        
        # Create main content
        content = []
        
        # Summary section
        content.append("[bold cyan]📊 PAGE SUMMARY[/bold cyan]")
        content.append(f"├─ Total estimated pages: [bold]{total_pages}[/bold]")
        content.append(f"├─ Average pages per column: [bold]{avg_pages_per_col:.1f}[/bold]")
        content.append(f"└─ Columns analyzed: [bold]{len(columns_with_pages)}[/bold]\n")
        
        # Column with most pages
        most_pages_col = max(columns_with_pages, key=lambda x: x.num_pages)
        content.append("[bold green]📈 MOST FRAGMENTED COLUMN[/bold green]")
        content.append(f"├─ Column: [bold]{most_pages_col.name[:40]}[/bold]")
        content.append(f"├─ Estimated pages: [bold]{most_pages_col.num_pages}[/bold]")
        content.append(f"├─ Avg page size: [bold]{most_pages_col.uncompressed_size // most_pages_col.num_pages / 1024:.0f}KB[/bold]")
        content.append(f"└─ Page efficiency: [bold]{most_pages_col.compression_ratio:.1%}[/bold]\n")
        
        # Column with least pages (most efficient)
        least_pages_col = min(columns_with_pages, key=lambda x: x.num_pages)
        content.append("[bold blue]🎯 MOST EFFICIENT COLUMN[/bold blue]")
        content.append(f"├─ Column: [bold]{least_pages_col.name[:40]}[/bold]")
        content.append(f"├─ Estimated pages: [bold]{least_pages_col.num_pages}[/bold]")
        content.append(f"├─ Avg page size: [bold]{least_pages_col.uncompressed_size // least_pages_col.num_pages / 1024:.0f}KB[/bold]")
        content.append(f"└─ Page efficiency: [bold]{least_pages_col.compression_ratio:.1%}[/bold]\n")
        
        # Page size distribution
        content.append("[bold magenta]📏 PAGE SIZE DISTRIBUTION[/bold magenta]")
        
        # Categorize columns by estimated page size
        large_page_cols = [col for col in columns_with_pages if col.uncompressed_size // col.num_pages > 1024*1024]  # >1MB
        medium_page_cols = [col for col in columns_with_pages if 256*1024 <= col.uncompressed_size // col.num_pages <= 1024*1024]  # 256KB-1MB
        small_page_cols = [col for col in columns_with_pages if col.uncompressed_size // col.num_pages < 256*1024]  # <256KB
        
        content.append(f"├─ Large pages (>1MB): [bold]{len(large_page_cols)}[/bold] columns")
        content.append(f"├─ Medium pages (256KB-1MB): [bold]{len(medium_page_cols)}[/bold] columns")
        content.append(f"└─ Small pages (<256KB): [bold]{len(small_page_cols)}[/bold] columns\n")
        
        # Recommendations
        content.append("[bold yellow]💡 PAGE OPTIMIZATION TIPS[/bold yellow]")
        
        if len(small_page_cols) > len(large_page_cols):
            content.append("├─ ⚠️ Many small pages detected")
            content.append("│  └─ Consider larger row groups or page sizes")
        
        if len(large_page_cols) > 0:
            content.append("├─ ✅ Some large pages found")
            content.append("│  └─ Good for compression efficiency")
        
        content.append("├─ 📝 Optimal page size: 1MB uncompressed")
        content.append("├─ 🔄 Balance: Compression vs Query performance")
        content.append("└─ 🎯 Fewer pages = Better compression")
        
        content.append("\n[dim]Note: Page estimates based on column metadata.\nActual page structure may vary.[/dim]")
        
        text_content = "\n".join(content)
        return Panel(text_content, title="📄 Page-Level Analysis", border_style="magenta")
    
    def create_optimization_panel(self) -> Panel:
        """Create the optimization recommendations panel"""
        if not self.analysis or not self.analysis.columns:
            return Panel("No data loaded", title="Optimization")
        
        # Find columns for different optimization strategies
        worst_ratio_col = max(self.analysis.columns, key=lambda x: x.compression_ratio)
        largest_col = max(self.analysis.columns, key=lambda x: x.compressed_size)
        double_cols = [col for col in self.analysis.columns if col.physical_type == "DOUBLE"]
        
        content = []
        
        # Header
        content.append("[bold yellow]🎯 OPTIMIZATION RECOMMENDATIONS[/bold yellow]\n")
        
        # File-level recommendations
        total_compressed_mb = self.analysis.total_compressed / (1024 * 1024)
        total_uncompressed_mb = self.analysis.total_uncompressed / (1024 * 1024)
        
        # Safe division for compression ratio
        if self.analysis.total_uncompressed > 0:
            overall_ratio = self.analysis.total_compressed / self.analysis.total_uncompressed
            ratio_text = f"{overall_ratio:.1%}"
        else:
            ratio_text = "N/A"
        
        content.append(f"[bold blue]📁 File Overview:[/bold blue]")
        content.append(f"├─ Current size: {total_compressed_mb:.1f}MB compressed")
        content.append(f"├─ Compression ratio: {ratio_text}")
        content.append(f"└─ {len(self.analysis.columns)} physical columns to optimize\n")
        
        # Quick wins based on largest column
        content.append("[bold green]⚡ QUICK WINS (Algorithm Changes):[/bold green]")
        
        # ZSTD compression savings estimate
        zstd_savings = self.analysis.total_compressed * 0.15  # Conservative 15% improvement
        content.append(f"├─ Switch to ZSTD compression")
        content.append(f"│  └─ Estimated savings: ~{zstd_savings/(1024*1024):.1f}MB ({(zstd_savings/self.analysis.total_compressed)*100:.0f}% reduction)")
        
        # LZ4 for speed vs compression tradeoff
        lz4_savings = self.analysis.total_compressed * 0.08  # Smaller savings but faster
        content.append(f"├─ Use LZ4 for faster access")
        content.append(f"│  └─ Moderate savings: ~{lz4_savings/(1024*1024):.1f}MB, much faster decompression")
        
        content.append("")
        
        # Data type optimizations
        content.append("[bold blue]🔧 DATA TYPE OPTIMIZATIONS:[/bold blue]")
        
        if double_cols:
            double_savings = sum(col.compressed_size * 0.4 for col in double_cols)  # 40% savings from DOUBLE->FLOAT32
            content.append(f"├─ Convert DOUBLE to FLOAT32 ({len(double_cols)} columns)")
            content.append(f"│  └─ Potential savings: ~{double_savings/(1024*1024):.1f}MB")
            
            # Show specific columns if not too many
            if len(double_cols) <= 3:
                for col in double_cols[:3]:
                    col_name = col.name[:40] + "..." if len(col.name) > 40 else col.name
                    content.append(f"│     • {col_name}")
        
        # Financial data specific optimizations
        price_cols = [col for col in self.analysis.columns if 'price' in col.name.lower()]
        if price_cols:
            price_savings = sum(col.compressed_size * 0.6 for col in price_cols)
            content.append(f"├─ Price data → Integer basis points")
            content.append(f"│  └─ High impact: ~{price_savings/(1024*1024):.1f}MB savings")
        
        content.append("├─ Sort data by timestamp before writing")
        content.append("│  └─ Improves compression for time-series data")
        content.append("└─ Use dictionary encoding for repeated strings")
        
        content.append("")
        
        # Structural improvements
        content.append("[bold magenta]🏗️ STRUCTURAL IMPROVEMENTS:[/bold magenta]")
        
        # Look for nested structures
        nested_cols = [col for col in self.analysis.columns if '.' in col.name]
        if nested_cols:
            content.append(f"├─ Flatten nested structures ({len(nested_cols)} nested columns)")
            content.append("│  └─ Can improve compression and query performance")
        
        content.append("├─ Separate hot/cold columns")
        content.append("│  └─ Store frequently accessed columns separately")
        content.append("├─ Partition by date/category")
        content.append("│  └─ Enable column pruning and better compression")
        content.append("└─ Use smaller row groups for better parallelism")
        
        content.append("")
        
        # Total potential savings
        total_algorithm_savings = zstd_savings
        total_datatype_savings = sum(col.compressed_size * 0.4 for col in double_cols) if double_cols else 0
        total_price_savings = sum(col.compressed_size * 0.6 for col in price_cols) if price_cols else 0
        total_potential = total_algorithm_savings + total_datatype_savings + total_price_savings
        
        content.append(f"[bold yellow]💎 TOTAL POTENTIAL SAVINGS:[/bold yellow]")
        content.append(f"├─ Algorithm changes: ~{total_algorithm_savings/(1024*1024):.1f}MB")
        if total_datatype_savings > 0:
            content.append(f"├─ Data type optimization: ~{total_datatype_savings/(1024*1024):.1f}MB")
        if total_price_savings > 0:
            content.append(f"├─ Price encoding: ~{total_price_savings/(1024*1024):.1f}MB")
        content.append(f"└─ [bold]Combined potential: ~{total_potential/(1024*1024):.1f}MB ({(total_potential/self.analysis.total_compressed)*100:.0f}% reduction)[/bold]")
        
        # Add implementation priority
        content.append("")
        content.append("[bold cyan]📋 IMPLEMENTATION PRIORITY:[/bold cyan]")
        content.append("1. Switch to ZSTD compression (easy, immediate gains)")
        if double_cols:
            content.append("2. Convert DOUBLE columns to FLOAT32 (medium effort)")
        if price_cols:
            content.append("3. Implement basis point encoding for prices (high impact)")
        content.append("4. Sort data by timestamp before writing")
        content.append("5. Consider structural changes for long-term benefits")
        
        text_content = "\n".join(content)
        
        return Panel(text_content, title="🚀 Optimization Recommendations", border_style="magenta")

    def create_data_panel(self) -> Panel:
        """Create the data preview panel"""
        if not self.analysis:
            return Panel("No data loaded", title="Data Preview")
        
        try:
            # Get paginated data sample  
            analyzer = ParquetAnalyzer()
            rows_per_page = 20  # Show 20 rows at a time
            df = analyzer.get_data_sample_paginated(self.file_path, max_rows=rows_per_page, offset=self.data_row_offset)
            
            # Get total row count for pagination info
            total_rows = self.analysis.total_rows
            current_page = (self.data_row_offset // rows_per_page) + 1
            total_pages = (total_rows + rows_per_page - 1) // rows_per_page  # Ceiling division
            
            # Get terminal size for responsive display
            terminal_width = self.console.size.width
            terminal_height = self.console.size.height
            
            # Reserve space for UI elements
            reserved_space = 15  # Conservative estimate for borders, title, controls
            available_height = max(5, terminal_height - reserved_space)
            max_display_rows = max(3, available_height - 4)  # Reserve space for headers and info
            
            # Create table for data display
            table = Table(show_header=True, header_style="bold cyan", box=box.ROUNDED)
            
            # Prioritize showing complex/interesting columns alongside simple ones
            simple_cols = []
            complex_cols = []
            
            for col in df.columns:
                dtype_str = str(df[col].dtype)
                if dtype_str == 'object':
                    # Check if this column has complex nested data
                    sample_val = df[col].iloc[0] if len(df) > 0 else None
                    if isinstance(sample_val, (list, dict)) or (hasattr(sample_val, '__array__') and hasattr(sample_val, 'shape') and len(getattr(sample_val, 'shape', [])) > 0):
                        complex_cols.append(col)
                    else:
                        simple_cols.append(col)
                else:
                    simple_cols.append(col)
            
            # Determine initial column selection
            available_col_width = terminal_width - 15  # Reserve space for borders and padding
            min_col_width = 8  # Minimum usable column width
            
            # Start with a reasonable number of columns to try
            max_possible_cols = max(1, available_col_width // min_col_width)
            
            # Select columns to display - mix of simple and complex
            display_cols = []
            
            # Always show at least one simple column for context (like ID)
            if simple_cols:
                display_cols.append(simple_cols[0])
            
            # Prioritize complex columns as they're more interesting
            for col in complex_cols:
                if len(display_cols) < max_possible_cols:
                    display_cols.append(col)
                else:
                    break
            
            # Fill remaining slots with simple columns
            for col in simple_cols[1:]:  # Skip first simple column if already added
                if len(display_cols) < max_possible_cols:
                    display_cols.append(col)
                else:
                    break
            
            # If we still only have one column, take more columns to use space better
            if len(display_cols) == 1 and len(df.columns) > 1:
                # Add more columns up to a reasonable limit
                all_remaining = [col for col in df.columns if col not in display_cols]
                for col in all_remaining:
                    if len(display_cols) < min(4, len(df.columns)):  # Cap at 4 columns max
                        display_cols.append(col)
                    else:
                        break
            
            truncated_cols = len(df.columns) > len(display_cols)
            
            # Calculate dynamic column widths based on content and available space
            def calculate_content_width(col_name, sample_data, is_complex=False, min_width=8):
                """Calculate appropriate width for a column based on content"""
                # Start with column name length
                header_width = len(col_name)
                
                # Give complex columns more priority
                if is_complex:
                    base_complex_width = 25  # Start with a good width for complex data
                    # Check content width from sample rows
                    content_widths = []
                    for i in range(min(3, len(sample_data))):  # Check first 3 rows
                        try:
                            value = sample_data.iloc[i]
                            # Estimate display width for complex content
                            if isinstance(value, (list, dict)) or hasattr(value, '__array__'):
                                # Complex content needs more space to be useful
                                content_widths.append(40)
                            else:
                                content_widths.append(15)
                        except:
                            content_widths.append(base_complex_width)
                    
                    avg_content_width = sum(content_widths) / len(content_widths) if content_widths else base_complex_width
                    ideal_width = max(header_width, avg_content_width, base_complex_width)
                    return int(max(min_width, min(ideal_width, 60)))  # Cap at 60 chars and ensure integer
                else:
                    # Simple columns - more conservative sizing
                    content_widths = []
                    for i in range(min(5, len(sample_data))):  # Check first 5 rows
                        try:
                            value = sample_data.iloc[i]
                            if pd.isna(value):
                                content_widths.append(4)  # "NULL"
                            elif isinstance(value, (int, float)):
                                content_widths.append(min(len(str(value)), 12))
                            elif isinstance(value, str):
                                content_widths.append(min(len(value), 20))
                            else:
                                content_widths.append(10)
                        except:
                            content_widths.append(10)
                    
                    avg_content_width = sum(content_widths) / len(content_widths) if content_widths else 10
                    ideal_width = max(header_width, avg_content_width)
                    return int(max(min_width, min(ideal_width, 25)))  # Cap simple columns at 25 chars and ensure integer
            
            # Calculate widths for each column, marking complex ones
            column_widths = {}
            total_ideal_width = 0
            
            for col in display_cols:
                is_complex = col in complex_cols
                ideal_width = calculate_content_width(col, df[col], is_complex)
                column_widths[col] = ideal_width
                total_ideal_width += ideal_width
            
            # Adjust widths to fit available space
            if total_ideal_width > available_col_width:
                # Scale down, but protect complex columns more
                scale_factor = available_col_width / total_ideal_width
                for col in display_cols:
                    current_width = column_widths[col]
                    # Ensure we have a numeric value
                    if not isinstance(current_width, (int, float)):
                        current_width = min_col_width
                    
                    if col in complex_cols:
                        # Complex columns get less aggressive scaling
                        adjusted_scale = max(0.7, scale_factor)  # Don't scale below 70%
                        column_widths[col] = max(min_col_width, int(current_width * adjusted_scale))
                    else:
                        # Simple columns can be scaled more aggressively
                        column_widths[col] = max(min_col_width, int(current_width * scale_factor))
                
                # If we still exceed available width, trim simple columns first
                current_total = sum(column_widths.values())
                if current_total > available_col_width:
                    excess = current_total - available_col_width
                    simple_display_cols = [col for col in display_cols if col not in complex_cols]
                    if simple_display_cols and len(simple_display_cols) > 0:
                        reduction_per_simple = max(1, excess // len(simple_display_cols))
                        for col in simple_display_cols:
                            column_widths[col] = max(min_col_width, column_widths[col] - reduction_per_simple)
            elif total_ideal_width < available_col_width and len(display_cols) > 0:
                # Distribute extra space, favoring complex columns
                extra_space = available_col_width - total_ideal_width
                complex_display_cols = [col for col in display_cols if col in complex_cols]
                simple_display_cols = [col for col in display_cols if col not in complex_cols]
                
                if complex_display_cols and len(complex_display_cols) > 0:
                    # Give 70% of extra space to complex columns
                    complex_extra = int(extra_space * 0.7)
                    simple_extra = extra_space - complex_extra
                    
                    complex_extra_per_col = max(1, complex_extra // len(complex_display_cols))
                    for col in complex_display_cols:
                        column_widths[col] = min(70, column_widths[col] + complex_extra_per_col)
                    
                    if simple_display_cols and len(simple_display_cols) > 0:
                        simple_extra_per_col = max(1, simple_extra // len(simple_display_cols))
                        for col in simple_display_cols:
                            column_widths[col] = min(30, column_widths[col] + simple_extra_per_col)
                else:
                    # No complex columns, distribute evenly
                    if len(display_cols) > 0:
                        extra_per_col = max(1, extra_space // len(display_cols))
                        for col in display_cols:
                            column_widths[col] = min(50, column_widths[col] + extra_per_col)
            
            # Add columns to table with calculated widths
            for col in display_cols:
                # Truncate column names if needed for header
                col_width = column_widths[col]
                if len(col) > col_width:
                    col_display = col[:col_width-3] + "..." if col_width > 3 else col[:col_width]
                else:
                    col_display = col
                
                table.add_column(col_display, style="white", width=col_width, no_wrap=True)
            
            # Add rows to table (limit to display size)
            display_rows = min(len(df), max_display_rows)
            
            for i in range(display_rows):
                row_data = []
                for col in display_cols:
                    try:
                        value = df.iloc[i][col]
                        
                        # Format the value for display with dynamic width awareness
                        col_width = column_widths[col]
                        try:
                            # Handle pandas NA values (but be careful with arrays)
                            try:
                                is_na = pd.isna(value)
                                # If pd.isna returns an array, check if any/all are NA
                                if hasattr(is_na, '__len__') and not isinstance(is_na, str):
                                    # This is an array of boolean values
                                    is_actually_na = is_na.all() if len(is_na) > 0 else False
                                else:
                                    # This is a single boolean value
                                    is_actually_na = bool(is_na)
                            except (ValueError, TypeError):
                                is_actually_na = False
                            
                            if is_actually_na:
                                formatted_value = "[dim]NULL[/dim]"
                            # Handle simple numeric types
                            elif isinstance(value, (int, float, complex)) and not pd.isna(value):
                                if isinstance(value, float):
                                    formatted_value = f"{value:.3g}"
                                else:
                                    formatted_value = str(value)
                            # Handle string types
                            elif isinstance(value, str):
                                if len(value) > col_width:
                                    formatted_value = value[:max(3, col_width-3)] + "..."
                                else:
                                    formatted_value = value
                            # Handle complex/nested data (lists, dicts, etc.)
                            elif isinstance(value, (list, dict)):
                                # Try to provide more meaningful information about nested structures
                                if isinstance(value, list) and len(value) > 0:
                                    first_item = value[0]
                                    if isinstance(first_item, dict):
                                        # Show meaningful content from nested objects
                                        content_parts = []
                                        for item in value[:2]:  # Show first 2 items
                                            if isinstance(item, dict):
                                                # Look for interesting keys
                                                interesting_keys = ['data', 'maxPrice', 'minPrice', 'isBid', 'exchange', 'pair', 'lastPrice', 'timestamp']
                                                shown_parts = []
                                                for key in interesting_keys:
                                                    if key in item:
                                                        val = item[key]
                                                        if isinstance(val, (int, float)):
                                                            shown_parts.append(f"{key}:{val:.2g}")
                                                        elif isinstance(val, bool):
                                                            shown_parts.append(f"{key}:{val}")
                                                        elif isinstance(val, str) and len(val) < 8:
                                                            shown_parts.append(f"{key}:{val}")
                                                        elif key == 'data' and hasattr(val, '__len__') and len(val) > 0:
                                                            # Handle the special 'data' field with price/quantity arrays
                                                            try:
                                                                first_pair = val[0]
                                                                if hasattr(first_pair, '__len__') and len(first_pair) == 2:
                                                                    # Show first price/quantity pair
                                                                    if hasattr(first_pair, '__getitem__'):
                                                                        price, qty = first_pair[0], first_pair[1]
                                                                        shown_parts.append(f"data:[{price:.1f},{qty:.2f}...]")
                                                            except:
                                                                # Fallback for data field
                                                                shown_parts.append(f"data:[{len(val)}]")
                                                        if len(shown_parts) >= 2:
                                                            break
                                                
                                                if shown_parts:
                                                    content_parts.append('{' + ','.join(shown_parts) + '}')
                                                else:
                                                    # Fallback to showing key names
                                                    keys = list(item.keys())[:2]
                                                    content_parts.append('{' + ','.join(keys) + '...}')
                                        
                                        if content_parts:
                                            content_str = ' '.join(content_parts)
                                            # Use available column width minus brackets and dim formatting
                                            max_content_len = max(10, col_width - 6)  # Reserve space for [dim] and []
                                            if len(content_str) > max_content_len:
                                                content_str = content_str[:max_content_len-3] + "..."
                                            formatted_value = f"[dim][{content_str}][/dim]"
                                        else:
                                            formatted_value = f"[dim]list[{len(value)}×dict][/dim]"
                                    elif isinstance(first_item, list):
                                        # Handle nested lists - check for price/quantity patterns
                                        if len(first_item) == 2 and all(isinstance(x, (int, float)) for x in first_item):
                                            # Looks like price/quantity pairs
                                            pairs = []
                                            for item in value[:3]:
                                                if isinstance(item, list) and len(item) == 2:
                                                    pairs.append(f"[{item[0]:.1f},{item[1]:.2f}]")
                                            if len(value) > 3:
                                                pairs.append("...")
                                            formatted_value = f"[dim][{','.join(pairs)}][/dim]"
                                        else:
                                            inner_len = len(first_item) if hasattr(first_item, '__len__') else '?'
                                            formatted_value = f"[dim]list[{len(value)}×{inner_len}][/dim]"
                                    else:
                                        # Array of simple values
                                        if all(isinstance(x, (int, float)) for x in value):
                                            vals = [f"{x:.2g}" for x in value[:4]]
                                            if len(value) > 4:
                                                vals.append("...")
                                            formatted_value = f"[dim][{','.join(vals)}][/dim]"
                                        else:
                                            vals = [str(x)[:6] for x in value[:3]]
                                            if len(value) > 3:
                                                vals.append("...")
                                            formatted_value = f"[dim][{','.join(vals)}][/dim]"
                                elif isinstance(value, dict):
                                    # Show dictionary content with values when possible
                                    items_to_show = []
                                    for key, val in list(value.items())[:3]:
                                        if isinstance(val, (int, float)):
                                            items_to_show.append(f"{key}:{val:.2g}")
                                        elif isinstance(val, str) and len(val) < 6:
                                            items_to_show.append(f"{key}:{val}")
                                        else:
                                            items_to_show.append(key)
                                    
                                    if items_to_show:
                                        content = ','.join(items_to_show)
                                        if len(value) > 3:
                                            content += "..."
                                        # Use available column width for dictionary content
                                        max_dict_len = max(8, col_width - 4)  # Reserve space for {} and dim formatting
                                        if len(content) > max_dict_len:
                                            content = content[:max_dict_len-3] + "..."
                                        formatted_value = f"[dim]{{{content}}}[/dim]"
                                    else:
                                        # Fallback to just keys
                                        keys = list(value.keys())[:3]
                                        key_str = ','.join(keys)
                                        if len(value) > 3:
                                            key_str += "..."
                                        formatted_value = f"[dim]{{{key_str}}}[/dim]"
                                else:
                                    formatted_value = f"[dim]{type(value).__name__}[{len(value) if hasattr(value, '__len__') else '?'}][/dim]"
                            # Handle numpy arrays and pandas Series (but not scalar numpy types)
                            elif hasattr(value, '__array__') and hasattr(value, 'shape') and len(getattr(value, 'shape', [])) > 0:
                                # This is likely a numpy array (not a scalar)
                                try:
                                    if len(value.shape) == 1:
                                        # 1D array - check what's inside
                                        if len(value) <= 5 and len(value) > 0:
                                            # Small array - show actual content
                                            first_item = value[0] if len(value) > 0 else None
                                            if isinstance(first_item, dict):
                                                # Array of dictionaries - show meaningful content
                                                content_parts = []
                                                for item in value[:2]:  # Show first 2 items
                                                    if isinstance(item, dict):
                                                        # Look for interesting keys (prices, data, etc.)
                                                        interesting_keys = ['data', 'maxPrice', 'minPrice', 'isBid', 'exchange', 'pair', 'lastPrice']
                                                        shown_parts = []
                                                        for key in interesting_keys:
                                                            if key in item:
                                                                val = item[key]
                                                                if isinstance(val, (int, float)):
                                                                    shown_parts.append(f"{key}:{val:.2g}")
                                                                elif isinstance(val, bool):
                                                                    shown_parts.append(f"{key}:{val}")
                                                                elif isinstance(val, str) and len(val) < 8:
                                                                    shown_parts.append(f"{key}:{val}")
                                                                elif key == 'data' and hasattr(val, '__len__') and len(val) > 0:
                                                                    # Handle the special 'data' field with price/quantity arrays
                                                                    try:
                                                                        # Check if this is an array of price/quantity pairs
                                                                        first_pair = val[0]
                                                                        if hasattr(first_pair, '__len__') and len(first_pair) == 2:
                                                                            # Show first price/quantity pair
                                                                            if hasattr(first_pair, '__getitem__'):
                                                                                price, qty = first_pair[0], first_pair[1]
                                                                                shown_parts.append(f"data:[{price:.1f},{qty:.2f}...]")
                                                                    except:
                                                                        # Fallback for data field
                                                                        shown_parts.append(f"data:[{len(val)}]")
                                                                elif isinstance(val, list) and len(val) > 0:
                                                                    if len(val) <= 3 and all(isinstance(x, (int, float)) for x in val):
                                                                        # Show numeric arrays directly
                                                                        vals_str = ','.join(f"{x:.2g}" for x in val)
                                                                        shown_parts.append(f"{key}:[{vals_str}]")
                                                                    elif len(val) <= 3 and all(isinstance(x, list) and len(x) == 2 for x in val):
                                                                        # Show price/quantity pairs
                                                                        pairs_str = ','.join(f"[{x[0]:.1f},{x[1]:.2f}]" for x in val[:2])
                                                                        shown_parts.append(f"{key}:[{pairs_str}...]")
                                                                if len(shown_parts) >= 2:
                                                                    break
                                                        
                                                        if shown_parts:
                                                            content_parts.append('{' + ','.join(shown_parts) + '}')
                                                        else:
                                                            # Fallback to showing a few key names
                                                            keys = list(item.keys())[:2]
                                                            content_parts.append('{' + ','.join(keys) + '...}')
                                                
                                                if content_parts:
                                                    content_str = ' '.join(content_parts)
                                                    # Use available column width for numpy array content
                                                    max_numpy_len = max(10, col_width - 6)  # Reserve space for [dim] and []
                                                    if len(content_str) > max_numpy_len:
                                                        content_str = content_str[:max_numpy_len-3] + "..."
                                                    formatted_value = f"[dim][{content_str}][/dim]"
                                                else:
                                                    formatted_value = f"[dim][dict×{len(value)}][/dim]"
                                            elif isinstance(first_item, list):
                                                # Array of lists - show some content
                                                if len(first_item) == 2 and all(isinstance(x, (int, float)) for x in first_item):
                                                    # Looks like price/quantity pairs
                                                    pairs = []
                                                    for item in value[:3]:
                                                        if isinstance(item, list) and len(item) == 2:
                                                            pairs.append(f"[{item[0]:.1f},{item[1]:.2f}]")
                                                    formatted_value = f"[dim][{','.join(pairs)}...][/dim]"
                                                else:
                                                    inner_len = len(first_item) if hasattr(first_item, '__len__') else '?'
                                                    formatted_value = f"[dim]list[{len(value)}×{inner_len}][/dim]"
                                            else:
                                                # Array of simple values - show them directly
                                                if all(isinstance(x, (int, float)) for x in value):
                                                    vals = [f"{x:.2g}" for x in value[:4]]
                                                    if len(value) > 4:
                                                        vals.append("...")
                                                    formatted_value = f"[dim][{','.join(vals)}][/dim]"
                                                else:
                                                    vals = [str(x)[:6] for x in value[:3]]  
                                                    if len(value) > 3:
                                                        vals.append("...")
                                                    formatted_value = f"[dim][{','.join(vals)}][/dim]"
                                        else:
                                            formatted_value = f"[dim]array[{len(value)}][/dim]"
                                    else:
                                        shape_str = '×'.join(map(str, value.shape))
                                        formatted_value = f"[dim]array[{shape_str}][/dim]"
                                except:
                                    formatted_value = f"[dim]{type(value).__name__}[/dim]"
                            # Handle numpy scalar types (int64, float64, etc.)
                            elif str(type(value)).startswith("<class 'numpy.") and not hasattr(value, 'shape'):
                                # This is a numpy scalar, treat it as a regular number/value
                                if hasattr(value, 'item'):
                                    # Convert numpy scalar to Python type
                                    python_val = value.item()
                                    if isinstance(python_val, float):
                                        formatted_value = f"{python_val:.3g}"
                                    else:
                                        formatted_value = str(python_val)
                                else:
                                    formatted_value = str(value)
                            # Handle other types (datetime, etc.)
                            else:
                                str_value = str(value)
                                if len(str_value) > col_width:
                                    formatted_value = str_value[:max(3, col_width-3)] + "..."
                                else:
                                    formatted_value = str_value
                        except (ValueError, TypeError, AttributeError):
                            # Fallback for any problematic values
                            formatted_value = f"[dim]{type(value).__name__}[/dim]"
                        
                        row_data.append(formatted_value)
                        
                    except Exception as e:
                        # Ultimate fallback
                        row_data.append(f"[red]Error[/red]")
                
                table.add_row(*row_data)
            
            # Create summary info
            info_lines = []
            info_lines.append(f"📊 Showing {display_rows} of {len(df):,} rows")
            info_lines.append(f"📈 Displaying {len(display_cols)} of {len(df.columns)} columns")
            
            if truncated_cols:
                info_lines.append(f"⚠️  {len(df.columns) - len(display_cols)} columns hidden (terminal width)")
            
            if len(df) > max_display_rows:
                info_lines.append(f"⚠️  {len(df) - display_rows:,} rows hidden (terminal height)")
            
            # Add schema info for complex columns
            complex_cols = []
            for col in display_cols:
                dtype_str = str(df[col].dtype)
                if dtype_str == 'object':
                    # Check if this column has complex nested data
                    sample_val = df[col].iloc[0] if len(df) > 0 else None
                    if isinstance(sample_val, (list, dict)) or hasattr(sample_val, '__array__'):
                        complex_cols.append(col)
            
            if complex_cols:
                info_lines.append("")
                info_lines.append("� Complex Columns (use Schema view for details):")
                for col in complex_cols[:3]:  # Show first few complex columns
                    col_display = col if len(col) <= 25 else col[:22] + "..."
                    info_lines.append(f"   {col_display}: nested structure")
                if len(complex_cols) > 3:
                    info_lines.append(f"   ... and {len(complex_cols) - 3} more complex columns")
            
            # Add data types info for simple columns  
            simple_cols = [col for col in display_cols if col not in complex_cols]
            if simple_cols:
                info_lines.append("")
                info_lines.append("📋 Simple Column Types:")
                for col in simple_cols[:5]:  # Show types for first few columns
                    dtype = str(df[col].dtype)
                    if dtype.startswith('<M8'):
                        dtype = 'datetime'
                    elif dtype == 'object':
                        dtype = 'string'
                    info_lines.append(f"   {col}: {dtype}")
            
            # Create title with pagination info
            start_row = self.data_row_offset + 1
            end_row = min(self.data_row_offset + len(df), total_rows)
            title_info = f"📋 Data Preview (rows {start_row:,}-{end_row:,} of {total_rows:,}, page {current_page}/{total_pages}, {len(display_cols)}/{len(df.columns)} cols)"
            
            return Panel(table, title=title_info, border_style="green")
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            error_msg = (
                f"[bold red]Error loading data:[/bold red]\n\n"
                f"Error: {str(e)}\n\n"
                f"[dim]This might be due to complex data structures or column formatting issues.\n"
                f"Full error details:\n{error_details}[/dim]"
            )
            return Panel(error_msg, title="📋 Data Preview - Error", border_style="red")
    
    def create_column_detail_panel(self) -> Panel:
        """Create detailed view of selected column"""
        if not self.analysis or self.selected_column >= len(self.analysis.columns):
            return Panel("No column selected", title="Column Details")
        
        col = sorted(self.analysis.columns, key=lambda x: x.compression_ratio, reverse=True)[self.selected_column]
        
        table = Table(show_header=False, box=box.SIMPLE, padding=(0, 1))
        table.add_column("Property", style="bold cyan", no_wrap=True)
        table.add_column("Value", style="white", no_wrap=False)
        
        # Compact name display
        name_display = col.name if len(col.name) <= 25 else col.name[:22] + "..."
        table.add_row("📝 Name", name_display)
        table.add_row("🔤 Physical", col.physical_type)
        table.add_row("🏷️ Logical", col.logical_type)
        table.add_row("🗜️ Compress", col.compression)
        
        # Compact encodings
        encodings = ", ".join(col.encodings[:2]) if col.encodings else "N/A"
        if len(col.encodings) > 2:
            encodings += f" +{len(col.encodings)-2} more"
        table.add_row("� Encodings", encodings)
        
        table.add_row("�📊 Values", f"{col.values:,}")
        
        if col.null_count is not None:
            table.add_row("❌ Nulls", f"{col.null_count:,}")
        if col.distinct_count is not None:
            table.add_row("🔢 Distinct", f"{col.distinct_count:,}")
        
        # Compact size info helper function
        def format_bytes(b):
            mb = b / (1024 * 1024)
            kb = b / 1024
            if mb >= 1:
                return f"{mb:.1f}MB"
            elif kb >= 1:
                return f"{kb:.0f}KB"
            else:
                return f"{b}B"
        
        # Page information
        if col.pages and col.num_pages > 0:
            table.add_row("📄 Pages", f"{col.num_pages}")
            avg_page_size = col.uncompressed_size // col.num_pages
            table.add_row("📏 Avg Page", format_bytes(avg_page_size))
            
            # Show page efficiency
            if col.pages:
                avg_page_ratio = sum(p.compression_ratio for p in col.pages) / len(col.pages)
                table.add_row("📈 Page Eff", f"{avg_page_ratio:.1%}")
        else:
            table.add_row("📄 Pages", "Est. N/A")
        
        table.add_row("📏 Uncompr", format_bytes(col.uncompressed_size))
        table.add_row("📦 Compr", format_bytes(col.compressed_size))
        table.add_row("📈 Ratio", f"{col.compression_ratio:.1%}")
        table.add_row("💰 Saved", format_bytes(col.uncompressed_size - col.compressed_size))
        
        # Min/Max if available (compact format)
        if col.min_value is not None and col.max_value is not None:
            min_str = str(col.min_value)[:15] + "..." if len(str(col.min_value)) > 15 else str(col.min_value)
            max_str = str(col.max_value)[:15] + "..." if len(str(col.max_value)) > 15 else str(col.max_value)
            table.add_row("📉 Min", min_str)
            table.add_row("📈 Max", max_str)
        
        # Compact title
        title = f"🔍 Details: {col.name[:20]}..." if len(col.name) > 20 else f"🔍 Details: {col.name}"
        return Panel(table, title=title, border_style="yellow")
    
    def create_help_panel(self) -> Panel:
        """Create help panel"""
        help_text = """[bold cyan]Navigation:[/bold cyan]
├─ [bold]f[/bold]: File Browser - Load a different Parquet file
├─ [bold]1[/bold]: Overview - File summary and basic statistics
├─ [bold]2[/bold]: Schema - Nested structure and field types  
├─ [bold]3[/bold]: Row Groups - Compression analysis with navigation
├─ [bold]4[/bold]: Pages - Page-level data organization
├─ [bold]5[/bold]: Optimization - Improvement recommendations
├─ [bold]6[/bold]: Data - Preview actual data content
├─ [bold]↑/↓ or j/k[/bold]: Navigate columns (Row Groups) / Page through data (Data view)
├─ [bold]q[/bold]: Quit
└─ [bold]h[/bold]: Toggle this help

[bold cyan]Views:[/bold cyan]
├─ [bold]1[/bold]: Overview - File summary and statistics
├─ [bold]2[/bold]: Data - Preview actual data content (j/k to page)  
├─ [bold]3[/bold]: Schema - Nested structure analysis  
├─ [bold]4[/bold]: Compression - Column-by-column analysis (j/k to navigate)
├─ [bold]5[/bold]: Pages - Page-level data organization
├─ [bold]6[/bold]: Optimization - Improvement recommendations
└─ [bold]j/k[/bold]: Navigate columns (in Row Groups view)"""
        
        return Panel(help_text, title="❓ Help", border_style="dim")
    
    def create_rowgroup_summary_panel(self) -> Panel:
        """Create summary panel for selected row group"""
        if not self.analysis or not self.analysis.row_groups:
            return Panel("No row group data", title="Row Group Summary")
        
        if self.selected_rowgroup >= len(self.analysis.row_groups):
            return Panel("Invalid row group selected", title="Row Group Summary")
        
        rg = self.analysis.row_groups[self.selected_rowgroup]
        
        table = Table(show_header=False, box=box.SIMPLE, padding=(0, 1))
        table.add_column("Property", style="bold cyan", no_wrap=True)
        table.add_column("Value", style="white", no_wrap=False)
        
        def format_bytes(b):
            mb = b / (1024 * 1024)
            kb = b / 1024
            if mb >= 1:
                return f"{mb:.1f}MB"
            elif kb >= 1:
                return f"{kb:.0f}KB"
            else:
                return f"{b}B"
        
        table.add_row("🗂️ Row Group", f"#{rg.index}")
        table.add_row("📊 Rows", f"{rg.num_rows:,}")
        table.add_row("🔢 Columns", f"{len(rg.columns)}")
        table.add_row("📏 Uncompr", format_bytes(rg.total_uncompressed_size))
        table.add_row("📦 Compr", format_bytes(rg.total_compressed_size))
        table.add_row("📈 Ratio", f"{rg.compression_ratio:.1%}")
        table.add_row("💰 Saved", format_bytes(rg.total_uncompressed_size - rg.total_compressed_size))
        
        # Show best and worst columns in this row group
        sorted_cols = sorted(rg.columns, key=lambda x: x.compression_ratio)
        if sorted_cols:
            best_col = sorted_cols[0]
            worst_col = sorted_cols[-1]
            table.add_row("🎯 Best Col", f"{best_col.name[:12]}... ({best_col.compression_ratio:.1%})")
            table.add_row("⚠️ Worst Col", f"{worst_col.name[:12]}... ({worst_col.compression_ratio:.1%})")
        
        # Show hint about min/max
        hint = rg.get_min_max_hint()
        if len(hint) > 40:
            hint = hint[:37] + "..."
        table.add_row("📋 Ranges", hint)
        
        return Panel(table, title=f"🗂️ Row Group {rg.index} Summary - SIDE PANEL", border_style="blue")
    
    def create_rowgroup_column_detail_panel(self) -> Panel:
        """Create detailed view of selected column in selected row group"""
        if not self.analysis or not self.analysis.row_groups:
            return Panel("No row group data", title="Column Details")
        
        if self.selected_rowgroup >= len(self.analysis.row_groups):
            return Panel("Invalid row group selected", title="Column Details")
        
        rg = self.analysis.row_groups[self.selected_rowgroup]
        
        if self.selected_rowgroup_column >= len(rg.columns):
            return Panel("No column selected", title="Column Details")
        
        # Sort columns by compression ratio (worst first) to match main display
        sorted_rg_columns = sorted(rg.columns, key=lambda x: x.compression_ratio, reverse=True)
        selected_rg_col = sorted_rg_columns[self.selected_rowgroup_column]
        
        # Find the corresponding file-level column for detailed info
        file_col = None
        for col in self.analysis.columns:
            if col.name == selected_rg_col.name:
                file_col = col
                break
        
        if file_col is None:
            return Panel(f"Column '{selected_rg_col.name}' not found in file analysis", title="Column Details")
        
        # Use the file-level column for detailed display, but show row group specific metrics
        table = Table(show_header=False, box=box.SIMPLE, padding=(0, 1))
        table.add_column("Property", style="bold cyan", no_wrap=True)
        table.add_column("Value", style="white", no_wrap=False)
        
        # Compact name display
        name_display = file_col.name if len(file_col.name) <= 25 else file_col.name[:22] + "..."
        table.add_row("📝 Name", name_display)
        table.add_row("� Physical", file_col.physical_type)
        table.add_row("🏷️ Logical", file_col.logical_type)
        table.add_row("🗜️ Compress", selected_rg_col.compression)  # Use row group specific compression
        
        # Compact encodings
        encodings = ", ".join(file_col.encodings[:2]) if file_col.encodings else "N/A"
        if len(file_col.encodings) > 2:
            encodings += f" +{len(file_col.encodings)-2} more"
        table.add_row("�️ Encodings", encodings)
        
        # Show row group specific min/max if available
        if selected_rg_col.min_value is not None and selected_rg_col.max_value is not None:
            min_str = str(selected_rg_col.min_value)[:15] + "..." if len(str(selected_rg_col.min_value)) > 15 else str(selected_rg_col.min_value)
            max_str = str(selected_rg_col.max_value)[:15] + "..." if len(str(selected_rg_col.max_value)) > 15 else str(selected_rg_col.max_value)
            table.add_row("📉 Min", min_str)
            table.add_row("📈 Max", max_str)
        
        # Show file-level stats for context
        if file_col.null_count is not None:
            table.add_row("❌ Nulls", f"{file_col.null_count:,}")
        if file_col.distinct_count is not None:
            table.add_row("� Distinct", f"{file_col.distinct_count:,}")
        
        return Panel(table, title="🔍 Column Detail", border_style="cyan")
    
    def create_layout(self, show_help: bool = False) -> Layout:
        """Create the main layout"""
        layout = Layout()
        
        if show_help:
            layout.split_column(
                Layout(self.create_help_panel(), size=12),
                Layout(name="main")
            )
            main_layout = layout["main"]
        else:
            main_layout = layout
        
        if self.current_view == "overview":
            main_layout.update(self.create_overview_panel())
        elif self.current_view == "schema":
            main_layout.update(self.create_schema_panel())
        elif self.current_view == "rowgroups":
            if self.compression_level == "file":
                # File level: show compression panel + column detail
                main_layout.split_row(
                    Layout(self.create_compression_panel(), ratio=2),
                    Layout(self.create_column_detail_panel(), ratio=1)
                )
            elif self.compression_level == "rowgroups":
                # Row groups level: show row groups browser + row group detail
                main_layout.split_row(
                    Layout(self.create_compression_panel(), ratio=2),
                    Layout(self.create_rowgroup_summary_panel(), ratio=1)
                )
            elif self.compression_level == "rowgroup_detail":
                # Row group detail: show column details + selected column info
                main_layout.split_row(
                    Layout(self.create_compression_panel(), ratio=2),
                    Layout(self.create_rowgroup_column_detail_panel(), ratio=1)
                )
            else:
                # Fallback
                main_layout.update(self.create_compression_panel())
        elif self.current_view == "pages":
            main_layout.update(self.create_pages_panel())
        elif self.current_view == "optimization":
            main_layout.update(self.create_optimization_panel())
        elif self.current_view == "data":
            main_layout.update(self.create_data_panel())
        
        return layout
    
    def show_file_browser(self):
        """Show file browser and return selected file path"""
        # Save current console state
        self.console.show_cursor(True)
        
        # Clear and show file browser
        self.console.clear()
        self.console.print("[bold blue]📁 Select a Parquet file[/bold blue]\n")
        
        # Get current directory
        current_dir = Path(self.file_path).parent if self.file_path else Path.cwd()
        
        # Simple file browser implementation
        while True:
            try:
                # List files in current directory
                files = []
                
                # Add parent directory option if not at root
                if current_dir != current_dir.parent:
                    files.append(("📁 ..", current_dir.parent, True))
                
                # Add subdirectories and parquet files
                try:
                    for item in sorted(current_dir.iterdir()):
                        if item.is_dir():
                            files.append((f"📁 {item.name}", item, True))
                        elif item.suffix.lower() in ['.parquet', '.pq']:
                            files.append((f"📄 {item.name}", item, False))
                except PermissionError:
                    self.console.print(f"[red]Permission denied: {current_dir}[/red]")
                    break
                
                if not files:
                    self.console.print("[yellow]No parquet files or directories found[/yellow]")
                    self.console.print("[cyan]Press any key to return...[/cyan]")
                    sys.stdin.read(1)
                    break
                
                # Display current directory and files
                self.console.print(f"[bold]Current directory:[/bold] {current_dir}")
                self.console.print()
                
                for i, (display_name, path, is_dir) in enumerate(files):
                    prefix = f"[cyan]{i+1:2d}[/cyan]. "
                    self.console.print(prefix + display_name)
                
                self.console.print()
                self.console.print("[cyan]Enter number to select, 'q' to cancel:[/cyan] ", end="")
                
                # Get user input
                choice = input().strip().lower()
                
                if choice == 'q':
                    break
                
                try:
                    index = int(choice) - 1
                    if 0 <= index < len(files):
                        _, selected_path, is_dir = files[index]
                        
                        if is_dir:
                            current_dir = selected_path
                            self.console.clear()
                            self.console.print("[bold blue]📁 Select a Parquet file[/bold blue]\n")
                        else:
                            # Selected a parquet file
                            self.console.show_cursor(False)
                            return str(selected_path)
                    else:
                        self.console.print("[red]Invalid selection[/red]")
                        input("Press Enter to continue...")
                        
                except ValueError:
                    self.console.print("[red]Please enter a valid number[/red]")
                    input("Press Enter to continue...")
                
                self.console.clear()
                self.console.print("[bold blue]📁 Select a Parquet file[/bold blue]\n")
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.console.print(f"[red]Error: {e}[/red]")
                input("Press Enter to continue...")
                break
        
        # Restore console state
        self.console.show_cursor(False)
        return None

    def run(self):
        """Run the TUI application with minimal flickering"""
        # If no file provided or file doesn't exist, start with file browser
        if not self.file_path or not Path(self.file_path).exists():
            file_selector = FileSelector()
            selected_file = file_selector.select_file()
            if not selected_file:
                return  # User cancelled
            self.file_path = selected_file
        
        if not self.load_parquet_file():
            return
        
        show_help = False
        
        # Hide cursor and enable alternate screen
        self.console.show_cursor(False)
        
        def render_current_view():
            """Render the current view without flickering"""
            # Use alternate screen buffer to avoid scrollback issues
            self.console.clear()
            
            # Main content
            if self.current_view == "overview":
                panel = self.create_overview_panel()
                self.console.print(panel)
            elif self.current_view == "schema":
                panel = self.create_schema_panel()
                self.console.print(panel)
            elif self.current_view == "rowgroups":
                # Print compression panel directly instead of using Layout to save vertical space
                compression_panel = self.create_compression_panel()
                self.console.print(compression_panel)
                
                # Show appropriate detail panel based on compression level
                if self.compression_level == "file":
                    # File level: show file column detail
                    if self.analysis and self.analysis.columns:
                        sorted_columns = sorted(self.analysis.columns, key=lambda x: x.compression_ratio, reverse=True)
                        selected_col = sorted_columns[min(self.selected_column, len(sorted_columns) - 1)]
                        detail_content = f"[bold cyan]{selected_col.name}[/bold cyan]\n"
                        detail_content += f"Physical Type: {selected_col.physical_type}\n"
                        detail_content += f"Logical Type: {selected_col.logical_type}\n"
                        detail_content += f"Encodings: {', '.join(selected_col.encodings)}\n"
                        if selected_col.min_value is not None:
                            detail_content += f"Min: {selected_col.min_value}\n"
                        if selected_col.max_value is not None:
                            detail_content += f"Max: {selected_col.max_value}\n"
                        if selected_col.null_count is not None:
                            detail_content += f"Nulls: {selected_col.null_count}\n"
                        
                        detail_panel = Panel(detail_content, title="🔍 Column Detail", border_style="blue")
                        self.console.print(detail_panel)
                elif self.compression_level == "rowgroups":
                    # Row groups level: show row group summary
                    detail_panel = self.create_rowgroup_summary_panel()
                    self.console.print(detail_panel)
                elif self.compression_level == "rowgroup_detail":
                    # Row group detail: show column detail for selected row group column
                    detail_panel = self.create_rowgroup_column_detail_panel()
                    self.console.print(detail_panel)
            elif self.current_view == "pages":
                panel = self.create_pages_panel()
                self.console.print(panel)
            elif self.current_view == "optimization":
                panel = self.create_optimization_panel()
                self.console.print(panel)
            elif self.current_view == "data":
                panel = self.create_data_panel()
                self.console.print(panel)
            
            # Help panel if requested
            if show_help:
                self.console.print("\n")
                self.console.print(self.create_help_panel())
            
            # Status and controls at bottom
            status_text = f"[bold green]View:[/bold green] [cyan]{self.current_view.title()}[/cyan]"
            if self.current_view == "rowgroups":
                status_text += f" | [bold green]Level:[/bold green] [cyan]{self.compression_level}[/cyan]"
                if self.compression_level == "file":
                    status_text += f" | [bold green]Column:[/bold green] [cyan]{self.selected_column + 1}/{len(self.analysis.columns)}[/cyan]"
                elif self.compression_level == "rowgroups":
                    status_text += f" | [bold green]Row Group:[/bold green] [cyan]{self.selected_rowgroup + 1}/{len(self.analysis.row_groups)}[/cyan]"
                elif self.compression_level == "rowgroup_detail":
                    rg = self.analysis.row_groups[self.selected_rowgroup] if self.analysis and self.analysis.row_groups else None
                    if rg:
                        status_text += f" | [bold green]RG{self.selected_rowgroup}:[/bold green] [cyan]{self.selected_rowgroup_column + 1}/{len(rg.columns)}[/cyan]"
            elif self.current_view == "data":
                rows_per_page = 20
                current_page = (self.data_row_offset // rows_per_page) + 1
                total_pages = (self.analysis.total_rows + rows_per_page - 1) // rows_per_page
                status_text += f" | [bold green]Page:[/bold green] [cyan]{current_page}/{total_pages}[/cyan]"
            
            controls_text = "[bold green]Controls:[/bold green] [cyan]f[/cyan] <file> [cyan]1[/cyan] <overview> [cyan]2[/cyan] <data> [cyan]3[/cyan] <schema> [cyan]4[/cyan] <row groups> [cyan]5[/cyan] <pages> [cyan]6[/cyan] <optimization> [cyan]ESC[/cyan] <back/exit> [cyan]↑/↓ or j/k[/cyan] <navigate> [cyan]h[/cyan] <help> [cyan]q[/cyan] <quit>"
            
            self.console.print(f"\n{status_text}")
            self.console.print(controls_text)
        
        try:
            # Initial render
            render_current_view()
            
            while True:
                # Get single character input
                fd = sys.stdin.fileno()
                old_settings = termios.tcgetattr(fd)
                try:
                    tty.setraw(sys.stdin.fileno())
                    key = sys.stdin.read(1)
                finally:
                    termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                
                # Track if we need to re-render
                needs_update = False
                
                # Handle key presses
                if key == 'q' or key == 'Q':
                    break
                elif key == 'h' or key == 'H':
                    show_help = not show_help
                    needs_update = True
                elif key == '1':
                    if self.current_view != "overview":
                        self.current_view = "overview"
                        self.selected_column = 0
                        needs_update = True
                elif key == '2':
                    if self.current_view != "data":
                        self.current_view = "data"
                        self.selected_column = 0
                        self.data_row_offset = 0  # Reset to first page
                        needs_update = True
                elif key == '3':
                    if self.current_view != "schema":
                        self.current_view = "schema"
                        self.selected_column = 0
                        needs_update = True
                elif key == '4':
                    if self.current_view != "rowgroups":
                        self.current_view = "rowgroups"
                        self.compression_level = "rowgroups"  # Start with browser
                        self.selected_rowgroup = 0
                        needs_update = True
                elif key == '5':
                    if self.current_view != "pages":
                        self.current_view = "pages"
                        self.selected_column = 0
                        needs_update = True
                elif key == '6':
                    if self.current_view != "optimization":
                        self.current_view = "optimization"
                        self.selected_column = 0
                        needs_update = True
                elif key == 'j' or key == 'J':
                    # Alternative navigation: j = down (vi-style)
                    if self.current_view == "rowgroups" and self.analysis:
                        if self.compression_level == "file":
                            max_column_index = len(self.analysis.columns) - 1
                            if self.selected_column < max_column_index:
                                self.selected_column += 1
                                needs_update = True
                        elif self.compression_level == "rowgroups":
                            max_rg_index = len(self.analysis.row_groups) - 1
                            if self.selected_rowgroup < max_rg_index:
                                self.selected_rowgroup += 1
                                needs_update = True
                        elif self.compression_level == "rowgroup_detail":
                            rg = self.analysis.row_groups[self.selected_rowgroup]
                            max_col_index = len(rg.columns) - 1
                            if self.selected_rowgroup_column < max_col_index:
                                self.selected_rowgroup_column += 1
                                needs_update = True
                    elif self.current_view == "data" and self.analysis:
                        # Page down in data view
                        rows_per_page = 20
                        max_offset = max(0, self.analysis.total_rows - rows_per_page)
                        if self.data_row_offset < max_offset:
                            self.data_row_offset = min(self.data_row_offset + rows_per_page, max_offset)
                            needs_update = True
                elif key == 'k' or key == 'K':
                    # Alternative navigation: k = up (vi-style)
                    if self.current_view == "rowgroups" and self.analysis:
                        if self.compression_level == "file":
                            if self.selected_column > 0:
                                self.selected_column -= 1
                                needs_update = True
                        elif self.compression_level == "rowgroups":
                            if self.selected_rowgroup > 0:
                                self.selected_rowgroup -= 1
                                needs_update = True
                        elif self.compression_level == "rowgroup_detail":
                            if self.selected_rowgroup_column > 0:
                                self.selected_rowgroup_column -= 1
                                needs_update = True
                    elif self.current_view == "data" and self.analysis:
                        # Page up in data view
                        rows_per_page = 20
                        if self.data_row_offset > 0:
                            self.data_row_offset = max(0, self.data_row_offset - rows_per_page)
                            needs_update = True
                elif key == 'f' or key == 'F' or key == '0':
                    # File browser - load a new file using the same selector as initial load
                    file_selector = FileSelector()
                    new_file = file_selector.select_file()
                    if new_file and new_file != self.file_path:
                        self.file_path = new_file
                        if self.load_parquet_file():
                            self.current_view = "overview"
                            self.selected_column = 0
                            needs_update = True
                        else:
                            # If loading failed, show error and continue with current file
                            needs_update = True
                elif key == '\x1b':  # Escape or arrow key sequence
                    # More robust arrow key detection
                    arrow_handled = False
                    try:
                        # Try to read a complete 3-character arrow sequence
                        char2 = sys.stdin.read(1)
                        if char2 == '[':
                            char3 = sys.stdin.read(1)
                            
                            # Check for standard arrow keys
                            if char3 == 'A':  # Up arrow
                                if self.current_view == "rowgroups" and self.analysis:
                                    if self.compression_level == "file":
                                        if self.selected_column > 0:
                                            self.selected_column -= 1
                                            needs_update = True
                                    elif self.compression_level == "rowgroups":
                                        if self.selected_rowgroup > 0:
                                            self.selected_rowgroup -= 1
                                            needs_update = True
                                    elif self.compression_level == "rowgroup_detail":
                                        if self.selected_rowgroup_column > 0:
                                            self.selected_rowgroup_column -= 1
                                            needs_update = True
                                    arrow_handled = True
                            elif char3 == 'B':  # Down arrow
                                if self.current_view == "rowgroups" and self.analysis:
                                    if self.compression_level == "file":
                                        max_column_index = len(self.analysis.columns) - 1
                                        if self.selected_column < max_column_index:
                                            self.selected_column += 1
                                            needs_update = True
                                    elif self.compression_level == "rowgroups":
                                        max_rg_index = len(self.analysis.row_groups) - 1
                                        if self.selected_rowgroup < max_rg_index:
                                            self.selected_rowgroup += 1
                                            needs_update = True
                                    elif self.compression_level == "rowgroup_detail":
                                        rg = self.analysis.row_groups[self.selected_rowgroup]
                                        max_col_index = len(rg.columns) - 1
                                        if self.selected_rowgroup_column < max_col_index:
                                            self.selected_rowgroup_column += 1
                                            needs_update = True
                                    arrow_handled = True
                            elif char3 == 'C':  # Right arrow
                                if self.current_view == "rowgroups" and self.analysis:
                                    if self.compression_level == "file":
                                        # Drill down to row groups browser
                                        self.compression_level = "rowgroups"
                                        self.selected_rowgroup = 0
                                        needs_update = True
                                    elif self.compression_level == "rowgroups":
                                        # Drill down to row group detail
                                        self.compression_level = "rowgroup_detail"
                                        self.selected_rowgroup_column = 0
                                        needs_update = True
                                    arrow_handled = True
                            elif char3 == 'D':  # Left arrow
                                if self.current_view == "rowgroups":
                                    if self.compression_level == "rowgroup_detail":
                                        # Go back to row groups browser
                                        self.compression_level = "rowgroups"
                                        needs_update = True
                                    elif self.compression_level == "rowgroups":
                                        # Go back to file level
                                        self.compression_level = "file"
                                        needs_update = True
                                    elif self.compression_level == "file":
                                        # Go back to main view
                                        self.current_view = "main"
                                        needs_update = True
                                    arrow_handled = True
                        # If not a complete arrow sequence, will fall through to ESC handling
                    except (OSError, ValueError):
                        # If we can't read the sequence, treat as ESC
                        pass
                    
                    # If arrow key wasn't handled, treat as ESC (back/exit)
                    if not arrow_handled:
                        if self.current_view == "overview":
                            break
                        else:
                            self.current_view = "overview"
                            self.selected_column = 0
                            self.data_row_offset = 0
                            needs_update = True
                elif ord(key) == 3:  # Ctrl+C
                    break
                
                # Only re-render if something actually changed
                if needs_update:
                    render_current_view()
                        
        except KeyboardInterrupt:
            pass
        finally:
            # Restore cursor
            self.console.show_cursor(True)
        
        self.console.print("\n[bold blue]Thanks for using Parquet TUI![/bold blue]")


def main():
    file_path = None
    
    if len(sys.argv) == 2:
        # File provided as argument
        file_path = sys.argv[1]
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            sys.exit(1)
    elif len(sys.argv) == 1:
        # No file provided, show file selector
        selector = FileSelector()
        file_path = selector.select_file()
        
        if file_path is None:
            print("No file selected. Exiting.")
            sys.exit(0)
    else:
        print("Usage: python parquet_tui.py [parquet_file]")
        print("  If no file is provided, an interactive file selector will be shown.")
        sys.exit(1)
    
    # Verify the selected file is a parquet file
    if not file_path.lower().endswith(('.parquet', '.pq')):
        print(f"Selected file is not a parquet file: {file_path}")
        sys.exit(1)
    
    tui = ParquetTUI(file_path)
    tui.run()


if __name__ == "__main__":
    main()
