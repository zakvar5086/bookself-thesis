"""
Enhanced reporting utilities for database comparison.
"""
from pathlib import Path
from typing import Dict, Any


def print_section(title: str, width: int = 80) -> None:
    """
    Print a section header with separator lines.
    
    Args:
        title: Section title
        width: Width of separator line
    """
    line = "=" * width
    print(f"\n{line}\n{title}\n{line}")


def print_subsection(title: str, width: int = 80) -> None:
    """
    Print a subsection header with dashed separator.
    
    Args:
        title: Subsection title
        width: Width of separator line
    """
    line = "-" * width
    print(f"\n{line}\n{title}\n{line}")


def print_match_summary(
    match_type: str,
    match_count: int,
    output_file: Path = None
) -> None:
    """
    Print a summary of matches found.
    
    Args:
        match_type: Description of match type (e.g., "Exact ISBN")
        match_count: Number of matches found
        output_file: Optional path to output file
    """
    print(f"\n{match_type}: {match_count} matches")
    if output_file:
        print(f"   Saved to: {output_file}")


def print_comparison_stats(stats: Dict[str, Any], indent: int = 3) -> None:
    """
    Print statistics from a comparison in a formatted way.
    
    Args:
        stats: Dictionary of statistics to print
        indent: Number of spaces to indent
    """
    prefix = " " * indent
    for key, value in stats.items():
        formatted_key = key.replace("_", " ").title()
        print(f"{prefix}{formatted_key}: {value}")


def print_table_summary(
    table_name: str,
    source_count: int,
    merged_count: int,
    missing_count: int = None
) -> None:
    """
    Print a summary for a single table comparison.
    
    Args:
        table_name: Name of the table
        source_count: Count from source
        merged_count: Count from merged
        missing_count: Optional count of missing rows
    """
    print(f"\n{table_name}:")
    print(f"   Source rows: {source_count}")
    print(f"   Merged rows: {merged_count}")
    if missing_count is not None:
        print(f"   Missing unique rows: {missing_count}")


def print_error(message: str, context: str = None) -> None:
    """
    Print an error message with optional context.
    
    Args:
        message: Error message
        context: Optional context (e.g., filename)
    """
    if context:
        print(f"[ERROR] {context}: {message}")
    else:
        print(f"[ERROR] {message}")


def print_warning(message: str, context: str = None) -> None:
    """
    Print a warning message with optional context.
    
    Args:
        message: Warning message
        context: Optional context (e.g., filename)
    """
    if context:
        print(f"[WARNING] {context}: {message}")
    else:
        print(f"[WARNING] {message}")


def print_success(message: str) -> None:
    """
    Print a success message.
    
    Args:
        message: Success message
    """
    print(f"[OK] {message}")