"""
Database utilities package for database migration and comparison.
"""
from .io import load_csv
from .align import align_columns, align_two
from .hashing import row_hash
from .fuzzy import fuzzy_title_score, confidence_label
from .normalize import (
    add_normalized_columns, 
    get_normalized_column_names, 
    normalize_df, 
    normalize_value
)
from .comparison import (
    load_table_pair,
    find_exact_matches,
    compare_row_sets,
    find_missing_rows
)
from .reporting import (
    print_section,
    print_subsection,
    print_match_summary,
    print_comparison_stats,
    print_table_summary,
    print_error,
    print_warning,
    print_success
)

__all__ = [
    "normalize_df",
    "normalize_value",
    "load_csv",
    "align_columns",
    "align_two",
    "row_hash",
    "fuzzy_title_score",
    "confidence_label",
    "print_section",
    "add_normalized_columns",
    "get_normalized_column_names",
    "load_table_pair",
    "find_exact_matches",
    "compare_row_sets",
    "find_missing_rows",
    "print_subsection",
    "print_match_summary",
    "print_comparison_stats",
    "print_table_summary",
    "print_error",
    "print_warning",
    "print_success",
]