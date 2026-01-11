"""
ISBN Analyzer Script

Processes book entries from db1 and db2 across tables: Books, Books1, MissingBooks, NB
Categorizes books based on ISBN availability and OpenLibrary API lookups.

Cases:
1. Book has ISBN → {ISBN: [tables]}
2. Book has no ISBN → Call OpenLibrary API:
   2A. No ISBN returned → {Title: [tables]}
   2B. Multiple ISBNs → {Title: {"tables": [tables], "isbns": [isbns]}}
   2C. Single ISBN (1 ISBN10 and/or 1 ISBN13) → {(isbn10, isbn13): {title: [tables]}}
"""

import json
import time
import requests
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
from dataclasses import dataclass, field
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from database_utils.io import load_csv
from database_utils.config import get_path
from database_utils.normalize import normalize_value


# Constants
TABLES_TO_PROCESS = ["Books", "Books1", "MissingBooks", "NB"]
OPENLIBRARY_API_URL = "https://openlibrary.org/search.json"
API_RATE_LIMIT_DELAY = 0.5  # seconds between API calls to be polite


@dataclass
class AnalysisResults:
    """Container for all analysis results."""
    # Case 1: Books with ISBN → {ISBN: [tables]}
    case1_with_isbn: Dict[str, List[str]] = field(default_factory=dict)
    
    # Case 2A: No ISBN returned → {Title: [tables]}
    case2a_no_isbn_found: Dict[str, List[str]] = field(default_factory=dict)
    
    # Case 2B: Multiple ISBNs → {Title: {"tables": [tables], "isbns": [isbns]}}
    case2b_multiple_isbns: Dict[str, Dict[str, List[str]]] = field(default_factory=dict)
    
    # Case 2C: Single ISBN → {(isbn10, isbn13): {title: [tables]}}
    # Using string key "isbn10|isbn13" since tuples can't be JSON keys
    case2c_single_isbn: Dict[str, Dict[str, List[str]]] = field(default_factory=dict)
    
    # Statistics
    stats: Dict[str, int] = field(default_factory=lambda: {
        "total_entries": 0,
        "with_isbn": 0,
        "without_isbn": 0,
        "api_calls": 0,
        "api_errors": 0,
        "case2a_count": 0,
        "case2b_count": 0,
        "case2c_count": 0
    })


def normalize_isbn(isbn: str) -> str:
    """Normalize ISBN by removing hyphens, spaces, and converting to uppercase."""
    if not isbn:
        return ""
    return isbn.replace("-", "").replace(" ", "").strip().upper()


def is_valid_isbn(isbn: str) -> bool:
    """
    Check if ISBN is valid after normalization.
    Valid ISBNs contain only digits and possibly 'X' (for ISBN-10 check digit).
    Must be 10 or 13 characters.
    """
    normalized = normalize_isbn(isbn)
    if not normalized or normalized.lower() in ["nan", "none", "null"]:
        return False
    
    # ISBN should only contain digits and possibly X (check digit for ISBN-10)
    valid_chars = set("0123456789X")
    if not all(c in valid_chars for c in normalized):
        return False
    
    # ISBN must be 10 or 13 characters
    if len(normalized) not in [10, 13]:
        return False
    
    return True


def classify_isbns(isbn_list: List[str]) -> Tuple[List[str], List[str]]:
    """
    Classify ISBNs into ISBN-10 and ISBN-13 categories.
    
    Returns:
        Tuple of (isbn10_list, isbn13_list)
    """
    isbn10s = []
    isbn13s = []
    
    for isbn in isbn_list:
        clean = normalize_isbn(isbn)
        if len(clean) == 10:
            isbn10s.append(clean)
        elif len(clean) == 13:
            isbn13s.append(clean)
        # Ignore invalid lengths
    
    return isbn10s, isbn13s


def fetch_isbn_from_openlibrary(title: str, max_retries: int = 3) -> Optional[List[str]]:
    """
    Fetch ISBN(s) from OpenLibrary API for a given title.
    
    Args:
        title: Book title to search
        max_retries: Number of retries on failure
        
    Returns:
        List of ISBNs if found, empty list if none found, None on error
    """
    if not title or title.strip() == "":
        return []
    
    params = {
        "title": title,
        "fields": "isbn",
        "limit": 1  # We only need the first match
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(
                OPENLIBRARY_API_URL,
                params=params,
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("numFound", 0) == 0 or not data.get("docs"):
                return []
            
            # Get ISBNs from first doc
            first_doc = data["docs"][0]
            isbns = first_doc.get("isbn", [])
            
            return isbns if isbns else []
            
        except requests.exceptions.RequestException as e:
            print(f"  [WARN] API request failed (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait before retry
            continue
        except json.JSONDecodeError as e:
            print(f"  [WARN] Failed to parse API response: {e}")
            return None
    
    return None  # All retries failed


def get_table_identifier(db_name: str, table_name: str) -> str:
    """Create a unique identifier for a table including database name."""
    return f"{db_name}/{table_name}"


def process_book_entry(
    isbn: str,
    title: str,
    table_id: str,
    results: AnalysisResults,
    api_cache: Dict[str, Any]
) -> None:
    """
    Process a single book entry and categorize it.
    
    Args:
        isbn: Book ISBN (may be empty)
        title: Book title
        table_id: Identifier for the source table (e.g., "db1/Books")
        results: AnalysisResults object to update
        api_cache: Cache for API results to avoid duplicate calls
    """
    results.stats["total_entries"] += 1
    
    # Normalize ISBN
    normalized_isbn = normalize_isbn(isbn)
    normalized_title = normalize_value(title) if title else ""
    
    # CASE 1: Book has ISBN
    if is_valid_isbn(normalized_isbn):
        results.stats["with_isbn"] += 1
        
        if normalized_isbn not in results.case1_with_isbn:
            results.case1_with_isbn[normalized_isbn] = []
        
        if table_id not in results.case1_with_isbn[normalized_isbn]:
            results.case1_with_isbn[normalized_isbn].append(table_id)
        
        return
    
    # CASE 2: Book doesn't have ISBN - need API lookup
    results.stats["without_isbn"] += 1
    
    if not normalized_title:
        print(f"  [SKIP] No ISBN and no title for entry in {table_id}")
        return
    
    # Check cache first
    if normalized_title in api_cache:
        api_result = api_cache[normalized_title]
    else:
        # Make API call
        print(f"  [API] Looking up: '{title[:50]}...' " if len(title) > 50 else f"  [API] Looking up: '{title}'")
        time.sleep(API_RATE_LIMIT_DELAY)  # Rate limiting
        
        api_result = fetch_isbn_from_openlibrary(title)
        results.stats["api_calls"] += 1
        
        if api_result is None:
            results.stats["api_errors"] += 1
            api_result = []  # Treat errors as no ISBN found
        
        api_cache[normalized_title] = api_result
    
    # Process API result
    if not api_result:
        # CASE 2A: No ISBN returned
        results.stats["case2a_count"] += 1
        
        if normalized_title not in results.case2a_no_isbn_found:
            results.case2a_no_isbn_found[normalized_title] = []
        
        if table_id not in results.case2a_no_isbn_found[normalized_title]:
            results.case2a_no_isbn_found[normalized_title].append(table_id)
        
        return
    
    # Classify ISBNs
    isbn10s, isbn13s = classify_isbns(api_result)
    
    # Check if it's a single ISBN case (1 ISBN10 and/or 1 ISBN13)
    is_single_isbn = len(isbn10s) <= 1 and len(isbn13s) <= 1
    
    if is_single_isbn and (isbn10s or isbn13s):
        # CASE 2C: Single ISBN (1 ISBN10 and/or 1 ISBN13)
        results.stats["case2c_count"] += 1
        
        isbn10 = isbn10s[0] if isbn10s else ""
        isbn13 = isbn13s[0] if isbn13s else ""
        
        # Create key as "isbn10|isbn13"
        isbn_key = f"{isbn10}|{isbn13}"
        
        if isbn_key not in results.case2c_single_isbn:
            results.case2c_single_isbn[isbn_key] = {}
        
        if normalized_title not in results.case2c_single_isbn[isbn_key]:
            results.case2c_single_isbn[isbn_key][normalized_title] = []
        
        if table_id not in results.case2c_single_isbn[isbn_key][normalized_title]:
            results.case2c_single_isbn[isbn_key][normalized_title].append(table_id)
    
    else:
        # CASE 2B: Multiple ISBNs
        results.stats["case2b_count"] += 1
        
        all_isbns = [normalize_isbn(i) for i in api_result]
        
        if normalized_title not in results.case2b_multiple_isbns:
            results.case2b_multiple_isbns[normalized_title] = {
                "tables": [],
                "isbns": all_isbns
            }
        
        if table_id not in results.case2b_multiple_isbns[normalized_title]["tables"]:
            results.case2b_multiple_isbns[normalized_title]["tables"].append(table_id)


def load_and_process_table(
    db_path: Path,
    db_name: str,
    table_name: str,
    results: AnalysisResults,
    api_cache: Dict[str, Any]
) -> int:
    """
    Load a table and process all book entries.
    
    Returns:
        Number of entries processed
    """
    csv_path = db_path / f"{table_name}.csv"
    
    if not csv_path.exists():
        print(f"  [SKIP] {csv_path} does not exist")
        return 0
    
    df = load_csv(csv_path, normalize=False)  # Don't normalize yet, we need original values
    
    if df is None or df.empty:
        print(f"  [SKIP] {csv_path} is empty or could not be loaded")
        return 0
    
    # Find ISBN and Title columns (case-insensitive)
    isbn_col = None
    title_col = None
    
    for col in df.columns:
        col_lower = col.lower()
        if col_lower == "isbn":
            isbn_col = col
        elif col_lower == "title":
            title_col = col
    
    if title_col is None:
        print(f"  [WARN] No 'Title' column found in {table_name}")
        return 0
    
    table_id = get_table_identifier(db_name, table_name)
    count = 0
    
    for idx, row in df.iterrows():
        isbn = str(row.get(isbn_col, "")) if isbn_col else ""
        title = str(row.get(title_col, ""))
        
        process_book_entry(isbn, title, table_id, results, api_cache)
        count += 1
    
    return count


def sort_case1_by_table_count(case1_map: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """
    Sort Case 1 map by the length of the tables array (descending).
    """
    sorted_items = sorted(
        case1_map.items(),
        key=lambda x: len(x[1]),
        reverse=True
    )
    return dict(sorted_items)


def convert_case2c_for_json(case2c: Dict[str, Dict[str, List[str]]]) -> List[Dict]:
    """
    Convert Case 2C to a JSON-friendly format with ISBN arrays as a field.
    
    Output format:
    [
        {
            "isbns": ["isbn10", "isbn13"],
            "titles": {
                "title1": ["table1", "table2"],
                ...
            }
        },
        ...
    ]
    """
    result = []
    
    for isbn_key, titles in case2c.items():
        isbn10, isbn13 = isbn_key.split("|")
        isbns = []
        if isbn10:
            isbns.append(isbn10)
        if isbn13:
            isbns.append(isbn13)
        
        result.append({
            "isbns": isbns,
            "titles": titles
        })
    
    return result


def run_analysis(db1_path: Path, db2_path: Path) -> AnalysisResults:
    """
    Run the full ISBN analysis on both databases.
    """
    results = AnalysisResults()
    api_cache: Dict[str, Any] = {}  # Cache API results by title
    
    databases = [
        ("db1", db1_path),
        ("db2", db2_path)
    ]
    
    for db_name, db_path in databases:
        print(f"\n{'=' * 60}")
        print(f"Processing {db_name}: {db_path}")
        print('=' * 60)
        
        for table_name in TABLES_TO_PROCESS:
            print(f"\n  Table: {table_name}")
            count = load_and_process_table(
                db_path, db_name, table_name, results, api_cache
            )
            print(f"  Processed {count} entries")
    
    # Sort Case 1 by table count
    results.case1_with_isbn = sort_case1_by_table_count(results.case1_with_isbn)
    
    return results


def save_results(results: AnalysisResults, output_dir: Path) -> None:
    """
    Save all results to JSON files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Case 1: Books with ISBN
    case1_file = output_dir / "case1_with_isbn.json"
    with open(case1_file, "w", encoding="utf-8") as f:
        json.dump(results.case1_with_isbn, f, indent=2, ensure_ascii=False)
    print(f"  Saved Case 1 to: {case1_file}")
    
    # Case 2A: No ISBN found
    case2a_file = output_dir / "case2a_no_isbn_found.json"
    with open(case2a_file, "w", encoding="utf-8") as f:
        json.dump(results.case2a_no_isbn_found, f, indent=2, ensure_ascii=False)
    print(f"  Saved Case 2A to: {case2a_file}")
    
    # Case 2B: Multiple ISBNs
    case2b_file = output_dir / "case2b_multiple_isbns.json"
    with open(case2b_file, "w", encoding="utf-8") as f:
        json.dump(results.case2b_multiple_isbns, f, indent=2, ensure_ascii=False)
    print(f"  Saved Case 2B to: {case2b_file}")
    
    # Case 2C: Single ISBN (converted for JSON)
    case2c_file = output_dir / "case2c_single_isbn.json"
    case2c_converted = convert_case2c_for_json(results.case2c_single_isbn)
    with open(case2c_file, "w", encoding="utf-8") as f:
        json.dump(case2c_converted, f, indent=2, ensure_ascii=False)
    print(f"  Saved Case 2C to: {case2c_file}")
    
    # Statistics
    stats_file = output_dir / "analysis_stats.json"
    with open(stats_file, "w", encoding="utf-8") as f:
        json.dump(results.stats, f, indent=2)
    print(f"  Saved stats to: {stats_file}")
    
    # Summary report
    summary_file = output_dir / "analysis_summary.txt"
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write("ISBN Analysis Summary\n")
        f.write("=" * 60 + "\n\n")
        
        f.write("Statistics:\n")
        for key, value in results.stats.items():
            f.write(f"  {key}: {value}\n")
        
        f.write(f"\nResults Summary:\n")
        f.write(f"  Case 1 (Has ISBN): {len(results.case1_with_isbn)} unique ISBNs\n")
        f.write(f"  Case 2A (No ISBN found): {len(results.case2a_no_isbn_found)} titles\n")
        f.write(f"  Case 2B (Multiple ISBNs): {len(results.case2b_multiple_isbns)} titles\n")
        f.write(f"  Case 2C (Single ISBN): {len(results.case2c_single_isbn)} ISBN pairs\n")
    
    print(f"  Saved summary to: {summary_file}")


def print_summary(results: AnalysisResults) -> None:
    """Print a summary of the analysis results."""
    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)
    
    print("\nStatistics:")
    for key, value in results.stats.items():
        print(f"  {key.replace('_', ' ').title()}: {value}")
    
    print(f"\nResults Summary:")
    print(f"  Case 1 (Has ISBN): {len(results.case1_with_isbn)} unique ISBNs")
    print(f"  Case 2A (No ISBN found via API): {len(results.case2a_no_isbn_found)} titles")
    print(f"  Case 2B (Multiple ISBNs from API): {len(results.case2b_multiple_isbns)} titles")
    print(f"  Case 2C (Single ISBN from API): {len(results.case2c_single_isbn)} ISBN pairs")
    
    # Show top entries from Case 1
    if results.case1_with_isbn:
        print(f"\nTop 5 ISBNs appearing in most tables:")
        for i, (isbn, tables) in enumerate(list(results.case1_with_isbn.items())[:5]):
            print(f"  {i+1}. {isbn}: appears in {len(tables)} tables")
            for t in tables:
                print(f"       - {t}")


def main():
    """Main entry point."""
    print("=" * 60)
    print("ISBN ANALYZER")
    print("Processing: Books, Books1, MissingBooks, NB")
    print("=" * 60)
    
    # Get paths from config
    try:
        db1_path = get_path("db1")
        db2_path = get_path("db2")
    except (FileNotFoundError, KeyError) as e:
        print(f"[ERROR] Could not load config: {e}")
        print("Make sure config.json exists with 'paths.db1' and 'paths.db2'")
        sys.exit(1)
    
    print(f"\nDatabase 1: {db1_path}")
    print(f"Database 2: {db2_path}")
    
    # Verify paths exist
    if not db1_path.exists():
        print(f"[ERROR] db1 path does not exist: {db1_path}")
        sys.exit(1)
    if not db2_path.exists():
        print(f"[ERROR] db2 path does not exist: {db2_path}")
        sys.exit(1)
    
    # Run analysis
    results = run_analysis(db1_path, db2_path)
    
    # Print summary
    print_summary(results)
    
    # Save results
    output_dir = Path("isbn_analysis_results")
    print(f"\nSaving results to: {output_dir}/")
    save_results(results, output_dir)
    
    print("\n[DONE] Analysis complete!")


if __name__ == "__main__":
    main()