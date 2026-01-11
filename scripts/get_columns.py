"""
Extract column schemas from database directories.

This script reads CSV headers from one or more database directories
and exports the schema information to a JSON file. Useful for:
- Documenting database structure
- Comparing schemas across databases
- Planning merge operations
"""
import json
import sys
from pathlib import Path
import pandas as pd
from database_utils.reporting import print_section, print_success, print_error

# Path to config file
CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.json"


def load_config() -> dict:
    """
    Load configuration from config.json.
    
    Returns:
        Configuration dictionary
    """
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_columns_from_csv(csv_file: Path) -> list:
    """
    Read column names from a CSV file header.
    
    Args:
        csv_file: Path to CSV file
        
    Returns:
        List of column names, or error message if read fails
    """
    try:
        # Read only the header row (nrows=0)
        df = pd.read_csv(csv_file, nrows=0)
        return list(df.columns)
    except Exception as e:
        return [f"ERROR reading {csv_file.name}: {e}"]


def extract_database_schema(db_path: Path) -> dict:
    """
    Extract schema (table names and columns) from a database directory.
    
    Args:
        db_path: Path to database directory containing CSV files
        
    Returns:
        Dictionary mapping table names to column lists
        
    Raises:
        ValueError: If path doesn't exist or isn't a directory
    """
    if not db_path.exists():
        raise ValueError(f"Path not found: {db_path}")
    
    if not db_path.is_dir():
        raise ValueError(f"Not a directory: {db_path}")

    schema = {}
    
    # Process each CSV file in the directory
    for csv_file in sorted(db_path.glob("*.csv")):
        table_name = csv_file.stem  # Filename without extension
        schema[table_name] = get_columns_from_csv(csv_file)

    return schema


def validate_database_keys(requested_keys: list, available_keys: dict) -> list:
    """
    Validate that requested database keys exist in config.
    
    Args:
        requested_keys: List of keys requested by user
        available_keys: Dictionary of available paths from config
        
    Returns:
        List of missing keys (empty if all valid)
    """
    return [key for key in requested_keys if key not in available_keys]


def generate_output_filename(db_keys: list) -> Path:
    """
    Generate output filename based on database keys.
    
    Args:
        db_keys: List of database keys
        
    Returns:
        Path object for output JSON file
    """
    name_part = "_".join(db_keys)
    return Path(__file__).resolve().parents[1] / f"schema_{name_part}.json"


def main():
    """Main entry point for schema extraction."""
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python get_columns.py <db_key1> [db_key2] [db_key3] ...")
        print("\nExamples:")
        print("  python get_columns.py db1")
        print("  python get_columns.py db1 db2")
        print("  python get_columns.py merged")
        print("\nExtract column schemas from database directories defined in config.json")
        sys.exit(1)

    # Get database keys from arguments
    db_keys = sys.argv[1:]
    
    print_section("Database Schema Extraction")
    
    # Load configuration
    try:
        config = load_config()
        paths = config.get("paths", {})
    except Exception as e:
        print_error(f"Failed to load config.json: {e}")
        sys.exit(1)
    
    # Validate all requested keys exist
    missing_keys = validate_database_keys(db_keys, paths)
    if missing_keys:
        print_error(f"Keys not found in config.json: {missing_keys}")
        print(f"Available keys: {list(paths.keys())}")
        sys.exit(1)
    
    # Extract schemas for each database
    result = {}
    for key in db_keys:
        db_path = Path(paths[key])
        print(f"\nExtracting schema from: {key} ({db_path})")
        
        try:
            schema = extract_database_schema(db_path)
            result[key] = schema
            print_success(f"Found {len(schema)} tables in {key}")
        except Exception as e:
            print_error(f"Failed to extract schema: {e}", key)
            sys.exit(1)
    
    # Generate output filename
    output_file = generate_output_filename(db_keys)
    
    # Write result to JSON file
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"\nSchemas extracted for: {', '.join(db_keys)}")
        print_success(f"Output saved to: {output_file}")
        
    except Exception as e:
        print_error(f"Failed to write output file: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()