"""
Clean CSV files by removing metadata lines from the beginning and end.

This script removes Access export metadata that appears at the start
and end of CSV files exported from Microsoft Access databases.
"""
from pathlib import Path
from database_utils.config import get_path, get_config_value
from database_utils.reporting import print_section, print_success, print_error


def clean_csv_file(file: Path, first: int, last: int) -> bool:
    """
    Remove first N and last M lines from a CSV file.
    
    Args:
        file: Path to CSV file
        first: Number of lines to remove from start
        last: Number of lines to remove from end
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Read all lines from file
        lines = file.read_text(encoding="utf-8", errors="ignore").splitlines()

        # Check if file has enough lines
        if len(lines) <= first + last:
            print_error(f"Too few lines ({len(lines)}), skipping", file.name)
            return False

        # Extract the middle portion (remove first and last)
        cleaned = lines[first:len(lines)-last]

        # Write cleaned content back to file
        file.write_text("\n".join(cleaned), encoding="utf-8")
        print_success(f"Cleaned {file.name}")
        return True
        
    except Exception as e:
        print_error(str(e), file.name)
        return False


def clean_folder(folder: Path, first: int, last: int) -> None:
    """
    Clean all CSV files in a folder.
    
    Args:
        folder: Directory containing CSV files
        first: Number of lines to remove from start of each file
        last: Number of lines to remove from end of each file
    """
    csv_files = list(folder.glob("*.csv"))
    
    if not csv_files:
        print_error(f"No CSV files found in {folder}")
        return
    
    # Process each CSV file
    success_count = 0
    for file in csv_files:
        if clean_csv_file(file, first, last):
            success_count += 1
    
    print(f"\nCleaned {success_count}/{len(csv_files)} files successfully")


def main():
    """Main entry point for cleaning CSV files."""
    # Load configuration
    folder1 = get_path("db1")
    folder2 = get_path("db2")
    first = get_config_value("clean_csv", "remove_first_lines")
    last = get_config_value("clean_csv", "remove_last_lines")

    print_section("Cleaning CSV Files")
    print(f"Configuration: remove first {first} lines, last {last} lines\n")

    # Clean both database folders
    print(f"Cleaning: {folder1}")
    clean_folder(folder1, first, last)

    print(f"\nCleaning: {folder2}")
    clean_folder(folder2, first, last)


if __name__ == "__main__":
    main()