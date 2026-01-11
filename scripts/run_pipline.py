"""
Run the complete database merge and analysis pipeline.

This script orchestrates the entire process by running scripts in sequence:
1. clean_csv.py - Remove metadata lines from source CSVs
2. merge_tables.py - Merge tables from both databases
3. compare_tables.py - Verify merge and find duplicates
4. match_tables.py - Find related records (optional)

Each step's success/failure is tracked and reported.
"""
import subprocess
import sys
from pathlib import Path
from typing import Dict, List
from database_utils.reporting import (
    print_section,
    print_success,
    print_error,
    print_warning
)


# Define the core pipeline steps
CORE_PIPELINE_STEPS = [
    {
        "name": "Clean CSV files",
        "script": "clean_csv.py",
        "description": "Remove metadata lines from source databases",
        "required": True
    },
    {
        "name": "Merge tables",
        "script": "merge_tables.py",
        "description": "Combine tables from both databases",
        "required": True
    },
    {
        "name": "Find duplicates",
        "script": "compare_tables.py",
        "args": ["--find-duplicates"],
        "description": "Identify duplicate rows between databases",
        "required": True
    },
    {
        "name": "Verify merge",
        "script": "compare_tables.py",
        "args": ["--verify-merge"],
        "description": "Confirm no data lost during merge",
        "required": True
    },
]

# Optional analysis steps
OPTIONAL_PIPELINE_STEPS = [
    {
        "name": "Match Books vs Books1",
        "script": "match_tables.py",
        "args": ["Books", "Books1", "--database", "merged", "--strategy", "books"],
        "description": "Find related book records",
        "required": False
    },
    {
        "name": "Match Journals vs OldJournals",
        "script": "match_tables.py",
        "args": ["Journals", "OldJournals", "--database", "merged", "--strategy", "journals"],
        "description": "Find related journal records",
        "required": False
    },
]


def run_script(
    script_path: Path,
    python_executable: str,
    args: List[str] = None
) -> bool:
    """
    Run a Python script and return whether it succeeded.
    
    Args:
        script_path: Path to the script to run
        python_executable: Path to Python interpreter
        args: Additional command line arguments
        
    Returns:
        True if script ran successfully (exit code 0), False otherwise
    """
    cmd = [python_executable, str(script_path)]
    if args:
        cmd.extend(args)
    
    print(f"\n>>> Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, cwd=script_path.parent)
        
        if result.returncode == 0:
            return True
        else:
            print_warning(f"Exit code: {result.returncode}")
            return False
            
    except Exception as e:
        print_error(f"Failed to execute: {e}")
        return False


def execute_pipeline(
    steps: List[Dict],
    python: str,
    scripts_dir: Path,
    stop_on_error: bool = True
) -> List[Dict]:
    """
    Execute a series of pipeline steps.
    
    Args:
        steps: List of step definitions
        python: Python executable path
        scripts_dir: Directory containing scripts
        stop_on_error: Whether to stop on first error
        
    Returns:
        List of result dictionaries
    """
    results = []
    
    for i, step in enumerate(steps, 1):
        print(f"\n{'=' * 80}")
        print(f"Step {i}/{len(steps)}: {step['name']}")
        print(f"Description: {step['description']}")
        print('=' * 80)
        
        script_path = scripts_dir / step['script']
        
        # Check if script exists
        if not script_path.exists():
            print_error(f"Script not found: {script_path}")
            results.append({
                "step": step['name'],
                "success": False,
                "error": "Script not found"
            })
            
            if stop_on_error and step.get('required', True):
                print_error("Required step failed, stopping pipeline")
                break
            continue
        
        # Run the script
        args = step.get('args', [])
        success = run_script(script_path, python, args)
        
        results.append({
            "step": step['name'],
            "success": success,
            "required": step.get('required', True)
        })
        
        if not success and stop_on_error and step.get('required', True):
            print_error("Required step failed, stopping pipeline")
            break
    
    return results


def print_results_summary(results: List[Dict], section_name: str) -> int:
    """
    Print summary of pipeline results.
    
    Args:
        results: List of result dictionaries
        section_name: Name of the section
        
    Returns:
        Number of successful steps
    """
    if not results:
        return 0
    
    print(f"\n{section_name}:")
    
    success_count = 0
    for i, result in enumerate(results, 1):
        status = "✓ SUCCESS" if result['success'] else "✗ FAILED"
        required = " (required)" if result.get('required', True) else " (optional)"
        print(f"{i}. {result['step']}: {status}{required}")
        
        if result['success']:
            success_count += 1
    
    return success_count


def main():
    """Main entry point for pipeline execution."""
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(
        description="Run the database merge and analysis pipeline"
    )
    parser.add_argument(
        "--skip-matching",
        action="store_true",
        help="Skip optional table matching steps"
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue pipeline even if non-critical steps fail"
    )
    args = parser.parse_args()
    
    print_section("Database Merge and Analysis Pipeline")
    print("This pipeline will process databases through multiple stages\n")
    
    # Get Python executable and script directory
    python = sys.executable
    scripts_dir = Path(__file__).parent
    
    # Execute core pipeline
    print_section("Core Pipeline Steps")
    core_results = execute_pipeline(
        CORE_PIPELINE_STEPS,
        python,
        scripts_dir,
        stop_on_error=not args.continue_on_error
    )
    
    # Check if core pipeline succeeded
    core_success = all(r['success'] for r in core_results if r.get('required', True))
    
    # Execute optional steps if requested and core succeeded
    optional_results = []
    if not args.skip_matching and core_success:
        print_section("Optional Analysis Steps")
        print("Running table matching analysis...\n")
        
        optional_results = execute_pipeline(
            OPTIONAL_PIPELINE_STEPS,
            python,
            scripts_dir,
            stop_on_error=False  # Never stop on optional step failures
        )
    
    # Print final summary
    print_section("Pipeline Summary")
    
    core_success_count = print_results_summary(core_results, "Core Steps")
    core_total = len(core_results)
    
    if optional_results:
        optional_success_count = print_results_summary(optional_results, "Optional Steps")
        optional_total = len(optional_results)
        
        print(f"\nOverall:")
        print(f"   Core: {core_success_count}/{core_total} succeeded")
        print(f"   Optional: {optional_success_count}/{optional_total} succeeded")
        print(f"   Total: {core_success_count + optional_success_count}/{core_total + optional_total} succeeded")
    else:
        print(f"\nCore: {core_success_count}/{core_total} steps completed successfully")
        if args.skip_matching:
            print("   (Optional matching steps skipped)")
    
    # Determine overall status
    required_failed = any(
        not r['success'] and r.get('required', True)
        for r in core_results + optional_results
    )
    
    if required_failed:
        print_warning("Pipeline completed with errors in required steps")
        sys.exit(1)
    elif core_success_count == core_total:
        print_success("Pipeline completed successfully!")
        sys.exit(0)
    else:
        print_warning("Pipeline completed with some failures")
        sys.exit(1)


if __name__ == "__main__":
    main()