#!/usr/bin/env python3
"""
Test runner for the ETL Lakehouse project.
Runs all unit tests and generates coverage reports.
"""

import sys
import os
import unittest
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure logging for tests
logging.basicConfig(
    level=logging.WARNING,  # Reduce noise during tests
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def discover_and_run_tests(test_directory: str = "unit", verbosity: int = 2):
    """
    Discover and run tests from specified directory.
    
    Args:
        test_directory: Directory containing tests (unit, integration)
        verbosity: Test output verbosity (0=quiet, 1=normal, 2=verbose)
    """
    # Get test directory path
    test_dir = Path(__file__).parent / test_directory
    
    if not test_dir.exists():
        print(f"Test directory {test_dir} does not exist!")
        return False
    
    print(f"\n{'='*60}")
    print(f"RUNNING {test_directory.upper()} TESTS")
    print(f"{'='*60}")
    print(f"Test directory: {test_dir}")
    
    # Discover tests
    loader = unittest.TestLoader()
    suite = loader.discover(
        start_dir=str(test_dir),
        pattern='test_*.py',
        top_level_dir=str(project_root)
    )
    
    # Count tests
    test_count = suite.countTestCases()
    print(f"Discovered {test_count} tests")
    
    if test_count == 0:
        print("No tests found!")
        return True
    
    # Run tests
    runner = unittest.TextTestRunner(
        verbosity=verbosity,
        stream=sys.stdout,
        failfast=False,
        buffer=True
    )
    
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"TEST SUMMARY - {test_directory.upper()}")
    print(f"{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    
    if result.failures:
        print(f"\nFAILED TESTS:")
        for test, traceback in result.failures:
            print(f"  - {test}")
    
    if result.errors:
        print(f"\nERROR TESTS:")
        for test, traceback in result.errors:
            print(f"  - {test}")
    
    # Return success status
    return len(result.failures) == 0 and len(result.errors) == 0

def run_specific_test(test_module: str, test_class: str = None, test_method: str = None):
    """
    Run a specific test module, class, or method.
    
    Args:
        test_module: Module name (e.g., 'test_ingestion.test_raw_loader')
        test_class: Optional class name (e.g., 'TestRawDataLoader')
        test_method: Optional method name (e.g., 'test_validate_input_data_valid')
    """
    print(f"\n{'='*60}")
    print(f"RUNNING SPECIFIC TEST")
    print(f"{'='*60}")
    
    # Build test name
    test_name = f"tests.unit.{test_module}"
    if test_class:
        test_name += f".{test_class}"
        if test_method:
            test_name += f".{test_method}"
    
    print(f"Test: {test_name}")
    
    try:
        # Load and run specific test
        loader = unittest.TestLoader()
        if test_method and test_class:
            suite = loader.loadTestsFromName(test_name)
        elif test_class:
            suite = loader.loadTestsFromName(test_name)
        else:
            suite = loader.loadTestsFromName(test_name)
        
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        
        return len(result.failures) == 0 and len(result.errors) == 0
        
    except Exception as e:
        print(f"Error running test {test_name}: {e}")
        return False

def main():
    """Main test runner entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run ETL Lakehouse tests")
    parser.add_argument(
        '--type', 
        choices=['unit', 'integration', 'all'],
        default='unit',
        help='Type of tests to run'
    )
    parser.add_argument(
        '--module',
        help='Specific test module to run (e.g., test_ingestion.test_raw_loader)'
    )
    parser.add_argument(
        '--class',
        dest='test_class',
        help='Specific test class to run'
    )
    parser.add_argument(
        '--method',
        help='Specific test method to run'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Quiet output'
    )
    
    args = parser.parse_args()
    
    # Determine verbosity
    if args.quiet:
        verbosity = 0
    elif args.verbose:
        verbosity = 2
    else:
        verbosity = 1
    
    success = True
    
    # Run specific test if specified
    if args.module:
        success = run_specific_test(args.module, args.test_class, args.method)
    else:
        # Run test suites
        if args.type in ['unit', 'all']:
            success &= discover_and_run_tests('unit', verbosity)
        
        if args.type in ['integration', 'all']:
            success &= discover_and_run_tests('integration', verbosity)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main() 