#!/usr/bin/env python3
"""Direct DRY refactoring validation - execute inline"""

import os
import sys
import time
from datetime import datetime

# Change to the project directory
os.chdir('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal')

print("="*60)
print(f"DRY Refactoring Validation Suite - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)

def test_import_and_function(test_name, import_statement, function_call=None):
    """Test import and optionally a function call"""
    print(f"\nTesting: {test_name}")
    start_time = time.time()
    
    try:
        # Execute import
        exec(import_statement)
        
        # If function call provided, execute it
        if function_call:
            result = eval(function_call)
            if result is not None:
                print(f"  Result: {result}")
        
        elapsed = time.time() - start_time
        print(f"✅ PASS: {test_name} ({elapsed:.3f}s)")
        return True, elapsed
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ FAIL: {test_name} - {str(e)}")
        return False, elapsed

# Test suite
tests = [
    ("Path setup import", "from utils.path_setup import setup_project_path"),
    ("Sanitization import", "from utils.sanitization import sanitize_filename"),
    ("Pattern extraction import", "from utils.patterns import extract_youtube_id, extract_drive_id"),
    ("CSV manager import", "from utils.csv_manager import safe_csv_read"),
    ("Config utilities import", "from utils.config import ensure_directory, get_config"),
    ("Logging import", "from utils.logging_config import get_logger"),
    ("Retry utils import", "from utils.retry_utils import retry_with_backoff"),
    ("FileMapper import", "from utils.comprehensive_file_mapper import FileMapper"),
    ("Validation functions import", "from utils.validation import is_valid_youtube_url, is_valid_drive_url, get_url_type"),
    ("Sanitization works", "from utils.sanitization import sanitize_filename", "sanitize_filename('Test/File:Name')"),
    ("YouTube ID extraction works", "from utils.patterns import extract_youtube_id", "extract_youtube_id('https://youtube.com/watch?v=abc123')"),
    ("Directory creation works", "from utils.config import ensure_directory", "ensure_directory('/tmp/test_dir')"),
    ("Logger creation works", "from utils.logging_config import get_logger", "get_logger('test')"),
]

passed = 0
failed = 0
total_time = 0.0
failed_tests = []

for test_name, import_stmt, func_call in tests:
    success, elapsed = test_import_and_function(test_name, import_stmt, func_call)
    total_time += elapsed
    if success:
        passed += 1
    else:
        failed += 1
        failed_tests.append(test_name)

print(f"\n{'='*60}")
print(f"SUMMARY: {passed} passed, {failed} failed (Total time: {total_time:.2f}s)")
print(f"{'='*60}")

# Print failed tests
if failed > 0:
    print("\nFailed tests:")
    for test_name in failed_tests:
        print(f"  - {test_name}")
else:
    print("\n🎉 All tests passed! DRY refactoring validation successful.")

# Write results to file
with open('validation_results.txt', 'w') as f:
    f.write(f"DRY Refactoring Validation Results - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("="*60 + "\n")
    f.write(f"SUMMARY: {passed} passed, {failed} failed (Total time: {total_time:.2f}s)\n")
    f.write("="*60 + "\n")
    
    if failed > 0:
        f.write("\nFailed tests:\n")
        for test_name in failed_tests:
            f.write(f"  - {test_name}\n")
    else:
        f.write("\n🎉 All tests passed! DRY refactoring validation successful.\n")

print(f"\nResults written to: validation_results.txt")

# Exit with appropriate code
sys.exit(0 if failed == 0 else 1)

# Execute this script
if __name__ == "__main__":
    pass