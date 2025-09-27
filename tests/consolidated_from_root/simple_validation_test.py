#!/usr/bin/env python3
"""Simple DRY refactoring validation test"""

import sys
import os
import time
from datetime import datetime

# Change to project directory
os.chdir('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal')

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

print("="*70)
print(f"DRY REFACTORING VALIDATION TEST - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*70)

# Initialize results tracking
results = []
total_tests = 0
passed_tests = 0

def run_test(test_name, test_func):
    """Run a single test and record results"""
    global total_tests, passed_tests
    total_tests += 1
    
    print(f"\n{total_tests:2d}. Testing: {test_name}")
    start_time = time.time()
    
    try:
        result = test_func()
        elapsed = time.time() - start_time
        
        if result:
            print(f"    ✅ PASS ({elapsed:.3f}s)")
            passed_tests += 1
            results.append((test_name, True, elapsed, None))
        else:
            print(f"    ❌ FAIL ({elapsed:.3f}s)")
            results.append((test_name, False, elapsed, "Test returned False"))
            
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"    ❌ FAIL ({elapsed:.3f}s) - {str(e)}")
        results.append((test_name, False, elapsed, str(e)))

# Test 1: Path setup import
def test_path_setup():
    from utils.path_setup import setup_project_path
    return True

run_test("Path setup import", test_path_setup)

# Test 2: Sanitization import
def test_sanitization():
    from utils.sanitization import sanitize_filename
    return True

run_test("Sanitization import", test_sanitization)

# Test 3: Pattern extraction import
def test_patterns():
    from utils.patterns import extract_youtube_id, extract_drive_id
    return True

run_test("Pattern extraction import", test_patterns)

# Test 4: CSV manager import
def test_csv_manager():
    from utils.csv_manager import safe_csv_read
    return True

run_test("CSV manager import", test_csv_manager)

# Test 5: Config utilities import
def test_config():
    from utils.config import ensure_directory, get_config
    return True

run_test("Config utilities import", test_config)

# Test 6: Logging import
def test_logging():
    from utils.logging_config import get_logger
    return True

run_test("Logging import", test_logging)

# Test 7: Retry utils import
def test_retry():
    from utils.retry_utils import retry_with_backoff
    return True

run_test("Retry utils import", test_retry)

# Test 8: FileMapper import
def test_filemapper():
    from utils.comprehensive_file_mapper import FileMapper
    return True

run_test("FileMapper import", test_filemapper)

# Test 9: Validation functions import
def test_validation():
    from utils.validation import is_valid_youtube_url, is_valid_drive_url, get_url_type
    return True

run_test("Validation functions import", test_validation)

# Test 10: Sanitization functionality
def test_sanitization_func():
    from utils.sanitization import sanitize_filename
    result = sanitize_filename('Test/File:Name')
    print(f"    Result: '{result}'")
    return len(result) > 0

run_test("Sanitization functionality", test_sanitization_func)

# Test 11: YouTube ID extraction functionality
def test_youtube_extraction():
    from utils.patterns import extract_youtube_id
    result = extract_youtube_id('https://youtube.com/watch?v=abc123')
    print(f"    Result: '{result}'")
    return result == 'abc123'

run_test("YouTube ID extraction functionality", test_youtube_extraction)

# Test 12: Drive ID extraction functionality
def test_drive_extraction():
    from utils.patterns import extract_drive_id
    result = extract_drive_id('https://drive.google.com/file/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/view')
    print(f"    Result: '{result}'")
    return result == '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'

run_test("Drive ID extraction functionality", test_drive_extraction)

# Test 13: Directory creation functionality
def test_directory_creation():
    from utils.config import ensure_directory
    import tempfile
    test_dir = os.path.join(tempfile.gettempdir(), 'test_dir_validation')
    ensure_directory(test_dir)
    exists = os.path.exists(test_dir)
    if exists:
        os.rmdir(test_dir)  # Clean up
    return exists

run_test("Directory creation functionality", test_directory_creation)

# Test 14: Logger creation functionality
def test_logger_creation():
    from utils.logging_config import get_logger
    logger = get_logger('test_validation')
    return logger is not None

run_test("Logger creation functionality", test_logger_creation)

# Summary
print(f"\n{'='*70}")
print(f"VALIDATION SUMMARY")
print(f"{'='*70}")
print(f"Total tests: {total_tests}")
print(f"Passed: {passed_tests}")
print(f"Failed: {total_tests - passed_tests}")
print(f"Success rate: {(passed_tests/total_tests)*100:.1f}%")

if passed_tests == total_tests:
    print(f"\n🎉 ALL TESTS PASSED! DRY refactoring validation successful!")
    print(f"All {total_tests} consolidated imports and functions work correctly.")
else:
    print(f"\n❌ {total_tests - passed_tests} tests failed. See details above.")
    print(f"\nFailed tests:")
    for test_name, success, elapsed, error in results:
        if not success:
            print(f"  - {test_name}: {error}")

# Write results to file
with open('validation_results.txt', 'w') as f:
    f.write(f"DRY Refactoring Validation Results - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("="*70 + "\n")
    f.write(f"Total tests: {total_tests}\n")
    f.write(f"Passed: {passed_tests}\n")
    f.write(f"Failed: {total_tests - passed_tests}\n")
    f.write(f"Success rate: {(passed_tests/total_tests)*100:.1f}%\n")
    f.write("\nDetailed results:\n")
    for i, (test_name, success, elapsed, error) in enumerate(results, 1):
        status = "PASS" if success else "FAIL"
        f.write(f"{i:2d}. {test_name}: {status} ({elapsed:.3f}s)\n")
        if error:
            f.write(f"    Error: {error}\n")

print(f"\n📄 Detailed results written to: validation_results.txt")
print(f"{'='*70}")

# Exit with appropriate code
sys.exit(0 if passed_tests == total_tests else 1)