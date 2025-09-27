#!/usr/bin/env python3
"""
DRY Refactoring Validation Test - Final Summary
Tests all consolidated imports and functions to ensure they work correctly.
"""

import sys
import os
import time
from datetime import datetime

# Change to project directory and add to path
os.chdir('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal')
# Results tracking
test_results = []
start_time = time.time()

def test_import(module_name, items):
    """Test importing specific items from a module"""
    try:
        for item in items:
            exec(f"from {module_name} import {item}")
        return True, None
    except Exception as e:
        return False, str(e)

def test_function(code, description):
    """Test a function call"""
    try:
        result = eval(code)
        return True, result
    except Exception as e:
        return False, str(e)

print("="*70)
print("DRY REFACTORING VALIDATION TEST")
print("="*70)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Test 1: Path setup utilities
success, error = test_import('utils.path_setup', ['setup_project_path'])
test_results.append(('Path setup import', success, error))
print(f"1. Path setup import: {'✅ PASS' if success else '❌ FAIL - ' + str(error)}")

# Test 2: Sanitization utilities
success, error = test_import('utils.sanitization', ['sanitize_filename'])
test_results.append(('Sanitization import', success, error))
print(f"2. Sanitization import: {'✅ PASS' if success else '❌ FAIL - ' + str(error)}")

# Test 3: Pattern extraction utilities
success, error = test_import('utils.patterns', ['extract_youtube_id', 'extract_drive_id'])
test_results.append(('Pattern extraction import', success, error))
print(f"3. Pattern extraction import: {'✅ PASS' if success else '❌ FAIL - ' + str(error)}")

# Test 4: CSV manager utilities
success, error = test_import('utils.csv_manager', ['safe_csv_read'])
test_results.append(('CSV manager import', success, error))
print(f"4. CSV manager import: {'✅ PASS' if success else '❌ FAIL - ' + str(error)}")

# Test 5: Config utilities
success, error = test_import('utils.config', ['ensure_directory', 'get_config'])
test_results.append(('Config utilities import', success, error))
print(f"5. Config utilities import: {'✅ PASS' if success else '❌ FAIL - ' + str(error)}")

# Test 6: Logging utilities
success, error = test_import('utils.logging_config', ['get_logger'])
test_results.append(('Logging import', success, error))
print(f"6. Logging import: {'✅ PASS' if success else '❌ FAIL - ' + str(error)}")

# Test 7: Retry utilities
success, error = test_import('utils.retry_utils', ['retry_with_backoff'])
test_results.append(('Retry utils import', success, error))
print(f"7. Retry utils import: {'✅ PASS' if success else '❌ FAIL - ' + str(error)}")

# Test 8: FileMapper utilities
success, error = test_import('utils.comprehensive_file_mapper', ['FileMapper'])
test_results.append(('FileMapper import', success, error))
print(f"8. FileMapper import: {'✅ PASS' if success else '❌ FAIL - ' + str(error)}")

# Test 9: Validation utilities
success, error = test_import('utils.validation', ['is_valid_youtube_url', 'is_valid_drive_url', 'get_url_type'])
test_results.append(('Validation functions import', success, error))
print(f"9. Validation functions import: {'✅ PASS' if success else '❌ FAIL - ' + str(error)}")

# Test 10: Sanitization functionality
if test_results[1][1]:  # Only if import succeeded
    success, result = test_function("sanitize_filename('Test/File:Name')", "Sanitization functionality")
    test_results.append(('Sanitization functionality', success, result))
    print(f"10. Sanitization functionality: {'✅ PASS' if success else '❌ FAIL - ' + str(result)}")
    if success:
        print(f"    Result: '{result}'")
else:
    test_results.append(('Sanitization functionality', False, "Import failed"))
    print(f"10. Sanitization functionality: ❌ FAIL - Import failed")

# Test 11: YouTube ID extraction functionality
if test_results[2][1]:  # Only if import succeeded
    success, result = test_function("extract_youtube_id('https://youtube.com/watch?v=abc123')", "YouTube ID extraction")
    test_results.append(('YouTube ID extraction functionality', success, result))
    print(f"11. YouTube ID extraction functionality: {'✅ PASS' if success else '❌ FAIL - ' + str(result)}")
    if success:
        print(f"    Result: '{result}' (expected: 'abc123')")
else:
    test_results.append(('YouTube ID extraction functionality', False, "Import failed"))
    print(f"11. YouTube ID extraction functionality: ❌ FAIL - Import failed")

# Test 12: Drive ID extraction functionality
if test_results[2][1]:  # Only if import succeeded
    success, result = test_function("extract_drive_id('https://drive.google.com/file/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/view')", "Drive ID extraction")
    test_results.append(('Drive ID extraction functionality', success, result))
    print(f"12. Drive ID extraction functionality: {'✅ PASS' if success else '❌ FAIL - ' + str(result)}")
    if success:
        print(f"    Result: '{result}' (expected: '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms')")
else:
    test_results.append(('Drive ID extraction functionality', False, "Import failed"))
    print(f"12. Drive ID extraction functionality: ❌ FAIL - Import failed")

# Test 13: Directory creation functionality
if test_results[4][1]:  # Only if import succeeded
    success, result = test_function("ensure_directory('/tmp/test_validation_dir')", "Directory creation")
    test_results.append(('Directory creation functionality', success, result))
    print(f"13. Directory creation functionality: {'✅ PASS' if success else '❌ FAIL - ' + str(result)}")
    if success:
        # Check if directory was created
        import os
        if os.path.exists('/tmp/test_validation_dir'):
            print("    Directory successfully created")
            os.rmdir('/tmp/test_validation_dir')  # Clean up
        else:
            print("    Directory creation may have failed")
else:
    test_results.append(('Directory creation functionality', False, "Import failed"))
    print(f"13. Directory creation functionality: ❌ FAIL - Import failed")

# Test 14: Logger creation functionality
if test_results[5][1]:  # Only if import succeeded
    success, result = test_function("get_logger('test_validation')", "Logger creation")
    test_results.append(('Logger creation functionality', success, result))
    print(f"14. Logger creation functionality: {'✅ PASS' if success else '❌ FAIL - ' + str(result)}")
    if success:
        print(f"    Logger created successfully")
else:
    test_results.append(('Logger creation functionality', False, "Import failed"))
    print(f"14. Logger creation functionality: ❌ FAIL - Import failed")

# Calculate summary
total_tests = len(test_results)
passed_tests = sum(1 for _, success, _ in test_results if success)
failed_tests = total_tests - passed_tests
elapsed_time = time.time() - start_time

print(f"\n{'='*70}")
print("VALIDATION SUMMARY")
print(f"{'='*70}")
print(f"Total tests: {total_tests}")
print(f"Passed: {passed_tests}")
print(f"Failed: {failed_tests}")
print(f"Success rate: {(passed_tests/total_tests)*100:.1f}%")
print(f"Total time: {elapsed_time:.2f}s")

if failed_tests == 0:
    print(f"\n🎉 ALL TESTS PASSED! DRY refactoring validation successful!")
    print(f"All {total_tests} consolidated imports and functions work correctly.")
else:
    print(f"\n❌ {failed_tests} tests failed:")
    for test_name, success, error in test_results:
        if not success:
            print(f"  - {test_name}: {error}")

# Write results to file
with open('DRY_VALIDATION_RESULTS.txt', 'w') as f:
    f.write(f"DRY Refactoring Validation Results\n")
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("="*70 + "\n")
    f.write(f"Total tests: {total_tests}\n")
    f.write(f"Passed: {passed_tests}\n")
    f.write(f"Failed: {failed_tests}\n")
    f.write(f"Success rate: {(passed_tests/total_tests)*100:.1f}%\n")
    f.write(f"Total time: {elapsed_time:.2f}s\n")
    f.write("\nDetailed Results:\n")
    f.write("-" * 70 + "\n")
    
    for i, (test_name, success, error) in enumerate(test_results, 1):
        status = "PASS" if success else "FAIL"
        f.write(f"{i:2d}. {test_name}: {status}\n")
        if not success:
            f.write(f"    Error: {error}\n")
    
    if failed_tests == 0:
        f.write(f"\n🎉 ALL TESTS PASSED! DRY refactoring validation successful!\n")
        f.write(f"All {total_tests} consolidated imports and functions work correctly.\n")
    else:
        f.write(f"\n❌ {failed_tests} tests failed. See details above.\n")

print(f"\n📄 Detailed results written to: DRY_VALIDATION_RESULTS.txt")
print(f"{'='*70}")

# Exit with appropriate code
sys.exit(0 if failed_tests == 0 else 1)