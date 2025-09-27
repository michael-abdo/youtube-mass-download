#!/usr/bin/env python3
"""Test DRY refactoring imports directly"""

import sys
import os
import time
from datetime import datetime

# Change to project directory
os.chdir('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal')

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

print("="*60)
print(f"DRY Refactoring Import Test - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)

# Test 1: Path setup import
print("\n1. Testing Path setup import...")
try:
    from utils.path_setup import setup_project_path
    print("✅ PASS: Path setup import successful")
    test1_pass = True
except Exception as e:
    print(f"❌ FAIL: Path setup import - {e}")
    test1_pass = False

# Test 2: Sanitization import
print("\n2. Testing Sanitization import...")
try:
    from utils.sanitization import sanitize_filename
    print("✅ PASS: Sanitization import successful")
    test2_pass = True
except Exception as e:
    print(f"❌ FAIL: Sanitization import - {e}")
    test2_pass = False

# Test 3: Pattern extraction import
print("\n3. Testing Pattern extraction import...")
try:
    from utils.patterns import extract_youtube_id, extract_drive_id
    print("✅ PASS: Pattern extraction import successful")
    test3_pass = True
except Exception as e:
    print(f"❌ FAIL: Pattern extraction import - {e}")
    test3_pass = False

# Test 4: CSV manager import
print("\n4. Testing CSV manager import...")
try:
    from utils.csv_manager import safe_csv_read
    print("✅ PASS: CSV manager import successful")
    test4_pass = True
except Exception as e:
    print(f"❌ FAIL: CSV manager import - {e}")
    test4_pass = False

# Test 5: Config utilities import
print("\n5. Testing Config utilities import...")
try:
    from utils.config import ensure_directory, get_config
    print("✅ PASS: Config utilities import successful")
    test5_pass = True
except Exception as e:
    print(f"❌ FAIL: Config utilities import - {e}")
    test5_pass = False

# Test 6: Logging import
print("\n6. Testing Logging import...")
try:
    from utils.logging_config import get_logger
    print("✅ PASS: Logging import successful")
    test6_pass = True
except Exception as e:
    print(f"❌ FAIL: Logging import - {e}")
    test6_pass = False

# Test 7: Retry utils import
print("\n7. Testing Retry utils import...")
try:
    from utils.retry_utils import retry_with_backoff
    print("✅ PASS: Retry utils import successful")
    test7_pass = True
except Exception as e:
    print(f"❌ FAIL: Retry utils import - {e}")
    test7_pass = False

# Test 8: FileMapper import
print("\n8. Testing FileMapper import...")
try:
    from utils.comprehensive_file_mapper import FileMapper
    print("✅ PASS: FileMapper import successful")
    test8_pass = True
except Exception as e:
    print(f"❌ FAIL: FileMapper import - {e}")
    test8_pass = False

# Test 9: Validation functions import
print("\n9. Testing Validation functions import...")
try:
    from utils.validation import is_valid_youtube_url, is_valid_drive_url, get_url_type
    print("✅ PASS: Validation functions import successful")
    test9_pass = True
except Exception as e:
    print(f"❌ FAIL: Validation functions import - {e}")
    test9_pass = False

# Test 10: Sanitization functionality
print("\n10. Testing Sanitization functionality...")
try:
    from utils.sanitization import sanitize_filename
    result = sanitize_filename('Test/File:Name')
    print(f"✅ PASS: Sanitization works - Result: '{result}'")
    test10_pass = True
except Exception as e:
    print(f"❌ FAIL: Sanitization functionality - {e}")
    test10_pass = False

# Test 11: YouTube ID extraction functionality
print("\n11. Testing YouTube ID extraction functionality...")
try:
    from utils.patterns import extract_youtube_id
    result = extract_youtube_id('https://youtube.com/watch?v=abc123')
    print(f"✅ PASS: YouTube ID extraction works - Result: '{result}'")
    test11_pass = True
except Exception as e:
    print(f"❌ FAIL: YouTube ID extraction functionality - {e}")
    test11_pass = False

# Test 12: Directory creation functionality
print("\n12. Testing Directory creation functionality...")
try:
    from utils.config import ensure_directory
    ensure_directory('/tmp/test_dir')
    print("✅ PASS: Directory creation works")
    test12_pass = True
except Exception as e:
    print(f"❌ FAIL: Directory creation functionality - {e}")
    test12_pass = False

# Test 13: Logger creation functionality
print("\n13. Testing Logger creation functionality...")
try:
    from utils.logging_config import get_logger
    logger = get_logger('test')
    print("✅ PASS: Logger creation works")
    test13_pass = True
except Exception as e:
    print(f"❌ FAIL: Logger creation functionality - {e}")
    test13_pass = False

# Count results
all_tests = [test1_pass, test2_pass, test3_pass, test4_pass, test5_pass, 
             test6_pass, test7_pass, test8_pass, test9_pass, test10_pass, 
             test11_pass, test12_pass, test13_pass]

passed = sum(all_tests)
failed = len(all_tests) - passed

print(f"\n{'='*60}")
print(f"SUMMARY: {passed} passed, {failed} failed")
print(f"{'='*60}")

if failed > 0:
    print("\nFailed tests:")
    test_names = [
        "Path setup import", "Sanitization import", "Pattern extraction import",
        "CSV manager import", "Config utilities import", "Logging import",
        "Retry utils import", "FileMapper import", "Validation functions import",
        "Sanitization functionality", "YouTube ID extraction functionality",
        "Directory creation functionality", "Logger creation functionality"
    ]
    for i, test_pass in enumerate(all_tests):
        if not test_pass:
            print(f"  - {test_names[i]}")
else:
    print("\n🎉 All tests passed! DRY refactoring validation successful.")

# Write results to file
with open('validation_results.txt', 'w') as f:
    f.write(f"DRY Refactoring Validation Results - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("="*60 + "\n")
    f.write(f"SUMMARY: {passed} passed, {failed} failed\n")
    f.write("="*60 + "\n")
    
    if failed > 0:
        f.write("\nFailed tests:\n")
        test_names = [
            "Path setup import", "Sanitization import", "Pattern extraction import",
            "CSV manager import", "Config utilities import", "Logging import",
            "Retry utils import", "FileMapper import", "Validation functions import",
            "Sanitization functionality", "YouTube ID extraction functionality",
            "Directory creation functionality", "Logger creation functionality"
        ]
        for i, test_pass in enumerate(all_tests):
            if not test_pass:
                f.write(f"  - {test_names[i]}\n")
    else:
        f.write("\n🎉 All tests passed! DRY refactoring validation successful.\n")

print(f"\nResults written to: validation_results.txt")