#!/usr/bin/env python3
"""Inline DRY refactoring validation"""

import os
import sys
import time
from datetime import datetime

# Change to the project directory
os.chdir('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal')

print("="*60)
print(f"DRY Refactoring Validation Suite - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)

passed = 0
failed = 0
total_time = 0.0
results = []

# Test 1: Path setup import
print("\nTesting: Path setup import")
start_time = time.time()
try:
    from utils.path_setup import setup_project_path
    elapsed = time.time() - start_time
    print(f"✅ PASS: Path setup import ({elapsed:.3f}s)")
    passed += 1
    results.append(("Path setup import", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: Path setup import - {str(e)}")
    failed += 1
    results.append(("Path setup import", False, elapsed))
total_time += elapsed

# Test 2: Sanitization import
print("\nTesting: Sanitization import")
start_time = time.time()
try:
    from utils.sanitization import sanitize_filename
    elapsed = time.time() - start_time
    print(f"✅ PASS: Sanitization import ({elapsed:.3f}s)")
    passed += 1
    results.append(("Sanitization import", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: Sanitization import - {str(e)}")
    failed += 1
    results.append(("Sanitization import", False, elapsed))
total_time += elapsed

# Test 3: Pattern extraction import
print("\nTesting: Pattern extraction import")
start_time = time.time()
try:
    from utils.patterns import extract_youtube_id, extract_drive_id
    elapsed = time.time() - start_time
    print(f"✅ PASS: Pattern extraction import ({elapsed:.3f}s)")
    passed += 1
    results.append(("Pattern extraction import", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: Pattern extraction import - {str(e)}")
    failed += 1
    results.append(("Pattern extraction import", False, elapsed))
total_time += elapsed

# Test 4: CSV manager import
print("\nTesting: CSV manager import")
start_time = time.time()
try:
    from utils.csv_manager import safe_csv_read
    elapsed = time.time() - start_time
    print(f"✅ PASS: CSV manager import ({elapsed:.3f}s)")
    passed += 1
    results.append(("CSV manager import", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: CSV manager import - {str(e)}")
    failed += 1
    results.append(("CSV manager import", False, elapsed))
total_time += elapsed

# Test 5: Config utilities import
print("\nTesting: Config utilities import")
start_time = time.time()
try:
    from utils.config import ensure_directory, get_config
    elapsed = time.time() - start_time
    print(f"✅ PASS: Config utilities import ({elapsed:.3f}s)")
    passed += 1
    results.append(("Config utilities import", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: Config utilities import - {str(e)}")
    failed += 1
    results.append(("Config utilities import", False, elapsed))
total_time += elapsed

# Test 6: Logging import
print("\nTesting: Logging import")
start_time = time.time()
try:
    from utils.logging_config import get_logger
    elapsed = time.time() - start_time
    print(f"✅ PASS: Logging import ({elapsed:.3f}s)")
    passed += 1
    results.append(("Logging import", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: Logging import - {str(e)}")
    failed += 1
    results.append(("Logging import", False, elapsed))
total_time += elapsed

# Test 7: Retry utils import
print("\nTesting: Retry utils import")
start_time = time.time()
try:
    from utils.retry_utils import retry_with_backoff
    elapsed = time.time() - start_time
    print(f"✅ PASS: Retry utils import ({elapsed:.3f}s)")
    passed += 1
    results.append(("Retry utils import", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: Retry utils import - {str(e)}")
    failed += 1
    results.append(("Retry utils import", False, elapsed))
total_time += elapsed

# Test 8: FileMapper import
print("\nTesting: FileMapper import")
start_time = time.time()
try:
    from utils.comprehensive_file_mapper import FileMapper
    elapsed = time.time() - start_time
    print(f"✅ PASS: FileMapper import ({elapsed:.3f}s)")
    passed += 1
    results.append(("FileMapper import", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: FileMapper import - {str(e)}")
    failed += 1
    results.append(("FileMapper import", False, elapsed))
total_time += elapsed

# Test 9: Validation functions import
print("\nTesting: Validation functions import")
start_time = time.time()
try:
    from utils.validation import is_valid_youtube_url, is_valid_drive_url, get_url_type
    elapsed = time.time() - start_time
    print(f"✅ PASS: Validation functions import ({elapsed:.3f}s)")
    passed += 1
    results.append(("Validation functions import", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: Validation functions import - {str(e)}")
    failed += 1
    results.append(("Validation functions import", False, elapsed))
total_time += elapsed

# Test 10: Sanitization works
print("\nTesting: Sanitization works")
start_time = time.time()
try:
    from utils.sanitization import sanitize_filename
    result = sanitize_filename('Test/File:Name')
    elapsed = time.time() - start_time
    print(f"✅ PASS: Sanitization works - Result: '{result}' ({elapsed:.3f}s)")
    passed += 1
    results.append(("Sanitization works", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: Sanitization works - {str(e)}")
    failed += 1
    results.append(("Sanitization works", False, elapsed))
total_time += elapsed

# Test 11: YouTube ID extraction works
print("\nTesting: YouTube ID extraction works")
start_time = time.time()
try:
    from utils.patterns import extract_youtube_id
    result = extract_youtube_id('https://youtube.com/watch?v=abc123')
    elapsed = time.time() - start_time
    print(f"✅ PASS: YouTube ID extraction works - Result: '{result}' ({elapsed:.3f}s)")
    passed += 1
    results.append(("YouTube ID extraction works", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: YouTube ID extraction works - {str(e)}")
    failed += 1
    results.append(("YouTube ID extraction works", False, elapsed))
total_time += elapsed

# Test 12: Directory creation works
print("\nTesting: Directory creation works")
start_time = time.time()
try:
    from utils.config import ensure_directory
    ensure_directory('/tmp/test_dir')
    elapsed = time.time() - start_time
    print(f"✅ PASS: Directory creation works ({elapsed:.3f}s)")
    passed += 1
    results.append(("Directory creation works", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: Directory creation works - {str(e)}")
    failed += 1
    results.append(("Directory creation works", False, elapsed))
total_time += elapsed

# Test 13: Logger creation works
print("\nTesting: Logger creation works")
start_time = time.time()
try:
    from utils.logging_config import get_logger
    logger = get_logger('test')
    elapsed = time.time() - start_time
    print(f"✅ PASS: Logger creation works ({elapsed:.3f}s)")
    passed += 1
    results.append(("Logger creation works", True, elapsed))
except Exception as e:
    elapsed = time.time() - start_time
    print(f"❌ FAIL: Logger creation works - {str(e)}")
    failed += 1
    results.append(("Logger creation works", False, elapsed))
total_time += elapsed

print(f"\n{'='*60}")
print(f"SUMMARY: {passed} passed, {failed} failed (Total time: {total_time:.2f}s)")
print(f"{'='*60}")

# Print failed tests
if failed > 0:
    print("\nFailed tests:")
    for desc, success, elapsed in results:
        if not success:
            print(f"  - {desc}")
else:
    print("\n🎉 All tests passed! DRY refactoring validation successful.")

# Exit with appropriate code
sys.exit(0 if failed == 0 else 1)