#!/usr/bin/env python3
"""Run DRY refactoring validation"""

import os
import sys
import time
from datetime import datetime

# Change to the project directory
os.chdir('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal')

def test_import(module_path, description):
    """Test a single import"""
    print(f"\nTesting: {description}")
    start_time = time.time()
    
    try:
        exec(f"from {module_path} import *")
        elapsed = time.time() - start_time
        print(f"✅ PASS: {description} ({elapsed:.3f}s)")
        return True, elapsed
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ FAIL: {description} - {str(e)}")
        return False, elapsed

def test_function(code, description):
    """Test a function call"""
    print(f"\nTesting: {description}")
    start_time = time.time()
    
    try:
        exec(code)
        elapsed = time.time() - start_time
        print(f"✅ PASS: {description} ({elapsed:.3f}s)")
        return True, elapsed
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ FAIL: {description} - {str(e)}")
        return False, elapsed

# Test suite
tests = [
    # Core imports
    ("utils.path_setup", "Path setup import"),
    ("utils.sanitization", "Sanitization import"),
    ("utils.patterns", "Pattern extraction import"),
    ("utils.csv_manager", "CSV manager import"),
    ("utils.config", "Config utilities import"),
    ("utils.logging_config", "Logging import"),
    ("utils.retry_utils", "Retry utils import"),
    ("utils.comprehensive_file_mapper", "FileMapper import"),
    ("utils.validation", "Validation functions import"),
]

# Function tests
function_tests = [
    ("from utils.sanitization import sanitize_filename; print(sanitize_filename('Test/File:Name'))", "Sanitization works"),
    ("from utils.patterns import extract_youtube_id; print(extract_youtube_id('https://youtube.com/watch?v=abc123'))", "YouTube ID extraction works"),
    ("from utils.config import ensure_directory; ensure_directory('/tmp/test_dir')", "Directory creation works"),
    ("from utils.logging_config import get_logger; logger = get_logger('test')", "Logger creation works"),
]

print("="*60)
print(f"DRY Refactoring Validation Suite - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)

passed = 0
failed = 0
total_time = 0.0
results = []

# Test imports
for module_path, desc in tests:
    success, elapsed = test_import(module_path, desc)
    results.append((desc, success, elapsed))
    total_time += elapsed
    if success:
        passed += 1
    else:
        failed += 1

# Test functions
for code, desc in function_tests:
    success, elapsed = test_function(code, desc)
    results.append((desc, success, elapsed))
    total_time += elapsed
    if success:
        passed += 1
    else:
        failed += 1

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