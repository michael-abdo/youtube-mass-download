#!/usr/bin/env python3
"""Validate DRY refactoring changes"""

import sys
import subprocess
import time
from datetime import datetime

def run_test(cmd, description):
    """Run a single test and report results"""
    print(f"\nTesting: {description}")
    start_time = time.time()
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        elapsed = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✅ PASS: {description} ({elapsed:.2f}s)")
            return True, elapsed
        else:
            print(f"❌ FAIL: {description}")
            print(f"Error: {result.stderr}")
            return False, elapsed
    except subprocess.TimeoutExpired:
        print(f"❌ TIMEOUT: {description} (>30s)")
        return False, 30.0
    except Exception as e:
        print(f"❌ ERROR: {description} - {str(e)}")
        return False, 0.0

# Test suite
tests = [
    # Core imports
    ("python3 -c 'from utils.path_setup import setup_project_path'", "Path setup import"),
    ("python3 -c 'from utils.sanitization import sanitize_filename'", "Sanitization import"),
    ("python3 -c 'from utils.patterns import extract_youtube_id, extract_drive_id'", "Pattern extraction import"),
    ("python3 -c 'from utils.csv_manager import safe_csv_read'", "CSV manager import"),
    ("python3 -c 'from utils.config import ensure_directory, get_config'", "Config utilities import"),
    ("python3 -c 'from utils.logging_config import get_logger'", "Logging import"),
    ("python3 -c 'from utils.retry_utils import retry_with_backoff'", "Retry utils import"),
    ("python3 -c 'from utils.comprehensive_file_mapper import FileMapper'", "FileMapper import"),
    
    # New validation functions (will fail until implemented)
    ("python3 -c 'from utils.validation import is_valid_youtube_url, is_valid_drive_url, get_url_type'", "Validation functions import"),
    
    # Script help commands
    ("python3 simple_workflow.py --help", "Simple workflow runs"),
    ("python3 download_all_minimal.py --help", "Download script runs"),
    ("python3 check_downloads.py --help", "Check downloads runs"),
    
    # Test actual functionality
    ("python3 -c 'from utils.sanitization import sanitize_filename; print(sanitize_filename(\"Test/File:Name\"))'", "Sanitization works"),
    ("python3 -c 'from utils.patterns import extract_youtube_id; print(extract_youtube_id(\"https://youtube.com/watch?v=abc123\"))'", "YouTube ID extraction works"),
]

print("="*60)
print(f"DRY Refactoring Validation Suite - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)

passed = 0
failed = 0
total_time = 0.0
results = []

for cmd, desc in tests:
    success, elapsed = run_test(cmd, desc)
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

# Exit with appropriate code
sys.exit(0 if failed == 0 else 1)