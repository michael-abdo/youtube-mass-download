#!/usr/bin/env python3

# Simple test to verify imports work
try:
    from utils.config import ensure_directory
    print("✅ Test 1 PASSED: ensure_directory import works")
except ImportError as e:
    print(f"❌ Test 1 FAILED: {e}")

try:
    import download_all_minimal
    print("✅ Test 2 PASSED: download_all_minimal import works")
except ImportError as e:
    print(f"❌ Test 2 FAILED: {e}")

try:
    from pathlib import Path
    test_dir = Path('test_dir')
    ensure_directory(test_dir)
    print("✅ Test 3 PASSED: ensure_directory function works")
    if test_dir.exists():
        test_dir.rmdir()
except Exception as e:
    print(f"❌ Test 3 FAILED: {e}")

try:
    from download_all_minimal import MinimalDownloader
    md = MinimalDownloader(output_dir='test_output')
    print("✅ Test 4 PASSED: MinimalDownloader instantiation works")
    import shutil
    if Path('test_output').exists():
        shutil.rmtree('test_output')
except Exception as e:
    print(f"❌ Test 4 FAILED: {e}")

print("\nAll tests completed!")