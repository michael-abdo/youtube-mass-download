#!/usr/bin/env python3
"""Test script to validate ensure_directory import and functionality"""

print("Testing ensure_directory import and functionality...")
print("=" * 70)

# Test 1: Import ensure_directory
try:
    from utils.config import ensure_directory
    print("✅ Test 1: ensure_directory import works")
except Exception as e:
    print(f"❌ Test 1: ensure_directory import failed: {e}")
    exit(1)

# Test 2: Import download_all_minimal
try:
    import download_all_minimal
    print("✅ Test 2: download_all_minimal import works with ensure_directory")
except Exception as e:
    print(f"❌ Test 2: download_all_minimal import failed: {e}")
    exit(1)

# Test 3: Test ensure_directory function
try:
    from pathlib import Path
    test_dir = Path('test_dir')
    ensure_directory(test_dir)
    if test_dir.exists():
        print("✅ Test 3: ensure_directory function works")
        # Clean up
        test_dir.rmdir()
    else:
        print("❌ Test 3: ensure_directory function failed - directory not created")
except Exception as e:
    print(f"❌ Test 3: ensure_directory function failed: {e}")

# Test 4: Test MinimalDownloader class
try:
    from download_all_minimal import MinimalDownloader
    md = MinimalDownloader(output_dir='test_output')
    print("✅ Test 4: MinimalDownloader works with ensure_directory")
    # Clean up
    import shutil
    if Path('test_output').exists():
        shutil.rmtree('test_output')
except Exception as e:
    print(f"❌ Test 4: MinimalDownloader failed: {e}")

print("\n" + "=" * 70)
print("All tests completed!")