#!/usr/bin/env python3
"""Test script to validate the sanitize_filename import in download_all_minimal.py"""

print("Testing imports...")

try:
    import download_all_minimal
    print("✅ download_all_minimal import successful")
except ImportError as e:
    print(f"❌ download_all_minimal import failed: {e}")

try:
    from download_all_minimal import MinimalDownloader
    print("✅ MinimalDownloader import successful")
except ImportError as e:
    print(f"❌ MinimalDownloader import failed: {e}")

try:
    from utils.sanitization import sanitize_filename
    print("✅ sanitize_filename import successful")
except ImportError as e:
    print(f"❌ sanitize_filename import failed: {e}")

try:
    test_result = sanitize_filename('Test/Name:123')
    print(f"✅ sanitize_filename function test: 'Test/Name:123' -> '{test_result}'")
except Exception as e:
    print(f"❌ sanitize_filename function test failed: {e}")

print("\nTesting help command...")
try:
    import subprocess
    result = subprocess.run(['python3', 'download_all_minimal.py', '--help'], 
                          capture_output=True, text=True, timeout=10)
    if result.returncode == 0:
        print("✅ --help command works")
        print(f"Help output (first 200 chars): {result.stdout[:200]}...")
    else:
        print(f"❌ --help command failed with code {result.returncode}")
        print(f"Error: {result.stderr}")
except Exception as e:
    print(f"❌ Help command test failed: {e}")

print("\nAll tests completed!")