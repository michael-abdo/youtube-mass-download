#!/usr/bin/env python3
"""
Test script to validate CSV reading consolidation is working correctly
"""

import sys
import os
from pathlib import Path

def test_safe_csv_read_import():
    """Test that safe_csv_read can be imported from utils.csv_manager"""
    print("1. Testing safe_csv_read import...")
    try:
        from utils.csv_manager import safe_csv_read
        print("   ✅ safe_csv_read import works")
        return True
    except ImportError as e:
        print(f"   ❌ safe_csv_read import failed: {e}")
        return False

def test_download_all_minimal_import():
    """Test that download_all_minimal can be imported"""
    print("2. Testing download_all_minimal import...")
    try:
        import download_all_minimal
        print("   ✅ download_all_minimal import works")
        return True
    except ImportError as e:
        print(f"   ❌ download_all_minimal import failed: {e}")
        return False

def test_safe_csv_read_function():
    """Test that safe_csv_read function works with actual CSV"""
    print("3. Testing safe_csv_read function...")
    try:
        from utils.csv_manager import safe_csv_read
        
        # Check if output.csv exists
        csv_path = 'outputs/output.csv'
        if not os.path.exists(csv_path):
            print(f"   ⚠️  CSV file not found: {csv_path}")
            return False
            
        df = safe_csv_read(csv_path)
        print(f"   ✅ safe_csv_read function works, shape: {df.shape}")
        
        # Show first few columns to verify data
        print(f"   📊 Columns: {list(df.columns)[:5]}...")
        print(f"   📊 First row ID: {df.iloc[0]['row_id'] if 'row_id' in df.columns else 'No row_id column'}")
        
        return True
    except Exception as e:
        print(f"   ❌ safe_csv_read function failed: {e}")
        return False

def test_download_all_minimal_help():
    """Test that download_all_minimal help still works"""
    print("4. Testing download_all_minimal help...")
    try:
        import subprocess
        result = subprocess.run([sys.executable, 'download_all_minimal.py', '--help'], 
                              capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("   ✅ download_all_minimal help works")
            # Show first few lines of help
            help_lines = result.stdout.split('\n')[:5]
            print(f"   📋 Help preview: {help_lines[0]}")
            return True
        else:
            print(f"   ❌ download_all_minimal help failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"   ❌ download_all_minimal help failed: {e}")
        return False

def main():
    """Run all tests"""
    print("CSV Reading Consolidation Validation Tests")
    print("=" * 50)
    
    tests = [
        test_safe_csv_read_import,
        test_download_all_minimal_import,
        test_safe_csv_read_function,
        test_download_all_minimal_help
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"   ❌ Test failed with exception: {e}")
        print()
    
    print("=" * 50)
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed! CSV reading consolidation is working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)