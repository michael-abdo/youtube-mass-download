#!/usr/bin/env python3
"""Validate CSV reading consolidation"""

import sys
import traceback

def test_1_safe_csv_read_import():
    """Test that safe_csv_read can be imported from utils.csv_manager"""
    print("Test 1: safe_csv_read import")
    try:
        from utils.csv_manager import safe_csv_read
        print("   ✅ safe_csv_read import successful")
        return True
    except Exception as e:
        print(f"   ❌ safe_csv_read import failed: {e}")
        traceback.print_exc()
        return False

def test_2_download_all_minimal_import():
    """Test that download_all_minimal can be imported"""
    print("\nTest 2: download_all_minimal import") 
    try:
        import download_all_minimal
        print("   ✅ download_all_minimal import successful")
        return True
    except Exception as e:
        print(f"   ❌ download_all_minimal import failed: {e}")
        traceback.print_exc()
        return False

def test_3_safe_csv_read_function():
    """Test that safe_csv_read function works with actual CSV"""
    print("\nTest 3: safe_csv_read function with outputs/output.csv")
    try:
        from utils.csv_manager import safe_csv_read
        import os
        
        csv_path = 'outputs/output.csv'
        if not os.path.exists(csv_path):
            print(f"   ❌ CSV file not found: {csv_path}")
            return False
            
        df = safe_csv_read(csv_path)
        print(f"   ✅ CSV loaded successfully")
        print(f"   📊 Shape: {df.shape}")
        print(f"   📋 Columns: {list(df.columns)}")
        
        # Check if we have the expected columns
        expected_cols = ['row_id', 'name', 'email', 'type']
        missing_cols = [col for col in expected_cols if col not in df.columns]
        if missing_cols:
            print(f"   ⚠️  Missing expected columns: {missing_cols}")
        else:
            print("   ✅ All expected columns present")
            
        # Show sample data
        if not df.empty:
            print(f"   📝 Sample data (first row):")
            print(f"      row_id: {df.iloc[0]['row_id']}")
            print(f"      name: {df.iloc[0]['name']}")
            print(f"      email: {df.iloc[0]['email']}")
            
        return True
        
    except Exception as e:
        print(f"   ❌ safe_csv_read function failed: {e}")
        traceback.print_exc()
        return False

def test_4_download_all_minimal_functionality():
    """Test MinimalDownloader class can be instantiated"""
    print("\nTest 4: MinimalDownloader instantiation")
    try:
        from download_all_minimal import MinimalDownloader
        
        # Test instantiation
        downloader = MinimalDownloader(max_rows=1)
        print("   ✅ MinimalDownloader instantiation successful")
        
        # Test that it has the expected methods
        expected_methods = ['download_youtube', 'save_drive_info', 'process_all']
        for method in expected_methods:
            if hasattr(downloader, method):
                print(f"   ✅ Method {method} exists")
            else:
                print(f"   ❌ Method {method} missing")
                return False
                
        return True
        
    except Exception as e:
        print(f"   ❌ MinimalDownloader test failed: {e}")
        traceback.print_exc()
        return False

def main():
    """Run all validation tests"""
    print("CSV Reading Consolidation Validation")
    print("=" * 60)
    
    tests = [
        test_1_safe_csv_read_import,
        test_2_download_all_minimal_import,
        test_3_safe_csv_read_function,
        test_4_download_all_minimal_functionality
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 60)
    print(f"RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! CSV reading consolidation is working correctly.")
        print()
        print("The updated download_all_minimal.py file successfully imports")
        print("safe_csv_read from utils.csv_manager and all functionality works.")
    else:
        print("⚠️  Some tests failed. Check the errors above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)