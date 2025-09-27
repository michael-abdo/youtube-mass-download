#!/usr/bin/env python3
"""
Manual verification of the updated download_all_minimal.py imports and fixes
"""

import sys
import traceback

def test_imports():
    """Test that all imports work correctly"""
    print("Testing imports...")
    
    try:
        # Test basic import
        import download_all_minimal
        print("✅ Basic import successful")
        
        # Test pattern imports
        from utils.patterns import extract_youtube_id, extract_drive_id, PatternRegistry
        print("✅ Pattern imports successful")
        
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        traceback.print_exc()
        return False

def test_function_behavior():
    """Test the behavior of the imported functions"""
    print("\nTesting function behavior...")
    
    try:
        from utils.patterns import extract_youtube_id, extract_drive_id, PatternRegistry
        
        # Test YouTube ID extraction
        youtube_id = extract_youtube_id('https://youtube.com/watch?v=abc123')
        print(f"YouTube ID: '{youtube_id}'")
        assert youtube_id == 'abc123', f"Expected 'abc123', got '{youtube_id}'"
        
        # Test Drive ID extraction  
        drive_id = extract_drive_id('https://drive.google.com/file/d/def456/view')
        print(f"Drive ID: '{drive_id}'")
        assert drive_id == 'def456', f"Expected 'def456', got '{drive_id}'"
        
        # Test playlist pattern
        match = PatternRegistry.YOUTUBE_LIST_PARAM.search('https://youtube.com/watch?v=abc&list=xyz789')
        playlist_id = match.group(1) if match else 'None'
        print(f"Playlist ID: '{playlist_id}'")
        assert playlist_id == 'xyz789', f"Expected 'xyz789', got '{playlist_id}'"
        
        # Test folder pattern
        folder_match = PatternRegistry.DRIVE_FOLDER_URL.search('https://drive.google.com/drive/folders/ghi012')
        folder_id = folder_match.group(1) if folder_match else 'None'
        print(f"Folder ID: '{folder_id}'")
        assert folder_id == 'ghi012', f"Expected 'ghi012', got '{folder_id}'"
        
        print("✅ All function tests passed")
        return True
        
    except Exception as e:
        print(f"❌ Function test failed: {e}")
        traceback.print_exc()
        return False

def test_code_usage():
    """Test that the usage in download_all_minimal.py is correct"""
    print("\nTesting code usage patterns...")
    
    try:
        # Read the file and check for the fixes we made
        with open('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/download_all_minimal.py', 'r') as f:
            content = f.read()
        
        # Check that we fixed the .group(1) issues
        problematic_patterns = [
            'video_id.group(1)',
            'file_id.group(1)',  
            'folder_id.group(1)'
        ]
        
        for pattern in problematic_patterns:
            if pattern in content:
                print(f"❌ Found problematic pattern: {pattern}")
                return False
        
        # Check that the imports are present
        required_imports = [
            'from utils.patterns import extract_youtube_id, extract_drive_id, PatternRegistry'
        ]
        
        for import_line in required_imports:
            if import_line not in content:
                print(f"❌ Missing import: {import_line}")
                return False
        
        # Check that the functions are used correctly
        correct_usages = [
            'video_id = extract_youtube_id(url)',
            'file_id = extract_drive_id(url)',
            'PatternRegistry.YOUTUBE_LIST_PARAM.search(url)',
            'PatternRegistry.DRIVE_FOLDER_URL.search(url)'
        ]
        
        for usage in correct_usages:
            if usage not in content:
                print(f"❌ Missing correct usage: {usage}")
                return False
        
        print("✅ Code usage patterns are correct")
        return True
        
    except Exception as e:
        print(f"❌ Code usage test failed: {e}")
        traceback.print_exc()
        return False

def main():
    print("Manual verification of download_all_minimal.py updates")
    print("=" * 60)
    
    tests = [
        test_imports,
        test_function_behavior,
        test_code_usage
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print("=" * 60)
    print(f"Manual Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All manual tests passed! The updated download_all_minimal.py appears to work correctly.")
        print("\nKey fixes made:")
        print("- Fixed video_id.group(1) → video_id (extract_youtube_id returns string)")
        print("- Fixed file_id.group(1) → file_id (extract_drive_id returns string)")
        print("- Fixed folder_id.group(1) → folder_id (already extracted as string)")
        print("- All pattern imports are working correctly")
    else:
        print("⚠️  Some tests failed. Check the output above for details.")

if __name__ == "__main__":
    main()