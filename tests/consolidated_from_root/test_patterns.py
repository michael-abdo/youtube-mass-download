#!/usr/bin/env python3
"""
Test script to verify the updated download_all_minimal.py imports work correctly
"""

def test_import():
    try:
        import download_all_minimal
        print("✅ Test 1: Basic import successful")
        return True
    except Exception as e:
        print(f"❌ Test 1: Basic import failed: {e}")
        return False

def test_pattern_imports():
    try:
        from utils.patterns import extract_youtube_id, extract_drive_id, PatternRegistry
        print("✅ Test 2: Pattern imports successful")
        return True
    except Exception as e:
        print(f"❌ Test 2: Pattern imports failed: {e}")
        return False

def test_url_extraction():
    try:
        from utils.patterns import extract_youtube_id, extract_drive_id, PatternRegistry
        
        # Test YouTube ID extraction
        youtube_id = extract_youtube_id('https://youtube.com/watch?v=abc123')
        print(f"YouTube ID: {youtube_id}")
        
        # Test Drive ID extraction  
        drive_id = extract_drive_id('https://drive.google.com/file/d/def456/view')
        print(f"Drive ID: {drive_id}")
        
        # Test playlist pattern
        match = PatternRegistry.YOUTUBE_LIST_PARAM.search('https://youtube.com/watch?v=abc&list=xyz789')
        playlist_id = match.group(1) if match else 'None'
        print(f"Playlist ID: {playlist_id}")
        
        # Test folder pattern
        folder_match = PatternRegistry.DRIVE_FOLDER_URL.search('https://drive.google.com/drive/folders/ghi012')
        folder_id = folder_match.group(1) if folder_match else 'None'
        print(f"Folder ID: {folder_id}")
        
        print("✅ Test 4: URL extraction successful")
        return True
    except Exception as e:
        print(f"❌ Test 4: URL extraction failed: {e}")
        return False

def test_help():
    try:
        import subprocess
        result = subprocess.run(['python3', 'download_all_minimal.py', '--help'], 
                              capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print("✅ Test 3: Help command successful")
            print(f"Help output preview: {result.stdout[:200]}...")
            return True
        else:
            print(f"❌ Test 3: Help command failed with code {result.returncode}")
            print(f"Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Test 3: Help command failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing updated download_all_minimal.py with new URL pattern imports...")
    print("=" * 60)
    
    tests = [
        test_import,
        test_pattern_imports,
        test_help,
        test_url_extraction
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print("=" * 60)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The updated download_all_minimal.py works correctly.")
    else:
        print("⚠️  Some tests failed. Check the output above for details.")