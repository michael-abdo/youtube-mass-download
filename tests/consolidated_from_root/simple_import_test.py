#!/usr/bin/env python3
"""Simple test to verify imports work"""

try:
    print("Testing basic import...")
    import download_all_minimal
    print("✅ Basic import successful")
    
    print("Testing pattern imports...")
    from utils.patterns import extract_youtube_id, extract_drive_id, PatternRegistry
    print("✅ Pattern imports successful")
    
    print("Testing function behavior...")
    # Test YouTube ID extraction
    youtube_id = extract_youtube_id('https://youtube.com/watch?v=abc123')
    print(f"YouTube ID: '{youtube_id}'")
    
    # Test Drive ID extraction
    drive_id = extract_drive_id('https://drive.google.com/file/d/def456/view')
    print(f"Drive ID: '{drive_id}'")
    
    # Test playlist pattern
    match = PatternRegistry.YOUTUBE_LIST_PARAM.search('https://youtube.com/watch?v=abc&list=xyz789')
    playlist_id = match.group(1) if match else 'None'
    print(f"Playlist ID: '{playlist_id}'")
    
    # Test folder pattern
    folder_match = PatternRegistry.DRIVE_FOLDER_URL.search('https://drive.google.com/drive/folders/ghi012')
    folder_id = folder_match.group(1) if folder_match else 'None'
    print(f"Folder ID: '{folder_id}'")
    
    print("✅ All tests passed!")
    
except Exception as e:
    print(f"❌ Test failed: {e}")
    import traceback
    traceback.print_exc()