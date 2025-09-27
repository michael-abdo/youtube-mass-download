#!/usr/bin/env python3
"""
Test script for the new validation functions:
- is_valid_youtube_url
- is_valid_drive_url  
- get_url_type
"""

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

import sys
from utils.validation import is_valid_youtube_url, is_valid_drive_url, get_url_type

def test_is_valid_youtube_url():
    """Test the is_valid_youtube_url function"""
    print("Testing is_valid_youtube_url:")
    
    # Valid YouTube URLs
    valid_urls = [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtu.be/dQw4w9WgXcQ",
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=10s",
        "https://youtu.be/dQw4w9WgXcQ?t=10",
    ]
    
    for url in valid_urls:
        result = is_valid_youtube_url(url)
        print(f"  ✓ {url} -> {result}")
        assert result == True, f"Expected True for {url}"
    
    # Invalid YouTube URLs
    invalid_urls = [
        "",
        None,
        "https://google.com",
        "https://youtube.com/watch?v=invalid",
        "https://youtube.com/watch?v=toolong123456789",
        "javascript:alert('xss')",
        "https://youtube.com/watch?v=dQw4w9WgX",  # Too short
    ]
    
    for url in invalid_urls:
        result = is_valid_youtube_url(url)
        print(f"  ✗ {url} -> {result}")
        assert result == False, f"Expected False for {url}"
    
    print("  ✓ All is_valid_youtube_url tests passed!")

def test_is_valid_drive_url():
    """Test the is_valid_drive_url function"""
    print("\nTesting is_valid_drive_url:")
    
    # Valid Drive URLs
    valid_urls = [
        "https://drive.google.com/file/d/1abc123def456/view",
        "https://docs.google.com/document/d/1abc123def456/edit",
        "https://drive.google.com/file/d/1u4vS-_WKpW-RHCpIS4hls6K5grOii56F/view",
        "https://drive.google.com/drive/folders/1abc123def456",
    ]
    
    for url in valid_urls:
        result = is_valid_drive_url(url)
        print(f"  ✓ {url} -> {result}")
        assert result == True, f"Expected True for {url}"
    
    # Invalid Drive URLs
    invalid_urls = [
        "",
        None,
        "https://google.com",
        "https://youtube.com/watch?v=dQw4w9WgXcQ",
        "javascript:alert('xss')",
        "https://drive.google.com/file/d/",  # Missing ID
    ]
    
    for url in invalid_urls:
        result = is_valid_drive_url(url)
        print(f"  ✗ {url} -> {result}")
        assert result == False, f"Expected False for {url}"
    
    print("  ✓ All is_valid_drive_url tests passed!")

def test_get_url_type():
    """Test the get_url_type function"""
    print("\nTesting get_url_type:")
    
    # Test cases with expected results
    test_cases = [
        ("https://www.youtube.com/watch?v=dQw4w9WgXcQ", "youtube"),
        ("https://youtu.be/dQw4w9WgXcQ", "youtube"),
        ("https://drive.google.com/file/d/1abc123def456/view", "drive_file"),
        ("https://docs.google.com/document/d/1abc123def456/edit", "drive_file"),
        ("https://drive.google.com/drive/folders/1abc123def456", "drive_folder"),
        ("https://google.com", "unknown"),
        ("", "unknown"),
        (None, "unknown"),
        ("javascript:alert('xss')", "unknown"),
    ]
    
    for url, expected in test_cases:
        result = get_url_type(url)
        print(f"  {url} -> {result} (expected: {expected})")
        assert result == expected, f"Expected {expected} for {url}, got {result}"
    
    print("  ✓ All get_url_type tests passed!")

def main():
    """Run all tests"""
    print("Running validation function tests...\n")
    
    try:
        test_is_valid_youtube_url()
        test_is_valid_drive_url()
        test_get_url_type()
        print("\n🎉 All tests passed successfully!")
        return 0
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())