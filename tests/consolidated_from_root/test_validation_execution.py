#!/usr/bin/env python3
"""
Execute validation tests and capture results
"""

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

import sys
import os
# Set up test results
test_results = []

try:
    # Import the validation functions
    from utils.validation import is_valid_youtube_url, is_valid_drive_url, get_url_type
    
    test_results.append("✅ Successfully imported validation functions")
    
    # Test 1: is_valid_youtube_url
    test_results.append("\n=== Testing is_valid_youtube_url ===")
    
    # Valid YouTube URLs
    valid_youtube = [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtu.be/dQw4w9WgXcQ",
    ]
    
    for url in valid_youtube:
        result = is_valid_youtube_url(url)
        test_results.append(f"✓ {url} -> {result} (expected: True)")
        assert result == True, f"Expected True for valid YouTube URL: {url}"
    
    # Invalid YouTube URLs
    invalid_youtube = [
        "",
        "https://google.com",
        "https://youtube.com/watch?v=invalid",
    ]
    
    for url in invalid_youtube:
        result = is_valid_youtube_url(url)
        test_results.append(f"✗ {url} -> {result} (expected: False)")
        assert result == False, f"Expected False for invalid YouTube URL: {url}"
    
    test_results.append("✅ is_valid_youtube_url tests PASSED")
    
    # Test 2: is_valid_drive_url
    test_results.append("\n=== Testing is_valid_drive_url ===")
    
    # Valid Drive URLs
    valid_drive = [
        "https://drive.google.com/file/d/1abc123def456/view",
        "https://docs.google.com/document/d/1abc123def456/edit",
    ]
    
    for url in valid_drive:
        result = is_valid_drive_url(url)
        test_results.append(f"✓ {url} -> {result} (expected: True)")
        assert result == True, f"Expected True for valid Drive URL: {url}"
    
    # Invalid Drive URLs
    invalid_drive = [
        "",
        "https://google.com",
        "https://youtube.com/watch?v=dQw4w9WgXcQ",
    ]
    
    for url in invalid_drive:
        result = is_valid_drive_url(url)
        test_results.append(f"✗ {url} -> {result} (expected: False)")
        assert result == False, f"Expected False for invalid Drive URL: {url}"
    
    test_results.append("✅ is_valid_drive_url tests PASSED")
    
    # Test 3: get_url_type
    test_results.append("\n=== Testing get_url_type ===")
    
    type_tests = [
        ("https://www.youtube.com/watch?v=dQw4w9WgXcQ", "youtube"),
        ("https://youtu.be/dQw4w9WgXcQ", "youtube"),
        ("https://drive.google.com/file/d/1abc123def456/view", "drive_file"),
        ("https://docs.google.com/document/d/1abc123def456/edit", "drive_file"),
        ("https://drive.google.com/drive/folders/1abc123def456", "drive_folder"),
        ("https://google.com", "unknown"),
        ("", "unknown"),
    ]
    
    for url, expected in type_tests:
        result = get_url_type(url)
        status = "✓" if result == expected else "✗"
        test_results.append(f"{status} {url} -> {result} (expected: {expected})")
        assert result == expected, f"Expected {expected} for {url}, got {result}"
    
    test_results.append("✅ get_url_type tests PASSED")
    
    # Final summary
    test_results.append("\n🎉 ALL VALIDATION TESTS PASSED SUCCESSFULLY!")
    test_results.append("\nThe new validation functions are working correctly:")
    test_results.append("- is_valid_youtube_url: Properly validates YouTube URLs")
    test_results.append("- is_valid_drive_url: Properly validates Google Drive URLs")
    test_results.append("- get_url_type: Correctly identifies URL types")
    
except Exception as e:
    test_results.append(f"❌ TEST FAILED: {e}")
    import traceback
    test_results.append(traceback.format_exc())

# Write results to file
with open('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/test_results.txt', 'w') as f:
    f.write('\n'.join(test_results))

print("Test execution completed. Results saved to test_results.txt")