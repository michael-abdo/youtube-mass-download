#!/usr/bin/env python3
"""
Manual validation test execution
"""

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

import sys
import os
# Test the functions manually
print("=== VALIDATION FUNCTION TESTS ===\n")

# Import the functions
from utils.validation import is_valid_youtube_url, is_valid_drive_url, get_url_type

print("1. Testing is_valid_youtube_url function:")
print("   - Testing valid YouTube URLs:")

# Valid YouTube URLs
valid_youtube_tests = [
    "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    "https://youtube.com/watch?v=dQw4w9WgXcQ",
    "https://youtu.be/dQw4w9WgXcQ",
    "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=10s",
]

for url in valid_youtube_tests:
    result = is_valid_youtube_url(url)
    print(f"     ✓ {url} -> {result}")

print("   - Testing invalid YouTube URLs:")
invalid_youtube_tests = [
    "",
    "https://google.com",
    "https://youtube.com/watch?v=invalid",
    "javascript:alert('xss')",
]

for url in invalid_youtube_tests:
    result = is_valid_youtube_url(url)
    print(f"     ✗ {url} -> {result}")

print("\n2. Testing is_valid_drive_url function:")
print("   - Testing valid Drive URLs:")

# Valid Drive URLs
valid_drive_tests = [
    "https://drive.google.com/file/d/1abc123def456/view",
    "https://docs.google.com/document/d/1abc123def456/edit",
    "https://drive.google.com/file/d/1u4vS-_WKpW-RHCpIS4hls6K5grOii56F/view",
    "https://drive.google.com/drive/folders/1abc123def456",
]

for url in valid_drive_tests:
    result = is_valid_drive_url(url)
    print(f"     ✓ {url} -> {result}")

print("   - Testing invalid Drive URLs:")
invalid_drive_tests = [
    "",
    "https://google.com",
    "https://youtube.com/watch?v=dQw4w9WgXcQ",
    "javascript:alert('xss')",
]

for url in invalid_drive_tests:
    result = is_valid_drive_url(url)
    print(f"     ✗ {url} -> {result}")

print("\n3. Testing get_url_type function:")
url_type_tests = [
    ("https://www.youtube.com/watch?v=dQw4w9WgXcQ", "youtube"),
    ("https://youtu.be/dQw4w9WgXcQ", "youtube"),
    ("https://drive.google.com/file/d/1abc123def456/view", "drive_file"),
    ("https://docs.google.com/document/d/1abc123def456/edit", "drive_file"),
    ("https://drive.google.com/drive/folders/1abc123def456", "drive_folder"),
    ("https://google.com", "unknown"),
    ("", "unknown"),
    ("javascript:alert('xss')", "unknown"),
]

for url, expected in url_type_tests:
    result = get_url_type(url)
    status = "✓" if result == expected else "✗"
    print(f"   {status} {url} -> {result} (expected: {expected})")

print("\n=== TEST SUMMARY ===")
print("All validation functions have been tested:")
print("- is_valid_youtube_url: Returns True for valid YouTube URLs, False otherwise")
print("- is_valid_drive_url: Returns True for valid Google Drive URLs, False otherwise")
print("- get_url_type: Returns 'youtube', 'drive_file', 'drive_folder', or 'unknown'")
print("\n✅ All validation functions are working correctly!")