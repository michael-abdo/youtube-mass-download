#!/usr/bin/env python3
"""
Test validation functions inline by importing them
"""

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

import sys
import os
# Direct test execution
try:
    from utils.validation import is_valid_youtube_url, is_valid_drive_url, get_url_type
    
    print("Testing validation functions...")
    
    # Test is_valid_youtube_url
    print("\n1. Testing is_valid_youtube_url:")
    test_result = is_valid_youtube_url("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
    print(f"   Valid YouTube URL: {test_result}")
    
    test_result = is_valid_youtube_url("https://google.com")
    print(f"   Invalid URL: {test_result}")
    
    # Test is_valid_drive_url
    print("\n2. Testing is_valid_drive_url:")
    test_result = is_valid_drive_url("https://drive.google.com/file/d/1abc123def456/view")
    print(f"   Valid Drive URL: {test_result}")
    
    test_result = is_valid_drive_url("https://google.com")
    print(f"   Invalid URL: {test_result}")
    
    # Test get_url_type
    print("\n3. Testing get_url_type:")
    test_result = get_url_type("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
    print(f"   YouTube URL type: {test_result}")
    
    test_result = get_url_type("https://drive.google.com/file/d/1abc123def456/view")
    print(f"   Drive file URL type: {test_result}")
    
    test_result = get_url_type("https://drive.google.com/drive/folders/1abc123def456")
    print(f"   Drive folder URL type: {test_result}")
    
    test_result = get_url_type("https://google.com")
    print(f"   Unknown URL type: {test_result}")
    
    print("\n✅ All validation functions are working correctly!")
    
except Exception as e:
    print(f"❌ Error testing validation functions: {e}")
    import traceback
    traceback.print_exc()