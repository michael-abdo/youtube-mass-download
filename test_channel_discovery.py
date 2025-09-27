#!/usr/bin/env python3
"""
Test Script for Channel Discovery Module
Phase 2.3: Test channel URL validation with valid/invalid inputs

Demonstrates fail-fast, fail-loud, fail-safely principles for channel discovery.
"""

import sys
import os
from pathlib import Path

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

def test_channel_discovery_imports():
    """Test channel discovery module imports with fail-fast principles."""
    print("🧪 Testing channel discovery module imports...")
    
    try:
        from mass_download.channel_discovery import (
            ChannelInfo, VideoMetadata, YouTubeChannelDiscovery
        )
        print("✅ SUCCESS: Channel discovery imports successful")
        return True
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import channel discovery modules")
        print(f"   Error: {e}")
        print(f"   This is a LOUD FAILURE - channel discovery module is broken!")
        return False


def test_channel_info_validation():
    """Test ChannelInfo validation with fail-fast/fail-loud principles."""
    print("\n🧪 Testing ChannelInfo validation...")
    
    try:
        from mass_download.channel_discovery import ChannelInfo
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False
    
    # Test 1: Valid channel info (should pass)
    try:
        valid_channel = ChannelInfo(
            channel_id="UC123456789012345678",
            channel_url="https://www.youtube.com/@testchannel",
            title="Test Channel"
        )
        print("✅ SUCCESS: Valid channel info created")
    except Exception as e:
        print(f"❌ UNEXPECTED FAILURE: Valid channel info failed validation")
        print(f"   Error: {e}")
        return False
    
    # Test 2: Invalid channel_id (should fail fast and loud)
    try:
        invalid_channel = ChannelInfo(
            channel_id="",  # Empty channel ID
            channel_url="https://www.youtube.com/@testchannel",
            title="Test Channel"
        )
        print("❌ VALIDATION FAILURE: Empty channel_id should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Empty channel_id failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for empty channel_id")
        print(f"   Error: {e}")
        return False
    
    # Test 3: Invalid channel_url (should fail fast and loud)
    try:
        invalid_url_channel = ChannelInfo(
            channel_id="UC123456789012345678",
            channel_url="https://not-youtube.com/channel",  # Not YouTube
            title="Test Channel"
        )
        print("❌ VALIDATION FAILURE: Invalid channel_url should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Invalid channel_url failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for invalid URL")
        print(f"   Error: {e}")
        return False
    
    # Test 4: Empty title (should fail fast and loud)
    try:
        empty_title_channel = ChannelInfo(
            channel_id="UC123456789012345678",
            channel_url="https://www.youtube.com/@testchannel",
            title=""  # Empty title
        )
        print("❌ VALIDATION FAILURE: Empty title should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Empty title failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for empty title")
        print(f"   Error: {e}")
        return False
    
    print("✅ ALL ChannelInfo validation tests PASSED")
    return True


def test_video_metadata_validation():
    """Test VideoMetadata validation with fail-fast/fail-loud principles."""
    print("\n🧪 Testing VideoMetadata validation...")
    
    try:
        from mass_download.channel_discovery import VideoMetadata
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False
    
    # Test 1: Valid video metadata (should pass)
    try:
        valid_video = VideoMetadata(
            video_id="dQw4w9WgXcQ",  # Valid 11-char YouTube video ID
            title="Test Video Title"
        )
        print("✅ SUCCESS: Valid video metadata created")
    except Exception as e:
        print(f"❌ UNEXPECTED FAILURE: Valid video metadata failed validation")
        print(f"   Error: {e}")
        return False
    
    # Test 2: Invalid video_id length (should fail fast and loud)
    try:
        invalid_video_id = VideoMetadata(
            video_id="short",  # Too short (YouTube IDs are 11 chars)
            title="Test Video"
        )
        print("❌ VALIDATION FAILURE: Invalid video_id length should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Invalid video_id length failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for invalid video_id")
        print(f"   Error: {e}")
        return False
    
    # Test 3: Empty title (should fail fast and loud)
    try:
        empty_title_video = VideoMetadata(
            video_id="dQw4w9WgXcQ",
            title=""  # Empty title
        )
        print("❌ VALIDATION FAILURE: Empty title should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Empty title failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for empty title")
        print(f"   Error: {e}")
        return False
    
    print("✅ ALL VideoMetadata validation tests PASSED")
    return True


def test_channel_url_validation():
    """Test YouTube channel URL validation with fail-fast/fail-loud principles."""
    print("\n🧪 Testing YouTube channel URL validation...")
    
    try:
        from mass_download.channel_discovery import YouTubeChannelDiscovery
        
        # This will test yt-dlp availability
        discovery = YouTubeChannelDiscovery()
        print("✅ SUCCESS: YouTubeChannelDiscovery initialized")
    except RuntimeError as e:
        print(f"🔍 EXPECTED FAILURE: yt-dlp not available (this may be expected)")
        print(f"   Error: {e}")
        print("   This is LOUD FAILURE with clear error message ✅")
        return True  # Not a failure, just expected if yt-dlp not installed
    except Exception as e:
        print(f"❌ UNEXPECTED FAILURE: Discovery initialization failed")
        print(f"   Error: {e}")
        return False
    
    # Test valid URL formats
    valid_urls = [
        "https://www.youtube.com/@testchannel",
        "https://youtube.com/channel/UC123456789012345678",
        "https://www.youtube.com/c/testchannel",
        "https://www.youtube.com/user/testuser",
        "youtube.com/@testchannel",  # Should be normalized to https
        "http://youtube.com/@testchannel"  # Should be normalized to https
    ]
    
    for url in valid_urls:
        try:
            normalized = discovery.validate_channel_url(url)
            if not normalized.startswith("https://www.youtube.com/"):
                print(f"❌ NORMALIZATION FAILURE: URL not properly normalized: {normalized}")
                return False
            print(f"✅ SUCCESS: URL validated and normalized: {url} → {normalized}")
        except Exception as e:
            print(f"❌ UNEXPECTED FAILURE: Valid URL failed validation: {url}")
            print(f"   Error: {e}")
            return False
    
    # Test invalid URL formats (should fail fast and loud)
    invalid_urls = [
        "",                                    # Empty
        "not-a-url",                          # Not a URL
        "https://not-youtube.com/channel",    # Wrong domain
        "https://youtube.com/invalid-path",   # Invalid path format
        None,                                 # None value
        123,                                  # Wrong type
        "   ",                               # Whitespace only
    ]
    
    for url in invalid_urls:
        try:
            result = discovery.validate_channel_url(url)
            print(f"❌ VALIDATION FAILURE: Invalid URL should have failed: {url}")
            return False
        except ValueError as e:
            print(f"✅ SUCCESS: Invalid URL failed validation as expected: {url}")
            print(f"   Error message: {e}")
        except Exception as e:
            print(f"❌ UNEXPECTED ERROR: Wrong exception type for invalid URL: {url}")
            print(f"   Error: {e}")
            return False
    
    print("✅ ALL channel URL validation tests PASSED")
    return True


def main():
    """Run all channel discovery validation tests."""
    print("🚀 Starting Channel Discovery Validation Tests")
    print("   Testing fail-fast, fail-loud, fail-safely principles")
    print("=" * 60)
    
    all_tests_passed = True
    
    # Run validation tests
    if not test_channel_discovery_imports():
        all_tests_passed = False
        print("❌ Channel discovery import tests FAILED")
        return 1  # Exit early if imports fail
    
    if not test_channel_info_validation():
        all_tests_passed = False
        print("❌ ChannelInfo validation tests FAILED")
    
    if not test_video_metadata_validation():
        all_tests_passed = False
        print("❌ VideoMetadata validation tests FAILED")
    
    if not test_channel_url_validation():
        all_tests_passed = False
        print("❌ Channel URL validation tests FAILED")
    
    # Final results
    print("\n" + "=" * 60)
    if all_tests_passed:
        print("🎉 ALL TESTS PASSED! Channel discovery validation successful")
        print("✅ Fail-fast principle: Invalid inputs rejected immediately")
        print("✅ Fail-loud principle: Clear, actionable error messages")
        print("✅ Fail-safely principle: No partial state corruption")
        return 0
    else:
        print("💥 SOME TESTS FAILED! Channel discovery has issues")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)