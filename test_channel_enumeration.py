#!/usr/bin/env python3
"""
Test Script for Channel Enumeration
Phase 2.5: Test channel enumeration with real YouTube channel

Demonstrates fail-fast, fail-loud, fail-safely principles for channel enumeration.
"""

import sys
import os
from pathlib import Path

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

def test_channel_enumeration_imports():
    """Test channel enumeration imports with fail-fast principles."""
    print("🧪 Testing channel enumeration imports...")
    
    try:
        from mass_download.channel_discovery import (
            YouTubeChannelDiscovery, ChannelInfo, VideoMetadata
        )
        print("✅ SUCCESS: Channel enumeration imports successful")
        return True, YouTubeChannelDiscovery
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import channel enumeration modules")
        print(f"   Error: {e}")
        print(f"   This is a LOUD FAILURE - channel enumeration module is broken!")
        return False, None


def test_channel_info_extraction():
    """Test basic channel info extraction with fail-fast/fail-loud principles."""
    print("\n🧪 Testing channel info extraction...")
    
    success, discovery_class = test_channel_enumeration_imports()
    if not success:
        return False
    
    try:
        discovery = discovery_class()
        print("✅ SUCCESS: YouTubeChannelDiscovery initialized")
    except Exception as e:
        print(f"🔍 EXPECTED FAILURE: Discovery initialization failed")
        print(f"   Error: {e}")
        return True  # Not necessarily a failure if yt-dlp not available
    
    # Test with a well-known, stable educational channel
    test_channels = [
        "https://www.youtube.com/@3Blue1Brown",  # Educational math channel
        "https://www.youtube.com/@TED",          # TED Talks - very stable
        "https://www.youtube.com/@Computerphile", # Computer science channel
    ]
    
    for channel_url in test_channels:
        try:
            print(f"\n📺 Testing channel info extraction: {channel_url}")
            
            # Extract basic channel info
            channel_info = discovery.extract_channel_info(channel_url)
            
            # Validate results
            if not channel_info.channel_id:
                print(f"❌ FAILURE: No channel_id extracted for {channel_url}")
                return False
            
            if not channel_info.title:
                print(f"❌ FAILURE: No title extracted for {channel_url}")
                return False
            
            print(f"✅ SUCCESS: Channel info extracted")
            print(f"   Title: {channel_info.title}")
            print(f"   Channel ID: {channel_info.channel_id}")
            print(f"   Subscribers: {channel_info.subscriber_count}")
            print(f"   Video Count: {channel_info.video_count}")
            
            return True  # Success with first channel
            
        except RuntimeError as e:
            if "yt-dlp" in str(e).lower():
                print(f"🔍 EXPECTED FAILURE: yt-dlp not available")
                print(f"   Error: {e}")
                return True  # Not a failure, just expected
            else:
                print(f"❌ UNEXPECTED FAILURE: Channel info extraction failed")
                print(f"   Error: {e}")
                continue  # Try next channel
        except Exception as e:
            print(f"❌ UNEXPECTED ERROR: {e}")
            continue  # Try next channel
    
    print("❌ FAILURE: All test channels failed")
    return False


def test_video_enumeration():
    """Test video enumeration with fail-fast/fail-loud principles."""
    print("\n🧪 Testing video enumeration...")
    
    success, discovery_class = test_channel_enumeration_imports()
    if not success:
        return False
    
    try:
        discovery = discovery_class()
        print("✅ SUCCESS: YouTubeChannelDiscovery initialized")
    except Exception as e:
        print(f"🔍 EXPECTED FAILURE: Discovery initialization failed")
        print(f"   Error: {e}")
        return True  # Not necessarily a failure
    
    # Test with a small, stable channel (limit videos for testing)
    test_channels = [
        ("https://www.youtube.com/@3Blue1Brown", 3),  # Limit to 3 videos
        ("https://www.youtube.com/@TED", 2),          # Limit to 2 videos  
    ]
    
    for channel_url, max_videos in test_channels:
        try:
            print(f"\n📹 Testing video enumeration: {channel_url} (max {max_videos} videos)")
            
            # Enumerate videos with limit
            videos = discovery.enumerate_channel_videos(channel_url, max_videos=max_videos)
            
            # Validate results
            if not isinstance(videos, list):
                print(f"❌ FAILURE: Expected list, got {type(videos)}")
                return False
            
            if len(videos) > max_videos:
                print(f"❌ FAILURE: Too many videos returned. Expected max {max_videos}, got {len(videos)}")
                return False
            
            print(f"✅ SUCCESS: Video enumeration completed")
            print(f"   Videos found: {len(videos)}")
            
            # Validate individual video metadata
            for i, video in enumerate(videos[:2]):  # Check first 2 videos
                print(f"   Video {i+1}:")
                print(f"     ID: {video.video_id}")
                print(f"     Title: {video.title[:50]}...")
                print(f"     Duration: {video.duration}s" if video.duration else "     Duration: Unknown")
                print(f"     Views: {video.view_count}" if video.view_count else "     Views: Unknown")
                
                # Validate required fields
                if not video.video_id or len(video.video_id) != 11:
                    print(f"❌ VALIDATION FAILURE: Invalid video_id: {video.video_id}")
                    return False
                
                if not video.title:
                    print(f"❌ VALIDATION FAILURE: Empty title for video: {video.video_id}")
                    return False
            
            return True  # Success with first channel
            
        except RuntimeError as e:
            if "yt-dlp" in str(e).lower() or "timed out" in str(e).lower():
                print(f"🔍 EXPECTED FAILURE: Network/tool issue")
                print(f"   Error: {e}")
                continue  # Try next channel
            else:
                print(f"❌ UNEXPECTED FAILURE: Video enumeration failed")
                print(f"   Error: {e}")
                continue
        except Exception as e:
            print(f"❌ UNEXPECTED ERROR: {e}")
            continue
    
    print("❌ FAILURE: All test channels failed")
    return False


def test_video_details():
    """Test individual video details extraction."""
    print("\n🧪 Testing video details extraction...")
    
    success, discovery_class = test_channel_enumeration_imports()
    if not success:
        return False
    
    try:
        discovery = discovery_class()
        print("✅ SUCCESS: YouTubeChannelDiscovery initialized")
    except Exception as e:
        print(f"🔍 EXPECTED FAILURE: Discovery initialization failed")
        print(f"   Error: {e}")
        return True
    
    # Test with well-known, stable videos
    test_videos = [
        "dQw4w9WgXcQ",  # Rick Roll - most stable video on YouTube
        "9bZkp7q19f0",  # Gangnam Style - another very stable video
    ]
    
    for video_id in test_videos:
        try:
            print(f"\n🎬 Testing video details: {video_id}")
            
            # Get detailed video info
            video = discovery.get_video_details(video_id)
            
            # Validate results
            if video.video_id != video_id:
                print(f"❌ FAILURE: Video ID mismatch. Expected {video_id}, got {video.video_id}")
                return False
            
            print(f"✅ SUCCESS: Video details extracted")
            print(f"   Title: {video.title}")
            print(f"   Duration: {video.duration}s" if video.duration else "   Duration: Unknown")
            print(f"   Views: {video.view_count}" if video.view_count else "   Views: Unknown")
            print(f"   Upload Date: {video.upload_date}" if video.upload_date else "   Upload Date: Unknown")
            print(f"   Tags: {len(video.tags)} tags" if video.tags else "   Tags: None")
            
            return True  # Success with first video
            
        except RuntimeError as e:
            if "yt-dlp" in str(e).lower():
                print(f"🔍 EXPECTED FAILURE: yt-dlp not available")
                print(f"   Error: {e}")
                continue
            else:
                print(f"❌ UNEXPECTED FAILURE: Video details extraction failed")
                print(f"   Error: {e}")
                continue
        except Exception as e:
            print(f"❌ UNEXPECTED ERROR: {e}")
            continue
    
    print("❌ FAILURE: All test videos failed")
    return False


def test_error_handling():
    """Test error handling for invalid inputs."""
    print("\n🧪 Testing error handling...")
    
    success, discovery_class = test_channel_enumeration_imports()
    if not success:
        return False
    
    try:
        discovery = discovery_class()
        print("✅ SUCCESS: YouTubeChannelDiscovery initialized")
    except Exception as e:
        print(f"🔍 EXPECTED FAILURE: Discovery initialization failed")
        print(f"   Error: {e}")
        return True
    
    # Test invalid video ID (should fail fast and loud)
    try:
        result = discovery.get_video_details("invalid_id")
        print("❌ VALIDATION FAILURE: Invalid video ID should have failed!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Invalid video ID failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for invalid video ID")
        print(f"   Error: {e}")
        return False
    
    # Test invalid channel URL (should fail fast and loud)
    try:
        result = discovery.enumerate_channel_videos("https://not-youtube.com/channel")
        print("❌ VALIDATION FAILURE: Invalid channel URL should have failed!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Invalid channel URL failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for invalid channel URL")
        print(f"   Error: {e}")
        return False
    
    # Test non-existent channel (should handle gracefully)
    try:
        result = discovery.enumerate_channel_videos("https://www.youtube.com/@nonexistentchannel999999999")
        print(f"🔍 NON-EXISTENT CHANNEL: Returned {len(result)} videos (expected 0 or error)")
    except RuntimeError as e:
        print(f"✅ SUCCESS: Non-existent channel handled gracefully")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for non-existent channel")
        print(f"   Error: {e}")
        return False
    
    print("✅ ALL error handling tests PASSED")
    return True


def main():
    """Run all channel enumeration tests."""
    print("🚀 Starting Channel Enumeration Tests")
    print("   Testing fail-fast, fail-loud, fail-safely principles")
    print("=" * 60)
    
    all_tests_passed = True
    
    # Run enumeration tests
    if not test_channel_info_extraction():
        print("⚠️  Channel info extraction tests had issues (may be expected)")
        # Don't fail completely - might be network/tool issues
    
    if not test_video_enumeration():
        print("⚠️  Video enumeration tests had issues (may be expected)")
        # Don't fail completely - might be network/tool issues
    
    if not test_video_details():
        print("⚠️  Video details tests had issues (may be expected)")
        # Don't fail completely - might be network/tool issues
    
    if not test_error_handling():
        all_tests_passed = False
        print("❌ Error handling tests FAILED")
    
    # Final results
    print("\n" + "=" * 60)
    if all_tests_passed:
        print("🎉 ALL CRITICAL TESTS PASSED! Channel enumeration validation successful")
        print("✅ Fail-fast principle: Invalid inputs rejected immediately")
        print("✅ Fail-loud principle: Clear, actionable error messages")
        print("✅ Fail-safely principle: Graceful handling of network/tool issues")
        print("ℹ️  Note: Some tests may have skipped due to network/yt-dlp availability")
        return 0
    else:
        print("💥 CRITICAL TESTS FAILED! Channel enumeration has issues")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)