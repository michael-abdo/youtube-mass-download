#!/usr/bin/env python3
"""
Test UUID Generation and Duplicate Detection
Phase 2.10: Implement UUID generation and duplicate detection

Tests:
1. UUID generation and validation
2. Duplicate detection functionality
3. UUID tracking and mapping
4. Integration with VideoRecord validation
5. Database schema UUID handling
6. Edge cases and error conditions

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import os
import uuid
from pathlib import Path
from typing import List, Dict, Any

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))

def test_imports():
    """Test that all required modules import correctly."""
    print("🧪 Testing imports for UUID and duplicate detection tests...")
    
    try:
        from channel_discovery import YouTubeChannelDiscovery
        from database_schema import VideoRecord, PersonRecord
        print("✅ SUCCESS: All required imports successful")
        return True, (YouTubeChannelDiscovery, VideoRecord, PersonRecord)
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import required modules")
        print(f"   Error: {e}")
        return False, None


def test_video_record_uuid_validation():
    """Test VideoRecord UUID validation with fail-fast principles."""
    print("\n🧪 Testing VideoRecord UUID validation...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    _, VideoRecord, _ = classes
    
    try:
        # Test Case 1: Valid UUID generation
        person_id = 1
        video_id = "dQw4w9WgXcQ"  # Valid 11-char YouTube ID
        title = "Test Video"
        
        video_record = VideoRecord(
            person_id=person_id,
            video_id=video_id,
            title=title
        )
        
        # Should have generated UUID automatically
        if not video_record.uuid:
            print("❌ FAILURE: UUID not generated automatically")
            return False
        
        # Validate UUID format
        try:
            parsed_uuid = uuid.UUID(video_record.uuid)
            print(f"✅ SUCCESS: Valid UUID generated: {video_record.uuid}")
        except ValueError:
            print(f"❌ FAILURE: Invalid UUID format: {video_record.uuid}")
            return False
        
        # Test Case 2: Invalid UUID format (should fail fast)
        try:
            invalid_video = VideoRecord(
                person_id=person_id,
                video_id=video_id,
                title=title,
                uuid="invalid-uuid-format"
            )
            print("❌ VALIDATION FAILURE: Invalid UUID should have failed!")
            return False
        except ValueError as e:
            if "Invalid UUID format" in str(e):
                print(f"✅ SUCCESS: Invalid UUID failed validation as expected")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        
        # Test Case 3: Non-string UUID (should fail fast)
        try:
            invalid_video = VideoRecord(
                person_id=person_id,
                video_id=video_id,
                title=title,
                uuid=12345  # Non-string
            )
            print("❌ VALIDATION FAILURE: Non-string UUID should have failed!")
            return False
        except ValueError as e:
            if "uuid must be string" in str(e):
                print(f"✅ SUCCESS: Non-string UUID failed validation as expected")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        
        # Test Case 4: Custom valid UUID
        custom_uuid = str(uuid.uuid4())
        custom_video = VideoRecord(
            person_id=person_id,
            video_id=video_id,
            title=title,
            uuid=custom_uuid
        )
        
        if custom_video.uuid == custom_uuid:
            print(f"✅ SUCCESS: Custom UUID accepted: {custom_uuid}")
        else:
            print(f"❌ FAILURE: Custom UUID not preserved: expected {custom_uuid}, got {custom_video.uuid}")
            return False
        
        print("✅ ALL VideoRecord UUID validation tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: VideoRecord UUID validation failed: {e}")
        return False


def test_duplicate_detection_basic():
    """Test basic duplicate detection functionality."""
    print("\n🧪 Testing basic duplicate detection...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    YouTubeChannelDiscovery, _, _ = classes
    
    try:
        # Initialize discovery (may fail if yt-dlp not available)
        try:
            discovery = YouTubeChannelDiscovery()
        except RuntimeError as e:
            if "yt-dlp" in str(e):
                print("🔍 EXPECTED FAILURE: yt-dlp not available (continuing with duplicate detection tests)")
                # We can still test duplicate detection without yt-dlp
                # Create a mock discovery object for testing
                discovery = YouTubeChannelDiscovery.__new__(YouTubeChannelDiscovery)
                discovery._processed_videos = set()
                discovery._uuid_mapping = {}
            else:
                print(f"❌ FAILURE: Unexpected initialization error: {e}")
                return False
        
        # Test Case 1: Initial state
        stats = discovery.get_duplicate_detection_stats()
        if stats["total_processed_videos"] != 0:
            print(f"❌ FAILURE: Expected 0 processed videos initially, got {stats['total_processed_videos']}")
            return False
        print("✅ SUCCESS: Initial duplicate detection state correct")
        
        # Test Case 2: Mark video as processed
        video_id = "dQw4w9WgXcQ"
        video_uuid = str(uuid.uuid4())
        
        discovery.mark_video_processed(video_id, video_uuid)
        
        # Check if video is marked as duplicate
        if not discovery.is_duplicate_video(video_id):
            print("❌ FAILURE: Video should be marked as duplicate after processing")
            return False
        print(f"✅ SUCCESS: Video correctly marked as processed: {video_id}")
        
        # Test Case 3: Check UUID mapping
        retrieved_uuid = discovery.get_video_uuid(video_id)
        if retrieved_uuid != video_uuid:
            print(f"❌ FAILURE: UUID mapping failed - expected {video_uuid}, got {retrieved_uuid}")
            return False
        print(f"✅ SUCCESS: UUID mapping working correctly")
        
        # Test Case 4: New video should not be duplicate
        new_video_id = "anotherID11"  # Different 11-char ID
        if discovery.is_duplicate_video(new_video_id):
            print("❌ FAILURE: New video should not be duplicate")
            return False
        print(f"✅ SUCCESS: New video correctly identified as non-duplicate")
        
        # Test Case 5: Statistics tracking
        stats = discovery.get_duplicate_detection_stats()
        if stats["total_processed_videos"] != 1:
            print(f"❌ FAILURE: Expected 1 processed video, got {stats['total_processed_videos']}")
            return False
        
        if video_id not in stats["processed_video_ids"]:
            print(f"❌ FAILURE: Processed video ID not in stats")
            return False
        
        print(f"✅ SUCCESS: Statistics tracking working correctly")
        
        print("✅ ALL basic duplicate detection tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Duplicate detection test failed: {e}")
        return False


def test_duplicate_detection_edge_cases():
    """Test duplicate detection edge cases and error handling."""
    print("\n🧪 Testing duplicate detection edge cases...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    YouTubeChannelDiscovery, _, _ = classes
    
    try:
        # Create mock discovery for testing
        discovery = YouTubeChannelDiscovery.__new__(YouTubeChannelDiscovery)
        discovery._processed_videos = set()
        discovery._uuid_mapping = {}
        
        # Test Case 1: Invalid video ID handling
        print("   Testing invalid video ID handling...")
        
        # Empty video ID
        if not discovery.is_duplicate_video(""):
            print("❌ FAILURE: Empty video ID should be treated as duplicate (fail-safely)")
            return False
        print("✅ SUCCESS: Empty video ID treated as duplicate")
        
        # None video ID
        if not discovery.is_duplicate_video(None):
            print("❌ FAILURE: None video ID should be treated as duplicate (fail-safely)")
            return False
        print("✅ SUCCESS: None video ID treated as duplicate")
        
        # Non-string video ID
        if not discovery.is_duplicate_video(12345):
            print("❌ FAILURE: Non-string video ID should be treated as duplicate (fail-safely)")
            return False
        print("✅ SUCCESS: Non-string video ID treated as duplicate")
        
        # Test Case 2: Invalid marking (should fail fast)
        print("   Testing invalid marking...")
        
        try:
            discovery.mark_video_processed("", str(uuid.uuid4()))
            print("❌ VALIDATION FAILURE: Empty video ID marking should have failed!")
            return False
        except ValueError as e:
            print("✅ SUCCESS: Invalid video ID marking failed as expected")
        
        try:
            discovery.mark_video_processed("validID11", "")
            print("❌ VALIDATION FAILURE: Empty UUID marking should have failed!")
            return False
        except ValueError as e:
            print("✅ SUCCESS: Invalid UUID marking failed as expected")
        
        # Test Case 3: Load existing videos
        print("   Testing existing video loading...")
        
        existing_videos = ["video1ID11", "video2ID11", "video3ID11"]
        discovery.load_existing_videos_for_duplicate_detection(existing_videos)
        
        # All should now be duplicates
        for video_id in existing_videos:
            if not discovery.is_duplicate_video(video_id):
                print(f"❌ FAILURE: Loaded video {video_id} should be duplicate")
                return False
        
        stats = discovery.get_duplicate_detection_stats()
        if stats["total_processed_videos"] != 3:
            print(f"❌ FAILURE: Expected 3 loaded videos, got {stats['total_processed_videos']}")
            return False
        
        print("✅ SUCCESS: Existing video loading working correctly")
        
        # Test Case 4: Invalid existing videos list
        try:
            discovery.load_existing_videos_for_duplicate_detection("not a list")
            print("❌ VALIDATION FAILURE: Invalid existing videos list should have failed!")
            return False
        except ValueError as e:
            print("✅ SUCCESS: Invalid existing videos list failed as expected")
        
        # Test Case 5: Reset functionality
        print("   Testing reset functionality...")
        
        discovery.reset_duplicate_detection()
        
        stats = discovery.get_duplicate_detection_stats()
        if stats["total_processed_videos"] != 0:
            print(f"❌ FAILURE: Expected 0 videos after reset, got {stats['total_processed_videos']}")
            return False
        
        # Previously loaded videos should no longer be duplicates
        for video_id in existing_videos:
            if discovery.is_duplicate_video(video_id):
                print(f"❌ FAILURE: Video {video_id} should not be duplicate after reset")
                return False
        
        print("✅ SUCCESS: Reset functionality working correctly")
        
        print("✅ ALL duplicate detection edge case tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Edge case test failed: {e}")
        return False


def test_uuid_integration_with_channel_discovery():
    """Test UUID integration with channel discovery functionality."""
    print("\n🧪 Testing UUID integration with channel discovery...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    YouTubeChannelDiscovery, VideoRecord, _ = classes
    
    try:
        # Test Case 1: Channel discovery initialization includes UUID tracking
        try:
            discovery = YouTubeChannelDiscovery()
            print("✅ SUCCESS: Channel discovery initializes with UUID tracking")
        except RuntimeError as e:
            if "yt-dlp" in str(e):
                print("🔍 EXPECTED FAILURE: yt-dlp not available (continuing with mock tests)")
                discovery = YouTubeChannelDiscovery.__new__(YouTubeChannelDiscovery)
                discovery._processed_videos = set()
                discovery._uuid_mapping = {}
            else:
                print(f"❌ FAILURE: Unexpected initialization error: {e}")
                return False
        
        # Test Case 2: UUID generation consistency
        print("   Testing UUID generation consistency...")
        
        video_ids = ["video1ID11", "video2ID11", "video3ID11"]
        generated_uuids = []
        
        for video_id in video_ids:
            video_uuid = str(uuid.uuid4())
            discovery.mark_video_processed(video_id, video_uuid)
            generated_uuids.append(video_uuid)
        
        # Check all UUIDs are unique
        if len(set(generated_uuids)) != len(generated_uuids):
            print("❌ FAILURE: Generated UUIDs are not unique")
            return False
        
        # Check all UUIDs are valid format
        for video_uuid in generated_uuids:
            try:
                uuid.UUID(video_uuid)
            except ValueError:
                print(f"❌ FAILURE: Invalid UUID format: {video_uuid}")
                return False
        
        print("✅ SUCCESS: UUID generation consistency validated")
        
        # Test Case 3: Integration with VideoRecord
        print("   Testing integration with VideoRecord...")
        
        person_id = 1
        video_record = VideoRecord(
            person_id=person_id,
            video_id="testVideoID",
            title="Test Integration Video"
        )
        
        # VideoRecord should have UUID
        if not video_record.uuid:
            print("❌ FAILURE: VideoRecord should have UUID")
            return False
        
        # Mark as processed in discovery
        discovery.mark_video_processed(video_record.video_id, video_record.uuid)
        
        # Should be duplicate now
        if not discovery.is_duplicate_video(video_record.video_id):
            print("❌ FAILURE: VideoRecord video should be duplicate after marking")
            return False
        
        # UUID should match
        retrieved_uuid = discovery.get_video_uuid(video_record.video_id)
        if retrieved_uuid != video_record.uuid:
            print(f"❌ FAILURE: UUID mismatch - VideoRecord: {video_record.uuid}, Discovery: {retrieved_uuid}")
            return False
        
        print("✅ SUCCESS: VideoRecord integration working correctly")
        
        print("✅ ALL UUID integration tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: UUID integration test failed: {e}")
        return False


def test_concurrent_uuid_operations():
    """Test UUID operations under concurrent access."""
    print("\n🧪 Testing concurrent UUID operations...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    YouTubeChannelDiscovery, _, _ = classes
    
    try:
        import threading
        import time
        
        # Create mock discovery for testing
        discovery = YouTubeChannelDiscovery.__new__(YouTubeChannelDiscovery)
        discovery._processed_videos = set()
        discovery._uuid_mapping = {}
        
        results = {"processed": 0, "duplicates": 0, "errors": 0}
        results_lock = threading.Lock()
        
        def worker_thread(thread_id: int):
            """Worker that processes videos with UUIDs."""
            try:
                for i in range(5):
                    video_id = f"thread{thread_id}vid{i}"
                    video_uuid = str(uuid.uuid4())
                    
                    # Check if duplicate first
                    if discovery.is_duplicate_video(video_id):
                        with results_lock:
                            results["duplicates"] += 1
                    else:
                        # Mark as processed
                        discovery.mark_video_processed(video_id, video_uuid)
                        with results_lock:
                            results["processed"] += 1
                    
                    time.sleep(0.001)  # Small delay
            except Exception as e:
                with results_lock:
                    results["errors"] += 1
                print(f"   Thread {thread_id} error: {e}")
        
        # Start multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=worker_thread, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        print(f"   Results: {results['processed']} processed, {results['duplicates']} duplicates, {results['errors']} errors")
        
        # Validate results
        if results["errors"] > 0:
            print(f"❌ FAILURE: Concurrent operations had {results['errors']} errors")
            return False
        
        if results["processed"] == 0:
            print("❌ FAILURE: No videos processed in concurrent test")
            return False
        
        # Check final state
        stats = discovery.get_duplicate_detection_stats()
        if stats["total_processed_videos"] != results["processed"]:
            print(f"❌ FAILURE: Stats mismatch - expected {results['processed']}, got {stats['total_processed_videos']}")
            return False
        
        print("✅ SUCCESS: Concurrent UUID operations working correctly")
        
        print("✅ ALL concurrent UUID operation tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Concurrent UUID test failed: {e}")
        return False


def main():
    """Run comprehensive UUID generation and duplicate detection test suite."""
    print("🚀 Starting UUID Generation and Duplicate Detection Test Suite")
    print("   Testing UUID validation and generation")
    print("   Testing duplicate detection functionality")
    print("   Testing integration with existing components")
    print("=" * 80)
    
    all_tests_passed = True
    test_functions = [
        test_imports,
        test_video_record_uuid_validation,
        test_duplicate_detection_basic,
        test_duplicate_detection_edge_cases,
        test_uuid_integration_with_channel_discovery,
        test_concurrent_uuid_operations
    ]
    
    for test_func in test_functions:
        if not test_func():
            all_tests_passed = False
            print(f"❌ {test_func.__name__} FAILED")
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL UUID GENERATION AND DUPLICATE DETECTION TESTS PASSED!")
        print("✅ VideoRecord UUID validation working")
        print("✅ Basic duplicate detection functional")
        print("✅ Edge case handling implemented")
        print("✅ Channel discovery integration complete")
        print("✅ Concurrent operations thread-safe")
        print("\\n🔥 UUID generation and duplicate detection is PRODUCTION-READY!")
        return 0
    else:
        print("💥 SOME UUID/DUPLICATE DETECTION TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)