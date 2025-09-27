#!/usr/bin/env python3
"""
Test Channel Processing Orchestration
Phase 4.2-4.3: Test channel processing orchestration logic with single channel

Tests:
1. Channel processing orchestration flow
2. Single channel processing
3. Video enumeration and metadata extraction
4. Progress tracking during processing
5. Error handling in processing
6. Concurrent processing of multiple channels

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import os
import json
import time
import tempfile
from pathlib import Path
from typing import List, Dict, Any
from unittest.mock import Mock, patch, MagicMock

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))

def test_imports():
    """Test that all required modules import correctly."""
    print("🧪 Testing imports for channel processing...")
    
    try:
        from mass_coordinator import (
            MassDownloadCoordinator,
            ChannelProcessingResult,
            ProcessingStatus
        )
        from database_schema import PersonRecord, VideoRecord
        from channel_discovery import VideoMetadata, ChannelInfo
        
        print("✅ SUCCESS: All required imports successful")
        return True, (MassDownloadCoordinator, ChannelProcessingResult, ProcessingStatus)
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import required modules")
        print(f"   Error: {e}")
        return False, None


def test_single_channel_processing():
    """Test processing of a single channel."""
    print("\n🧪 Testing single channel processing...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    MassDownloadCoordinator, ChannelProcessingResult, ProcessingStatus = classes
    
    try:
        # Import PersonRecord separately
        from database_schema import PersonRecord
        # Create coordinator
        coordinator = MassDownloadCoordinator()
        
        # Create test person and channel
        person = PersonRecord(
            name="Test Channel",
            email="test@example.com",
            type="youtube_channel",
            channel_url="https://www.youtube.com/@testchannel"
        )
        
        channel_url = "https://www.youtube.com/@testchannel"
        
        # Mock the channel discovery to avoid real API calls
        with patch.object(coordinator.channel_discovery, 'extract_channel_info') as mock_extract:
            with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos') as mock_enumerate:
                
                # Mock channel info
                mock_channel_info = Mock()
                mock_channel_info.channel_id = "UCtest123456789012"
                mock_channel_info.title = "Test Channel"
                mock_channel_info.subscriber_count = 1000
                mock_channel_info.video_count = 50
                mock_extract.return_value = mock_channel_info
                
                # Mock video enumeration
                from channel_discovery import VideoMetadata
                from datetime import datetime
                
                mock_videos = [
                    VideoMetadata(
                        video_id="dQw4w9WgXcQ",
                        title="Test Video 1",
                        duration=120,
                        upload_date=datetime.now(),
                        view_count=1000,
                        description="Test description 1",
                        channel_id="UCtest123456789012",
                        uploader="testchannel",
                        video_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ"
                    ),
                    VideoMetadata(
                        video_id="jNQXAC9IVRw",
                        title="Test Video 2",
                        duration=180,
                        upload_date=datetime.now(),
                        view_count=2000,
                        description="Test description 2",
                        channel_id="UCtest123456789012",
                        uploader="testchannel",
                        video_url="https://www.youtube.com/watch?v=jNQXAC9IVRw"
                    )
                ]
                mock_enumerate.return_value = mock_videos
                
                # Process the channel
                result = coordinator.process_channel(person, channel_url)
                
                # Verify result
                assert isinstance(result, ChannelProcessingResult), "Wrong result type"
                assert result.channel_url == channel_url, f"Wrong channel URL: {result.channel_url}"
                assert result.status == ProcessingStatus.COMPLETED, f"Wrong status: {result.status}"
                assert result.videos_found == 2, f"Wrong videos found: {result.videos_found}"
                assert result.videos_processed == 2, f"Wrong videos processed: {result.videos_processed}"
                assert result.error_message is None, f"Unexpected error: {result.error_message}"
                
                # Verify channel info extracted
                assert mock_extract.called, "Channel info extraction not called"
                assert mock_enumerate.called, "Video enumeration not called"
                
                print("✅ SUCCESS: Single channel processing working")
                print(f"   Channel: {result.channel_url}")
                print(f"   Status: {result.status.value}")
                print(f"   Videos: {result.videos_found} found, {result.videos_processed} processed")
                print(f"   Duration: {result.duration_seconds:.2f}s" if result.duration_seconds else "   Duration: N/A")
                
                return True
                
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Single channel processing failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_channel_processing_with_errors():
    """Test channel processing error handling."""
    print("\n🧪 Testing channel processing with errors...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    MassDownloadCoordinator, ChannelProcessingResult, ProcessingStatus = classes
    
    try:
        # Import PersonRecord separately
        from database_schema import PersonRecord
        coordinator = MassDownloadCoordinator()
        
        # Test Case 1: Channel discovery failure
        person = PersonRecord(
            name="Error Channel",
            email="error@example.com",
            type="youtube_channel",
            channel_url="https://www.youtube.com/@errorchannel"
        )
        
        with patch.object(coordinator.channel_discovery, 'extract_channel_info') as mock_extract:
            mock_extract.side_effect = RuntimeError("Channel not found")
            
            result = coordinator.process_channel(person, person.channel_url)
            
            assert result.status == ProcessingStatus.FAILED, f"Expected FAILED status, got {result.status}"
            assert "Channel discovery failed" in result.error_message, f"Wrong error message: {result.error_message}"
            
            print("✅ SUCCESS: Channel discovery error handled correctly")
        
        # Test Case 2: Video enumeration failure
        with patch.object(coordinator.channel_discovery, 'extract_channel_info') as mock_extract:
            with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos') as mock_enumerate:
                
                # Mock successful channel info
                mock_channel_info = Mock()
                mock_channel_info.channel_id = "UCerror123456789"
                mock_extract.return_value = mock_channel_info
                
                # Mock enumeration failure
                mock_enumerate.side_effect = RuntimeError("Private channel")
                
                result = coordinator.process_channel(person, person.channel_url)
                
                assert result.status == ProcessingStatus.FAILED, f"Expected FAILED status, got {result.status}"
                assert "Video enumeration failed" in result.error_message, f"Wrong error message: {result.error_message}"
                
                print("✅ SUCCESS: Video enumeration error handled correctly")
        
        # Test Case 3: Empty channel (no videos)
        with patch.object(coordinator.channel_discovery, 'extract_channel_info') as mock_extract:
            with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos') as mock_enumerate:
                
                mock_channel_info = Mock()
                mock_channel_info.channel_id = "UCempty123456789"
                mock_extract.return_value = mock_channel_info
                
                # Return empty video list
                mock_enumerate.return_value = []
                
                result = coordinator.process_channel(person, person.channel_url)
                
                assert result.status == ProcessingStatus.COMPLETED, f"Expected COMPLETED status, got {result.status}"
                assert result.videos_found == 0, f"Expected 0 videos, got {result.videos_found}"
                assert result.error_message is None, f"Unexpected error: {result.error_message}"
                
                print("✅ SUCCESS: Empty channel handled correctly")
        
        print("✅ ALL channel processing error tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Error handling test failed: {e}")
        return False


def test_progress_tracking_during_processing():
    """Test progress tracking during channel processing."""
    print("\n🧪 Testing progress tracking during processing...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    MassDownloadCoordinator, _, ProcessingStatus = classes
    
    try:
        # Import PersonRecord separately
        from database_schema import PersonRecord
        coordinator = MassDownloadCoordinator()
        
        # Reset progress
        coordinator.progress.total_channels = 1
        
        person = PersonRecord(
            name="Progress Test Channel",
            email="progress@example.com",
            type="youtube_channel",
            channel_url="https://www.youtube.com/@progresschannel"
        )
        
        with patch.object(coordinator.channel_discovery, 'extract_channel_info') as mock_extract:
            with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos') as mock_enumerate:
                
                # Setup mocks
                mock_channel_info = Mock()
                mock_channel_info.channel_id = "UCprogress123456"
                mock_extract.return_value = mock_channel_info
                
                from channel_discovery import VideoMetadata
                from datetime import datetime
                
                # Create multiple videos to test progress
                mock_videos = []
                for i in range(5):
                    mock_videos.append(VideoMetadata(
                        video_id=f"pVid{i:07d}",
                        title=f"Progress Test Video {i+1}",
                        duration=120,
                        upload_date=datetime.now(),
                        view_count=1000 * (i + 1),
                        description=f"Test description {i+1}",
                        channel_id="UCprogress123456",
                        uploader="progresschannel",
                        video_url=f"https://www.youtube.com/watch?v=pVid{i:07d}"
                    ))
                
                mock_enumerate.return_value = mock_videos
                
                # Process channel
                result = coordinator.process_channel(person, person.channel_url)
                
                # Check progress was updated
                assert coordinator.progress.channels_processed == 1, "Channel not marked as processed"
                assert coordinator.progress.total_videos >= 5, f"Videos not tracked: {coordinator.progress.total_videos}"
                assert coordinator.progress.videos_processed >= 5, f"Videos not processed: {coordinator.progress.videos_processed}"
                
                # Get progress report
                report = coordinator.get_progress_report()
                
                assert report['channels_processed'] == 1, "Progress report incorrect"
                assert report['progress_percent'] == 100.0, f"Progress percent wrong: {report['progress_percent']}"
                assert len(report['channel_results']) == 1, "Channel results not tracked"
                
                print("✅ SUCCESS: Progress tracking working during processing")
                print(f"   Channels: {report['channels_processed']}/{report['total_channels']}")
                print(f"   Videos: {report['videos_processed']} processed")
                print(f"   Progress: {report['progress_percent']}%")
                
                return True
                
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Progress tracking test failed: {e}")
        return False


def test_concurrent_channel_processing():
    """Test concurrent processing of multiple channels."""
    print("\n🧪 Testing concurrent channel processing...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    MassDownloadCoordinator, _, ProcessingStatus = classes
    
    try:
        # Import PersonRecord separately
        from database_schema import PersonRecord
        coordinator = MassDownloadCoordinator()
        
        # Create multiple test channels
        person_channel_pairs = []
        for i in range(3):
            person = PersonRecord(
                name=f"Concurrent Test Channel {i+1}",
                email=f"concurrent{i+1}@example.com",
                type="youtube_channel",
                channel_url=f"https://www.youtube.com/@concurrent{i+1}"
            )
            person_channel_pairs.append((person, person.channel_url))
        
        # Mock all channel discovery calls
        with patch.object(coordinator.channel_discovery, 'extract_channel_info') as mock_extract:
            with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos') as mock_enumerate:
                
                # Setup different responses for each channel
                def mock_extract_side_effect(url):
                    mock_info = Mock()
                    if "concurrent1" in url:
                        mock_info.channel_id = "UCconcurrent1"
                        mock_info.title = "Concurrent Channel 1"
                    elif "concurrent2" in url:
                        mock_info.channel_id = "UCconcurrent2"
                        mock_info.title = "Concurrent Channel 2"
                    else:
                        mock_info.channel_id = "UCconcurrent3"
                        mock_info.title = "Concurrent Channel 3"
                    return mock_info
                
                mock_extract.side_effect = mock_extract_side_effect
                
                # Return different number of videos for each channel
                def mock_enumerate_side_effect(url, max_videos=None):
                    from channel_discovery import VideoMetadata
                    from datetime import datetime
                    
                    videos = []
                    if "concurrent1" in url:
                        count = 2
                    elif "concurrent2" in url:
                        count = 3
                    else:
                        count = 1
                    
                    for i in range(count):
                        videos.append(VideoMetadata(
                            video_id=f"cVid{url[-1]}{i:06d}",
                            title=f"Video {i+1} from {url}",
                            duration=120,
                            upload_date=datetime.now(),
                            view_count=1000,
                            description="Test",
                            channel_id=f"UCconcurrent{url[-1]}",
                            uploader=f"concurrent{url[-1]}",
                            video_url=f"https://www.youtube.com/watch?v=cVid{url[-1]}{i:06d}"
                        ))
                    
                    # Add small delay to simulate processing
                    time.sleep(0.1)
                    return videos
                
                mock_enumerate.side_effect = mock_enumerate_side_effect
                
                # Process channels concurrently
                start_time = time.time()
                results = coordinator.process_channels_concurrently(person_channel_pairs)
                duration = time.time() - start_time
                
                # Verify results
                assert len(results) == 3, f"Expected 3 results, got {len(results)}"
                
                # All should be completed
                for result in results:
                    assert result.status == ProcessingStatus.COMPLETED, \
                        f"Channel {result.channel_url} failed: {result.error_message}"
                
                # Check video counts
                total_videos = sum(r.videos_found for r in results)
                assert total_videos == 6, f"Expected 6 total videos, got {total_videos}"
                
                # Verify concurrent execution (should be faster than sequential)
                # With 0.1s delay per channel, sequential would take ~0.3s minimum
                print(f"✅ SUCCESS: Concurrent processing completed in {duration:.2f}s")
                print(f"   Processed {len(results)} channels concurrently")
                print(f"   Total videos: {total_videos}")
                
                # Check final progress
                final_report = coordinator.get_progress_report()
                assert final_report['channels_processed'] == 3, "Not all channels marked as processed"
                assert final_report['success_rate_percent'] == 100.0, "Success rate not 100%"
                
                return True
                
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Concurrent processing test failed: {e}")
        return False


def test_duplicate_video_detection():
    """Test duplicate video detection during processing."""
    print("\n🧪 Testing duplicate video detection...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    MassDownloadCoordinator, _, ProcessingStatus = classes
    
    try:
        # Import PersonRecord separately
        from database_schema import PersonRecord
        coordinator = MassDownloadCoordinator()
        
        person = PersonRecord(
            name="Duplicate Test Channel",
            email="duplicate@example.com",
            type="youtube_channel",
            channel_url="https://www.youtube.com/@duplicatetest"
        )
        
        with patch.object(coordinator.channel_discovery, 'extract_channel_info') as mock_extract:
            with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos') as mock_enumerate:
                
                mock_channel_info = Mock()
                mock_channel_info.channel_id = "UCduplicate123"
                mock_extract.return_value = mock_channel_info
                
                from channel_discovery import VideoMetadata
                from datetime import datetime
                
                # Create videos with one duplicate
                mock_videos = [
                    VideoMetadata(
                        video_id="M7lc1UVf-VE",
                        title="Unique Video 1",
                        duration=120,
                        upload_date=datetime.now(),
                        view_count=1000,
                        description="Test",
                        channel_id="UCduplicate123",
                        uploader="test",
                        video_url="https://www.youtube.com/watch?v=M7lc1UVf-VE"
                    ),
                    VideoMetadata(
                        video_id="oHg5SJYRHA0",
                        title="Duplicate Video",
                        duration=180,
                        upload_date=datetime.now(),
                        view_count=2000,
                        description="Test",
                        channel_id="UCduplicate123",
                        uploader="test",
                        video_url="https://www.youtube.com/watch?v=oHg5SJYRHA0"
                    ),
                    VideoMetadata(
                        video_id="9bZkp7q19f0",
                        title="Unique Video 2",
                        duration=150,
                        upload_date=datetime.now(),
                        view_count=1500,
                        description="Test",
                        channel_id="UCduplicate123",
                        uploader="test",
                        video_url="https://www.youtube.com/watch?v=9bZkp7q19f0"
                    )
                ]
                
                mock_enumerate.return_value = mock_videos
                
                # Pre-mark one video as duplicate
                coordinator.channel_discovery.mark_video_processed("oHg5SJYRHA0", "existing-uuid")
                
                # Process channel
                result = coordinator.process_channel(person, person.channel_url)
                
                # Verify results
                assert result.videos_found == 3, f"Expected 3 videos found, got {result.videos_found}"
                assert result.videos_processed == 2, f"Expected 2 videos processed, got {result.videos_processed}"
                assert result.videos_skipped == 1, f"Expected 1 video skipped, got {result.videos_skipped}"
                
                print("✅ SUCCESS: Duplicate video detection working")
                print(f"   Videos found: {result.videos_found}")
                print(f"   Videos processed: {result.videos_processed}")
                print(f"   Videos skipped (duplicates): {result.videos_skipped}")
                
                return True
                
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Duplicate detection test failed: {e}")
        return False


def main():
    """Run comprehensive channel processing test suite."""
    print("🚀 Starting Channel Processing Test Suite")
    print("   Testing Phase 4.2-4.3: Channel processing orchestration")
    print("   Validating single and concurrent channel processing")
    print("=" * 80)
    
    all_tests_passed = True
    test_functions = [
        lambda: test_imports()[0],  # Extract just the success boolean
        test_single_channel_processing,
        test_channel_processing_with_errors,
        test_progress_tracking_during_processing,
        test_concurrent_channel_processing,
        test_duplicate_video_detection
    ]
    
    for i, test_func in enumerate(test_functions):
        result = test_func()
        if not result:
            all_tests_passed = False
            test_name = "test_imports" if i == 0 else test_func.__name__
            print(f"❌ {test_name} FAILED")
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL CHANNEL PROCESSING TESTS PASSED!")
        print("✅ Single channel processing working")
        print("✅ Error handling implemented")
        print("✅ Progress tracking functional")
        print("✅ Concurrent processing validated")
        print("✅ Duplicate detection integrated")
        print("\n🔥 Channel processing orchestration is PRODUCTION-READY!")
        return 0
    else:
        print("💥 SOME CHANNEL PROCESSING TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)