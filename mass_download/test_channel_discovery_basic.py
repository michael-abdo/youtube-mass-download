#!/usr/bin/env python3
"""
Basic Unit Tests for Channel Discovery Module
Phase 6.4: Run channel discovery tests with mocked data

This is a basic test suite that tests the channel discovery module
without mocking external calls. It focuses on testing the internal
functionality that doesn't require network access.
"""
import unittest
from datetime import datetime
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mass_download.channel_discovery import (
    YouTubeChannelDiscovery, ChannelInfo, VideoMetadata
)


class TestChannelDiscoveryBasic(unittest.TestCase):
    """Test basic channel discovery functionality."""
    
    def setUp(self):
        """Set up test instance."""
        # Create discovery instance
        self.discovery = YouTubeChannelDiscovery()
    
    def test_duplicate_detection(self):
        """Test duplicate video detection."""
        print("\n  Testing duplicate detection...")
        
        # Initially no videos should be marked as duplicates
        self.assertFalse(self.discovery.is_duplicate_video("new_video_1"))
        self.assertFalse(self.discovery.is_duplicate_video("new_video_2"))
        
        # Mark some videos as processed
        self.discovery.mark_video_processed("video1", "uuid-123-456")
        self.discovery.mark_video_processed("video2", "uuid-789-012")
        
        # Check duplicates
        self.assertTrue(self.discovery.is_duplicate_video("video1"))
        self.assertTrue(self.discovery.is_duplicate_video("video2"))
        self.assertFalse(self.discovery.is_duplicate_video("video3"))
        
        print("  ✓ Duplicate detection working correctly")
    
    def test_get_video_uuid(self):
        """Test retrieving video UUID."""
        print("\n  Testing UUID retrieval...")
        
        # Mark video with UUID
        test_uuid = "test-uuid-abc-123"
        self.discovery.mark_video_processed("test_video", test_uuid)
        
        # Retrieve UUID
        retrieved_uuid = self.discovery.get_video_uuid("test_video")
        self.assertEqual(retrieved_uuid, test_uuid)
        
        # Non-existent video should return None
        non_existent_uuid = self.discovery.get_video_uuid("non_existent")
        self.assertIsNone(non_existent_uuid)
        
        print("  ✓ UUID retrieval working correctly")
    
    def test_duplicate_tracking_persistence(self):
        """Test that duplicate tracking persists within instance."""
        print("\n  Testing duplicate tracking persistence...")
        
        # Add multiple videos
        videos = {
            "vid1": "uuid-1",
            "vid2": "uuid-2", 
            "vid3": "uuid-3",
            "vid4": "uuid-4",
            "vid5": "uuid-5"
        }
        
        for video_id, uuid in videos.items():
            self.discovery.mark_video_processed(video_id, uuid)
        
        # Verify all are tracked
        for video_id, expected_uuid in videos.items():
            self.assertTrue(self.discovery.is_duplicate_video(video_id))
            self.assertEqual(self.discovery.get_video_uuid(video_id), expected_uuid)
        
        # Verify count
        self.assertEqual(len(self.discovery._processed_videos), 5)
        
        print("  ✓ Duplicate tracking persistence working correctly")
    
    def test_channel_info_dataclass(self):
        """Test ChannelInfo dataclass creation."""
        print("\n  Testing ChannelInfo dataclass...")
        
        # Create channel info
        channel_info = ChannelInfo(
            channel_id="UCtest123",
            channel_url="https://youtube.com/channel/UCtest123",
            title="Test Channel",
            description="Test description",
            subscriber_count=1000
        )
        
        # Verify attributes
        self.assertEqual(channel_info.title, "Test Channel")
        self.assertEqual(channel_info.channel_id, "UCtest123")
        self.assertEqual(channel_info.subscriber_count, 1000)
        
        print("  ✓ ChannelInfo dataclass working correctly")
    
    def test_video_metadata_dataclass(self):
        """Test VideoMetadata dataclass creation."""
        print("\n  Testing VideoMetadata dataclass...")
        
        # Create video metadata
        from datetime import datetime
        metadata = VideoMetadata(
            video_id="testvideo01",  # Must be 11 chars for YouTube
            title="Test Video Title",
            description="Test video description",
            duration=300,
            upload_date=datetime(2024, 1, 15),
            view_count=10000,
            like_count=500,
            comment_count=100,
            tags=["test", "video", "example"],
            categories=["Education"],
            thumbnail_url="https://example.com/thumb.jpg",
            video_url="https://youtube.com/watch?v=test_video_1",
            channel_id="UCtest123",
            uploader="Test Channel",
            is_live=False,
            age_restricted=False
        )
        
        # Verify attributes
        self.assertEqual(metadata.video_id, "testvideo01")
        self.assertEqual(metadata.title, "Test Video Title")
        self.assertEqual(metadata.duration, 300)
        self.assertEqual(metadata.view_count, 10000)
        self.assertEqual(len(metadata.tags), 3)
        self.assertFalse(metadata.is_live)
        self.assertFalse(metadata.age_restricted)
        
        print("  ✓ VideoMetadata dataclass working correctly")
    
    def test_initialization(self):
        """Test that YouTubeChannelDiscovery initializes correctly."""
        print("\n  Testing initialization...")
        
        # Should have empty processed videos
        self.assertEqual(len(self.discovery._processed_videos), 0)
        
        # Should have config loaded
        self.assertIsNotNone(self.discovery.config)
        
        # Should have yt-dlp path set
        self.assertEqual(self.discovery.yt_dlp_path, "yt-dlp")
        
        print("  ✓ Initialization working correctly")


def run_basic_tests():
    """Run the basic test suite."""
    print("=" * 70)
    print("Running Basic Channel Discovery Tests")
    print("=" * 70)
    print("This tests the internal functionality without external dependencies")
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestChannelDiscoveryBasic)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 70)
    if result.wasSuccessful():
        print("✓ All basic tests passed!")
        print(f"  Ran {result.testsRun} tests successfully")
    else:
        print(f"✗ {len(result.failures)} failures, {len(result.errors)} errors")
        if result.failures:
            print("\nFailures:")
            for test, traceback in result.failures:
                print(f"  - {test}: {traceback.split(chr(10))[-2]}")
        if result.errors:
            print("\nErrors:")
            for test, traceback in result.errors:
                print(f"  - {test}: {traceback.split(chr(10))[-2]}")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_basic_tests()
    sys.exit(0 if success else 1)