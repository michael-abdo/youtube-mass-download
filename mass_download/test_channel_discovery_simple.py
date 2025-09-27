#!/usr/bin/env python3
"""
Simple Unit Tests for Channel Discovery Module
Phase 6.4: Run channel discovery tests with mocked data

This is a simplified test suite that tests the actual methods available
in the channel discovery module.
"""
import unittest
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mass_download.channel_discovery import (
    YouTubeChannelDiscovery, ChannelInfo, VideoMetadata
)


class TestChannelDiscovery(unittest.TestCase):
    """Test channel discovery functionality."""
    
    def setUp(self):
        """Set up test instance."""
        # Create discovery instance (yt-dlp validation will happen automatically)
        self.discovery = YouTubeChannelDiscovery()
    
    @patch('mass_download.channel_discovery.yt_dlp')
    def test_extract_channel_info_success(self, mock_yt_dlp):
        """Test successful channel info extraction."""
        # Mock YoutubeDL instance
        mock_ydl = MagicMock()
        mock_yt_dlp.YoutubeDL.return_value.__enter__.return_value = mock_ydl
        
        # Mock yt-dlp response
        mock_ydl.extract_info.return_value = {
            'channel': 'Test Channel',
            'channel_id': 'UCtest123456789012345678',
            'channel_url': 'https://youtube.com/channel/UCtest123456789012345678',
            'uploader': 'Test Channel',
            'uploader_id': '@testchannel',
            'description': 'This is a test channel description',
            'channel_follower_count': 12345,
            '_type': 'channel'
        }
        
        # Extract channel info
        channel_info = self.discovery.extract_channel_info("https://youtube.com/@testchannel")
        
        # Verify results
        self.assertIsInstance(channel_info, ChannelInfo)
        self.assertEqual(channel_info.channel_name, "Test Channel")
        self.assertEqual(channel_info.channel_id, "UCtest123456789012345678")
        self.assertEqual(channel_info.description, "This is a test channel description")
        self.assertEqual(channel_info.subscriber_count, 12345)
        print("✓ test_extract_channel_info_success passed")
    
    @patch('mass_download.channel_discovery.yt_dlp')
    def test_enumerate_channel_videos_success(self, mock_yt_dlp):
        """Test successful video enumeration."""
        # Mock YoutubeDL instance
        mock_ydl = MagicMock()
        mock_yt_dlp.YoutubeDL.return_value.__enter__.return_value = mock_ydl
        
        # Mock channel with videos
        mock_ydl.extract_info.return_value = {
            'channel': 'Test Channel',
            'channel_id': 'UCtest123',
            '_type': 'channel',
            'entries': [
                {
                    'id': 'video1',
                    'title': 'Test Video 1',
                    'duration': 300,
                    'upload_date': '20240101',
                    'view_count': 1000,
                    'like_count': 100,
                    'description': 'Video 1 description',
                    'thumbnail': 'https://example.com/thumb1.jpg',
                    'webpage_url': 'https://youtube.com/watch?v=video1'
                },
                {
                    'id': 'video2',
                    'title': 'Test Video 2',
                    'duration': 600,
                    'upload_date': '20240102',
                    'view_count': 2000,
                    'like_count': 200,
                    'description': 'Video 2 description',
                    'thumbnail': 'https://example.com/thumb2.jpg',
                    'webpage_url': 'https://youtube.com/watch?v=video2'
                }
            ]
        }
        
        # Enumerate videos
        videos = self.discovery.enumerate_channel_videos("https://youtube.com/@test")
        
        # Verify results
        self.assertEqual(len(videos), 2)
        
        # Check first video
        self.assertIsInstance(videos[0], VideoMetadata)
        self.assertEqual(videos[0].video_id, "video1")
        self.assertEqual(videos[0].title, "Test Video 1")
        self.assertEqual(videos[0].duration, 300)
        self.assertEqual(videos[0].view_count, 1000)
        
        # Check second video
        self.assertEqual(videos[1].video_id, "video2")
        self.assertEqual(videos[1].title, "Test Video 2")
        self.assertEqual(videos[1].duration, 600)
        print("✓ test_enumerate_channel_videos_success passed")
    
    @patch('mass_download.channel_discovery.yt_dlp')
    def test_enumerate_videos_with_limit(self, mock_yt_dlp):
        """Test video enumeration with max_videos limit."""
        # Mock YoutubeDL instance
        mock_ydl = MagicMock()
        mock_yt_dlp.YoutubeDL.return_value.__enter__.return_value = mock_ydl
        
        # Mock channel with many videos
        entries = [
            {
                'id': f'video{i}',
                'title': f'Video {i}',
                'duration': 100 * i,
                'upload_date': f'2024010{i}',
                'webpage_url': f'https://youtube.com/watch?v=video{i}'
            }
            for i in range(1, 11)  # 10 videos
        ]
        
        mock_ydl.extract_info.return_value = {
            'channel': 'Test Channel',
            '_type': 'channel',
            'entries': entries
        }
        
        # Enumerate with limit
        videos = self.discovery.enumerate_channel_videos("https://youtube.com/@test", max_videos=5)
        
        # Should return only 5 videos
        self.assertEqual(len(videos), 5)
        self.assertEqual(videos[0].video_id, "video1")
        self.assertEqual(videos[-1].video_id, "video5")
        print("✓ test_enumerate_videos_with_limit passed")
    
    def test_duplicate_detection(self):
        """Test duplicate video detection."""
        # Mark some videos as processed
        self.discovery.mark_video_processed("video1", "uuid1")
        self.discovery.mark_video_processed("video2", "uuid2")
        
        # Check duplicates
        self.assertTrue(self.discovery.is_duplicate_video("video1"))
        self.assertTrue(self.discovery.is_duplicate_video("video2"))
        self.assertFalse(self.discovery.is_duplicate_video("video3"))
        print("✓ test_duplicate_detection passed")
    
    def test_get_video_uuid(self):
        """Test retrieving video UUID."""
        # Mark video with UUID
        self.discovery.mark_video_processed("video_uuid", "test-uuid-123")
        
        # Retrieve UUID
        uuid = self.discovery.get_video_uuid("video_uuid")
        self.assertEqual(uuid, "test-uuid-123")
        
        # Non-existent video
        uuid = self.discovery.get_video_uuid("non_existent")
        self.assertIsNone(uuid)
        print("✓ test_get_video_uuid passed")
    
    @patch('mass_download.channel_discovery.yt_dlp')
    def test_extract_video_metadata(self, mock_yt_dlp):
        """Test video metadata extraction."""
        # Mock YoutubeDL instance
        mock_ydl = MagicMock()
        mock_yt_dlp.YoutubeDL.return_value.__enter__.return_value = mock_ydl
        
        # Mock channel with detailed video metadata
        mock_ydl.extract_info.return_value = {
            'channel': 'Test Channel',
            '_type': 'channel',
            'entries': [
                {
                    'id': 'test123',
                    'title': 'Test Video Title',
                    'duration': 3661,  # 1h 1m 1s
                    'upload_date': '20240115',
                    'view_count': 12345,
                    'like_count': 1000,
                    'dislike_count': 50,
                    'comment_count': 200,
                    'description': 'Test description',
                    'thumbnail': 'https://example.com/thumb.jpg',
                    'webpage_url': 'https://youtube.com/watch?v=test123',
                    'uploader': 'Test Channel',
                    'uploader_id': '@testchannel',
                    'tags': ['tag1', 'tag2', 'tag3'],
                    'categories': ['Education'],
                    'age_limit': 0,
                    'live_status': 'not_live',
                    'availability': 'public',
                    'language': 'en'
                }
            ]
        }
        
        # Extract videos to get metadata
        videos = self.discovery.enumerate_channel_videos("https://youtube.com/@test")
        
        # Verify metadata
        self.assertEqual(len(videos), 1)
        metadata = videos[0]
        self.assertEqual(metadata.video_id, "test123")
        self.assertEqual(metadata.title, "Test Video Title")
        self.assertEqual(metadata.duration, 3661)
        self.assertEqual(metadata.view_count, 12345)
        self.assertEqual(metadata.like_count, 1000)
        self.assertEqual(metadata.tags, ['tag1', 'tag2', 'tag3'])
        print("✓ test_extract_video_metadata passed")
    
    @patch('mass_download.channel_discovery.yt_dlp')
    def test_error_handling(self, mock_yt_dlp):
        """Test error handling."""
        # Mock YoutubeDL instance
        mock_ydl = MagicMock()
        mock_yt_dlp.YoutubeDL.return_value.__enter__.return_value = mock_ydl
        
        # Mock extraction error
        mock_ydl.extract_info.side_effect = Exception("Network error")
        
        # Should raise exception
        with self.assertRaises(Exception):
            self.discovery.extract_channel_info("https://youtube.com/@error")
        print("✓ test_error_handling passed")


def run_simple_tests():
    """Run the simple test suite."""
    print("Running simplified channel discovery tests...")
    print("=" * 60)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestChannelDiscovery)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print("✓ All tests passed!")
    else:
        print(f"✗ {len(result.failures)} failures, {len(result.errors)} errors")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_simple_tests()
    sys.exit(0 if success else 1)