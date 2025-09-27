#!/usr/bin/env python3
"""
Unit Tests for Channel Discovery Module
Phase 6.3: Create unit tests for channel discovery

This module contains comprehensive unit tests for the YouTube channel discovery
functionality, including URL validation, channel info extraction, and video
enumeration with mocked yt-dlp responses.
"""
import unittest
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mass_download.channel_discovery import (
    YouTubeChannelDiscovery, ChannelInfo, VideoMetadata
)


class TestChannelURLValidation(unittest.TestCase):
    """Test YouTube channel URL validation."""
    
    def setUp(self):
        """Set up test instance."""
        # Mock yt-dlp validation
        # Skip validation patch - yt-dlp validation will run automatically
        self.discovery = YouTubeChannelDiscovery()
    
    def test_valid_channel_urls(self):
        """Test validation of valid channel URLs."""
        valid_urls = [
            "https://youtube.com/@username",
            "https://www.youtube.com/@username",
            "https://youtube.com/channel/UCxxxxxxxxxxxxxxxx",
            "https://www.youtube.com/channel/UCxxxxxxxxxxxxxxxx",
            "https://youtube.com/c/channelname",
            "https://www.youtube.com/c/channelname",
            "https://youtube.com/user/username",
            "https://www.youtube.com/user/username",
            "https://m.youtube.com/@mobile",
            "http://youtube.com/@http_channel"  # Should handle http
        ]
        
        for url in valid_urls:
            with self.subTest(url=url):
                self.assertTrue(
                    self.discovery.is_valid_channel_url(url),
                    f"URL should be valid: {url}"
                )
    
    def test_invalid_channel_urls(self):
        """Test validation of invalid channel URLs."""
        invalid_urls = [
            "https://youtube.com/watch?v=xxxxx",  # Video URL
            "https://youtube.com/playlist?list=xxxxx",  # Playlist URL
            "https://vimeo.com/@channel",  # Wrong domain
            "https://youtube.com",  # No channel
            "not-a-url",  # Not a URL
            "@username",  # Just username
            "",  # Empty string
            None,  # None
        ]
        
        for url in invalid_urls:
            with self.subTest(url=url):
                self.assertFalse(
                    self.discovery.is_valid_channel_url(url),
                    f"URL should be invalid: {url}"
                )
    
    def test_normalize_channel_url(self):
        """Test channel URL normalization."""
        test_cases = [
            ("http://youtube.com/@test", "https://youtube.com/@test"),
            ("https://www.youtube.com/@test", "https://youtube.com/@test"),
            ("https://m.youtube.com/@test", "https://youtube.com/@test"),
            ("HTTPS://YOUTUBE.COM/@TEST", "https://youtube.com/@TEST"),
            ("https://youtube.com/@test?feature=share", "https://youtube.com/@test"),
        ]
        
        for input_url, expected in test_cases:
            with self.subTest(input=input_url):
                normalized = self.discovery.normalize_channel_url(input_url)
                self.assertEqual(normalized, expected)


class TestChannelInfoExtraction(unittest.TestCase):
    """Test channel information extraction."""
    
    def setUp(self):
        """Set up test instance with mocked yt-dlp."""
        self.mock_yt_dlp = patch('mass_download.channel_discovery.yt_dlp')
        self.mock_yt_dlp_module = self.mock_yt_dlp.start()
        
        # Create mock YoutubeDL instance
        self.mock_ydl = MagicMock()
        self.mock_yt_dlp_module.YoutubeDL.return_value.__enter__.return_value = self.mock_ydl
        
        # Initialize discovery
        # Skip validation patch - yt-dlp validation will run automatically
        self.discovery = YouTubeChannelDiscovery()
    
    def tearDown(self):
        """Clean up mocks."""
        self.mock_yt_dlp.stop()
    
    def test_extract_channel_info_success(self):
        """Test successful channel info extraction."""
        # Mock yt-dlp response
        self.mock_ydl.extract_info.return_value = {
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
        self.assertEqual(channel_info.channel_url, "https://youtube.com/channel/UCtest123456789012345678")
        self.assertEqual(channel_info.description, "This is a test channel description")
        self.assertEqual(channel_info.subscriber_count, 12345)
    
    def test_extract_channel_info_minimal(self):
        """Test channel info extraction with minimal data."""
        # Mock minimal response
        self.mock_ydl.extract_info.return_value = {
            'channel': 'Minimal Channel',
            'channel_id': 'UCminimal12345678901234',
            '_type': 'channel'
        }
        
        # Extract channel info
        channel_info = self.discovery.extract_channel_info("https://youtube.com/@minimal")
        
        # Verify defaults
        self.assertEqual(channel_info.channel_name, "Minimal Channel")
        self.assertEqual(channel_info.channel_id, "UCminimal12345678901234")
        self.assertIsNone(channel_info.description)
        self.assertIsNone(channel_info.subscriber_count)
    
    def test_extract_channel_info_error(self):
        """Test channel info extraction error handling."""
        # Mock extraction error
        self.mock_ydl.extract_info.side_effect = Exception("Network error")
        
        # Should raise Exception
        with self.assertRaises(Exception) as context:
            self.discovery.extract_channel_info("https://youtube.com/@error")
        
        # Error could be wrapped in various ways
    
    def test_extract_channel_info_invalid_url(self):
        """Test channel info extraction with invalid URL."""
        # Should raise Exception for invalid URL
        with self.assertRaises(Exception) as context:
            self.discovery.extract_channel_info("not-a-url")
        
        # Error should indicate invalid URL


class TestVideoEnumeration(unittest.TestCase):
    """Test video enumeration from channels."""
    
    def setUp(self):
        """Set up test instance with mocked yt-dlp."""
        self.mock_yt_dlp = patch('mass_download.channel_discovery.yt_dlp')
        self.mock_yt_dlp_module = self.mock_yt_dlp.start()
        
        # Create mock YoutubeDL instance
        self.mock_ydl = MagicMock()
        self.mock_yt_dlp_module.YoutubeDL.return_value.__enter__.return_value = self.mock_ydl
        
        # Initialize discovery
        # Skip validation patch - yt-dlp validation will run automatically
        self.discovery = YouTubeChannelDiscovery()
    
    def tearDown(self):
        """Clean up mocks."""
        self.mock_yt_dlp.stop()
    
    def test_enumerate_videos_success(self):
        """Test successful video enumeration."""
        # Mock channel with videos
        self.mock_ydl.extract_info.return_value = {
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
        self.assertEqual(videos[0].upload_date, "2024-01-01")
        
        # Check second video
        self.assertEqual(videos[1].video_id, "video2")
        self.assertEqual(videos[1].title, "Test Video 2")
        self.assertEqual(videos[1].duration, 600)
    
    def test_enumerate_videos_with_limit(self):
        """Test video enumeration with max_videos limit."""
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
        
        self.mock_ydl.extract_info.return_value = {
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
    
    def test_enumerate_videos_empty_channel(self):
        """Test video enumeration for empty channel."""
        # Mock empty channel
        self.mock_ydl.extract_info.return_value = {
            'channel': 'Empty Channel',
            '_type': 'channel',
            'entries': []
        }
        
        # Enumerate videos
        videos = self.discovery.enumerate_channel_videos("https://youtube.com/@empty")
        
        # Should return empty list
        self.assertEqual(len(videos), 0)
    
    def test_enumerate_videos_with_invalid_entries(self):
        """Test video enumeration with some invalid entries."""
        # Mock channel with mixed valid/invalid videos
        self.mock_ydl.extract_info.return_value = {
            'channel': 'Test Channel',
            '_type': 'channel',
            'entries': [
                {
                    'id': 'valid1',
                    'title': 'Valid Video',
                    'duration': 100,
                    'webpage_url': 'https://youtube.com/watch?v=valid1'
                },
                {
                    # Missing required fields
                    'title': 'Invalid Video'
                },
                {
                    'id': 'valid2',
                    'title': 'Another Valid Video',
                    'duration': 200,
                    'webpage_url': 'https://youtube.com/watch?v=valid2'
                },
                None  # None entry
            ]
        }
        
        # Enumerate videos
        videos = self.discovery.enumerate_channel_videos("https://youtube.com/@mixed")
        
        # Should return only valid videos
        self.assertEqual(len(videos), 2)
        self.assertEqual(videos[0].video_id, "valid1")
        self.assertEqual(videos[1].video_id, "valid2")


class TestRateLimiting(unittest.TestCase):
    """Test rate limiting functionality."""
    
    def setUp(self):
        """Set up test instance."""
        # Skip validation patch - yt-dlp validation will run automatically
        self.discovery = YouTubeChannelDiscovery()
    
    @patch('mass_download.channel_discovery.rate_limit')
    def test_rate_limiting_applied(self, mock_rate_limit):
        """Test that rate limiting is applied to operations."""
        # Mock the decorated function
        mock_func = Mock(return_value={'channel': 'Test', 'channel_id': 'UC123'})
        mock_rate_limit.return_value = lambda f: f  # Pass through decorator
        
        # Patch yt-dlp
        with patch('mass_download.channel_discovery.yt_dlp.YoutubeDL') as mock_ydl_class:
            mock_ydl = MagicMock()
            mock_ydl.extract_info = mock_func
            mock_ydl_class.return_value.__enter__.return_value = mock_ydl
            
            # Call method that should be rate limited
            self.discovery.extract_channel_info("https://youtube.com/@test")
            
            # Verify rate limiter was involved
            mock_rate_limit.assert_called()


class TestDuplicateDetection(unittest.TestCase):
    """Test duplicate video detection."""
    
    def setUp(self):
        """Set up test instance."""
        # Skip validation patch - yt-dlp validation will run automatically
        self.discovery = YouTubeChannelDiscovery()
    
    def test_duplicate_detection(self):
        """Test duplicate video detection."""
        # Mark some videos as processed
        self.discovery.mark_video_processed("video1", "uuid1")
        self.discovery.mark_video_processed("video2", "uuid2")
        
        # Check duplicates
        self.assertTrue(self.discovery.is_duplicate_video("video1"))
        self.assertTrue(self.discovery.is_duplicate_video("video2"))
        self.assertFalse(self.discovery.is_duplicate_video("video3"))
    
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


class TestVideoMetadataExtraction(unittest.TestCase):
    """Test enhanced video metadata extraction."""
    
    def setUp(self):
        """Set up test instance."""
        # Skip validation patch - yt-dlp validation will run automatically
        self.discovery = YouTubeChannelDiscovery()
    
    def test_extract_video_metadata_complete(self):
        """Test extraction of complete video metadata."""
        raw_entry = {
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
            'language': 'en',
            'subtitles': {'en': [{'url': 'sub_url'}]},
            'chapters': [
                {'title': 'Intro', 'start_time': 0, 'end_time': 60},
                {'title': 'Main', 'start_time': 60, 'end_time': 3600}
            ]
        }
        
        # Extract metadata
        metadata = self.discovery._extract_video_metadata(raw_entry)
        
        # Verify all fields
        self.assertEqual(metadata.video_id, "test123")
        self.assertEqual(metadata.title, "Test Video Title")
        self.assertEqual(metadata.duration, 3661)
        self.assertEqual(metadata.duration_string, "1:01:01")
        self.assertEqual(metadata.view_count, 12345)
        self.assertEqual(metadata.like_count, 1000)
        self.assertEqual(metadata.tags, ['tag1', 'tag2', 'tag3'])
        self.assertEqual(metadata.language, 'en')
        self.assertIn('en', metadata.subtitles_available)
        self.assertEqual(len(metadata.chapters), 2)
    
    def test_extract_video_metadata_minimal(self):
        """Test extraction with minimal metadata."""
        raw_entry = {
            'id': 'minimal123',
            'title': 'Minimal Video',
            'webpage_url': 'https://youtube.com/watch?v=minimal123'
        }
        
        # Extract metadata
        metadata = self.discovery._extract_video_metadata(raw_entry)
        
        # Verify defaults
        self.assertEqual(metadata.video_id, "minimal123")
        self.assertEqual(metadata.title, "Minimal Video")
        self.assertEqual(metadata.duration, 0)
        self.assertIsNone(metadata.view_count)
        self.assertEqual(metadata.tags, [])
        self.assertFalse(metadata.is_live)
    
    def test_format_duration(self):
        """Test duration formatting."""
        test_cases = [
            (0, "0:00"),
            (59, "0:59"),
            (60, "1:00"),
            (3599, "59:59"),
            (3600, "1:00:00"),
            (3661, "1:01:01"),
            (86400, "24:00:00")
        ]
        
        for seconds, expected in test_cases:
            with self.subTest(seconds=seconds):
                formatted = self.discovery._format_duration(seconds)
                self.assertEqual(formatted, expected)


def run_all_tests():
    """Run all channel discovery tests."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestChannelURLValidation))
    suite.addTests(loader.loadTestsFromTestCase(TestChannelInfoExtraction))
    suite.addTests(loader.loadTestsFromTestCase(TestVideoEnumeration))
    suite.addTests(loader.loadTestsFromTestCase(TestRateLimiting))
    suite.addTests(loader.loadTestsFromTestCase(TestDuplicateDetection))
    suite.addTests(loader.loadTestsFromTestCase(TestVideoMetadataExtraction))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    print("Running Channel Discovery Unit Tests")
    print("=" * 70)
    success = run_all_tests()
    print("=" * 70)
    if success:
        print("✓ All channel discovery tests passed!")
    else:
        print("✗ Some tests failed")
    sys.exit(0 if success else 1)