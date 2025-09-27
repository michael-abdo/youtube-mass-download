#!/usr/bin/env python3
"""Test cases for YouTube URL validation"""

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

import unittest
import sys
import os
from utils.validation import validate_youtube_url, validate_youtube_playlist_url, validate_youtube_video_id, ValidationError

class TestYouTubeValidation(unittest.TestCase):
    """Test YouTube URL validation functions"""
    
    def test_validate_youtube_video_id_valid(self):
        """Test validation of valid YouTube video IDs"""
        valid_ids = [
            "dQw4w9WgXcQ",  # Classic Rick Roll
            "kCTS91Sded4",  # From our CSV
            "a1b2c3d4e5f",  # Generic 11 chars
            "ABC-123_xyz",  # With hyphen and underscore
            "_-_-_-_-_-_",  # All special chars
        ]
        for video_id in valid_ids:
            with self.subTest(video_id=video_id):
                self.assertTrue(validate_youtube_video_id(video_id))
    
    def test_validate_youtube_video_id_invalid(self):
        """Test validation of invalid YouTube video IDs"""
        invalid_ids = [
            "",  # Empty
            "short",  # Too short
            "toolongvideoid123",  # Too long
            "exactlyten",  # Exactly 10 chars (not 11)
            "has space11",  # Contains space
            "has\x0btab11",  # Contains control char
            "u000bOrigin",  # Our problematic ID from CSV
            "test@video1",  # Contains invalid char
            None,  # None type
            123456789012,  # Not a string
        ]
        for video_id in invalid_ids:
            with self.subTest(video_id=video_id):
                with self.assertRaises(ValidationError):
                    validate_youtube_video_id(video_id)
    
    def test_validate_youtube_url_valid(self):
        """Test validation of valid single YouTube video URLs"""
        test_cases = [
            ("https://www.youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            ("https://youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            ("https://youtu.be/dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            ("https://www.youtube.com/watch?v=kCTS91Sded4&list=PLrAXtmErZgOeiKm4sgNOknGvNjby9efdf", "kCTS91Sded4"),
        ]
        for url, expected_id in test_cases:
            with self.subTest(url=url):
                result_url, video_id = validate_youtube_url(url)
                self.assertEqual(video_id, expected_id)
    
    def test_validate_youtube_url_invalid(self):
        """Test validation of invalid YouTube URLs"""
        invalid_urls = [
            "https://youtube.com/watch?v=short",  # Video ID too short
            "https://youtube.com/watch?v=toolongvideoid",  # Video ID too long
            "https://youtube.com/watch?v=has space11",  # Video ID with space
            "https://youtube.com/watch",  # No video ID
            "https://vimeo.com/123456789",  # Wrong domain
            "not a url",  # Not a URL
        ]
        for url in invalid_urls:
            with self.subTest(url=url):
                with self.assertRaises(ValidationError):
                    validate_youtube_url(url)
    
    def test_validate_youtube_playlist_url_watch_videos(self):
        """Test validation of YouTube watch_videos playlist URLs"""
        test_cases = [
            # Single video
            ("https://www.youtube.com/watch_videos?video_ids=dQw4w9WgXcQ", 
             ["dQw4w9WgXcQ"]),
            # Multiple videos
            ("https://www.youtube.com/watch_videos?video_ids=dQw4w9WgXcQ,kCTS91Sded4,AR4h4x1VMac", 
             ["dQw4w9WgXcQ", "kCTS91Sded4", "AR4h4x1VMac"]),
            # With invalid IDs mixed in (should filter them out)
            ("https://www.youtube.com/watch_videos?video_ids=dQw4w9WgXcQ,u000bOrigin,kCTS91Sded4", 
             ["dQw4w9WgXcQ", "kCTS91Sded4"]),
            # With control characters
            ("https://www.youtube.com/watch_videos?video_ids=dQw4w9WgXcQ,test\x0bextra,kCTS91Sded4", 
             ["dQw4w9WgXcQ", "kCTS91Sded4"]),
        ]
        for url, expected_ids in test_cases:
            with self.subTest(url=url):
                result_url, video_ids = validate_youtube_playlist_url(url)
                self.assertEqual(video_ids, expected_ids)
                # Check that the result URL only contains valid IDs
                if video_ids:
                    self.assertEqual(result_url, f"https://www.youtube.com/watch_videos?video_ids={','.join(expected_ids)}")
    
    def test_validate_youtube_playlist_url_regular_playlist(self):
        """Test validation of regular YouTube playlist URLs"""
        test_cases = [
            "https://www.youtube.com/playlist?list=PLrAXtmErZgOeiKm4sgNOknGvNjby9efdf",
            "https://www.youtube.com/playlist?list=PL_T1vWvK0xstLL7YYcHpxgSlxjZJniWcb",
            "https://youtube.com/playlist?list=PLBqRr0HmqaH2ZOI8selUhhhECQyXe0qzI",
        ]
        for url in test_cases:
            with self.subTest(url=url):
                result_url, video_ids = validate_youtube_playlist_url(url)
                self.assertEqual(result_url, url)
                self.assertEqual(video_ids, [])  # Regular playlists don't return individual IDs
    
    def test_validate_youtube_playlist_url_invalid(self):
        """Test validation of invalid YouTube playlist URLs"""
        invalid_urls = [
            "https://youtube.com/watch_videos?video_ids=",  # Empty video IDs
            "https://youtube.com/watch_videos?video_ids=short,tooshort",  # All IDs invalid
            "https://vimeo.com/watch_videos?video_ids=dQw4w9WgXcQ",  # Wrong domain
            "not a url",  # Not a URL
        ]
        for url in invalid_urls:
            with self.subTest(url=url):
                with self.assertRaises(ValidationError):
                    validate_youtube_playlist_url(url)
    
    def test_real_world_corrupted_urls(self):
        """Test handling of actual corrupted URLs from the CSV"""
        # This is the actual problematic URL from the CSV
        url = "https://www.youtube.com/watch_videos?video_ids=kCTS91Sded4,u000bOrigin"
        
        # Should clean it to only include valid video ID
        result_url, video_ids = validate_youtube_playlist_url(url)
        self.assertEqual(video_ids, ["kCTS91Sded4"])
        self.assertEqual(result_url, "https://www.youtube.com/watch_videos?video_ids=kCTS91Sded4")

if __name__ == "__main__":
    unittest.main()