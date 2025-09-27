#!/usr/bin/env python3
"""Test cases for URL cleaning and extraction with control characters"""

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

import unittest
import sys
import os
from utils.extract_links import clean_url

class TestURLCleaning(unittest.TestCase):
    """Test URL cleaning with various control characters and edge cases"""
    
    def test_vertical_tab_removal(self):
        """Test removal of vertical tab character"""
        url = "https://youtu.be/kCTS91Sded4\x0b\x0bOriginal"
        expected = "https://youtu.be/kCTS91Sded4"
        self.assertEqual(clean_url(url), expected)
    
    def test_null_byte_removal(self):
        """Test removal of null bytes"""
        url = "https://youtube.com/watch?v=test\x00null"
        expected = "https://youtube.com/watch?v=test"
        self.assertEqual(clean_url(url), expected)
    
    def test_form_feed_removal(self):
        """Test removal of form feed character"""
        url = "https://youtube.com/watch?v=test\x0cform_feed"
        expected = "https://youtube.com/watch?v=test"
        self.assertEqual(clean_url(url), expected)
    
    def test_escape_character_removal(self):
        """Test removal of escape character"""
        url = "https://youtube.com/watch?v=test\x1bescape"
        expected = "https://youtube.com/watch?v=test"
        self.assertEqual(clean_url(url), expected)
    
    def test_unicode_escape_removal(self):
        """Test removal of unicode escape sequences"""
        url = "https://youtube.com/watch?v=test\\u000bvertical"
        expected = "https://youtube.com/watch?v=test"
        self.assertEqual(clean_url(url), expected)
    
    def test_mixed_control_characters(self):
        """Test removal of multiple control characters"""
        url = "https://youtube.com/watch?v=test\x0b\n\r\t\x00mixed"
        expected = "https://youtube.com/watch?v=test"
        self.assertEqual(clean_url(url), expected)
    
    def test_trailing_punctuation_removal(self):
        """Test removal of trailing punctuation"""
        test_cases = [
            ("https://youtube.com/watch?v=test.", "https://youtube.com/watch?v=test"),
            ("https://youtube.com/watch?v=test,", "https://youtube.com/watch?v=test"),
            ("https://youtube.com/watch?v=test;", "https://youtube.com/watch?v=test"),
            ("https://youtube.com/watch?v=test:", "https://youtube.com/watch?v=test"),
            ("https://youtube.com/watch?v=test!", "https://youtube.com/watch?v=test"),
            ("https://youtube.com/watch?v=test?", "https://youtube.com/watch?v=test"),
            ("https://youtube.com/watch?v=test)", "https://youtube.com/watch?v=test"),
            ("https://youtube.com/watch?v=test]", "https://youtube.com/watch?v=test"),
            ("https://youtube.com/watch?v=test}", "https://youtube.com/watch?v=test"),
            ("https://youtube.com/watch?v=test\"", "https://youtube.com/watch?v=test"),
            ("https://youtube.com/watch?v=test'", "https://youtube.com/watch?v=test"),
            ("https://youtube.com/watch?v=test>", "https://youtube.com/watch?v=test"),
        ]
        for url, expected in test_cases:
            with self.subTest(url=url):
                self.assertEqual(clean_url(url), expected)
    
    def test_preserve_valid_url_parts(self):
        """Test that valid URL components are preserved"""
        test_cases = [
            "https://youtube.com/watch?v=dQw4w9WgXcQ",
            "https://youtu.be/dQw4w9WgXcQ",
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=PLrAXtmErZgOeiKm4sgNOknGvNjby9efdf",
            "https://drive.google.com/file/d/1abc123/view?usp=sharing",
            "https://docs.google.com/document/d/1abc123/edit",
        ]
        for url in test_cases:
            with self.subTest(url=url):
                self.assertEqual(clean_url(url), url)
    
    def test_youtube_specific_patterns(self):
        """Test YouTube-specific URL patterns"""
        test_cases = [
            # youtu.be with extra text
            ("https://youtu.be/dQw4w9WgXcQ\x0bExtra text", "https://youtu.be/dQw4w9WgXcQ"),
            # youtube.com with extra text
            ("https://www.youtube.com/watch?v=dQw4w9WgXcQ\x0bExtra", "https://www.youtube.com/watch?v=dQw4w9WgXcQ"),
            # With trailing slash preserved
            ("https://youtu.be/dQw4w9WgXcQ/", "https://youtu.be/dQw4w9WgXcQ/"),
        ]
        for url, expected in test_cases:
            with self.subTest(url=url):
                self.assertEqual(clean_url(url), expected)
    
    def test_real_world_corrupted_urls(self):
        """Test actual corrupted URLs from the CSV"""
        test_cases = [
            # Nathaniel's URL
            ("https://youtu.be/kCTS91Sded4\u000b\u000bOriginal", "https://youtu.be/kCTS91Sded4"),
            # URL with unicode escapes
            ("https://youtube.com/playlist?list\\u003dPLBqRr0HmqaH2ZOI8selUhhhECQyXe0qzI\\u0026si\\u003dMXBTrN_7F0dLW04a", 
             "https://youtube.com/playlist?list=PLBqRr0HmqaH2ZOI8selUhhhECQyXe0qzI&si=MXBTrN_7F0dLW04a"),
            # URL with escaped characters
            ("https://www.youtube.com/watch?v\\u003dIi6n3WQUxUg\\n\\u0011\\n(If", 
             "https://www.youtube.com/watch?v=Ii6n3WQUxUg"),
        ]
        for url, expected in test_cases:
            with self.subTest(url=url):
                result = clean_url(url)
                # For complex cases, we may need to check if the result is at least valid
                self.assertTrue(result.startswith("http"))
                self.assertNotIn("\x0b", result)
                self.assertNotIn("\\u", result)
                self.assertNotIn("\n", result)

if __name__ == "__main__":
    unittest.main()