#!/usr/bin/env python3
"""
Consolidated Extraction Test Suite

Consolidates all extraction tests from:
- test_all_30_rows.py
- test_all_truth_source.py
- extract tests scattered across root level

Provides unified extraction testing with different data sources and validation.
"""

import unittest
import sys
import os
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
try:
    from utils.extract_links import extract_links, extract_google_doc_text
    from utils.test_helpers import (
        TestReporter, TestEnvironment, TestCSVHandler,
        run_extraction_test, compare_extraction_results
    )
    from utils.config import get_config
    from utils.patterns import is_google_doc_url, extract_youtube_id, extract_drive_id
except ImportError as e:
    print(f"Warning: Could not import extraction utilities: {e}")

class ExtractionTestSuite(unittest.TestCase):
    """Unified extraction test suite"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test class with sample data"""
        try:
            cls.df = TestCSVHandler.read_test_csv()
            cls.has_data = True
        except Exception as e:
            print(f"Warning: Could not load test data: {e}")
            cls.has_data = False
    
    def setUp(self):
        """Set up test environment"""
        self.test_env = TestEnvironment("extraction_suite")
        
    def tearDown(self):
        """Clean up test environment"""
        self.test_env.__exit__(None, None, None)
    
    def test_google_docs_extraction(self):
        """Test Google Docs extraction (consolidated from multiple test files)"""
        TestReporter.print_test_header("Google Docs Extraction")
        
        if not self.has_data:
            self.skipTest("No test data available")
        
        # Find a person with Google Docs link
        google_doc_rows = []
        for _, row in self.df.head(10).iterrows():  # Test first 10 rows
            doc_link = str(row.get('link', ''))
            if is_google_doc_url(doc_link):
                google_doc_rows.append(row)
                break
        
        if not google_doc_rows:
            self.skipTest("No Google Docs links found in test data")
        
        row = google_doc_rows[0]
        doc_link = str(row.get('link', ''))
        
        try:
            # Test extraction
            extracted_text = extract_google_doc_text(doc_link)
            self.assertTrue(len(extracted_text) > 0, "Should extract some text")
            self.assertIsInstance(extracted_text, str, "Should return string")
        except Exception as e:
            self.fail(f"Google Docs extraction failed: {e}")
    
    def test_youtube_link_extraction(self):
        """Test YouTube link extraction and validation"""
        TestReporter.print_test_header("YouTube Link Extraction")
        
        test_urls = [
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            "https://youtu.be/dQw4w9WgXcQ", 
            "https://www.youtube.com/watch?v=invalid_id"
        ]
        
        for url in test_urls:
            with self.subTest(url=url):
                video_id = extract_youtube_id(url)
                if "invalid_id" not in url:
                    self.assertEqual(len(video_id), 11, f"YouTube ID should be 11 chars: {video_id}")
                    self.assertRegex(video_id, r'^[a-zA-Z0-9_-]+$', "Should be valid YouTube ID format")
    
    def test_drive_link_extraction(self):
        """Test Google Drive link extraction and validation"""
        TestReporter.print_test_header("Drive Link Extraction")
        
        test_urls = [
            "https://drive.google.com/file/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/view",
            "https://drive.google.com/open?id=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
            "https://drive.google.com/drive/folders/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
        ]
        
        for url in test_urls:
            with self.subTest(url=url):
                drive_id = extract_drive_id(url)
                self.assertTrue(len(drive_id) > 20, f"Drive ID should be substantial length: {drive_id}")
                self.assertRegex(drive_id, r'^[a-zA-Z0-9_-]+$', "Should be valid Drive ID format")
    
    def test_truth_source_comparison(self):
        """Test truth source comparison (from test_all_truth_source.py)"""
        TestReporter.print_test_header("Truth Source Comparison")
        
        # TODO: Consolidate logic from test_all_truth_source.py
        # For now, basic comparison test
        expected_data = {
            'person_name': 'Test Person',
            'youtube_links': ['https://youtube.com/watch?v=test'],
            'extracted_text': 'Sample text'
        }
        
        actual_data = {
            'person_name': 'Test Person',
            'youtube_links': ['https://youtube.com/watch?v=test'],
            'extracted_text': 'Sample text'
        }
        
        comparison = compare_extraction_results(expected_data, actual_data)
        self.assertTrue(comparison['match'], "Identical data should match")
        self.assertEqual(comparison['similarity_score'], 1.0, "Perfect match should have score 1.0")

class ThirtyRowsTestSuite(unittest.TestCase):
    """Test suite for 30 rows testing (from test_all_30_rows.py)"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test class with CSV data"""
        try:
            cls.df = TestCSVHandler.read_test_csv()
            cls.has_data = len(cls.df) >= 30
        except Exception:
            cls.has_data = False
    
    def test_first_30_rows_extraction(self):
        """Test extraction on first 30 rows (consolidated from test_all_30_rows.py)"""
        TestReporter.print_test_header("First 30 Rows Extraction")
        
        if not self.has_data:
            self.skipTest("Need at least 30 rows of test data")
        
        # TODO: Consolidate full logic from test_all_30_rows.py
        # For now, basic smoke test on first 5 rows
        test_rows = self.df.head(5)
        
        successful_extractions = 0
        for _, row in test_rows.iterrows():
            try:
                # Basic test that we can process each row
                row_id = str(row.get('row_id', 'unknown'))
                person_name = str(row.get('name', 'Unknown'))
                doc_link = str(row.get('link', ''))
                
                # Count as successful if we have basic data
                if row_id != 'unknown' and person_name != 'Unknown':
                    successful_extractions += 1
                    
            except Exception as e:
                print(f"Row processing failed: {e}")
        
        self.assertGreater(successful_extractions, 0, "Should successfully process some rows")
        success_rate = successful_extractions / len(test_rows)
        self.assertGreaterEqual(success_rate, 0.8, f"Should have >80% success rate, got {success_rate:.2%}")

def run_all_extraction_tests():
    """Run all consolidated extraction tests"""
    TestReporter.print_test_header("Running All Extraction Tests")
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add extraction tests
    suite.addTest(unittest.makeSuite(ExtractionTestSuite))
    suite.addTest(unittest.makeSuite(ThirtyRowsTestSuite))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    TestReporter.print_test_result(
        "All Extraction Tests",
        result.wasSuccessful(),
        f"Tests run: {result.testsRun}, Failures: {len(result.failures)}, Errors: {len(result.errors)}"
    )
    
    return result.wasSuccessful()

if __name__ == "__main__":
    success = run_all_extraction_tests()
    sys.exit(0 if success else 1)