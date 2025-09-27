#!/usr/bin/env python3
"""
Simple Integration Tests for Mass Download Feature
Phase 6.5: Create integration tests for end-to-end flow

This module contains simplified integration tests that verify basic workflow
components working together.
"""
import unittest
import tempfile
import shutil
import json
import csv
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import modules
from mass_download.input_handler import InputHandler
from mass_download.channel_discovery import YouTubeChannelDiscovery
from mass_download.database_operations_ext import MassDownloadDatabaseOperations


class TestBasicIntegration(unittest.TestCase):
    """Test basic integration between components."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_db = Path(self.temp_dir) / "test.db"
    
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_input_handler_integration(self):
        """Test input handler with various file types."""
        print("\n=== Testing Input Handler Integration ===")
        
        # Create test CSV file
        csv_file = Path(self.temp_dir) / "test_channels.csv"
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["channel_url", "channel_name"])
            writer.writerow(["https://youtube.com/@testchannel1", "Test Channel 1"])
            writer.writerow(["https://youtube.com/@testchannel2", "Test Channel 2"])
        
        # Create test JSON file
        json_file = Path(self.temp_dir) / "test_channels.json"
        with open(json_file, 'w') as f:
            json.dump({
                "channels": [
                    {"channel_url": "https://youtube.com/@testchannel3", "name": "Test Channel 3"}
                ]
            }, f)
        
        # Create test TXT file
        txt_file = Path(self.temp_dir) / "test_channels.txt"
        with open(txt_file, 'w') as f:
            f.write("https://youtube.com/@testchannel4\n")
            f.write("https://youtube.com/@testchannel5\n")
        
        # Initialize input handler
        handler = InputHandler()
        
        # Test CSV parsing
        csv_channels = handler.parse_file(str(csv_file))
        self.assertEqual(len(csv_channels), 2)
        self.assertEqual(csv_channels[0]['channel_url'], "https://youtube.com/@testchannel1")
        self.assertEqual(csv_channels[0]['channel_name'], "Test Channel 1")
        
        # Test JSON parsing
        json_channels = handler.parse_file(str(json_file))
        self.assertEqual(len(json_channels), 1)
        self.assertEqual(json_channels[0]['channel_url'], "https://youtube.com/@testchannel3")
        
        # Test TXT parsing
        txt_channels = handler.parse_file(str(txt_file))
        self.assertEqual(len(txt_channels), 2)
        self.assertEqual(txt_channels[0]['channel_url'], "https://youtube.com/@testchannel4")
        
        print("✓ Input handler integration test passed")
    
    @patch('mass_download.channel_discovery.subprocess.run')
    def test_channel_discovery_integration(self, mock_subprocess):
        """Test channel discovery with database operations."""
        print("\n=== Testing Channel Discovery Integration ===")
        
        # Mock yt-dlp validation
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "2025.08.22"
        mock_subprocess.return_value = mock_result
        
        # Initialize components
        discovery = YouTubeChannelDiscovery()
        db_ops = MassDownloadDatabaseOperations(str(self.test_db))
        
        # Test duplicate detection
        self.assertFalse(discovery.is_duplicate_video("test_video_1"))
        discovery.mark_video_processed("test_video_1", "uuid-123")
        self.assertTrue(discovery.is_duplicate_video("test_video_1"))
        
        # Test UUID retrieval
        uuid = discovery.get_video_uuid("test_video_1")
        self.assertEqual(uuid, "uuid-123")
        
        # Test database operations
        from mass_download.database_schema import PersonRecord, VideoRecord
        
        # Create and save person
        person = PersonRecord(
            name="Integration Test Person",
            channel_url="https://youtube.com/@integration_test"
        )
        person_id = db_ops.save_person(person)
        self.assertIsNotNone(person_id)
        
        # Create and save video
        video = VideoRecord(
            video_id="testvideo01",  # 11 chars
            person_id=person_id,
            title="Integration Test Video",
            duration=300
        )
        video_id = db_ops.save_video(video)
        self.assertIsNotNone(video_id)
        
        # Verify database operations
        stats = db_ops.get_download_statistics()
        self.assertEqual(stats['total_persons'], 1)
        self.assertEqual(stats['total_videos'], 1)
        
        print("✓ Channel discovery integration test passed")
    
    def test_database_operations_integration(self):
        """Test database operations with multiple components."""
        print("\n=== Testing Database Operations Integration ===")
        
        # Initialize database operations
        db_ops = MassDownloadDatabaseOperations(str(self.test_db))
        
        from mass_download.database_schema import PersonRecord, VideoRecord
        
        # Test batch operations
        persons = []
        for i in range(3):
            person = PersonRecord(
                name=f"Batch Person {i}",
                channel_url=f"https://youtube.com/@batch{i}"
            )
            person_id = db_ops.save_person(person)
            persons.append((person_id, person))
        
        # Create videos for each person
        videos = []
        for person_id, person in persons:
            for j in range(2):
                video = VideoRecord(
                    video_id=f"batchvid{person_id}{j:02d}",  # Ensure 11 chars
                    person_id=person_id,
                    title=f"Video {j} for {person.name}",
                    duration=100 * (j + 1)
                )
                video_id = db_ops.save_video(video)
                videos.append(video_id)
        
        # Verify statistics
        stats = db_ops.get_download_statistics()
        self.assertEqual(stats['total_persons'], 3)
        self.assertEqual(stats['total_videos'], 6)
        
        # Test progress tracking
        from mass_download.database_operations_ext import ProgressRecord
        progress = ProgressRecord(
            job_id="integration_test_job",
            total_channels=3,
            channels_processed=2,
            total_videos=6,
            videos_processed=4
        )
        
        progress_id = db_ops.save_progress(progress)
        self.assertIsNotNone(progress_id)
        
        # Retrieve progress
        retrieved = db_ops.get_progress_by_job_id("integration_test_job")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved['channels_processed'], 2)
        
        print("✓ Database operations integration test passed")
    
    def test_error_handling_integration(self):
        """Test error handling across components."""
        print("\n=== Testing Error Handling Integration ===")
        
        # Test invalid input files
        handler = InputHandler()
        
        # Test non-existent file
        with self.assertRaises(Exception):
            handler.parse_file("/non/existent/file.csv")
        
        # Test invalid JSON file
        invalid_json = Path(self.temp_dir) / "invalid.json"
        with open(invalid_json, 'w') as f:
            f.write("{invalid json")
        
        with self.assertRaises(Exception):
            handler.parse_file(str(invalid_json))
        
        # Test database with invalid data
        db_ops = MassDownloadDatabaseOperations(str(self.test_db))
        
        from mass_download.database_schema import VideoRecord
        
        # Try to create video with invalid video_id (wrong length)
        with self.assertRaises(ValueError):
            VideoRecord(
                video_id="short",  # Too short for YouTube
                person_id=1,
                title="Invalid Video",
                duration=100
            )
        
        print("✓ Error handling integration test passed")
    
    def test_component_workflow(self):
        """Test a simplified end-to-end workflow."""
        print("\n=== Testing Component Workflow ===")
        
        # Create test input file
        test_csv = Path(self.temp_dir) / "workflow.csv"
        with open(test_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["channel_url", "channel_name", "person_name"])
            writer.writerow(["https://youtube.com/@workflow1", "Workflow Channel", "Workflow Person"])
        
        # Step 1: Parse input
        handler = InputHandler()
        channels = handler.parse_file(str(test_csv))
        self.assertEqual(len(channels), 1)
        
        channel_data = channels[0]
        self.assertEqual(channel_data['channel_url'], "https://youtube.com/@workflow1")
        
        # Step 2: Database operations
        db_ops = MassDownloadDatabaseOperations(str(self.test_db))
        
        from mass_download.database_schema import PersonRecord, VideoRecord
        
        # Save person
        person = PersonRecord(
            name=channel_data.get('person_name', channel_data.get('channel_name', 'Unknown')),
            channel_url=channel_data['channel_url']
        )
        person_id = db_ops.save_person(person)
        
        # Step 3: Simulate video processing
        # In real workflow, this would come from channel discovery
        mock_videos = [
            VideoRecord(
                video_id="workflow001",
                person_id=person_id,
                title="Workflow Test Video",
                duration=200,
                view_count=1000
            )
        ]
        
        for video in mock_videos:
            video_id = db_ops.save_video(video)
            self.assertIsNotNone(video_id)
        
        # Step 4: Verify workflow results
        stats = db_ops.get_download_statistics()
        self.assertEqual(stats['total_persons'], 1)
        self.assertEqual(stats['total_videos'], 1)
        
        # Step 5: Check person-video relationships
        videos = db_ops.get_videos_by_person(person_id)
        self.assertEqual(len(videos), 1)
        self.assertEqual(videos[0]['title'], "Workflow Test Video")
        
        print("✓ Component workflow test passed")


def run_simple_integration_tests():
    """Run simple integration tests."""
    print("\n" + "=" * 60)
    print("Simple Mass Download Integration Tests")
    print("=" * 60)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestBasicIntegration)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print("✓ All integration tests passed!")
        print(f"  Ran {result.testsRun} tests successfully")
    else:
        print(f"✗ {len(result.failures)} failures, {len(result.errors)} errors")
        if result.failures:
            for test, traceback in result.failures:
                print(f"  FAILURE {test}: {traceback.split(chr(10))[-2]}")
        if result.errors:
            for test, traceback in result.errors:
                print(f"  ERROR {test}: {traceback.split(chr(10))[-2]}")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_simple_integration_tests()
    sys.exit(0 if success else 1)