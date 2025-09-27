#!/usr/bin/env python3
"""
Simple Unit Tests for Database Operations
Phase 6.2: Run unit tests and validate all pass

This is a simplified test suite that tests the actual methods available
in the database operations module.
"""
import unittest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mass_download.database_schema import PersonRecord, VideoRecord
from mass_download.database_operations_ext import MassDownloadDatabaseOperations, ProgressRecord


class TestDatabaseOperationsSimple(unittest.TestCase):
    """Simple tests for database operations."""
    
    def setUp(self):
        """Set up test database operations."""
        # Create database operations with default database
        self.db_ops = MassDownloadDatabaseOperations()
    
    def test_save_person(self):
        """Test saving a person record."""
        person = PersonRecord(
            name="Test Person",
            channel_url="https://youtube.com/@testchannel"
        )
        
        # Save person
        person_id = self.db_ops.save_person(person)
        
        # Should return an ID
        self.assertIsNotNone(person_id)
        self.assertIsInstance(person_id, int)
        print(f"✓ Saved person with ID: {person_id}")
    
    def test_get_person_by_channel(self):
        """Test retrieving person by channel URL."""
        # Save a person first
        person = PersonRecord(
            name="Channel Test Person",
            channel_url="https://youtube.com/@channeltest"
        )
        person_id = self.db_ops.save_person(person)
        
        # Retrieve by channel URL
        retrieved = self.db_ops.get_person_by_channel_url("https://youtube.com/@channeltest")
        
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved['name'], "Channel Test Person")
        print(f"✓ Retrieved person by channel URL")
    
    def test_save_video(self):
        """Test saving a video record."""
        # First save a person
        person = PersonRecord(
            name="Video Test Person",
            channel_url="https://youtube.com/@videotest"
        )
        person_id = self.db_ops.save_person(person)
        
        # Create and save video
        video = VideoRecord(
            video_id="test_video_123",
            person_id=person_id,
            title="Test Video",
            duration=300,
            upload_date="2024-01-01"
        )
        
        video_id = self.db_ops.save_video(video)
        self.assertIsNotNone(video_id)
        print(f"✓ Saved video with ID: {video_id}")
    
    def test_get_videos_by_person(self):
        """Test retrieving videos by person."""
        # Save person
        person = PersonRecord(
            name="Multi Video Person",
            channel_url="https://youtube.com/@multitest"
        )
        person_id = self.db_ops.save_person(person)
        
        # Save multiple videos
        for i in range(3):
            video = VideoRecord(
                video_id=f"multi_video_{i}",
                person_id=person_id,
                title=f"Video {i}",
                duration=100 * (i + 1)
            )
            self.db_ops.save_video(video)
        
        # Get videos
        videos = self.db_ops.get_videos_by_person(person_id)
        self.assertEqual(len(videos), 3)
        print(f"✓ Retrieved {len(videos)} videos for person")
    
    def test_save_progress(self):
        """Test saving job progress."""
        progress = ProgressRecord(
            job_id="test_job_001",
            total_channels=10,
            channels_processed=5,
            total_videos=100,
            videos_processed=50
        )
        
        progress_id = self.db_ops.save_progress(progress)
        self.assertIsNotNone(progress_id)
        print(f"✓ Saved progress with ID: {progress_id}")
    
    def test_get_progress_by_job_id(self):
        """Test retrieving progress by job ID."""
        # Save progress
        progress = ProgressRecord(
            job_id="test_job_002",
            total_channels=20,
            channels_processed=10
        )
        self.db_ops.save_progress(progress)
        
        # Retrieve by job ID
        retrieved = self.db_ops.get_progress_by_job_id("test_job_002")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved['total_channels'], 20)
        self.assertEqual(retrieved['channels_processed'], 10)
        print(f"✓ Retrieved progress by job ID")
    
    def test_update_video_status(self):
        """Test updating video status."""
        # Save person and video
        person = PersonRecord(
            name="Status Test Person",
            channel_url="https://youtube.com/@statustest"
        )
        person_id = self.db_ops.save_person(person)
        
        video = VideoRecord(
            video_id="status_video_123",
            person_id=person_id,
            title="Status Test Video",
            duration=200
        )
        video_id = self.db_ops.save_video(video)
        
        # Update status
        success = self.db_ops.update_video_status(
            video_id,
            status="downloaded",
            s3_path="s3://bucket/status_video_123.mp4"
        )
        self.assertTrue(success)
        print(f"✓ Updated video status")
    
    def test_get_download_statistics(self):
        """Test getting download statistics."""
        stats = self.db_ops.get_download_statistics()
        
        # Should return a dictionary with expected keys
        self.assertIsInstance(stats, dict)
        self.assertIn('total_persons', stats)
        self.assertIn('total_videos', stats)
        self.assertIn('videos_downloaded', stats)
        print(f"✓ Retrieved download statistics: {stats}")
    
    def test_batch_save_videos(self):
        """Test batch saving videos."""
        # Save person
        person = PersonRecord(
            name="Batch Test Person",
            channel_url="https://youtube.com/@batchtest"
        )
        person_id = self.db_ops.save_person(person)
        
        # Create multiple videos
        videos = []
        for i in range(5):
            video = VideoRecord(
                video_id=f"batch_video_{i}",
                person_id=person_id,
                title=f"Batch Video {i}",
                duration=100
            )
            videos.append(video)
        
        # Batch save
        saved_count = self.db_ops.batch_save_videos(videos)
        self.assertEqual(saved_count, 5)
        print(f"✓ Batch saved {saved_count} videos")


def run_simple_tests():
    """Run the simple test suite."""
    print("Running simplified database operation tests...")
    print("=" * 60)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestDatabaseOperationsSimple)
    
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