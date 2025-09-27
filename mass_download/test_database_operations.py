#!/usr/bin/env python3
"""
Unit Tests for Database Operations
Phase 6.1: Create unit tests for database operations

This module contains comprehensive unit tests for the database operations
of the mass download feature, testing all CRUD operations, error handling,
and edge cases.
"""
import unittest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import uuid

# Add parent directory to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from mass_download.database_schema import (
    DatabaseSchemaManager, PersonRecord, VideoRecord
)
from mass_download.database_operations_ext import MassDownloadDatabaseOperations

# Define JobProgressRecord for testing
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class JobProgressRecord:
    """Job progress record for testing."""
    job_id: str
    total_channels: int = 0
    channels_processed: int = 0
    channels_failed: int = 0
    channels_skipped: int = 0
    total_videos: int = 0
    videos_processed: int = 0
    videos_failed: int = 0
    videos_skipped: int = 0
    status: str = "running"
    started_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class TestDatabaseSchema(unittest.TestCase):
    """Test database schema creation and validation."""
    
    def setUp(self):
        """Create temporary database for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "test.db"
        self.schema_manager = DatabaseSchemaManager(str(self.db_path))
    
    def tearDown(self):
        """Clean up temporary files."""
        if hasattr(self, 'schema_manager'):
            self.schema_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_create_schema(self):
        """Test schema creation."""
        # Schema should be created successfully
        self.assertTrue(self.db_path.exists())
        
        # Verify tables exist
        cursor = self.schema_manager.connection.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name IN ('persons', 'videos')
        """)
        tables = {row[0] for row in cursor.fetchall()}
        self.assertIn('persons', tables)
        self.assertIn('videos', tables)
    
    def test_person_record_validation(self):
        """Test PersonRecord validation."""
        # Valid person
        person = PersonRecord(
            name="Test Person",
            channel_url="https://youtube.com/@testchannel"
        )
        self.assertIsNotNone(person.person_id)
        self.assertEqual(person.name, "Test Person")
        
        # Invalid person - empty name
        with self.assertRaises(ValueError):
            PersonRecord(name="", channel_url="https://youtube.com/@test")
        
        # Invalid person - invalid URL
        with self.assertRaises(ValueError):
            PersonRecord(name="Test", channel_url="not-a-url")
    
    def test_video_record_validation(self):
        """Test VideoRecord validation."""
        # Valid video
        video = VideoRecord(
            video_id="test123",
            person_id=1,
            title="Test Video",
            duration=120,
            upload_date="2024-01-01",
            view_count=1000,
            s3_path="s3://bucket/video.mp4"
        )
        self.assertIsNotNone(video.uuid)
        self.assertEqual(video.video_id, "test123")
        
        # Invalid video - empty video_id
        with self.assertRaises(ValueError):
            VideoRecord(
                video_id="",
                person_id=1,
                title="Test",
                duration=120
            )
        
        # Invalid video - negative duration
        with self.assertRaises(ValueError):
            VideoRecord(
                video_id="test",
                person_id=1,
                title="Test",
                duration=-10
            )


class TestDatabaseOperations(unittest.TestCase):
    """Test database CRUD operations."""
    
    def setUp(self):
        """Create temporary database and operations instance."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "test.db"
        self.db_ops = MassDownloadDatabaseOperations(str(self.db_path))
    
    def tearDown(self):
        """Clean up temporary files."""
        if hasattr(self, 'db_ops'):
            self.db_ops.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_insert_person(self):
        """Test person insertion."""
        person = PersonRecord(
            name="Test Person",
            channel_url="https://youtube.com/@test"
        )
        
        person_id = self.db_ops.insert_person(person)
        self.assertIsNotNone(person_id)
        self.assertIsInstance(person_id, int)
        
        # Verify person was inserted
        cursor = self.db_ops.schema_manager.connection.cursor()
        cursor.execute("SELECT name, channel_url FROM persons WHERE person_id = ?", (person_id,))
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "Test Person")
        self.assertEqual(row[1], "https://youtube.com/@test")
    
    def test_insert_duplicate_person(self):
        """Test handling of duplicate person insertion."""
        person = PersonRecord(
            name="Duplicate Person",
            channel_url="https://youtube.com/@duplicate"
        )
        
        # First insertion should succeed
        person_id1 = self.db_ops.insert_person(person)
        self.assertIsNotNone(person_id1)
        
        # Second insertion should return existing ID
        person_id2 = self.db_ops.insert_person(person)
        self.assertEqual(person_id1, person_id2)
    
    def test_get_person_by_channel(self):
        """Test retrieving person by channel URL."""
        # Insert person
        person = PersonRecord(
            name="Channel Person",
            channel_url="https://youtube.com/@channel"
        )
        person_id = self.db_ops.insert_person(person)
        
        # Retrieve by channel
        retrieved_id = self.db_ops.get_person_by_channel("https://youtube.com/@channel")
        self.assertEqual(person_id, retrieved_id)
        
        # Non-existent channel
        non_existent = self.db_ops.get_person_by_channel("https://youtube.com/@nonexistent")
        self.assertIsNone(non_existent)
    
    def test_insert_video(self):
        """Test video insertion."""
        # First insert person
        person = PersonRecord(
            name="Video Person",
            channel_url="https://youtube.com/@videos"
        )
        person_id = self.db_ops.insert_person(person)
        
        # Create video
        video = VideoRecord(
            video_id="vid123",
            person_id=person_id,
            title="Test Video",
            duration=300,
            upload_date="2024-01-15",
            view_count=5000,
            s3_path="s3://test-bucket/vid123.mp4"
        )
        
        # Insert video
        success = self.db_ops.insert_video(video)
        self.assertTrue(success)
        
        # Verify video was inserted
        cursor = self.db_ops.schema_manager.connection.cursor()
        cursor.execute("SELECT title, duration FROM videos WHERE video_id = ?", ("vid123",))
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "Test Video")
        self.assertEqual(row[1], 300)
    
    def test_insert_duplicate_video(self):
        """Test handling of duplicate video insertion."""
        # Insert person
        person = PersonRecord(
            name="Dup Video Person",
            channel_url="https://youtube.com/@dupvids"
        )
        person_id = self.db_ops.insert_person(person)
        
        # Create video
        video = VideoRecord(
            video_id="dupvid123",
            person_id=person_id,
            title="Duplicate Video",
            duration=200
        )
        
        # First insertion should succeed
        success1 = self.db_ops.insert_video(video)
        self.assertTrue(success1)
        
        # Second insertion should fail (duplicate)
        success2 = self.db_ops.insert_video(video)
        self.assertFalse(success2)
    
    def test_get_person_videos(self):
        """Test retrieving videos for a person."""
        # Insert person
        person = PersonRecord(
            name="Multi Video Person",
            channel_url="https://youtube.com/@multivids"
        )
        person_id = self.db_ops.insert_person(person)
        
        # Insert multiple videos
        for i in range(3):
            video = VideoRecord(
                video_id=f"mvid{i}",
                person_id=person_id,
                title=f"Video {i}",
                duration=100 * (i + 1),
                upload_date=f"2024-01-{i+1:02d}"
            )
            self.db_ops.insert_video(video)
        
        # Get videos
        videos = self.db_ops.get_person_videos(person_id)
        self.assertEqual(len(videos), 3)
        
        # Verify order (should be by upload_date DESC)
        self.assertEqual(videos[0]['video_id'], "mvid2")
        self.assertEqual(videos[1]['video_id'], "mvid1")
        self.assertEqual(videos[2]['video_id'], "mvid0")
    
    def test_video_exists(self):
        """Test checking if video exists."""
        # Insert person and video
        person = PersonRecord(
            name="Exists Person",
            channel_url="https://youtube.com/@exists"
        )
        person_id = self.db_ops.insert_person(person)
        
        video = VideoRecord(
            video_id="exists123",
            person_id=person_id,
            title="Existing Video",
            duration=150
        )
        self.db_ops.insert_video(video)
        
        # Check existence
        self.assertTrue(self.db_ops.video_exists("exists123"))
        self.assertFalse(self.db_ops.video_exists("notexists123"))
    
    def test_get_channel_statistics(self):
        """Test channel statistics calculation."""
        # Insert person
        person = PersonRecord(
            name="Stats Person",
            channel_url="https://youtube.com/@stats"
        )
        person_id = self.db_ops.insert_person(person)
        
        # Insert videos with different stats
        videos = [
            VideoRecord(
                video_id="stat1",
                person_id=person_id,
                title="Video 1",
                duration=100,
                view_count=1000,
                upload_date="2024-01-01"
            ),
            VideoRecord(
                video_id="stat2",
                person_id=person_id,
                title="Video 2",
                duration=200,
                view_count=2000,
                upload_date="2024-01-02"
            ),
            VideoRecord(
                video_id="stat3",
                person_id=person_id,
                title="Video 3",
                duration=300,
                view_count=3000,
                upload_date="2024-01-03"
            )
        ]
        
        for video in videos:
            self.db_ops.insert_video(video)
        
        # Get statistics
        stats = self.db_ops.get_channel_statistics(person_id)
        
        self.assertEqual(stats['video_count'], 3)
        self.assertEqual(stats['total_duration'], 600)
        self.assertEqual(stats['total_views'], 6000)
        self.assertEqual(stats['avg_duration'], 200)
        self.assertEqual(stats['avg_views'], 2000)


class TestJobProgress(unittest.TestCase):
    """Test job progress tracking."""
    
    def setUp(self):
        """Create temporary database and operations instance."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "test.db"
        self.db_ops = MassDownloadDatabaseOperations(str(self.db_path))
    
    def tearDown(self):
        """Clean up temporary files."""
        if hasattr(self, 'db_ops'):
            self.db_ops.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_save_and_get_progress(self):
        """Test saving and retrieving job progress."""
        # Create progress record
        progress = JobProgressRecord(
            job_id="test_job_123",
            total_channels=10,
            channels_processed=3,
            channels_failed=1,
            channels_skipped=0,
            total_videos=100,
            videos_processed=30,
            videos_failed=2,
            videos_skipped=5,
            status="running",
            started_at=datetime.now()
        )
        
        # Save progress
        self.db_ops.save_progress(progress)
        
        # Retrieve progress
        retrieved = self.db_ops.get_progress("test_job_123")
        
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved['job_id'], "test_job_123")
        self.assertEqual(retrieved['total_channels'], 10)
        self.assertEqual(retrieved['channels_processed'], 3)
        self.assertEqual(retrieved['videos_processed'], 30)
        self.assertEqual(retrieved['status'], "running")
    
    def test_update_progress(self):
        """Test updating existing progress."""
        # Create initial progress
        progress1 = JobProgressRecord(
            job_id="update_job",
            total_channels=5,
            channels_processed=1,
            status="running",
            started_at=datetime.now()
        )
        self.db_ops.save_progress(progress1)
        
        # Update progress
        progress2 = JobProgressRecord(
            job_id="update_job",
            total_channels=5,
            channels_processed=3,
            channels_failed=1,
            videos_processed=50,
            status="running",
            started_at=progress1.started_at
        )
        self.db_ops.save_progress(progress2)
        
        # Verify update
        retrieved = self.db_ops.get_progress("update_job")
        self.assertEqual(retrieved['channels_processed'], 3)
        self.assertEqual(retrieved['channels_failed'], 1)
        self.assertEqual(retrieved['videos_processed'], 50)
    
    def test_list_jobs(self):
        """Test listing all jobs."""
        # Create multiple jobs
        jobs = [
            JobProgressRecord(
                job_id=f"job_{i}",
                total_channels=10,
                channels_processed=i,
                status="running" if i < 3 else "completed",
                started_at=datetime.now() - timedelta(hours=i)
            )
            for i in range(5)
        ]
        
        for job in jobs:
            self.db_ops.save_progress(job)
        
        # List all jobs
        job_list = self.db_ops.list_jobs()
        self.assertEqual(len(job_list), 5)
        
        # Should be ordered by started_at DESC
        self.assertEqual(job_list[0]['job_id'], "job_0")
        self.assertEqual(job_list[-1]['job_id'], "job_4")
        
        # List only running jobs
        running_jobs = self.db_ops.list_jobs(status="running")
        self.assertEqual(len(running_jobs), 3)
        
        # List only completed jobs
        completed_jobs = self.db_ops.list_jobs(status="completed")
        self.assertEqual(len(completed_jobs), 2)


class TestTransactionHandling(unittest.TestCase):
    """Test transaction handling and rollback."""
    
    def setUp(self):
        """Create temporary database and operations instance."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "test.db"
        self.db_ops = MassDownloadDatabaseOperations(str(self.db_path))
    
    def tearDown(self):
        """Clean up temporary files."""
        if hasattr(self, 'db_ops'):
            self.db_ops.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_bulk_insert_videos_success(self):
        """Test successful bulk video insertion."""
        # Insert person
        person = PersonRecord(
            name="Bulk Person",
            channel_url="https://youtube.com/@bulk"
        )
        person_id = self.db_ops.insert_person(person)
        
        # Create videos
        videos = [
            VideoRecord(
                video_id=f"bulk{i}",
                person_id=person_id,
                title=f"Bulk Video {i}",
                duration=100
            )
            for i in range(5)
        ]
        
        # Bulk insert
        success = self.db_ops.bulk_insert_videos(videos)
        self.assertTrue(success)
        
        # Verify all inserted
        for video in videos:
            self.assertTrue(self.db_ops.video_exists(video.video_id))
    
    def test_bulk_insert_videos_rollback(self):
        """Test rollback on bulk insert failure."""
        # Insert person
        person = PersonRecord(
            name="Rollback Person",
            channel_url="https://youtube.com/@rollback"
        )
        person_id = self.db_ops.insert_person(person)
        
        # Create videos with one duplicate
        existing_video = VideoRecord(
            video_id="existing",
            person_id=person_id,
            title="Existing",
            duration=100
        )
        self.db_ops.insert_video(existing_video)
        
        # Try bulk insert with duplicate
        videos = [
            VideoRecord(
                video_id=f"new{i}",
                person_id=person_id,
                title=f"New Video {i}",
                duration=100
            )
            for i in range(3)
        ]
        videos.append(existing_video)  # This will cause failure
        
        # Bulk insert should fail
        success = self.db_ops.bulk_insert_videos(videos)
        self.assertFalse(success)
        
        # Verify none of the new videos were inserted (rollback)
        for i in range(3):
            self.assertFalse(self.db_ops.video_exists(f"new{i}"))


class TestErrorHandling(unittest.TestCase):
    """Test error handling and edge cases."""
    
    def setUp(self):
        """Create temporary database and operations instance."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = Path(self.temp_dir) / "test.db"
        self.db_ops = MassDownloadDatabaseOperations(str(self.db_path))
    
    def tearDown(self):
        """Clean up temporary files."""
        if hasattr(self, 'db_ops'):
            self.db_ops.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_invalid_person_id(self):
        """Test operations with invalid person ID."""
        # Try to get videos for non-existent person
        videos = self.db_ops.get_person_videos(99999)
        self.assertEqual(len(videos), 0)
        
        # Try to get statistics for non-existent person
        stats = self.db_ops.get_channel_statistics(99999)
        self.assertEqual(stats['video_count'], 0)
    
    def test_null_handling(self):
        """Test handling of null/optional fields."""
        # Insert person
        person = PersonRecord(
            name="Null Test",
            channel_url="https://youtube.com/@nulltest"
        )
        person_id = self.db_ops.insert_person(person)
        
        # Insert video with minimal fields
        video = VideoRecord(
            video_id="null123",
            person_id=person_id,
            title="Null Test Video",
            duration=0  # Edge case: 0 duration
        )
        success = self.db_ops.insert_video(video)
        self.assertTrue(success)
        
        # Retrieve and verify
        videos = self.db_ops.get_person_videos(person_id)
        self.assertEqual(len(videos), 1)
        self.assertIsNone(videos[0]['s3_path'])
        self.assertIsNone(videos[0]['view_count'])
    
    def test_unicode_handling(self):
        """Test handling of unicode characters."""
        # Insert person with unicode name
        person = PersonRecord(
            name="Test 日本語 Channel 🎬",
            channel_url="https://youtube.com/@unicode"
        )
        person_id = self.db_ops.insert_person(person)
        
        # Insert video with unicode title
        video = VideoRecord(
            video_id="unicode123",
            person_id=person_id,
            title="Test Video 测试 βίντεο 🎥",
            duration=100
        )
        success = self.db_ops.insert_video(video)
        self.assertTrue(success)
        
        # Retrieve and verify
        videos = self.db_ops.get_person_videos(person_id)
        self.assertEqual(videos[0]['title'], "Test Video 测试 βίντεο 🎥")


def run_all_tests():
    """Run all database operation tests."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestDatabaseSchema))
    suite.addTests(loader.loadTestsFromTestCase(TestDatabaseOperations))
    suite.addTests(loader.loadTestsFromTestCase(TestJobProgress))
    suite.addTests(loader.loadTestsFromTestCase(TestTransactionHandling))
    suite.addTests(loader.loadTestsFromTestCase(TestErrorHandling))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)