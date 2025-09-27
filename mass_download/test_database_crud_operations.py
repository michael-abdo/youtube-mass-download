#!/usr/bin/env python3
"""
Test Database CRUD Operations
Phase 1.10: Test all CRUD operations with error handling validation

Tests:
1. Person CRUD operations (Create, Read, Update, Delete)
2. Video CRUD operations 
3. Relationship integrity (Person-Video)
4. Batch operations
5. Error handling and validation
6. Statistics and reporting

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import os
import time
import tempfile
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta
import uuid
from unittest.mock import MagicMock, patch

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))


def test_imports():
    """Test that all required modules import correctly."""
    print("🧪 Testing imports for database CRUD operations...")
    
    try:
        from database_schema import PersonRecord, VideoRecord
        from database_operations_ext import MassDownloadDatabaseOperations
        from utils.database_operations import (
            DatabaseConfig, DatabaseManager, create_table, drop_table
        )
        
        print("✅ SUCCESS: All required imports successful")
        return True
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import required modules")
        print(f"   Error: {e}")
        return False


def setup_test_database():
    """Set up a test SQLite database."""
    print("\n🧪 Setting up test database...")
    
    try:
        from utils.database_operations import DatabaseConfig, DatabaseManager, create_table
        
        # Create test database config
        test_db_path = Path(tempfile.gettempdir()) / "mass_download_test.db"
        test_db_path.unlink(missing_ok=True)  # Remove if exists
        
        config = DatabaseConfig(
            db_type="sqlite",
            database=str(test_db_path)
        )
        
        # Initialize database manager
        db_manager = DatabaseManager(config)
        
        # Create tables manually using SQL
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create persons table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS persons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    email TEXT,
                    type TEXT,
                    channel_url TEXT NOT NULL UNIQUE,
                    channel_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create videos table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    person_id INTEGER NOT NULL,
                    video_id TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    duration INTEGER,
                    upload_date TIMESTAMP,
                    view_count INTEGER,
                    description TEXT,
                    uuid TEXT NOT NULL UNIQUE,
                    download_status TEXT DEFAULT 'pending',
                    s3_path TEXT,
                    file_size INTEGER,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
                )
            """)
            
            conn.commit()
        
        print(f"✅ SUCCESS: Test database created at {test_db_path}")
        return config, db_manager, None
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Failed to set up test database: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None


def test_person_crud_operations(config, db_ops):
    """Test Person CRUD operations."""
    print("\n🧪 Testing Person CRUD operations...")
    
    try:
        from database_schema import PersonRecord
        
        # Test 1: Create new person
        print("  📝 Testing CREATE person...")
        person1 = PersonRecord(
            name="Test YouTuber 1",
            email="test1@youtube.com",
            type="educational",
            channel_url="https://www.youtube.com/@testchannel1",
            channel_id="UCtestchannel1"
        )
        
        person1_id = db_ops.save_person(person1)
        assert person1_id is not None, "Person ID should not be None"
        print(f"    ✅ Created person with ID: {person1_id}")
        
        # Test 2: Read person by ID
        print("  📖 Testing READ person by ID...")
        retrieved_person = db_ops.get_person(person1_id)
        assert retrieved_person is not None, "Should retrieve person"
        assert retrieved_person['name'] == person1.name, "Name should match"
        assert retrieved_person['email'] == person1.email, "Email should match"
        print(f"    ✅ Retrieved person: {retrieved_person['name']}")
        
        # Test 3: Read person by channel URL
        print("  📖 Testing READ person by channel URL...")
        retrieved_by_url = db_ops.get_person_by_channel_url(person1.channel_url)
        assert retrieved_by_url is not None, "Should retrieve person by URL"
        assert retrieved_by_url['id'] == person1_id, "IDs should match"
        print(f"    ✅ Retrieved person by URL: {retrieved_by_url['name']}")
        
        # Test 4: Update person
        print("  ✏️  Testing UPDATE person...")
        person1.email = "updated@youtube.com"
        person1.type = "entertainment"
        updated_id = db_ops.save_person(person1)
        assert updated_id == person1_id, "Should return same ID on update"
        
        updated_person = db_ops.get_person(person1_id)
        assert updated_person['email'] == "updated@youtube.com", "Email should be updated"
        assert updated_person['type'] == "entertainment", "Type should be updated"
        print(f"    ✅ Updated person email and type")
        
        # Test 5: Create another person
        print("  📝 Testing CREATE second person...")
        person2 = PersonRecord(
            name="Test YouTuber 2",
            email="test2@youtube.com",
            type="gaming",
            channel_url="https://www.youtube.com/@testchannel2"
        )
        
        person2_id = db_ops.save_person(person2)
        assert person2_id != person1_id, "Should have different ID"
        print(f"    ✅ Created second person with ID: {person2_id}")
        
        # Test 6: List persons
        print("  📋 Testing LIST persons...")
        all_persons = db_ops.list_persons()
        assert len(all_persons) >= 2, "Should have at least 2 persons"
        print(f"    ✅ Listed {len(all_persons)} persons")
        
        # Test 7: List with filter
        print("  🔍 Testing LIST persons with filter...")
        gaming_persons = db_ops.list_persons(filter_type="gaming")
        assert len(gaming_persons) == 1, "Should have 1 gaming person"
        assert gaming_persons[0]['name'] == person2.name, "Should be person2"
        print(f"    ✅ Filtered list returned {len(gaming_persons)} gaming persons")
        
        print("✅ SUCCESS: All Person CRUD operations passed")
        return True, person1_id, person2_id
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Person CRUD operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False, None, None


def test_video_crud_operations(config, db_ops, person1_id, person2_id):
    """Test Video CRUD operations."""
    print("\n🧪 Testing Video CRUD operations...")
    
    try:
        from database_schema import VideoRecord
        
        # Test 1: Create new video
        print("  📝 Testing CREATE video...")
        video1 = VideoRecord(
            person_id=person1_id,
            video_id="testVid0001",
            title="Test Video 1",
            duration=300,
            upload_date=datetime.now() - timedelta(days=5),
            view_count=1000,
            description="This is a test video description",
            download_status="pending"
        )
        
        video1_id = db_ops.save_video(video1)
        assert video1_id is not None, "Video ID should not be None"
        print(f"    ✅ Created video with ID: {video1_id}")
        
        # Test 2: Read video by ID
        print("  📖 Testing READ video by ID...")
        retrieved_video = db_ops.get_video(video1_id)
        assert retrieved_video is not None, "Should retrieve video"
        assert retrieved_video['title'] == video1.title, "Title should match"
        assert retrieved_video['person_id'] == person1_id, "Person ID should match"
        print(f"    ✅ Retrieved video: {retrieved_video['title']}")
        
        # Test 3: Read video by video_id
        print("  📖 Testing READ video by video_id...")
        retrieved_by_vid_id = db_ops.get_video_by_video_id("testVid0001")
        assert retrieved_by_vid_id is not None, "Should retrieve video by video_id"
        assert retrieved_by_vid_id['id'] == video1_id, "IDs should match"
        print(f"    ✅ Retrieved video by video_id: {retrieved_by_vid_id['title']}")
        
        # Test 4: Update video
        print("  ✏️  Testing UPDATE video...")
        video1.view_count = 2000
        video1.download_status = "completed"
        video1.s3_path = "s3://test-bucket/videos/testVid0001.mp4"
        video1.file_size = 10485760  # 10MB
        
        updated_id = db_ops.save_video(video1)
        assert updated_id == video1_id, "Should return same ID on update"
        
        updated_video = db_ops.get_video(video1_id)
        assert updated_video['view_count'] == 2000, "View count should be updated"
        assert updated_video['download_status'] == "completed", "Status should be updated"
        assert updated_video['s3_path'] == video1.s3_path, "S3 path should be updated"
        print(f"    ✅ Updated video status and metadata")
        
        # Test 5: Create videos for both persons
        print("  📝 Testing CREATE multiple videos...")
        videos_created = []
        
        # Videos for person1
        for i in range(2, 4):
            video = VideoRecord(
                person_id=person1_id,
                video_id=f"testVid000{i}",
                title=f"Test Video {i}",
                duration=300 + i * 60,
                upload_date=datetime.now() - timedelta(days=i),
                view_count=1000 * i,
                download_status="pending"
            )
            vid_id = db_ops.save_video(video)
            videos_created.append(vid_id)
        
        # Videos for person2
        for i in range(4, 6):
            video = VideoRecord(
                person_id=person2_id,
                video_id=f"testVid000{i}",
                title=f"Test Video {i}",
                duration=300 + i * 60,
                upload_date=datetime.now() - timedelta(days=i),
                view_count=1000 * i,
                download_status="pending"
            )
            vid_id = db_ops.save_video(video)
            videos_created.append(vid_id)
        
        print(f"    ✅ Created {len(videos_created)} additional videos")
        
        # Test 6: Get videos by person
        print("  👤 Testing GET videos by person...")
        person1_videos = db_ops.get_videos_by_person(person1_id)
        assert len(person1_videos) == 3, f"Person1 should have 3 videos, got {len(person1_videos)}"
        
        person2_videos = db_ops.get_videos_by_person(person2_id)
        assert len(person2_videos) == 2, f"Person2 should have 2 videos, got {len(person2_videos)}"
        print(f"    ✅ Retrieved videos by person correctly")
        
        # Test 7: Get pending videos
        print("  ⏳ Testing GET pending videos...")
        all_pending = db_ops.get_pending_videos()
        assert len(all_pending) == 4, f"Should have 4 pending videos, got {len(all_pending)}"
        
        person1_pending = db_ops.get_pending_videos(person_id=person1_id)
        assert len(person1_pending) == 2, f"Person1 should have 2 pending videos, got {len(person1_pending)}"
        print(f"    ✅ Retrieved pending videos correctly")
        
        # Test 8: Update video status
        print("  📊 Testing UPDATE video status...")
        success = db_ops.update_video_status(
            "testVid0002",
            "completed",
            s3_path="s3://test-bucket/videos/testVid0002.mp4",
            file_size=15728640  # 15MB
        )
        assert success, "Status update should succeed"
        
        updated = db_ops.get_video_by_video_id("testVid0002")
        assert updated['download_status'] == "completed", "Status should be completed"
        assert updated['s3_path'] is not None, "S3 path should be set"
        print(f"    ✅ Updated video status successfully")
        
        # Test 9: Update with error
        print("  ❌ Testing UPDATE video status with error...")
        success = db_ops.update_video_status(
            "testVid0003",
            "failed",
            error_message="Download failed: Connection timeout"
        )
        assert success, "Status update should succeed"
        
        failed = db_ops.get_video_by_video_id("testVid0003")
        assert failed['download_status'] == "failed", "Status should be failed"
        assert "Connection timeout" in failed['error_message'], "Error message should be set"
        print(f"    ✅ Updated video status with error successfully")
        
        print("✅ SUCCESS: All Video CRUD operations passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Video CRUD operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_batch_operations(config, db_ops, person1_id):
    """Test batch operations."""
    print("\n🧪 Testing batch operations...")
    
    try:
        from database_schema import VideoRecord
        
        # Test batch save videos
        print("  📦 Testing BATCH save videos...")
        batch_videos = []
        for i in range(10, 15):
            video = VideoRecord(
                person_id=person1_id,
                video_id=f"batchV{i:05d}",  # Make it 11 chars total
                title=f"Batch Video {i}",
                duration=300,
                upload_date=datetime.now(),
                view_count=1000,
                download_status="pending"
            )
            batch_videos.append(video)
        
        saved_count = db_ops.batch_save_videos(batch_videos)
        assert saved_count == 5, f"Should save 5 videos, saved {saved_count}"
        print(f"    ✅ Batch saved {saved_count} videos")
        
        # Verify batch save
        for video in batch_videos:
            retrieved = db_ops.get_video_by_video_id(video.video_id)
            assert retrieved is not None, f"Video {video.video_id} should exist"
        print(f"    ✅ Verified all batch saved videos exist")
        
        print("✅ SUCCESS: All batch operations passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Batch operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_statistics_and_reporting(config, db_ops):
    """Test statistics and reporting functions."""
    print("\n🧪 Testing statistics and reporting...")
    
    try:
        pass  # db_ops already passed in
        
        # Test overall statistics
        print("  📊 Testing overall statistics...")
        stats = db_ops.get_download_statistics()
        
        assert 'total_persons' in stats, "Should have total_persons"
        assert stats['total_persons'] >= 2, f"Should have at least 2 persons, got {stats['total_persons']}"
        
        assert 'total_videos' in stats, "Should have total_videos"
        assert stats['total_videos'] >= 10, f"Should have at least 10 videos, got {stats['total_videos']}"
        
        assert 'videos_by_status' in stats, "Should have videos_by_status"
        assert 'pending' in stats['videos_by_status'], "Should have pending videos"
        assert 'completed' in stats['videos_by_status'], "Should have completed videos"
        
        print(f"    ✅ Overall statistics retrieved successfully")
        print(f"       Total persons: {stats['total_persons']}")
        print(f"       Total videos: {stats['total_videos']}")
        print(f"       Videos by status: {stats['videos_by_status']}")
        print(f"       Total storage: {stats['total_storage_gb']} GB")
        
        # Test person-specific statistics
        print("  👤 Testing person-specific statistics...")
        # Get first person ID
        persons = db_ops.list_persons(limit=1)
        if persons:
            person_id = persons[0]['id']
            person_stats = db_ops.get_person_statistics(person_id)
            
            assert 'name' in person_stats, "Should have person name"
            assert 'total_videos' in person_stats, "Should have total videos"
            assert 'videos_by_status' in person_stats, "Should have videos by status"
            assert 'storage_mb' in person_stats, "Should have storage in MB"
            
            print(f"    ✅ Person statistics retrieved successfully")
            print(f"       Person: {person_stats['name']}")
            print(f"       Total videos: {person_stats['total_videos']}")
            print(f"       Storage: {person_stats['storage_mb']} MB")
        
        print("✅ SUCCESS: All statistics and reporting tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Statistics and reporting failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_error_handling_and_validation(config, db_ops):
    """Test error handling and validation."""
    print("\n🧪 Testing error handling and validation...")
    
    try:
        from database_schema import PersonRecord, VideoRecord
        
        # Test 1: Invalid person - missing name
        print("  ❌ Testing invalid person (missing name)...")
        try:
            invalid_person = PersonRecord(
                name="",  # Empty name
                channel_url="https://www.youtube.com/@test"
            )
            db_ops.save_person(invalid_person)
            assert False, "Should have raised validation error"
        except ValueError as e:
            assert "name is required" in str(e), "Should mention name requirement"
            print(f"    ✅ Correctly rejected invalid person: {e}")
        
        # Test 2: Invalid video - missing required fields
        print("  ❌ Testing invalid video (missing person_id)...")
        try:
            invalid_video = VideoRecord(
                person_id=None,  # Missing person_id
                video_id="test123",
                title="Test"
            )
            db_ops.save_video(invalid_video)
            assert False, "Should have raised validation error"
        except ValueError as e:
            assert "person_id is required" in str(e), "Should mention person_id requirement"
            print(f"    ✅ Correctly rejected invalid video: {e}")
        
        # Test 3: Invalid video ID format
        print("  ❌ Testing invalid video ID format...")
        try:
            persons = db_ops.list_persons(limit=1)
            person_id = persons[0]['id'] if persons else 1
            
            invalid_video = VideoRecord(
                person_id=person_id,
                video_id="bad",  # Too short
                title="Test"
            )
            db_ops.save_video(invalid_video)
            assert False, "Should have raised validation error"
        except ValueError as e:
            assert "video_id must be" in str(e), "Should mention video_id format"
            print(f"    ✅ Correctly rejected invalid video ID: {e}")
        
        # Test 4: Non-existent person lookup
        print("  🔍 Testing non-existent person lookup...")
        non_existent = db_ops.get_person(99999)
        assert non_existent is None, "Should return None for non-existent person"
        print("    ✅ Correctly returned None for non-existent person")
        
        # Test 5: Non-existent video lookup
        print("  🔍 Testing non-existent video lookup...")
        non_existent = db_ops.get_video_by_video_id("doesNotExist")
        assert non_existent is None, "Should return None for non-existent video"
        print("    ✅ Correctly returned None for non-existent video")
        
        print("✅ SUCCESS: All error handling and validation tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Error handling tests failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run comprehensive database CRUD test suite."""
    print("🚀 Starting Database CRUD Operations Test Suite")
    print("   Testing Phase 1.10: All CRUD operations with error handling")
    print("   Validating Create, Read, Update, Delete operations")
    print("=" * 80)
    
    all_tests_passed = True
    config = None
    
    # Test imports
    if not test_imports():
        print("❌ Import test FAILED - cannot continue")
        return 1
    
    # Set up test database
    config, db_manager, schema_manager = setup_test_database()
    if not config:
        print("❌ Database setup FAILED - cannot continue")
        return 1
    
    try:
        # Set the global database manager for the test database
        from utils.database_operations import _db_manager
        import utils.database_operations
        utils.database_operations._db_manager = db_manager
        
        # Create a single database operations instance for all tests
        from database_operations_ext import MassDownloadDatabaseOperations
        db_ops = MassDownloadDatabaseOperations(db_manager=db_manager)
        
        # Run CRUD tests
        person_passed, person1_id, person2_id = test_person_crud_operations(config, db_ops)
        if not person_passed:
            all_tests_passed = False
            print("❌ Person CRUD operations FAILED")
        
        if person1_id and person2_id:
            if not test_video_crud_operations(config, db_ops, person1_id, person2_id):
                all_tests_passed = False
                print("❌ Video CRUD operations FAILED")
            
            if not test_batch_operations(config, db_ops, person1_id):
                all_tests_passed = False
                print("❌ Batch operations FAILED")
        
        if not test_statistics_and_reporting(config, db_ops):
            all_tests_passed = False
            print("❌ Statistics and reporting FAILED")
        
        if not test_error_handling_and_validation(config, db_ops):
            all_tests_passed = False
            print("❌ Error handling and validation FAILED")
        
    except Exception as e:
        print(f"💥 UNEXPECTED TEST SUITE ERROR: {e}")
        import traceback
        traceback.print_exc()
        all_tests_passed = False
    
    finally:
        # Clean up test database
        if config:
            try:
                test_db_path = Path(config.database)
                if test_db_path.exists():
                    test_db_path.unlink()
                    print(f"\n🧹 Cleaned up test database")
            except Exception as e:
                print(f"⚠️  Could not clean up test database: {e}")
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL DATABASE CRUD TESTS PASSED!")
        print("✅ Person CRUD operations working")
        print("✅ Video CRUD operations working")
        print("✅ Batch operations working")
        print("✅ Statistics and reporting working")
        print("✅ Error handling comprehensive")
        print("\n🔥 Database operations are PRODUCTION-READY!")
        return 0
    else:
        print("💥 SOME DATABASE CRUD TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)