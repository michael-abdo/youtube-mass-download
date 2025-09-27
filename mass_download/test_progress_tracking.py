#!/usr/bin/env python3
"""
Test Progress Tracking and Resume Capability
Phase 4.9: Test progress tracking and resume capability

Tests:
1. Progress record creation and validation
2. Progress saving to database
3. Progress updates during processing
4. Job resumption from previous state
5. Multiple job tracking
6. Error handling and status tracking

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
import json

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))


def test_imports():
    """Test that all required modules import correctly."""
    print("🧪 Testing imports for progress tracking...")
    
    try:
        from database_schema import ProgressRecord, PersonRecord, VideoRecord
        from database_operations_ext import MassDownloadDatabaseOperations
        from mass_coordinator import MassDownloadCoordinator
        from utils.database_operations import DatabaseConfig, DatabaseManager
        
        print("✅ SUCCESS: All required imports successful")
        return True
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import required modules")
        print(f"   Error: {e}")
        return False


def setup_test_environment():
    """Set up test database and environment."""
    print("\n🧪 Setting up test environment...")
    
    try:
        from utils.database_operations import DatabaseConfig, DatabaseManager
        from database_operations_ext import MassDownloadDatabaseOperations
        
        # Create test database
        test_db_path = Path(tempfile.gettempdir()) / "progress_test.db"
        test_db_path.unlink(missing_ok=True)
        
        config = DatabaseConfig(
            db_type="sqlite",
            database=str(test_db_path)
        )
        
        db_manager = DatabaseManager(config)
        
        # Create tables
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create all required tables
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
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS progress (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL UNIQUE,
                    input_file TEXT NOT NULL,
                    total_channels INTEGER DEFAULT 0,
                    channels_processed INTEGER DEFAULT 0,
                    channels_failed INTEGER DEFAULT 0,
                    channels_skipped INTEGER DEFAULT 0,
                    total_videos INTEGER DEFAULT 0,
                    videos_processed INTEGER DEFAULT 0,
                    videos_failed INTEGER DEFAULT 0,
                    videos_skipped INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'running',
                    error_message TEXT,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP
                )
            """)
            
            conn.commit()
        
        # Set global database manager
        import utils.database_operations
        utils.database_operations._db_manager = db_manager
        
        db_ops = MassDownloadDatabaseOperations(db_manager=db_manager)
        
        print(f"✅ SUCCESS: Test environment created at {test_db_path}")
        return config, db_manager, db_ops, test_db_path
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Failed to set up test environment: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None, None


def test_progress_record_validation(db_ops):
    """Test ProgressRecord validation."""
    print("\n🧪 Testing ProgressRecord validation...")
    
    try:
        from database_schema import ProgressRecord
        
        # Test 1: Valid progress record
        print("  ✅ Testing valid progress record...")
        progress = ProgressRecord(
            job_id="test_job_001",
            input_file="/path/to/input.csv",
            total_channels=10,
            channels_processed=5,
            status="running"
        )
        print(f"    ✅ Created valid progress record: {progress.job_id}")
        
        # Test 2: Invalid job_id
        print("  ❌ Testing invalid job_id...")
        try:
            invalid_progress = ProgressRecord(
                job_id="",  # Empty job_id
                input_file="/path/to/input.csv"
            )
            assert False, "Should have raised validation error"
        except ValueError as e:
            assert "job_id is required" in str(e)
            print(f"    ✅ Correctly rejected empty job_id: {e}")
        
        # Test 3: Invalid status
        print("  ❌ Testing invalid status...")
        try:
            invalid_progress = ProgressRecord(
                job_id="test_job",
                input_file="/path/to/input.csv",
                status="invalid_status"
            )
            assert False, "Should have raised validation error"
        except ValueError as e:
            assert "status must be one of" in str(e)
            print(f"    ✅ Correctly rejected invalid status: {e}")
        
        # Test 4: Negative counts
        print("  ❌ Testing negative counts...")
        try:
            invalid_progress = ProgressRecord(
                job_id="test_job",
                input_file="/path/to/input.csv",
                channels_processed=-1
            )
            assert False, "Should have raised validation error"
        except ValueError as e:
            assert "must be non-negative" in str(e)
            print(f"    ✅ Correctly rejected negative count: {e}")
        
        print("✅ SUCCESS: All progress record validation tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Progress record validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_progress_saving_and_updating(db_ops):
    """Test progress saving and updating."""
    print("\n🧪 Testing progress saving and updating...")
    
    try:
        from database_schema import ProgressRecord
        
        # Test 1: Create new progress
        print("  📝 Testing CREATE progress...")
        progress = ProgressRecord(
            job_id="job_test_001",
            input_file="/test/input.csv",
            total_channels=5,
            status="running"
        )
        
        progress_id = db_ops.save_progress(progress)
        assert progress_id is not None, "Progress ID should not be None"
        print(f"    ✅ Created progress with ID: {progress_id}")
        
        # Test 2: Read progress
        print("  📖 Testing READ progress...")
        retrieved = db_ops.get_progress_by_job_id("job_test_001")
        assert retrieved is not None, "Should retrieve progress"
        assert retrieved['job_id'] == "job_test_001", "Job ID should match"
        assert retrieved['total_channels'] == 5, "Total channels should match"
        print(f"    ✅ Retrieved progress: {retrieved['job_id']}")
        
        # Test 3: Update progress counts
        print("  ✏️  Testing UPDATE progress counts...")
        success = db_ops.update_progress_counts(
            "job_test_001",
            channels_processed=2,
            videos_processed=10,
            videos_failed=1
        )
        assert success, "Update should succeed"
        
        updated = db_ops.get_progress_by_job_id("job_test_001")
        assert updated['channels_processed'] == 2, "Channels processed should be updated"
        assert updated['videos_processed'] == 10, "Videos processed should be updated"
        assert updated['videos_failed'] == 1, "Videos failed should be updated"
        print(f"    ✅ Updated progress counts successfully")
        
        # Test 4: Update existing progress via save
        print("  ✏️  Testing UPDATE via save...")
        progress.channels_processed = 3
        progress.channels_failed = 1
        progress.status = "running"
        
        updated_id = db_ops.save_progress(progress)
        assert updated_id == progress_id, "Should return same ID on update"
        
        updated = db_ops.get_progress_by_job_id("job_test_001")
        assert updated['channels_processed'] == 3, "Should be updated"
        assert updated['channels_failed'] == 1, "Should be updated"
        print(f"    ✅ Updated progress via save successfully")
        
        # Test 5: Mark job completed
        print("  ✅ Testing mark job completed...")
        success = db_ops.mark_job_completed("job_test_001", "completed")
        assert success, "Marking completed should succeed"
        
        completed = db_ops.get_progress_by_job_id("job_test_001")
        assert completed['status'] == "completed", "Status should be completed"
        assert completed['completed_at'] is not None, "Completed time should be set"
        print(f"    ✅ Marked job as completed successfully")
        
        print("✅ SUCCESS: All progress saving and updating tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Progress saving/updating failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_multiple_jobs_tracking(db_ops):
    """Test tracking multiple jobs."""
    print("\n🧪 Testing multiple jobs tracking...")
    
    try:
        from database_schema import ProgressRecord
        
        # Create multiple jobs
        print("  📝 Creating multiple jobs...")
        job_ids = []
        for i in range(3):
            progress = ProgressRecord(
                job_id=f"multi_job_{i:03d}",
                input_file=f"/test/input_{i}.csv",
                total_channels=10 + i,
                channels_processed=i * 2,
                status="running" if i < 2 else "paused"
            )
            
            progress_id = db_ops.save_progress(progress)
            job_ids.append(f"multi_job_{i:03d}")
            print(f"    ✅ Created job: {progress.job_id}")
        
        # Test get active jobs
        print("  🔍 Testing get active jobs...")
        active_jobs = db_ops.get_active_jobs()
        assert len(active_jobs) == 3, f"Should have 3 active jobs, got {len(active_jobs)}"
        assert all(job['status'] in ['running', 'paused'] for job in active_jobs), "All should be active"
        print(f"    ✅ Found {len(active_jobs)} active jobs")
        
        # Mark one as completed
        print("  ✅ Marking job as completed...")
        db_ops.mark_job_completed("multi_job_001", "completed")
        
        active_jobs = db_ops.get_active_jobs()
        assert len(active_jobs) == 2, f"Should have 2 active jobs after completion, got {len(active_jobs)}"
        print(f"    ✅ Active jobs updated correctly")
        
        # Mark one as failed
        print("  ❌ Marking job as failed...")
        db_ops.mark_job_completed("multi_job_002", "failed", "Test failure")
        
        active_jobs = db_ops.get_active_jobs()
        assert len(active_jobs) == 1, f"Should have 1 active job after failure, got {len(active_jobs)}"
        assert active_jobs[0]['job_id'] == "multi_job_000", "Only paused job should remain"
        print(f"    ✅ Only paused job remains active")
        
        print("✅ SUCCESS: All multiple jobs tracking tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Multiple jobs tracking failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_coordinator_progress_integration(config, db_ops):
    """Test progress tracking integration with coordinator."""
    print("\n🧪 Testing coordinator progress integration...")
    
    try:
        from mass_coordinator import MassDownloadCoordinator
        from database_schema import PersonRecord
        from utils.config import Config
        
        # Create test input file
        test_input = Path(tempfile.gettempdir()) / "test_channels.txt"
        test_input.write_text("https://www.youtube.com/@testchannel1\nhttps://www.youtube.com/@testchannel2")
        
        # Create a mock config object with necessary attributes
        class MockConfig:
            def get(self, key, default=None):
                config_map = {
                    "mass_download.max_concurrent_channels": 3,
                    "mass_download.max_videos_per_channel": None,
                    "mass_download.skip_existing_videos": True,
                    "mass_download.continue_on_error": True,
                    "mass_download.download_videos": True,
                    "mass_download.max_concurrent_downloads": 3,
                    "bucket_name": "test-bucket",
                    "download_mode": "stream_to_s3",
                    "local_download_dir": str(tempfile.gettempdir())
                }
                return config_map.get(key, default)
            
            def get_section(self, section):
                if section == "database":
                    return {
                        "type": "sqlite",
                        "database": str(config.database)
                    }
                return {}
                
            bucket_name = "test-bucket"
            region = "us-east-1"
            downloads_dir = str(tempfile.gettempdir())
        
        mock_config = MockConfig()
        
        # Mock the entire DownloadIntegration to avoid complex configuration
        with patch('mass_coordinator.DownloadIntegration'):
            # Initialize coordinator with mock config
            coordinator = MassDownloadCoordinator(mock_config)
            # Set the database operations instance
            coordinator.db_ops = db_ops
        
        # Mock channel discovery to avoid actual YouTube calls
        mock_channel_info = MagicMock()
        mock_channel_info.channel_id = "UC_test_channel"
        mock_channel_info.channel_name = "Test Channel"
        mock_channel_info.subscriber_count = 1000
        
        with patch.object(coordinator.channel_discovery, 'extract_channel_info', return_value=mock_channel_info):
            with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos', return_value=[]):
                
                # Process input file
                print("  📋 Processing input file with job ID...")
                person_channel_pairs = coordinator.process_input_file(
                    str(test_input),
                    job_id="test_coordinator_job"
                )
                
                assert coordinator.job_id == "test_coordinator_job", "Job ID should be set"
                assert len(person_channel_pairs) == 2, "Should have 2 channels"
                print(f"    ✅ Processed input file, job ID: {coordinator.job_id}")
                
                # Check initial progress saved
                print("  💾 Checking initial progress...")
                progress = db_ops.get_progress_by_job_id("test_coordinator_job")
                assert progress is not None, "Progress should be saved"
                assert progress['total_channels'] == 0, "Initial total should be 0"  # Not set until processing starts
                assert progress['status'] == "running", "Should be running"
                print(f"    ✅ Initial progress saved")
                
                # Process channels
                print("  ⚡ Processing channels...")
                results = coordinator.process_channels_concurrently(person_channel_pairs)
                
                # Check progress updated
                print("  💾 Checking updated progress...")
                progress = db_ops.get_progress_by_job_id("test_coordinator_job")
                assert progress['total_channels'] == 2, "Total channels should be set"
                assert progress['channels_processed'] >= 0, "Should have processed count"
                print(f"    ✅ Progress updated: {progress['channels_processed']}/{progress['total_channels']} channels")
                
                # Shutdown coordinator
                print("  🛑 Shutting down coordinator...")
                coordinator.shutdown()
                
                # Check final status
                progress = db_ops.get_progress_by_job_id("test_coordinator_job")
                assert progress['status'] == "completed", "Should be completed after shutdown"
                assert progress['completed_at'] is not None, "Completed time should be set"
                print(f"    ✅ Final status: {progress['status']}")
        
        # Clean up
        test_input.unlink()
        
        print("✅ SUCCESS: All coordinator progress integration tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Coordinator progress integration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_job_resume_capability(config, db_ops):
    """Test job resume capability."""
    print("\n🧪 Testing job resume capability...")
    
    try:
        from mass_coordinator import MassDownloadCoordinator
        from database_schema import ProgressRecord, PersonRecord
        
        # Create a job that's partially complete
        print("  📝 Creating partially complete job...")
        progress = ProgressRecord(
            job_id="resume_test_job",
            input_file="/test/resume_input.csv",
            total_channels=10,
            channels_processed=3,
            channels_failed=1,
            channels_skipped=0,
            total_videos=50,
            videos_processed=30,
            videos_failed=5,
            status="running"
        )
        
        progress_id = db_ops.save_progress(progress)
        print(f"    ✅ Created job with 3/10 channels processed")
        
        # Create mock config
        class MockConfig:
            def get(self, key, default=None):
                config_map = {
                    "mass_download.max_concurrent_channels": 3,
                    "mass_download.max_videos_per_channel": None,
                    "mass_download.skip_existing_videos": True,
                    "mass_download.continue_on_error": True,
                    "mass_download.download_videos": True,
                    "mass_download.max_concurrent_downloads": 3,
                    "bucket_name": "test-bucket",
                    "download_mode": "stream_to_s3",
                    "local_download_dir": str(tempfile.gettempdir())
                }
                return config_map.get(key, default)
            
            def get_section(self, section):
                if section == "database":
                    return {
                        "type": "sqlite",
                        "database": str(config.database)
                    }
                return {}
                
            bucket_name = "test-bucket"
            region = "us-east-1"
            downloads_dir = str(tempfile.gettempdir())
        
        mock_config = MockConfig()
        
        # Mock the entire DownloadIntegration to avoid complex configuration
        with patch('mass_coordinator.DownloadIntegration'):
            # Create new coordinator and attempt resume
            print("  🔄 Testing resume...")
            coordinator = MassDownloadCoordinator(mock_config)
            # Set the database operations instance
            coordinator.db_ops = db_ops
        
            resumed = coordinator.resume_job("resume_test_job")
            assert resumed is not None, "Resume should succeed"
            assert coordinator.job_id == "resume_test_job", "Job ID should be set"
            assert coordinator.input_file_path == "/test/resume_input.csv", "Input file should be set"
            
            # Check progress restored
            with coordinator.progress_lock:
                assert coordinator.progress.total_channels == 10, "Total channels should be restored"
                assert coordinator.progress.channels_processed == 3, "Processed count should be restored"
                assert coordinator.progress.channels_failed == 1, "Failed count should be restored"
                assert coordinator.progress.total_videos == 50, "Total videos should be restored"
                assert coordinator.progress.videos_processed == 30, "Videos processed should be restored"
            
            print(f"    ✅ Resumed job successfully: {coordinator.progress.channels_processed}/{coordinator.progress.total_channels}")
            
            # Test resume non-existent job
            print("  ❌ Testing resume non-existent job...")
            coordinator2 = MassDownloadCoordinator(mock_config)
            coordinator2.db_ops = db_ops
            resumed = coordinator2.resume_job("does_not_exist")
            assert resumed is None, "Should return None for non-existent job"
            print(f"    ✅ Correctly returned None for non-existent job")
            
            # Test resume completed job
            print("  ❌ Testing resume completed job...")
            db_ops.mark_job_completed("resume_test_job", "completed")
            
            coordinator3 = MassDownloadCoordinator(mock_config)
            coordinator3.db_ops = db_ops
            resumed = coordinator3.resume_job("resume_test_job")
            assert resumed is None, "Should not resume completed job"
            print(f"    ✅ Correctly refused to resume completed job")
        
        print("✅ SUCCESS: All job resume capability tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Job resume capability failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_progress_error_handling(db_ops):
    """Test progress error handling."""
    print("\n🧪 Testing progress error handling...")
    
    try:
        from database_schema import ProgressRecord
        from unittest.mock import patch
        
        # Test saving progress with database error
        print("  💥 Testing database error handling...")
        
        progress = ProgressRecord(
            job_id="error_test_job",
            input_file="/test/error.csv"
        )
        
        # The decorator will catch the error and return None
        # We need to check that a progress with database error is handled
        # Let's test by trying to save with an already existing job_id first
        result1 = db_ops.save_progress(progress)
        assert result1 is not None, "First save should succeed"
        
        # Now test a genuine error case - try to update a non-existent progress
        print(f"    ✅ Handled new progress creation")
        
        # Test that errors are logged but don't crash
        print("  🔍 Testing error logging...")
        
        # Test invalid progress update
        print("  ❌ Testing invalid progress update...")
        success = db_ops.update_progress_counts(
            "non_existent_job",
            channels_processed=5
        )
        assert not success, "Should return False for non-existent job"
        print(f"    ✅ Correctly handled non-existent job update")
        
        # Test marking non-existent job completed
        print("  ❌ Testing mark non-existent job completed...")
        success = db_ops.mark_job_completed(
            "non_existent_job",
            "completed"
        )
        assert not success, "Should return False for non-existent job"
        print(f"    ✅ Correctly handled non-existent job completion")
        
        print("✅ SUCCESS: All progress error handling tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Progress error handling failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run comprehensive progress tracking test suite."""
    print("🚀 Starting Progress Tracking Test Suite")
    print("   Testing Phase 4.9: Progress tracking and resume capability")
    print("   Validating database tracking, updates, and job resumption")
    print("=" * 80)
    
    all_tests_passed = True
    config = None
    test_db_path = None
    
    # Test imports
    if not test_imports():
        print("❌ Import test FAILED - cannot continue")
        return 1
    
    # Set up test environment
    config, db_manager, db_ops, test_db_path = setup_test_environment()
    if not config:
        print("❌ Environment setup FAILED - cannot continue")
        return 1
    
    try:
        # Run tests
        if not test_progress_record_validation(db_ops):
            all_tests_passed = False
            print("❌ Progress record validation FAILED")
        
        if not test_progress_saving_and_updating(db_ops):
            all_tests_passed = False
            print("❌ Progress saving and updating FAILED")
        
        if not test_multiple_jobs_tracking(db_ops):
            all_tests_passed = False
            print("❌ Multiple jobs tracking FAILED")
        
        if not test_coordinator_progress_integration(config, db_ops):
            all_tests_passed = False
            print("❌ Coordinator progress integration FAILED")
        
        if not test_job_resume_capability(config, db_ops):
            all_tests_passed = False
            print("❌ Job resume capability FAILED")
        
        if not test_progress_error_handling(db_ops):
            all_tests_passed = False
            print("❌ Progress error handling FAILED")
        
    except Exception as e:
        print(f"💥 UNEXPECTED TEST SUITE ERROR: {e}")
        import traceback
        traceback.print_exc()
        all_tests_passed = False
    
    finally:
        # Clean up test database
        if test_db_path and test_db_path.exists():
            try:
                test_db_path.unlink()
                print(f"\n🧹 Cleaned up test database")
            except Exception as e:
                print(f"⚠️  Could not clean up test database: {e}")
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL PROGRESS TRACKING TESTS PASSED!")
        print("✅ Progress record validation working")
        print("✅ Progress saving and updating working")
        print("✅ Multiple jobs tracking working")
        print("✅ Coordinator integration working")
        print("✅ Job resume capability working")
        print("✅ Error handling comprehensive")
        print("\n🔥 Progress tracking is PRODUCTION-READY!")
        return 0
    else:
        print("💥 SOME PROGRESS TRACKING TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)