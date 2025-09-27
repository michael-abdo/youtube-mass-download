#!/usr/bin/env python3
"""
Test Mass Coordinator Module Structure
Phase 4.1: Validate mass coordinator module structure

Tests:
1. Module imports and initialization
2. Data structures validation
3. Input file processing
4. Progress tracking functionality
5. Error handling and recovery
6. Concurrent processing setup

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import os
import json
import csv
import tempfile
from pathlib import Path
from typing import List, Dict, Any

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))

def test_imports():
    """Test that all required modules import correctly."""
    print("🧪 Testing imports for mass coordinator...")
    
    try:
        from mass_coordinator import (
            MassDownloadCoordinator, 
            ChannelProcessingResult,
            MassDownloadProgress,
            ProcessingStatus
        )
        from input_handler import InputHandler, ChannelInput
        from channel_discovery import YouTubeChannelDiscovery
        from database_schema import PersonRecord, VideoRecord
        
        print("✅ SUCCESS: All required imports successful")
        return True, (MassDownloadCoordinator, ChannelProcessingResult, 
                     MassDownloadProgress, ProcessingStatus)
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import required modules")
        print(f"   Error: {e}")
        return False, None


def test_data_structures():
    """Test mass coordinator data structures."""
    print("\n🧪 Testing data structures...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    _, ChannelProcessingResult, MassDownloadProgress, ProcessingStatus = classes
    
    try:
        # Test ProcessingStatus enum
        statuses = [
            ProcessingStatus.PENDING,
            ProcessingStatus.DISCOVERING,
            ProcessingStatus.DOWNLOADING,
            ProcessingStatus.COMPLETED,
            ProcessingStatus.FAILED
        ]
        print(f"✅ SUCCESS: ProcessingStatus enum has {len(statuses)} states")
        
        # Test ChannelProcessingResult
        result = ChannelProcessingResult(
            channel_url="https://youtube.com/@testchannel",
            status=ProcessingStatus.PENDING
        )
        
        # Validate properties
        assert result.duration_seconds is None, "Duration should be None without times"
        assert result.success_rate == 0.0, "Success rate should be 0 with no videos"
        
        print("✅ SUCCESS: ChannelProcessingResult validated")
        
        # Test MassDownloadProgress
        progress = MassDownloadProgress(total_channels=10)
        
        # Test progress calculations
        progress.channels_processed = 3
        progress.channels_failed = 1
        progress.channels_skipped = 1
        
        assert progress.channels_remaining == 5, f"Expected 5 remaining, got {progress.channels_remaining}"
        assert progress.overall_progress_percent == 50.0, f"Expected 50%, got {progress.overall_progress_percent}"
        
        # Test status dict
        status_dict = progress.get_status_dict()
        required_keys = {
            'total_channels', 'channels_processed', 'channels_failed',
            'progress_percent', 'elapsed_seconds', 'errors_count'
        }
        
        missing_keys = required_keys - set(status_dict.keys())
        if missing_keys:
            print(f"❌ FAILURE: Missing keys in status dict: {missing_keys}")
            return False
        
        print("✅ SUCCESS: MassDownloadProgress validated")
        
        print("✅ ALL data structure tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Data structure test failed: {e}")
        return False


def test_coordinator_initialization():
    """Test mass coordinator initialization."""
    print("\n🧪 Testing coordinator initialization...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    MassDownloadCoordinator, _, _, _ = classes
    
    try:
        # Test initialization
        coordinator = MassDownloadCoordinator()
        
        # Verify components initialized
        assert hasattr(coordinator, 'channel_discovery'), "Missing channel_discovery"
        assert hasattr(coordinator, 'database_manager'), "Missing database_manager"
        assert hasattr(coordinator, 'input_handler'), "Missing input_handler"
        assert hasattr(coordinator, 'progress'), "Missing progress tracker"
        assert hasattr(coordinator, 'executor'), "Missing thread pool executor"
        
        print(f"✅ SUCCESS: Coordinator initialized with all components")
        
        # Check configuration
        print(f"   Max concurrent channels: {coordinator.max_concurrent_channels}")
        print(f"   Skip existing videos: {coordinator.skip_existing_videos}")
        print(f"   Continue on error: {coordinator.continue_on_error}")
        
        print("✅ ALL coordinator initialization tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Coordinator initialization failed: {e}")
        return False


def test_input_file_processing():
    """Test input file processing functionality."""
    print("\n🧪 Testing input file processing...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    MassDownloadCoordinator, _, _, _ = classes
    
    try:
        coordinator = MassDownloadCoordinator()
        
        # Test Case 1: CSV file processing
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(['name', 'channel_url', 'email'])
            csv_writer.writerow(['Test Channel 1', 'https://youtube.com/@channel1', 'test1@example.com'])
            csv_writer.writerow(['Test Channel 2', 'https://youtube.com/@channel2', 'test2@example.com'])
            csv_file = f.name
        
        try:
            pairs = coordinator.process_input_file(csv_file)
            
            if len(pairs) != 2:
                print(f"❌ FAILURE: Expected 2 channel pairs, got {len(pairs)}")
                return False
            
            # Validate first pair
            person, url = pairs[0]
            assert person.name == "Test Channel 1", f"Wrong name: {person.name}"
            assert person.email == "test1@example.com", f"Wrong email: {person.email}"
            assert url == "https://www.youtube.com/@channel1", f"Wrong URL: {url}"
            
            print("✅ SUCCESS: CSV file processing working")
            
        finally:
            os.unlink(csv_file)
        
        # Test Case 2: JSON file processing
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json_data = {
                "channels": [
                    {
                        "name": "JSON Channel",
                        "channel_url": "https://www.youtube.com/@jsonchannel",
                        "type": "educational"
                    }
                ]
            }
            json.dump(json_data, f)
            json_file = f.name
        
        try:
            pairs = coordinator.process_input_file(json_file)
            
            if len(pairs) != 1:
                print(f"❌ FAILURE: Expected 1 channel pair from JSON, got {len(pairs)}")
                return False
            
            person, url = pairs[0]
            assert person.name == "JSON Channel", f"Wrong name: {person.name}"
            assert person.type == "educational", f"Wrong type: {person.type}"
            
            print("✅ SUCCESS: JSON file processing working")
            
        finally:
            os.unlink(json_file)
        
        # Test Case 3: Invalid file
        try:
            coordinator.process_input_file("/nonexistent/file.txt")
            print("❌ FAILURE: Should have raised error for nonexistent file")
            return False
        except (ValueError, RuntimeError) as e:
            if "not found" in str(e):
                print("✅ SUCCESS: Invalid file handled correctly")
            else:
                print(f"❌ FAILURE: Wrong error for invalid file: {e}")
                return False
        
        print("✅ ALL input file processing tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Input file processing test failed: {e}")
        return False


def test_progress_tracking():
    """Test progress tracking functionality."""
    print("\n🧪 Testing progress tracking...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    MassDownloadCoordinator, _, _, _ = classes
    
    try:
        coordinator = MassDownloadCoordinator()
        
        # Test progress updates
        coordinator._update_progress(
            total_channels=5,
            channels_processed=2,
            current_channel="https://youtube.com/@test"
        )
        
        # Verify updates
        assert coordinator.progress.total_channels == 5, "Progress update failed"
        assert coordinator.progress.channels_processed == 2, "Progress update failed"
        assert coordinator.progress.current_channel == "https://youtube.com/@test", "Progress update failed"
        
        print("✅ SUCCESS: Progress updates working")
        
        # Test error tracking
        coordinator._add_error("Test error message")
        
        assert len(coordinator.progress.errors) == 1, "Error tracking failed"
        assert "Test error message" in coordinator.progress.errors[0], "Error message not recorded"
        
        print("✅ SUCCESS: Error tracking working")
        
        # Test progress report
        report = coordinator.get_progress_report()
        
        required_keys = {
            'total_channels', 'channels_processed', 'progress_percent',
            'elapsed_seconds', 'errors_count', 'channel_results'
        }
        
        missing_keys = required_keys - set(report.keys())
        if missing_keys:
            print(f"❌ FAILURE: Missing keys in progress report: {missing_keys}")
            return False
        
        print("✅ SUCCESS: Progress reporting working")
        
        print("✅ ALL progress tracking tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Progress tracking test failed: {e}")
        return False


def test_concurrent_processing_setup():
    """Test concurrent processing setup."""
    print("\n🧪 Testing concurrent processing setup...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    MassDownloadCoordinator, _, _, _ = classes
    
    try:
        coordinator = MassDownloadCoordinator()
        
        # Verify thread pool setup
        assert coordinator.executor is not None, "Thread pool not initialized"
        assert coordinator.executor._max_workers == coordinator.max_concurrent_channels, \
            f"Thread pool size mismatch: {coordinator.executor._max_workers} != {coordinator.max_concurrent_channels}"
        
        print(f"✅ SUCCESS: Thread pool initialized with {coordinator.max_concurrent_channels} workers")
        
        # Test shutdown
        coordinator.shutdown()
        
        # Verify clean shutdown
        assert coordinator.executor._shutdown, "Thread pool not properly shut down"
        
        print("✅ SUCCESS: Clean shutdown working")
        
        print("✅ ALL concurrent processing setup tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Concurrent processing test failed: {e}")
        return False


def main():
    """Run comprehensive mass coordinator test suite."""
    print("🚀 Starting Mass Coordinator Module Structure Test Suite")
    print("   Testing Phase 4.1: Mass coordinator module structure")
    print("   Validating components, data structures, and functionality")
    print("=" * 80)
    
    all_tests_passed = True
    test_functions = [
        test_imports,
        test_data_structures,
        test_coordinator_initialization,
        test_input_file_processing,
        test_progress_tracking,
        test_concurrent_processing_setup
    ]
    
    for test_func in test_functions:
        if not test_func():
            all_tests_passed = False
            print(f"❌ {test_func.__name__} FAILED")
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL MASS COORDINATOR MODULE STRUCTURE TESTS PASSED!")
        print("✅ Module imports working")
        print("✅ Data structures validated")
        print("✅ Input file processing functional")
        print("✅ Progress tracking implemented")
        print("✅ Error handling in place")
        print("✅ Concurrent processing ready")
        print("\\n🔥 Mass coordinator module structure is COMPLETE!")
        return 0
    else:
        print("💥 SOME MASS COORDINATOR TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)