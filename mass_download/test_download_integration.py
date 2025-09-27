#!/usr/bin/env python3
"""
Test Download Integration
Phase 4.5: Test download integration with error handling

Tests:
1. Download integration module initialization
2. Stream to S3 mode
3. Local then upload mode
4. Local only mode
5. Error handling scenarios
6. Batch download functionality

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import os
import json
import time
import tempfile
from pathlib import Path
from typing import List, Dict, Any
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import uuid

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))

def test_imports():
    """Test that all required modules import correctly."""
    print("🧪 Testing imports for download integration...")
    
    try:
        from download_integration import (
            DownloadIntegration,
            DownloadResult,
            DownloadMode
        )
        from database_schema import VideoRecord
        
        print("✅ SUCCESS: All required imports successful")
        return True
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import required modules")
        print(f"   Error: {e}")
        return False


def test_download_result_validation():
    """Test DownloadResult validation."""
    print("\n🧪 Testing DownloadResult validation...")
    
    try:
        from download_integration import DownloadResult
        
        # Test Case 1: Valid result
        result = DownloadResult(
            video_id="dQw4w9WgXcQ",
            video_uuid=str(uuid.uuid4()),
            status="completed",
            s3_path="s3://bucket/path/video.mp4",
            file_size=1048576
        )
        print("✅ SUCCESS: Valid DownloadResult created")
        
        # Test Case 2: Invalid video_id
        try:
            invalid_result = DownloadResult(
                video_id="",
                video_uuid=str(uuid.uuid4()),
                status="completed"
            )
            print("❌ FAILURE: Should have rejected empty video_id")
            return False
        except ValueError as e:
            if "video_id is required" in str(e):
                print("✅ SUCCESS: Empty video_id rejected correctly")
            else:
                print(f"❌ FAILURE: Wrong error for empty video_id: {e}")
                return False
        
        # Test Case 3: Invalid status
        try:
            invalid_result = DownloadResult(
                video_id="dQw4w9WgXcQ",
                video_uuid=str(uuid.uuid4()),
                status="invalid_status"
            )
            print("❌ FAILURE: Should have rejected invalid status")
            return False
        except ValueError as e:
            if "status must be one of" in str(e):
                print("✅ SUCCESS: Invalid status rejected correctly")
            else:
                print(f"❌ FAILURE: Wrong error for invalid status: {e}")
                return False
        
        print("✅ ALL DownloadResult validation tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: DownloadResult validation test failed: {e}")
        return False


def test_download_integration_initialization():
    """Test DownloadIntegration initialization."""
    print("\n🧪 Testing DownloadIntegration initialization...")
    
    try:
        from download_integration import DownloadIntegration, DownloadMode
        
        # Mock config to avoid real S3 initialization
        mock_config = MagicMock()
        mock_config.get.side_effect = lambda key, default=None: {
            "mass_download.download_mode": "local_only",
            "mass_download.local_download_dir": tempfile.gettempdir(),
            "mass_download.delete_after_upload": True,
            "mass_download.download_resolution": "720",
            "mass_download.download_format": "mp4",
            "mass_download.download_subtitles": True,
            "s3.default_bucket": "test-bucket",
            "mass_download.s3_prefix": "mass-download"
        }.get(key, default)
        
        # Test initialization
        with patch('download_integration.UnifiedS3Manager'):
            integration = DownloadIntegration(config=mock_config)
            
            assert integration.download_mode == DownloadMode.LOCAL_ONLY
            assert integration.download_resolution == "720"
            assert integration.download_format == "mp4"
            assert integration.download_subtitles == True
            
            print("✅ SUCCESS: DownloadIntegration initialized correctly")
        
        # Test invalid configuration (stream mode without S3 bucket)
        mock_config.get.side_effect = lambda key, default=None: {
            "mass_download.download_mode": "stream_to_s3",
            "s3.default_bucket": None  # Missing bucket
        }.get(key, default)
        
        try:
            with patch('download_integration.UnifiedS3Manager'):
                integration = DownloadIntegration(config=mock_config)
            print("❌ FAILURE: Should have rejected stream_to_s3 without bucket")
            return False
        except ValueError as e:
            if "S3 bucket is required" in str(e):
                print("✅ SUCCESS: Missing S3 bucket rejected correctly")
            else:
                print(f"❌ FAILURE: Wrong error for missing bucket: {e}")
                return False
        
        print("✅ ALL initialization tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Initialization test failed: {e}")
        return False


def test_stream_to_s3_mode():
    """Test stream to S3 download mode."""
    print("\n🧪 Testing stream to S3 mode...")
    
    try:
        from download_integration import DownloadIntegration, DownloadMode
        from database_schema import VideoRecord
        
        # Mock configuration
        mock_config = MagicMock()
        mock_config.get.side_effect = lambda key, default=None: {
            "mass_download.download_mode": "stream_to_s3",
            "mass_download.download_resolution": "720",
            "mass_download.download_format": "mp4",
            "mass_download.download_subtitles": True,
            "s3.default_bucket": "test-bucket",
            "mass_download.s3_prefix": "mass-download"
        }.get(key, default)
        
        # Create test video record
        video_record = VideoRecord(
            person_id=1,
            video_id="dQw4w9WgXcQ",
            title="Test Video",
            uuid=str(uuid.uuid4()),
            duration=180,
            upload_date=datetime.now()
        )
        
        # Mock S3 manager
        with patch('download_integration.UnifiedS3Manager') as mock_s3_class:
            mock_s3_manager = MagicMock()
            mock_s3_class.return_value = mock_s3_manager
            
            # Mock successful streaming
            mock_s3_manager.generate_uuid_s3_key.return_value = "mass-download/uuid/video.mp4"
            mock_s3_manager.stream_youtube_to_s3.return_value = {
                "success": True,
                "file_size": 10485760  # 10MB
            }
            
            integration = DownloadIntegration(config=mock_config)
            result = integration.download_video(video_record)
            
            # Verify result
            assert result.status == "completed"
            assert result.s3_path == "mass-download/uuid/video.mp4"
            assert result.file_size == 10485760
            assert result.download_mode == DownloadMode.STREAM_TO_S3
            
            # Verify S3 manager was called correctly
            mock_s3_manager.stream_youtube_to_s3.assert_called_once()
            call_args = mock_s3_manager.stream_youtube_to_s3.call_args[1]
            assert call_args["youtube_url"] == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
            assert call_args["s3_bucket"] == "test-bucket"
            assert call_args["resolution"] == "720"
            
            print("✅ SUCCESS: Stream to S3 mode working correctly")
        
        # Test streaming failure
        with patch('download_integration.UnifiedS3Manager') as mock_s3_class:
            mock_s3_manager = MagicMock()
            mock_s3_class.return_value = mock_s3_manager
            
            # Mock failed streaming
            mock_s3_manager.generate_uuid_s3_key.return_value = "mass-download/uuid/video.mp4"
            mock_s3_manager.stream_youtube_to_s3.return_value = {
                "success": False,
                "error": "Network error"
            }
            
            integration = DownloadIntegration(config=mock_config)
            result = integration.download_video(video_record)
            
            # Verify failure result
            assert result.status == "failed"
            assert "Network error" in result.error_message
            assert result.s3_path is None
            
            print("✅ SUCCESS: Stream failure handled correctly")
        
        print("✅ ALL stream to S3 tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Stream to S3 test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_local_then_upload_mode():
    """Test local download then upload mode."""
    print("\n🧪 Testing local then upload mode...")
    
    try:
        from download_integration import DownloadIntegration, DownloadMode
        from database_schema import VideoRecord
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp()
        
        # Mock configuration
        mock_config = MagicMock()
        mock_config.get.side_effect = lambda key, default=None: {
            "mass_download.download_mode": "local_then_upload",
            "mass_download.local_download_dir": temp_dir,
            "mass_download.delete_after_upload": True,
            "mass_download.download_resolution": "720",
            "mass_download.download_format": "mp4",
            "mass_download.download_subtitles": True,
            "s3.default_bucket": "test-bucket",
            "mass_download.s3_prefix": "mass-download"
        }.get(key, default)
        
        # Create test video record
        video_record = VideoRecord(
            person_id=1,
            video_id="M7lc1UVf-VE",
            title="Test Video 2",
            uuid=str(uuid.uuid4()),
            duration=240
        )
        
        # Create mock file
        mock_video_path = Path(temp_dir) / "1" / "M7lc1UVf-VE.mp4"
        mock_video_path.parent.mkdir(parents=True, exist_ok=True)
        mock_video_path.write_text("mock video content")
        
        # Mock download and upload
        with patch('download_integration.download_single_video') as mock_download:
            with patch('download_integration.UnifiedS3Manager') as mock_s3_class:
                mock_s3_manager = MagicMock()
                mock_s3_class.return_value = mock_s3_manager
                
                # Mock successful download
                mock_download.return_value = {
                    "success": True,
                    "video_path": str(mock_video_path)
                }
                
                # Mock successful upload
                mock_s3_manager.generate_uuid_s3_key.return_value = "mass-download/uuid/video.mp4"
                mock_s3_manager.upload_file_to_s3.return_value = {
                    "success": True
                }
                
                integration = DownloadIntegration(config=mock_config)
                result = integration.download_video(video_record)
                
                # Verify result
                assert result.status == "completed"
                assert result.s3_path == "mass-download/uuid/video.mp4"
                assert result.file_size > 0
                assert result.download_mode == DownloadMode.LOCAL_THEN_UPLOAD
                
                # Verify file was deleted after upload
                assert not mock_video_path.exists()
                
                print("✅ SUCCESS: Local then upload mode working correctly")
        
        # Clean up
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
        
        print("✅ ALL local then upload tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Local then upload test failed: {e}")
        return False


def test_error_handling():
    """Test error handling scenarios."""
    print("\n🧪 Testing error handling...")
    
    try:
        from download_integration import DownloadIntegration, DownloadMode
        from database_schema import VideoRecord
        
        # Mock configuration for local only mode
        mock_config = MagicMock()
        mock_config.get.side_effect = lambda key, default=None: {
            "mass_download.download_mode": "local_only",
            "mass_download.local_download_dir": tempfile.gettempdir(),
            "mass_download.download_resolution": "720",
            "mass_download.download_format": "mp4",
            "mass_download.download_subtitles": True
        }.get(key, default)
        
        # Create test video record
        video_record = VideoRecord(
            person_id=1,
            video_id="9bZkp7q19f0",
            title="Test Video 3",
            uuid=str(uuid.uuid4())
        )
        
        # Test Case 1: Download failure
        with patch('download_integration.download_single_video') as mock_download:
            with patch('download_integration.UnifiedS3Manager'):
                # Mock download failure
                mock_download.return_value = {
                    "success": False,
                    "error": "Video is private"
                }
                
                integration = DownloadIntegration(config=mock_config)
                result = integration.download_video(video_record)
                
                assert result.status == "failed"
                assert "Video is private" in result.error_message
                
                print("✅ SUCCESS: Download failure handled correctly")
        
        # Test Case 2: File not found after download
        with patch('download_integration.download_single_video') as mock_download:
            with patch('download_integration.UnifiedS3Manager'):
                # Mock download claims success but file doesn't exist
                mock_download.return_value = {
                    "success": True,
                    "video_path": "/nonexistent/path.mp4"
                }
                
                integration = DownloadIntegration(config=mock_config)
                result = integration.download_video(video_record)
                
                assert result.status == "failed"
                assert "not found" in result.error_message
                
                print("✅ SUCCESS: Missing file handled correctly")
        
        # Test Case 3: Exception during download
        with patch('download_integration.download_single_video') as mock_download:
            with patch('download_integration.UnifiedS3Manager'):
                # Mock exception
                mock_download.side_effect = Exception("Network timeout")
                
                integration = DownloadIntegration(config=mock_config)
                result = integration.download_video(video_record)
                
                assert result.status == "failed"
                assert "Network timeout" in result.error_message
                
                print("✅ SUCCESS: Exception handled correctly")
        
        print("✅ ALL error handling tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Error handling test failed: {e}")
        return False


def test_batch_download():
    """Test batch download functionality."""
    print("\n🧪 Testing batch download...")
    
    try:
        from download_integration import DownloadIntegration, DownloadMode
        from database_schema import VideoRecord
        
        # Mock configuration
        mock_config = MagicMock()
        mock_config.get.side_effect = lambda key, default=None: {
            "mass_download.download_mode": "local_only",
            "mass_download.local_download_dir": tempfile.gettempdir(),
            "mass_download.download_resolution": "720",
            "mass_download.download_format": "mp4",
            "mass_download.download_subtitles": False
        }.get(key, default)
        
        # Create test video records
        video_records = [
            VideoRecord(
                person_id=1,
                video_id=f"vid{i:08d}",
                title=f"Test Video {i+1}",
                uuid=str(uuid.uuid4())
            )
            for i in range(3)
        ]
        
        # Mock downloads
        with patch('download_integration.download_single_video') as mock_download:
            with patch('download_integration.UnifiedS3Manager'):
                # Mock different results
                mock_download.side_effect = [
                    {"success": True, "video_path": f"/tmp/{i}/video.mp4"}
                    if i < 2 else {"success": False, "error": "Failed"}
                    for i in range(3)
                ]
                
                # Create mock files for successful downloads
                for i in range(2):
                    mock_path = Path(f"/tmp/{i}/video.mp4")
                    mock_path.parent.mkdir(parents=True, exist_ok=True)
                    mock_path.write_text("mock content")
                
                integration = DownloadIntegration(config=mock_config)
                results = integration.batch_download(video_records)
                
                # Verify results
                assert len(results) == 3
                assert sum(1 for r in results if r.status == "completed") == 2
                assert sum(1 for r in results if r.status == "failed") == 1
                
                # Verify video records were updated
                assert video_records[0].download_status == "completed"
                assert video_records[1].download_status == "completed"
                assert video_records[2].download_status == "failed"
                
                # Get statistics
                stats = integration.get_download_stats(results)
                assert stats["total_videos"] == 3
                assert stats["completed"] == 2
                assert stats["failed"] == 1
                assert stats["success_rate"] == pytest.approx(66.67, 0.1)
                
                print("✅ SUCCESS: Batch download working correctly")
                print(f"   Completed: {stats['completed']}/{stats['total_videos']}")
                print(f"   Success rate: {stats['success_rate']:.1f}%")
                
                # Clean up
                for i in range(2):
                    mock_path = Path(f"/tmp/{i}/video.mp4")
                    if mock_path.exists():
                        mock_path.unlink()
                    if mock_path.parent.exists():
                        mock_path.parent.rmdir()
        
        print("✅ ALL batch download tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Batch download test failed: {e}")
        return False


def main():
    """Run comprehensive download integration test suite."""
    print("🚀 Starting Download Integration Test Suite")
    print("   Testing Phase 4.5: Download integration with error handling")
    print("   Validating all download modes and error scenarios")
    print("=" * 80)
    
    # Import pytest for approximation
    global pytest
    try:
        import pytest
    except ImportError:
        # Simple approximation function if pytest not available
        class Approx:
            def __init__(self, value, tolerance):
                self.value = value
                self.tolerance = tolerance
            def __eq__(self, other):
                return abs(other - self.value) <= self.tolerance
        pytest = type('pytest', (), {'approx': lambda v, t: Approx(v, t)})()
    
    all_tests_passed = True
    test_functions = [
        test_imports,
        test_download_result_validation,
        test_download_integration_initialization,
        test_stream_to_s3_mode,
        test_local_then_upload_mode,
        test_error_handling,
        test_batch_download
    ]
    
    for test_func in test_functions:
        if not test_func():
            all_tests_passed = False
            print(f"❌ {test_func.__name__} FAILED")
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL DOWNLOAD INTEGRATION TESTS PASSED!")
        print("✅ Module imports working")
        print("✅ Data validation implemented")
        print("✅ Stream to S3 mode functional")
        print("✅ Local then upload mode working")
        print("✅ Error handling comprehensive")
        print("✅ Batch download validated")
        print("\n🔥 Download integration is PRODUCTION-READY!")
        return 0
    else:
        print("💥 SOME DOWNLOAD INTEGRATION TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)