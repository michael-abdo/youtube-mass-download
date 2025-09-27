#!/usr/bin/env python3
"""
Integration Tests for Mass Download Feature
Phase 6.5: Create integration tests for end-to-end flow

This module contains integration tests that verify the complete workflow
from input parsing through channel discovery, video processing, and
database updates.
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

# Import all modules
from mass_download.mass_coordinator import MassDownloadCoordinator
from mass_download.input_handler import InputHandler
from mass_download.channel_discovery import YouTubeChannelDiscovery
from mass_download.database_operations_ext import MassDownloadDatabaseOperations
from mass_download.progress_monitor import ProgressMonitor
from mass_download.concurrent_processor import ConcurrentProcessor
from mass_download.error_recovery import ErrorRecoveryManager


class TestMassDownloadIntegration(unittest.TestCase):
    """Test complete mass download workflow."""
    
    def setUp(self):
        """Set up test environment."""
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.test_db = Path(self.temp_dir) / "test.db"
        
        # Create test input files
        self._create_test_input_files()
        
        # Initialize configuration
        self.config = self._create_test_config()
    
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def _create_test_input_files(self):
        """Create various test input files."""
        # CSV file
        self.csv_file = Path(self.temp_dir) / "channels.csv"
        with open(self.csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["channel_url", "channel_name", "person_name"])
            writer.writerow(["https://youtube.com/@testchannel1", "Test Channel 1", "Test Person 1"])
            writer.writerow(["https://youtube.com/@testchannel2", "Test Channel 2", "Test Person 2"])
        
        # JSON file
        self.json_file = Path(self.temp_dir) / "channels.json"
        with open(self.json_file, 'w') as f:
            json.dump({
                "channels": [
                    {
                        "channel_url": "https://youtube.com/@testchannel3",
                        "name": "Test Channel 3",
                        "person_name": "Test Person 3"
                    }
                ]
            }, f)
        
        # Text file
        self.txt_file = Path(self.temp_dir) / "channels.txt"
        with open(self.txt_file, 'w') as f:
            f.write("https://youtube.com/@testchannel4\n")
            f.write("https://youtube.com/@testchannel5\n")
    
    def _create_test_config(self):
        """Create test configuration."""
        return {
            'mass_download': {
                'max_concurrent_channels': 2,
                'max_concurrent_downloads': 3,
                'max_videos_per_channel': 10,
                'skip_existing_videos': True,
                'continue_on_error': True,
                'download_videos': False,  # Don't actually download in tests
                'resource_limits': {
                    'max_cpu_percent': 80.0,
                    'max_memory_percent': 80.0
                },
                'error_recovery': {
                    'circuit_breaker': {
                        'failure_threshold': 3,
                        'recovery_timeout': 30,
                        'half_open_requests': 1
                    },
                    'retry': {
                        'max_retries': 2,
                        'initial_delay': 1.0,
                        'max_delay': 10.0,
                        'exponential_base': 2.0
                    }
                }
            },
            'bucket_name': 'test-bucket'
        }
    
    @patch('mass_download.channel_discovery.subprocess.run')
    def test_csv_input_workflow(self, mock_subprocess):
        """Test complete workflow with CSV input."""
        print("\n=== Testing CSV Input Workflow ===")
        
        # Mock yt-dlp
        self._mock_yt_dlp_validation(mock_subprocess)
        
        # Create coordinator
        coordinator = MassDownloadCoordinator(
            job_id="test_csv_job",
            config=self.config,
            database_path=str(self.test_db)
        )
        
        # Mock channel discovery
        with patch.object(coordinator.channel_discovery, 'extract_channel_info') as mock_extract:
            with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos') as mock_enumerate:
                # Mock channel info
                mock_extract.side_effect = [
                    Mock(
                        channel_id="UCtest1",
                        channel_url="https://youtube.com/channel/UCtest1",
                        title="Test Channel 1",
                        subscriber_count=1000
                    ),
                    Mock(
                        channel_id="UCtest2",
                        channel_url="https://youtube.com/channel/UCtest2",
                        title="Test Channel 2",
                        subscriber_count=2000
                    )
                ]
                
                # Mock video enumeration
                mock_enumerate.side_effect = [
                    [Mock(video_id=f"video1_{i}", title=f"Video {i}", duration=100+i*10)
                     for i in range(3)],
                    [Mock(video_id=f"video2_{i}", title=f"Video {i}", duration=200+i*10)
                     for i in range(2)]
                ]
                
                # Process file
                result = coordinator.process_file(str(self.csv_file))
                
                # Verify results
                self.assertTrue(result)
                self.assertEqual(mock_extract.call_count, 2)
                self.assertEqual(mock_enumerate.call_count, 2)
                
                # Check database
                db_ops = MassDownloadDatabaseOperations(str(self.test_db))
                stats = db_ops.get_download_statistics()
                self.assertEqual(stats['total_persons'], 2)
                self.assertEqual(stats['total_videos'], 5)
        
        print("✓ CSV input workflow test passed")
    
    @patch('mass_download.channel_discovery.subprocess.run')
    def test_json_input_workflow(self, mock_subprocess):
        """Test complete workflow with JSON input."""
        print("\n=== Testing JSON Input Workflow ===")
        
        # Mock yt-dlp
        self._mock_yt_dlp_validation(mock_subprocess)
        
        # Create coordinator
        coordinator = MassDownloadCoordinator(
            job_id="test_json_job",
            config=self.config,
            database_path=str(self.test_db)
        )
        
        # Mock channel discovery
        with patch.object(coordinator.channel_discovery, 'extract_channel_info') as mock_extract:
            with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos') as mock_enumerate:
                # Mock channel info
                mock_extract.return_value = Mock(
                    channel_id="UCtest3",
                    channel_url="https://youtube.com/channel/UCtest3",
                    title="Test Channel 3",
                    subscriber_count=3000
                )
                
                # Mock video enumeration
                mock_enumerate.return_value = [
                    Mock(video_id=f"video3_{i}", title=f"Video {i}", duration=300+i*10)
                    for i in range(4)
                ]
                
                # Process file
                result = coordinator.process_file(str(self.json_file))
                
                # Verify results
                self.assertTrue(result)
                self.assertEqual(mock_extract.call_count, 1)
                self.assertEqual(mock_enumerate.call_count, 1)
        
        print("✓ JSON input workflow test passed")
    
    @patch('mass_download.channel_discovery.subprocess.run')
    def test_error_recovery_workflow(self, mock_subprocess):
        """Test workflow with errors and recovery."""
        print("\n=== Testing Error Recovery Workflow ===")
        
        # Mock yt-dlp
        self._mock_yt_dlp_validation(mock_subprocess)
        
        # Create coordinator
        coordinator = MassDownloadCoordinator(
            job_id="test_error_job",
            config=self.config,
            database_path=str(self.test_db)
        )
        
        # Mock channel discovery with errors
        with patch.object(coordinator.channel_discovery, 'extract_channel_info') as mock_extract:
            with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos') as mock_enumerate:
                # First channel succeeds, second fails, third succeeds
                mock_extract.side_effect = [
                    Mock(
                        channel_id="UCtest1",
                        channel_url="https://youtube.com/channel/UCtest1",
                        title="Success Channel 1",
                        subscriber_count=1000
                    ),
                    Exception("Channel is private"),
                    Mock(
                        channel_id="UCtest3",
                        channel_url="https://youtube.com/channel/UCtest3",
                        title="Success Channel 3",
                        subscriber_count=3000
                    )
                ]
                
                # Mock video enumeration
                mock_enumerate.side_effect = [
                    [Mock(video_id=f"video1_{i}", title=f"Video {i}", duration=100)
                     for i in range(2)],
                    [Mock(video_id=f"video3_{i}", title=f"Video {i}", duration=300)
                     for i in range(3)]
                ]
                
                # Create test file with 3 channels
                error_csv = Path(self.temp_dir) / "error_test.csv"
                with open(error_csv, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["channel_url", "channel_name"])
                    writer.writerow(["https://youtube.com/@success1", "Success 1"])
                    writer.writerow(["https://youtube.com/@error", "Error Channel"])
                    writer.writerow(["https://youtube.com/@success3", "Success 3"])
                
                # Process file
                result = coordinator.process_file(str(error_csv))
                
                # Should still succeed with continue_on_error=True
                self.assertTrue(result)
                self.assertEqual(mock_extract.call_count, 3)
                self.assertEqual(mock_enumerate.call_count, 2)  # Only for successful channels
        
        print("✓ Error recovery workflow test passed")
    
    @patch('mass_download.channel_discovery.subprocess.run')
    def test_concurrent_processing(self, mock_subprocess):
        """Test concurrent channel processing."""
        print("\n=== Testing Concurrent Processing ===")
        
        # Mock yt-dlp
        self._mock_yt_dlp_validation(mock_subprocess)
        
        # Create coordinator with concurrent processing
        self.config['mass_download']['max_concurrent_channels'] = 3
        coordinator = MassDownloadCoordinator(
            job_id="test_concurrent_job",
            config=self.config,
            database_path=str(self.test_db)
        )
        
        # Track processing order
        processing_order = []
        
        def mock_extract(url):
            processing_order.append(url)
            channel_num = url.split('@')[-1][-1]
            return Mock(
                channel_id=f"UCtest{channel_num}",
                channel_url=f"https://youtube.com/channel/UCtest{channel_num}",
                title=f"Test Channel {channel_num}",
                subscriber_count=int(channel_num) * 1000
            )
        
        # Mock channel discovery
        with patch.object(coordinator.channel_discovery, 'extract_channel_info', side_effect=mock_extract):
            with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos') as mock_enumerate:
                mock_enumerate.return_value = []  # No videos for simplicity
                
                # Process text file with multiple channels
                result = coordinator.process_file(str(self.txt_file))
                
                # Verify results
                self.assertTrue(result)
                self.assertEqual(len(processing_order), 2)  # Should process both channels
        
        print("✓ Concurrent processing test passed")
    
    @patch('mass_download.channel_discovery.subprocess.run')
    def test_progress_tracking(self, mock_subprocess):
        """Test progress tracking throughout workflow."""
        print("\n=== Testing Progress Tracking ===")
        
        # Mock yt-dlp
        self._mock_yt_dlp_validation(mock_subprocess)
        
        # Create coordinator
        coordinator = MassDownloadCoordinator(
            job_id="test_progress_job",
            config=self.config,
            database_path=str(self.test_db)
        )
        
        # Track progress updates
        progress_updates = []
        
        def progress_callback(metrics):
            progress_updates.append({
                'channels_processed': metrics.channels_processed,
                'total_channels': metrics.total_channels,
                'videos_processed': metrics.videos_processed
            })
        
        # Add progress callback
        coordinator.progress_monitor.add_callback(progress_callback)
        
        # Mock channel discovery
        with patch.object(coordinator.channel_discovery, 'extract_channel_info') as mock_extract:
            with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos') as mock_enumerate:
                mock_extract.return_value = Mock(
                    channel_id="UCtest1",
                    channel_url="https://youtube.com/channel/UCtest1",
                    title="Test Channel",
                    subscriber_count=1000
                )
                mock_enumerate.return_value = [
                    Mock(video_id=f"video_{i}", title=f"Video {i}", duration=100)
                    for i in range(5)
                ]
                
                # Process single channel file
                single_channel = Path(self.temp_dir) / "single.txt"
                with open(single_channel, 'w') as f:
                    f.write("https://youtube.com/@testchannel\n")
                
                result = coordinator.process_file(str(single_channel))
                
                # Verify progress was tracked
                self.assertTrue(result)
                self.assertGreater(len(progress_updates), 0)
                
                # Check final progress
                db_ops = MassDownloadDatabaseOperations(str(self.test_db))
                progress = db_ops.get_progress_by_job_id("test_progress_job")
                self.assertIsNotNone(progress)
                self.assertEqual(progress['channels_processed'], 1)
                self.assertEqual(progress['total_channels'], 1)
        
        print("✓ Progress tracking test passed")
    
    def _mock_yt_dlp_validation(self, mock_subprocess):
        """Mock yt-dlp validation."""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "2025.08.22"
        mock_subprocess.return_value = mock_result


class TestIntegrationWithMocks(unittest.TestCase):
    """Test integration with comprehensive mocking."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_db = Path(self.temp_dir) / "test.db"
    
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('mass_download.mass_coordinator.get_s3_bucket')
    @patch('mass_download.mass_coordinator.DatabaseManager')
    @patch('mass_download.channel_discovery.subprocess.run')
    def test_full_workflow_with_s3(self, mock_subprocess, mock_db_manager, mock_s3):
        """Test complete workflow including S3 operations."""
        print("\n=== Testing Full Workflow with S3 ===")
        
        # Mock yt-dlp validation
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "2025.08.22"
        mock_subprocess.return_value = mock_result
        
        # Mock S3 bucket
        mock_s3.return_value = "test-bucket"
        
        # Mock database manager
        mock_db = Mock()
        mock_db_manager.return_value = mock_db
        
        # Create test configuration
        config = {
            'mass_download': {
                'max_concurrent_channels': 1,
                'max_concurrent_downloads': 1,
                'max_videos_per_channel': 2,
                'skip_existing_videos': True,
                'continue_on_error': True,
                'download_videos': True,
                'download_mode': 'stream_to_s3',
                'resource_limits': {
                    'max_cpu_percent': 80.0,
                    'max_memory_percent': 80.0
                }
            },
            'bucket_name': 'test-bucket'
        }
        
        # Create test input
        test_csv = Path(self.temp_dir) / "test.csv"
        with open(test_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["channel_url", "channel_name"])
            writer.writerow(["https://youtube.com/@testchannel", "Test Channel"])
        
        # Create coordinator
        coordinator = MassDownloadCoordinator(
            job_id="test_s3_job",
            config=config,
            database_path=str(self.test_db)
        )
        
        # Mock video download
        with patch.object(coordinator, '_download_video') as mock_download:
            mock_download.return_value = {
                'success': True,
                's3_path': 's3://test-bucket/videos/test_video.mp4',
                'duration': 100,
                'size_bytes': 1024 * 1024 * 10  # 10MB
            }
            
            # Mock channel discovery
            with patch.object(coordinator.channel_discovery, 'extract_channel_info') as mock_extract:
                with patch.object(coordinator.channel_discovery, 'enumerate_channel_videos') as mock_enumerate:
                    mock_extract.return_value = Mock(
                        channel_id="UCtest",
                        channel_url="https://youtube.com/channel/UCtest",
                        title="Test Channel",
                        subscriber_count=1000
                    )
                    mock_enumerate.return_value = [
                        Mock(
                            video_id="testvideo01",
                            title="Test Video 1",
                            duration=100,
                            view_count=1000,
                            upload_date=datetime.now()
                        ),
                        Mock(
                            video_id="testvideo02",
                            title="Test Video 2",
                            duration=200,
                            view_count=2000,
                            upload_date=datetime.now()
                        )
                    ]
                    
                    # Process file
                    result = coordinator.process_file(str(test_csv))
                    
                    # Verify results
                    self.assertTrue(result)
                    self.assertEqual(mock_download.call_count, 2)  # Should download 2 videos
        
        print("✓ Full workflow with S3 test passed")


def run_integration_tests():
    """Run all integration tests."""
    print("\n" + "=" * 70)
    print("Mass Download Integration Tests")
    print("=" * 70)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestMassDownloadIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegrationWithMocks))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 70)
    if result.wasSuccessful():
        print("✓ All integration tests passed!")
        print(f"  Ran {result.testsRun} tests successfully")
    else:
        print(f"✗ {len(result.failures)} failures, {len(result.errors)} errors")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_integration_tests()
    sys.exit(0 if success else 1)