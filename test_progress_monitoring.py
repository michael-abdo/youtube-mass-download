#!/usr/bin/env python3
"""
Test Progress Monitoring with Real Processing
Phase 5.8: Test progress monitoring with real processing

This script tests the progress monitoring system with simulated channel processing
to verify that progress updates, ETA calculations, and reporting work correctly.
"""
import sys
import time
import threading
from pathlib import Path
from datetime import datetime

# Add paths for imports
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "mass_download"))

# Initialize logging
from init_logging import initialize_mass_download_logging
initialize_mass_download_logging(debug=False)

# Import modules
from mass_download.progress_monitor import (
    ProgressMonitor, ProgressReporter, ProgressMetrics
)


def simulate_channel_processing(monitor: ProgressMonitor, channel_url: str, 
                              channel_name: str, num_videos: int):
    """Simulate processing a channel with videos."""
    # Start channel
    monitor.start_channel(channel_url, channel_name)
    monitor.update_channel_videos(channel_url, num_videos)
    
    # Simulate video processing
    for i in range(num_videos):
        video_id = f"video_{channel_name.replace(' ', '_')}_{i}"
        video_title = f"{channel_name} - Video {i + 1}"
        
        # Simulate processing time
        time.sleep(0.2)
        
        # Simulate different outcomes
        if i % 10 == 0 and i > 0:
            # Every 10th video fails
            monitor.update_video_progress(video_id, video_title, 
                                        downloaded=False, failed=True)
        elif i % 5 == 0 and i > 0:
            # Every 5th video is skipped
            monitor.update_video_progress(video_id, video_title, 
                                        downloaded=False, failed=False)
        else:
            # Most videos download successfully
            monitor.update_video_progress(video_id, video_title, 
                                        downloaded=True, failed=False)
            # Simulate download size
            monitor.update_download_stats(1024 * 1024 * 50)  # 50MB per video
    
    # Complete channel
    monitor.complete_channel(channel_url, success=True)


def test_basic_progress_monitoring():
    """Test basic progress monitoring functionality."""
    print("\n=== TEST 1: Basic Progress Monitoring ===")
    
    monitor = ProgressMonitor(update_interval=0.5)
    monitor.start()
    
    # Set total channels
    monitor.update_channel_count(3)
    
    # Process 3 channels
    channels = [
        ("https://youtube.com/@channel1", "Tech Reviews", 15),
        ("https://youtube.com/@channel2", "Gaming Channel", 20),
        ("https://youtube.com/@channel3", "Music Videos", 10)
    ]
    
    for url, name, video_count in channels:
        simulate_channel_processing(monitor, url, name, video_count)
    
    # Stop and get report
    monitor.stop()
    
    # Print final report
    reporter = ProgressReporter(monitor)
    print("\nFinal Report:")
    print(reporter.generate_text_report())
    
    # Verify metrics
    metrics = monitor.get_current_metrics()
    assert metrics.channels_processed == 3
    assert metrics.total_channels == 3
    assert metrics.get_progress_percent() == 100.0
    
    print("\n✓ Basic progress monitoring test passed")


def test_concurrent_progress_monitoring():
    """Test progress monitoring with concurrent channel processing."""
    print("\n=== TEST 2: Concurrent Progress Monitoring ===")
    
    monitor = ProgressMonitor(update_interval=1.0)
    
    # Add progress callback
    def on_progress(metrics: ProgressMetrics):
        if metrics.channels_processed > 0:
            eta = metrics.get_eta()
            if eta:
                print(f"\nProgress: {metrics.get_progress_percent():.1f}% - ETA: {eta}")
    
    monitor.add_callback(on_progress)
    monitor.start()
    
    # Set total channels
    num_channels = 5
    monitor.update_channel_count(num_channels)
    
    # Process channels concurrently
    threads = []
    for i in range(num_channels):
        channel_url = f"https://youtube.com/@concurrent_channel{i}"
        channel_name = f"Concurrent Channel {i}"
        video_count = 10 + (i * 5)  # Variable video counts
        
        thread = threading.Thread(
            target=simulate_channel_processing,
            args=(monitor, channel_url, channel_name, video_count)
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all threads
    for thread in threads:
        thread.join()
    
    # Stop monitoring
    monitor.stop()
    
    # Get summary
    summary = monitor.get_summary_report()
    print(f"\nProcessing completed in {summary['performance']['elapsed_time']}")
    print(f"Total downloaded: {summary['performance']['total_downloaded_gb']:.2f} GB")
    print(f"Average speed: {summary['performance']['average_speed_mbps']:.2f} Mbps")
    
    print("\n✓ Concurrent progress monitoring test passed")


def test_progress_persistence():
    """Test progress saving and loading."""
    print("\n=== TEST 3: Progress Persistence ===")
    
    progress_file = Path("test_progress.json")
    
    # Create monitor and process some channels
    monitor1 = ProgressMonitor(progress_file=progress_file, persist_interval=1.0)
    monitor1.start()
    monitor1.update_channel_count(5)
    
    # Process 2 channels
    simulate_channel_processing(monitor1, "https://youtube.com/@persist1", "Persist Channel 1", 10)
    simulate_channel_processing(monitor1, "https://youtube.com/@persist2", "Persist Channel 2", 15)
    
    # Wait for persistence
    time.sleep(2)
    
    # Stop without finishing
    monitor1.pause()
    
    # Create new monitor and load progress
    monitor2 = ProgressMonitor(progress_file=progress_file)
    loaded = monitor2.load_progress()
    assert loaded, "Failed to load progress"
    
    metrics = monitor2.get_current_metrics()
    assert metrics.channels_processed == 2
    assert metrics.total_channels == 5
    print(f"Loaded progress: {metrics.channels_processed}/{metrics.total_channels} channels")
    
    # Clean up
    if progress_file.exists():
        progress_file.unlink()
    
    print("\n✓ Progress persistence test passed")


def test_error_handling():
    """Test progress monitoring with errors."""
    print("\n=== TEST 4: Error Handling ===")
    
    monitor = ProgressMonitor()
    monitor.start()
    monitor.update_channel_count(2)
    
    # Successful channel
    monitor.start_channel("https://youtube.com/@success", "Success Channel")
    monitor.update_channel_videos("https://youtube.com/@success", 5)
    for i in range(5):
        monitor.update_video_progress(f"video_{i}", f"Video {i}", downloaded=True)
        monitor.update_download_stats(1024 * 1024 * 10)
    monitor.complete_channel("https://youtube.com/@success", success=True)
    
    # Failed channel
    monitor.start_channel("https://youtube.com/@failed", "Failed Channel")
    monitor.complete_channel("https://youtube.com/@failed", success=False, 
                           error_message="Channel is private")
    
    monitor.stop()
    
    # Check metrics
    metrics = monitor.get_current_metrics()
    assert metrics.channels_processed == 2
    assert metrics.channels_failed == 1
    
    # Check channel progress
    assert "https://youtube.com/@success" in monitor.channel_progress
    assert monitor.channel_progress["https://youtube.com/@success"].status == "completed"
    assert monitor.channel_progress["https://youtube.com/@failed"].status == "failed"
    
    print("\n✓ Error handling test passed")


def test_real_time_updates():
    """Test real-time progress updates."""
    print("\n=== TEST 5: Real-Time Updates ===")
    
    monitor = ProgressMonitor(update_interval=0.5)
    
    # Track update count
    update_count = 0
    
    def count_updates(metrics: ProgressMetrics):
        nonlocal update_count
        update_count += 1
    
    monitor.add_callback(count_updates)
    monitor.start()
    
    # Process for 3 seconds
    monitor.update_channel_count(1)
    monitor.start_channel("https://youtube.com/@realtime", "Real-time Channel")
    monitor.update_channel_videos("https://youtube.com/@realtime", 30)
    
    start_time = time.time()
    for i in range(30):
        monitor.update_video_progress(f"rt_video_{i}", f"Video {i}", downloaded=True)
        monitor.update_download_stats(1024 * 1024 * 5)
        time.sleep(0.1)
    
    monitor.complete_channel("https://youtube.com/@realtime", success=True)
    monitor.stop()
    
    elapsed = time.time() - start_time
    print(f"\nProcessed 30 videos in {elapsed:.1f}s")
    print(f"Progress updates received: {update_count}")
    
    # Should have received multiple updates
    assert update_count >= 5, f"Expected at least 5 updates, got {update_count}"
    
    print("\n✓ Real-time updates test passed")


def main():
    """Run all progress monitoring tests."""
    print("Progress Monitoring Test Suite")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    
    try:
        # Run all tests
        test_basic_progress_monitoring()
        test_concurrent_progress_monitoring()
        test_progress_persistence()
        test_error_handling()
        test_real_time_updates()
        
        print("\n" + "=" * 80)
        print("All progress monitoring tests passed!")
        print(f"Finished at: {datetime.now()}")
        
        # Summary
        print("\nTest Summary:")
        print("- Basic progress monitoring: ✓")
        print("- Concurrent processing: ✓")
        print("- Progress persistence: ✓")
        print("- Error handling: ✓")
        print("- Real-time updates: ✓")
        
        return 0
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())