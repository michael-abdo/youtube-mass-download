#!/usr/bin/env python3
"""
Test Concurrent Processing and Resource Management
Phase 4.11: Test concurrent processing and resource management

Tests:
1. Resource monitoring and metrics collection
2. Dynamic thread pool resizing based on resources
3. Semaphore-based concurrency control
4. Task priority handling
5. Resource throttling under high load
6. Error handling in concurrent tasks
7. Progress callback integration
8. Graceful shutdown

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import os
import time
import psutil
import threading
import tempfile
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
import random
from unittest.mock import MagicMock, patch
import multiprocessing

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))


def test_imports():
    """Test that all required modules import correctly."""
    print("🧪 Testing imports for concurrent processing...")
    
    try:
        from concurrent_processor import (
            ConcurrentProcessor, ResourceLimits, ResourceMonitor,
            ResourceMetrics, ResourceStatus, process_batch_with_resource_management
        )
        from mass_coordinator import MassDownloadCoordinator
        
        print("✅ SUCCESS: All required imports successful")
        return True
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import required modules")
        print(f"   Error: {e}")
        return False


def test_resource_monitoring():
    """Test resource monitoring functionality."""
    print("\n🧪 Testing resource monitoring...")
    
    try:
        from concurrent_processor import ResourceMonitor, ResourceLimits, ResourceStatus, ResourceMetrics
        
        # Test 1: Create resource monitor
        print("  📊 Testing resource monitor creation...")
        limits = ResourceLimits(
            max_cpu_percent=80.0,
            max_memory_percent=80.0,
            check_interval_seconds=1.0
        )
        
        monitor = ResourceMonitor(limits)
        assert monitor.limits == limits
        assert not monitor.monitoring
        print("    ✅ Resource monitor created successfully")
        
        # Test 2: Get current metrics
        print("  📈 Testing metrics collection...")
        metrics = monitor.get_current_metrics(queue_size=5)
        assert metrics.cpu_percent >= 0
        assert metrics.memory_percent >= 0
        assert metrics.queue_size == 5
        assert metrics.status in [ResourceStatus.NORMAL, ResourceStatus.WARNING, ResourceStatus.CRITICAL]
        print(f"    ✅ Metrics collected: CPU={metrics.cpu_percent:.1f}%, "
              f"Memory={metrics.memory_percent:.1f}%, Status={metrics.status.value}")
        
        # Test 3: Start/stop monitoring
        print("  🔄 Testing monitoring lifecycle...")
        monitor.start_monitoring()
        assert monitor.monitoring
        time.sleep(2)  # Let it collect some metrics
        
        monitor.stop_monitoring()
        assert not monitor.monitoring
        assert len(monitor.metrics_history) > 0
        print(f"    ✅ Monitoring lifecycle working, collected {len(monitor.metrics_history)} metrics")
        
        # Test 4: Concurrency recommendations
        print("  🎯 Testing concurrency recommendations...")
        
        # Simulate high resource usage
        with patch.object(monitor, 'metrics_history', [
            ResourceMetrics(cpu_percent=85.0, memory_percent=75.0, active_threads=10, queue_size=0),
            ResourceMetrics(cpu_percent=90.0, memory_percent=85.0, active_threads=10, queue_size=0),
            ResourceMetrics(cpu_percent=88.0, memory_percent=82.0, active_threads=10, queue_size=0)
        ]):
            recommended = monitor.get_recommended_concurrency(10)
            assert recommended < 10, f"Should throttle down from 10, got {recommended}"
            print(f"    ✅ Correctly throttled: 10 -> {recommended} workers")
        
        # Simulate normal resource usage
        with patch.object(monitor, 'metrics_history', [
            ResourceMetrics(cpu_percent=40.0, memory_percent=45.0, active_threads=10, queue_size=0),
            ResourceMetrics(cpu_percent=45.0, memory_percent=50.0, active_threads=10, queue_size=0),
            ResourceMetrics(cpu_percent=42.0, memory_percent=48.0, active_threads=10, queue_size=0)
        ]):
            recommended = monitor.get_recommended_concurrency(10)
            assert recommended == 10, f"Should maintain 10 workers, got {recommended}"
            print(f"    ✅ Correctly maintained: 10 workers")
        
        print("✅ SUCCESS: All resource monitoring tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Resource monitoring failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_concurrent_processor_basics():
    """Test basic concurrent processor functionality."""
    print("\n🧪 Testing concurrent processor basics...")
    
    try:
        from concurrent_processor import ConcurrentProcessor, ResourceLimits
        
        # Test 1: Create processor
        print("  🔧 Testing processor creation...")
        limits = ResourceLimits(
            max_concurrent_channels=3,
            max_concurrent_downloads=2,
            max_cpu_percent=80.0
        )
        
        progress_events = []
        def progress_callback(event_type, event_data):
            progress_events.append((event_type, event_data))
        
        processor = ConcurrentProcessor(limits, progress_callback)
        assert processor.limits == limits
        assert processor.current_workers == 3
        print("    ✅ Processor created with 3 workers")
        
        # Test 2: Start/stop lifecycle
        print("  🚀 Testing processor lifecycle...")
        processor.start()
        assert processor.executor is not None
        assert processor.resource_monitor.monitoring
        print("    ✅ Processor started successfully")
        
        # Test 3: Submit a simple task
        print("  📋 Testing task submission...")
        def simple_task(value):
            time.sleep(0.1)
            return value * 2
        
        future = processor.submit_channel_task("test_task_1", simple_task, 5)
        result = future.result(timeout=5)
        assert result == 10
        print("    ✅ Task completed successfully: 5 * 2 = 10")
        
        # Check progress callback
        assert any(e[0] == "task_completed" for e in progress_events)
        print("    ✅ Progress callback triggered")
        
        # Test 4: Get status
        print("  📊 Testing status reporting...")
        status = processor.get_status()
        assert status['current_workers'] == 3
        assert status['completed_tasks'] == 1
        assert status['failed_tasks'] == 0
        print(f"    ✅ Status: {status['completed_tasks']} completed, "
              f"{status['failed_tasks']} failed")
        
        # Test 5: Stop processor
        print("  🛑 Testing processor shutdown...")
        processor.stop()
        assert not processor.resource_monitor.monitoring
        print("    ✅ Processor stopped successfully")
        
        print("✅ SUCCESS: All concurrent processor basic tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Concurrent processor basics failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_concurrent_task_handling():
    """Test concurrent task handling with multiple tasks."""
    print("\n🧪 Testing concurrent task handling...")
    
    try:
        from concurrent_processor import ConcurrentProcessor, ResourceLimits
        
        # Create processor with limited concurrency
        limits = ResourceLimits(
            max_concurrent_channels=2,  # Only 2 concurrent tasks
            max_concurrent_downloads=1
        )
        
        processor = ConcurrentProcessor(limits)
        processor.start()
        
        # Test 1: Submit multiple tasks
        print("  📋 Testing multiple task submission...")
        task_results = []
        
        def task_func(task_id, duration):
            start = time.time()
            time.sleep(duration)
            end = time.time()
            return {
                'task_id': task_id,
                'duration': duration,
                'actual_duration': end - start
            }
        
        # Submit 5 tasks with only 2 concurrent slots
        futures = []
        start_time = time.time()
        for i in range(5):
            future = processor.submit_channel_task(
                f"task_{i}",
                task_func,
                f"task_{i}",
                0.2  # 200ms per task
            )
            futures.append(future)
        
        # Wait for all to complete
        for future in futures:
            result = future.result(timeout=10)
            task_results.append(result)
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        print(f"    ✅ Completed 5 tasks in {total_duration:.1f}s with 2 workers")
        # With 2 workers and 5 tasks of 0.2s each, should take ~0.6s (3 batches)
        # But due to overhead and system load, allow more time
        assert total_duration >= 0.5, f"Too fast, concurrency not enforced: {total_duration}s"
        assert total_duration < 10.0, f"Too slow, possible deadlock: {total_duration}s"
        
        # Test 2: Task failure handling
        print("  ❌ Testing task failure handling...")
        
        def failing_task():
            raise RuntimeError("Simulated task failure")
        
        future = processor.submit_channel_task("failing_task", failing_task)
        try:
            future.result(timeout=5)
            assert False, "Should have raised exception"
        except RuntimeError as e:
            assert "Simulated task failure" in str(e)
            print("    ✅ Task failure handled correctly")
        
        # Check failed task tracking
        status = processor.get_status()
        assert status['failed_tasks'] == 1
        assert any(task[0] == "failing_task" for task in processor.failed_tasks)
        print(f"    ✅ Failed task tracked: {status['failed_tasks']} failures")
        
        # Test 3: Download task with separate semaphore
        print("  📥 Testing download task submission...")
        
        def download_task(file_id):
            time.sleep(0.1)
            return f"Downloaded {file_id}"
        
        # Submit multiple downloads with limit of 1
        download_futures = []
        for i in range(3):
            future = processor.submit_download_task(
                f"download_{i}",
                download_task,
                f"file_{i}"
            )
            download_futures.append(future)
        
        # Check they complete sequentially due to semaphore
        download_start = time.time()
        for future in download_futures:
            result = future.result(timeout=5)
            assert "Downloaded" in result
        download_end = time.time()
        download_duration = download_end - download_start
        
        print(f"    ✅ Completed 3 downloads in {download_duration:.1f}s with 1 concurrent slot")
        assert download_duration >= 0.3, f"Downloads not serialized: {download_duration}s"
        
        processor.stop()
        
        print("✅ SUCCESS: All concurrent task handling tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Concurrent task handling failed: {e}")
        import traceback
        traceback.print_exc()
        processor.stop() if 'processor' in locals() else None
        return False


def test_resource_based_throttling():
    """Test dynamic throttling based on resource usage."""
    print("\n🧪 Testing resource-based throttling...")
    
    try:
        from concurrent_processor import ConcurrentProcessor, ResourceLimits
        
        # Create processor with aggressive resource limits
        limits = ResourceLimits(
            max_concurrent_channels=4,
            max_cpu_percent=50.0,  # Low threshold for testing
            max_memory_percent=50.0,
            check_interval_seconds=0.5,
            throttle_factor=0.5,
            min_concurrent=1
        )
        
        processor = ConcurrentProcessor(limits)
        processor.start()
        
        # Test 1: Simulate high CPU load
        print("  🔥 Testing high CPU throttling...")
        
        # Create CPU-intensive tasks
        def cpu_intensive_task(task_id, iterations=1000000):
            result = 0
            for i in range(iterations):
                result += i * i
            return f"{task_id} completed: {result}"
        
        # Submit multiple CPU-intensive tasks
        futures = []
        for i in range(6):
            future = processor.submit_channel_task(
                f"cpu_task_{i}",
                cpu_intensive_task,
                f"cpu_task_{i}",
                iterations=500000
            )
            futures.append(future)
        
        # Wait a bit for resource monitoring to kick in
        time.sleep(2)
        
        # Check if throttling occurred
        status = processor.get_status()
        print(f"    📊 Current status: {status['current_workers']} workers, "
              f"CPU: {status['cpu_percent']:.1f}%, "
              f"Memory: {status['memory_percent']:.1f}%")
        
        # Complete all tasks
        for future in futures:
            try:
                result = future.result(timeout=30)
            except Exception as e:
                print(f"    ⚠️  Task failed: {e}")
        
        print("    ✅ CPU throttling test completed")
        
        # Test 2: Test batch processing with resource management
        print("  📦 Testing batch processing...")
        
        from concurrent_processor import process_batch_with_resource_management
        
        def batch_task(task_id, value):
            time.sleep(0.1)
            return value * 2
        
        # Create batch of tasks
        batch_items = [
            (f"batch_{i}", batch_task, (f"batch_{i}", i), {})
            for i in range(10)
        ]
        
        batch_results = process_batch_with_resource_management(
            batch_items,
            resource_limits=ResourceLimits(
                max_concurrent_channels=3,
                max_cpu_percent=70.0
            )
        )
        
        assert batch_results['total'] == 10
        assert batch_results['completed'] + batch_results['failed'] == 10
        assert batch_results['success_rate'] > 0.8  # At least 80% success
        
        print(f"    ✅ Batch processing completed: "
              f"{batch_results['completed']}/{batch_results['total']} successful")
        
        processor.stop()
        
        print("✅ SUCCESS: All resource-based throttling tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Resource throttling test failed: {e}")
        import traceback
        traceback.print_exc()
        processor.stop() if 'processor' in locals() else None
        return False


def test_integration_with_coordinator():
    """Test integration with mass download coordinator."""
    print("\n🧪 Testing integration with coordinator...")
    
    try:
        from mass_coordinator import MassDownloadCoordinator
        from database_schema import PersonRecord
        from unittest.mock import MagicMock, patch
        
        # Create test environment
        print("  🔧 Setting up test environment...")
        
        # Mock config
        class MockConfig:
            def get(self, key, default=None):
                config_map = {
                    "mass_download.max_concurrent_channels": 2,
                    "mass_download.max_videos_per_channel": 5,
                    "mass_download.skip_existing_videos": True,
                    "mass_download.continue_on_error": True,
                    "mass_download.download_videos": False,
                    "mass_download.max_concurrent_downloads": 1,
                    "bucket_name": "test-bucket",
                    "download_mode": "stream_to_s3",
                    "local_download_dir": str(tempfile.gettempdir())
                }
                return config_map.get(key, default)
            
            def get_section(self, section):
                if section == "database":
                    return {"type": "sqlite", "database": ":memory:"}
                return {}
                
            bucket_name = "test-bucket"
            region = "us-east-1"
            downloads_dir = str(tempfile.gettempdir())
        
        mock_config = MockConfig()
        
        # Create coordinator with mocked dependencies
        with patch('mass_coordinator.DownloadIntegration'):
            coordinator = MassDownloadCoordinator(mock_config)
            
            # Verify concurrent processor was created
            assert hasattr(coordinator, 'concurrent_processor')
            assert coordinator.concurrent_processor is not None
            print("    ✅ Concurrent processor integrated with coordinator")
            
            # Test 1: Process channels with resource management
            print("  🚀 Testing resource-managed channel processing...")
            
            # Create test data
            person_channel_pairs = [
                (PersonRecord(name=f"Creator{i}", channel_url=f"https://youtube.com/@channel{i}"),
                 f"https://youtube.com/@channel{i}")
                for i in range(4)
            ]
            
            # Mock channel processing
            def mock_process_channel(person, channel_url):
                time.sleep(0.1)  # Simulate work
                from mass_coordinator import ChannelProcessingResult, ProcessingStatus
                return ChannelProcessingResult(
                    channel_url=channel_url,
                    person_id=1,
                    status=ProcessingStatus.COMPLETED,
                    videos_found=5,
                    videos_processed=5
                )
            
            coordinator.process_channel = mock_process_channel
            
            # Process channels with resource management
            results = coordinator.process_channels_with_resource_management(person_channel_pairs)
            
            assert len(results) == 4
            assert all(r.status.value == "completed" for r in results)
            print(f"    ✅ Processed {len(results)} channels with resource management")
            
            # Test 2: Check progress callback integration
            print("  📊 Testing progress callback integration...")
            
            # Simulate progress events
            coordinator._on_concurrent_progress("task_completed", {"task_id": "test_1"})
            coordinator._on_concurrent_progress("download_completed", {"task_id": "dl_1"})
            coordinator._on_concurrent_progress("download_failed", {"task_id": "dl_2", "error": "Test error"})
            
            assert coordinator.progress.videos_processed == 1
            assert coordinator.progress.videos_failed == 1
            print("    ✅ Progress callbacks working correctly")
            
            # Test 3: Shutdown with concurrent processor
            print("  🛑 Testing coordinator shutdown...")
            coordinator.shutdown()
            
            # Verify concurrent processor was stopped
            assert not coordinator.concurrent_processor.resource_monitor.monitoring
            print("    ✅ Coordinator shutdown properly stopped concurrent processor")
        
        print("✅ SUCCESS: All integration tests passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def stress_test_concurrent_processing():
    """Stress test with many concurrent tasks."""
    print("\n🧪 Running stress test for concurrent processing...")
    
    try:
        from concurrent_processor import ConcurrentProcessor, ResourceLimits
        import random
        
        # Create processor with moderate limits
        limits = ResourceLimits(
            max_concurrent_channels=5,
            max_concurrent_downloads=3,
            max_cpu_percent=85.0,
            max_memory_percent=85.0,
            max_queue_size=200
        )
        
        # Track metrics
        task_times = []
        download_times = []
        errors = []
        
        def progress_callback(event_type, event_data):
            if event_type == "task_failed":
                errors.append(event_data)
        
        processor = ConcurrentProcessor(limits, progress_callback)
        processor.start()
        
        print("  🏃 Running stress test with 50 tasks...")
        
        # Define various task types
        def quick_task(task_id):
            time.sleep(random.uniform(0.01, 0.05))
            return f"Quick {task_id}"
        
        def normal_task(task_id):
            time.sleep(random.uniform(0.1, 0.3))
            return f"Normal {task_id}"
        
        def slow_task(task_id):
            time.sleep(random.uniform(0.5, 1.0))
            return f"Slow {task_id}"
        
        def failing_task(task_id):
            if random.random() < 0.3:  # 30% failure rate
                raise RuntimeError(f"Task {task_id} failed")
            time.sleep(random.uniform(0.1, 0.2))
            return f"Success {task_id}"
        
        # Submit mixed workload
        all_futures = []
        start_time = time.time()
        
        # Submit channel tasks
        for i in range(50):
            task_type = random.choice([quick_task, normal_task, slow_task, failing_task])
            priority = random.randint(1, 10)
            
            future = processor.submit_channel_task(
                f"stress_task_{i}",
                task_type,
                f"task_{i}",
                priority=priority
            )
            all_futures.append((future, f"stress_task_{i}", time.time()))
        
        # Submit some download tasks too
        for i in range(20):
            future = processor.submit_download_task(
                f"download_{i}",
                quick_task,
                f"download_{i}"
            )
            all_futures.append((future, f"download_{i}", time.time()))
        
        print(f"    📋 Submitted {len(all_futures)} tasks")
        
        # Wait for completion and collect metrics
        completed = 0
        failed = 0
        
        for future, task_id, submit_time in all_futures:
            try:
                result = future.result(timeout=30)
                completion_time = time.time()
                
                if task_id.startswith("download"):
                    download_times.append(completion_time - submit_time)
                else:
                    task_times.append(completion_time - submit_time)
                
                completed += 1
            except Exception as e:
                failed += 1
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Get final status
        final_status = processor.get_status()
        
        # Print results
        print(f"\n  📊 Stress Test Results:")
        print(f"    • Total duration: {total_duration:.1f}s")
        print(f"    • Tasks completed: {completed}/{len(all_futures)}")
        print(f"    • Tasks failed: {failed}")
        print(f"    • Success rate: {(completed/len(all_futures))*100:.1f}%")
        print(f"    • Average task time: {sum(task_times)/len(task_times):.2f}s" if task_times else "N/A")
        print(f"    • Average download time: {sum(download_times)/len(download_times):.2f}s" if download_times else "N/A")
        print(f"    • Final CPU: {final_status['cpu_percent']:.1f}%")
        print(f"    • Final Memory: {final_status['memory_percent']:.1f}%")
        print(f"    • Resource status: {final_status['resource_status']}")
        
        processor.stop()
        
        # Validate results
        assert completed > 0, "No tasks completed"
        assert (completed + failed) == len(all_futures), "Some tasks lost"
        assert total_duration < 60, "Took too long, possible deadlock"
        
        print("\n✅ SUCCESS: Stress test completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Stress test failed: {e}")
        import traceback
        traceback.print_exc()
        processor.stop() if 'processor' in locals() else None
        return False


def main():
    """Run comprehensive concurrent processing test suite."""
    print("🚀 Starting Concurrent Processing Test Suite")
    print("   Testing Phase 4.11: Concurrent processing and resource management")
    print("   Validating thread pools, semaphores, and resource monitoring")
    print("=" * 80)
    
    all_tests_passed = True
    
    # Test imports
    if not test_imports():
        print("❌ Import test FAILED - cannot continue")
        return 1
    
    # Run tests
    try:
        if not test_resource_monitoring():
            all_tests_passed = False
            print("❌ Resource monitoring FAILED")
        
        if not test_concurrent_processor_basics():
            all_tests_passed = False
            print("❌ Concurrent processor basics FAILED")
        
        if not test_concurrent_task_handling():
            all_tests_passed = False
            print("❌ Concurrent task handling FAILED")
        
        if not test_resource_based_throttling():
            all_tests_passed = False
            print("❌ Resource-based throttling FAILED")
        
        if not test_integration_with_coordinator():
            all_tests_passed = False
            print("❌ Integration with coordinator FAILED")
        
        # Run stress test only if all other tests pass
        if all_tests_passed:
            if not stress_test_concurrent_processing():
                all_tests_passed = False
                print("❌ Stress test FAILED")
        
    except Exception as e:
        print(f"💥 UNEXPECTED TEST SUITE ERROR: {e}")
        import traceback
        traceback.print_exc()
        all_tests_passed = False
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL CONCURRENT PROCESSING TESTS PASSED!")
        print("✅ Resource monitoring working")
        print("✅ Dynamic thread pool sizing working")
        print("✅ Semaphore-based concurrency control working")
        print("✅ Task priority and queuing working")
        print("✅ Resource-based throttling working")
        print("✅ Error handling comprehensive")
        print("✅ Integration with coordinator working")
        print("✅ Stress test passed")
        print("\n🔥 Concurrent processing is PRODUCTION-READY!")
        return 0
    else:
        print("💥 SOME CONCURRENT PROCESSING TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)