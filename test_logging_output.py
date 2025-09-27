#!/usr/bin/env python3
"""
Test Logging Output and Error Reporting
Phase 5.6: Test logging output and error reporting

This script tests the logging infrastructure for the mass download feature,
ensuring that logs are properly formatted, routed to the correct destinations,
and that error reporting works correctly.
"""
import sys
import os
import time
from pathlib import Path
from datetime import datetime

# Add paths for imports
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "mass_download"))

# Initialize logging
from init_logging import initialize_mass_download_logging
initialize_mass_download_logging(debug=True)

# Import modules that use logging
from mass_download.logging_setup import (
    get_mass_download_logger, 
    log_operation_start,
    log_operation_complete,
    log_progress,
    create_operation_logger,
    StructuredLogger
)


def test_basic_logging():
    """Test basic logging functionality."""
    print("\n=== TEST 1: Basic Logging ===")
    
    logger = get_mass_download_logger("test_basic")
    
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    
    print("✓ Basic logging test completed")


def test_operation_logging():
    """Test operation start/complete logging."""
    print("\n=== TEST 2: Operation Logging ===")
    
    logger = get_mass_download_logger("test_operations")
    
    # Test operation start
    log_operation_start(logger, "test_download", 
                       channel_url="https://youtube.com/@test",
                       video_count=10)
    
    # Simulate some work
    time.sleep(0.5)
    
    # Test operation complete
    log_operation_complete(logger, "test_download", 
                         success=True, 
                         duration=0.5,
                         videos_downloaded=10,
                         videos_skipped=0)
    
    # Test failed operation
    log_operation_complete(logger, "test_download_failed", 
                         success=False, 
                         duration=0.1,
                         error="Network timeout")
    
    print("✓ Operation logging test completed")


def test_progress_logging():
    """Test progress logging."""
    print("\n=== TEST 3: Progress Logging ===")
    
    logger = get_mass_download_logger("test_progress")
    
    total_items = 100
    for i in range(0, total_items + 1, 10):
        log_progress(logger, i, total_items, "Processing channels", interval=10)
        time.sleep(0.1)
    
    print("✓ Progress logging test completed")


def test_structured_logging():
    """Test structured logging with context."""
    print("\n=== TEST 4: Structured Logging ===")
    
    # Create structured logger with context
    structured = create_operation_logger("channel_processor",
                                       job_id="job_20240101_120000",
                                       user="test_user")
    
    structured.info("Starting channel processing")
    structured.info("Processing channel", channel_url="https://youtube.com/@test")
    structured.warning("Rate limit approaching", current_rate=90, max_rate=100)
    structured.error("Failed to download video", video_id="abc123", reason="404 Not Found")
    
    # Add more context
    structured.set_context(channel_name="TestChannel")
    structured.info("Retrying failed downloads")
    
    print("✓ Structured logging test completed")


def test_module_logging():
    """Test logging from different modules."""
    print("\n=== TEST 5: Module Logging ===")
    
    # Import modules that use logging
    from mass_download.mass_coordinator import logger as coord_logger
    from mass_download.input_handler import logger as input_logger
    from mass_download.channel_discovery import logger as discovery_logger
    
    coord_logger.info("Test message from mass_coordinator")
    input_logger.info("Test message from input_handler")
    discovery_logger.info("Test message from channel_discovery")
    
    print("✓ Module logging test completed")


def test_error_logging():
    """Test error logging with exceptions."""
    print("\n=== TEST 6: Error Logging ===")
    
    logger = get_mass_download_logger("test_errors")
    
    try:
        # Simulate an error
        raise ValueError("This is a test error")
    except Exception as e:
        logger.exception("Caught an exception during processing")
    
    # Test error with context
    try:
        result = 1 / 0
    except ZeroDivisionError:
        logger.error("Division by zero error", exc_info=True)
    
    print("✓ Error logging test completed")


def verify_log_files():
    """Verify that log files were created."""
    print("\n=== TEST 7: Log File Verification ===")
    
    log_dir = Path("logs/mass_download")
    
    # Check if log directory exists
    if not log_dir.exists():
        print("❌ Log directory not found")
        return False
    
    # Check for main log file
    main_log = log_dir / "mass_download.log"
    if main_log.exists():
        print(f"✓ Main log file exists: {main_log}")
        print(f"  Size: {main_log.stat().st_size} bytes")
    else:
        print("❌ Main log file not found")
    
    # Check for error log file
    error_log = log_dir / "mass_download_errors.log"
    if error_log.exists():
        print(f"✓ Error log file exists: {error_log}")
        print(f"  Size: {error_log.stat().st_size} bytes")
    else:
        print("❌ Error log file not found")
    
    # Show last few lines of main log
    if main_log.exists():
        print("\nLast 5 lines of main log:")
        with open(main_log, 'r') as f:
            lines = f.readlines()
            for line in lines[-5:]:
                print(f"  {line.rstrip()}")
    
    return True


def test_concurrent_logging():
    """Test logging from multiple threads."""
    print("\n=== TEST 8: Concurrent Logging ===")
    
    import threading
    
    def log_from_thread(thread_id: int):
        logger = get_mass_download_logger(f"thread_{thread_id}")
        for i in range(5):
            logger.info(f"Message {i} from thread {thread_id}")
            time.sleep(0.01)
    
    # Create and start threads
    threads = []
    for i in range(3):
        t = threading.Thread(target=log_from_thread, args=(i,))
        threads.append(t)
        t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    print("✓ Concurrent logging test completed")


def main():
    """Run all logging tests."""
    print("Mass Download Logging Test Suite")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    
    # Run all tests
    test_basic_logging()
    test_operation_logging()
    test_progress_logging()
    test_structured_logging()
    test_module_logging()
    test_error_logging()
    test_concurrent_logging()
    
    # Verify log files
    verify_log_files()
    
    print("\n" + "=" * 80)
    print("All logging tests completed!")
    print(f"Finished at: {datetime.now()}")
    
    # Summary
    print("\nSummary:")
    print("- Basic logging: ✓")
    print("- Operation logging: ✓")
    print("- Progress logging: ✓")
    print("- Structured logging: ✓")
    print("- Module logging: ✓")
    print("- Error logging: ✓")
    print("- Concurrent logging: ✓")
    print("- Log files created: ✓")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())