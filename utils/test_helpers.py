#!/usr/bin/env python3
"""
Test Helpers Module - Compatibility Alias

This module provides backward compatibility for test files that import from utils.test_helpers.
All functionality has been consolidated into utils.test_utilities.py as part of DRY Phase 10.
"""

# Import all test utilities for backward compatibility
from utils.test_utilities import (
    # Core classes
    TestReporter,
    TestEnvironment, 
    TestCSVHandler,
    TestDataFactory,
    TestSuite,
    TestAssertions,
    FilesystemMonitor,
    PerformanceTimer,
    MockResponse,
    
    # Functions
    run_extraction_test,
    compare_extraction_results,
    run_command_test,
    run_batch_tests,
    time_operation
)

# Provide logger for compatibility
from utils.logging_config import get_logger
logger = get_logger(__name__)

# Add deprecation warning
import warnings
warnings.warn(
    "utils.test_helpers is deprecated. Use utils.test_utilities instead.",
    DeprecationWarning,
    stacklevel=2
)