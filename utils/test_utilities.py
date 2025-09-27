#!/usr/bin/env python3
"""
Consolidated Test Utilities Module (DRY Phase 10)

Provides comprehensive testing utilities consolidating patterns from:
- test_helpers.py (archived)
- All test files in tests/ directory 
- Testing patterns found throughout the codebase
- Unified test reporters and environments
- Test data factories and validation utilities

Key consolidations:
- TestReporter: Standardized test output formatting
- TestEnvironment: Test setup/cleanup automation
- TestCSVHandler: CSV testing utilities
- TestDataFactory: Test data generation
- Validation helpers and assertion utilities
"""

import os
import sys
import time
import json
import shutil
import tempfile
import subprocess
import unittest
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable, Tuple, Iterator
from datetime import datetime
from io import BytesIO, StringIO
import pandas as pd

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

from utils.logging_config import get_logger
from utils.error_handling import handle_file_operations, ErrorMessages
from utils.config import get_config, get_project_root, ensure_directory
from utils.data_processing import read_csv_safe, write_csv_safe, read_json_safe, write_json_safe

logger = get_logger(__name__)


# ============================================================================
# TEST REPORTING AND OUTPUT
# ============================================================================

class TestReporter:
    """Standardized test reporting and output formatting."""
    
    @staticmethod
    def print_test_header(title: str, description: str = ""):
        """
        Print formatted test header.
        
        Consolidates header printing patterns from 15+ test files.
        
        Args:
            title: Test title
            description: Optional description
            
        Example:
            TestReporter.print_test_header("API Tests", "Testing all endpoints")
        """
        print(f"\n🧪 {title.upper()}")
        if description:
            print(f"📝 {description}")
        print("=" * 70)
    
    @staticmethod
    def print_test_result(test_name: str, success: bool, details: str = ""):
        """
        Print formatted test result.
        
        Consolidates result printing patterns.
        
        Args:
            test_name: Name of the test
            success: Whether test passed
            details: Additional details
            
        Example:
            TestReporter.print_test_result("Database Connection", True, "Connected in 0.5s")
        """
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{status} {test_name}")
        if details:
            print(f"   {details}")
    
    @staticmethod
    def print_summary(results: List[Dict[str, Any]]):
        """
        Print comprehensive test summary.
        
        Args:
            results: List of test result dictionaries
            
        Example:
            results = [{'success': True, 'name': 'Test 1'}, {'success': False, 'name': 'Test 2'}]
            TestReporter.print_summary(results)
        """
        total = len(results)
        passed = len([r for r in results if r.get('success', False)])
        failed = total - passed
        
        print("\n" + "=" * 70)
        print("📊 TEST SUMMARY")
        print(f"Total tests: {total}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"📈 Success rate: {(passed/total)*100:.1f}%" if total > 0 else "No tests run")
        
        if failed > 0:
            print("\n❌ FAILED TESTS:")
            for result in results:
                if not result.get('success', False):
                    error = result.get('error', result.get('details', 'Unknown error'))
                    print(f"   - {result.get('name', result.get('type', 'unknown'))}: {error}")
    
    @staticmethod
    def print_progress(current: int, total: int, message: str = ""):
        """
        Print progress indicator.
        
        Args:
            current: Current progress
            total: Total items
            message: Optional progress message
            
        Example:
            TestReporter.print_progress(3, 10, "Processing files")
        """
        percentage = (current / total) * 100 if total > 0 else 0
        bar_length = 30
        filled_length = int(bar_length * current // total) if total > 0 else 0
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        
        print(f'\r|{bar}| {percentage:.1f}% ({current}/{total}) {message}', end='', flush=True)
        
        if current == total:
            print()  # New line when complete
    
    @staticmethod
    def save_report(results: List[Dict[str, Any]], report_file: Union[str, Path]):
        """
        Save test report to JSON file.
        
        Args:
            results: Test results
            report_file: Output file path
            
        Example:
            TestReporter.save_report(test_results, 'reports/test_results.json')
        """
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'total_tests': len(results),
            'passed': len([r for r in results if r.get('success', False)]),
            'failed': len([r for r in results if not r.get('success', False)]),
            'results': results
        }
        
        report_file = Path(report_file)
        report_file.parent.mkdir(parents=True, exist_ok=True)
        
        write_json_safe(report_data, report_file)
        logger.info(f"Test report saved to {report_file}")


# ============================================================================
# TEST ENVIRONMENT MANAGEMENT
# ============================================================================

class TestEnvironment:
    """
    Manage test environment setup and cleanup.
    
    Consolidates environment management patterns from multiple test files.
    """
    
    def __init__(self, test_name: str, base_dir: Optional[Union[str, Path]] = None):
        """
        Initialize test environment.
        
        Args:
            test_name: Name of the test
            base_dir: Base directory for test files (default: temp dir)
            
        Example:
            with TestEnvironment("api_tests") as env:
                temp_file = env.create_temp_file("test data")
        """
        self.test_name = test_name
        self.base_dir = Path(base_dir) if base_dir else Path(tempfile.gettempdir())
        self.test_dir = self.base_dir / f"test_{test_name}_{int(time.time())}"
        self.created_dirs = []
        self.created_files = []
        self.cleanup_functions = []
        
    def __enter__(self):
        """Setup test environment."""
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.created_dirs.append(self.test_dir)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup test environment."""
        # Run custom cleanup functions first
        for cleanup_func in self.cleanup_functions:
            try:
                cleanup_func()
            except Exception as e:
                logger.warning(f"Cleanup function failed: {e}")
        
        # Remove created files
        for file_path in self.created_files:
            try:
                if Path(file_path).exists():
                    os.remove(file_path)
            except Exception as e:
                logger.warning(f"Failed to remove file {file_path}: {e}")
        
        # Remove created directories
        for dir_path in reversed(self.created_dirs):  # Remove in reverse order
            try:
                if dir_path.exists():
                    shutil.rmtree(dir_path)
            except Exception as e:
                logger.warning(f"Failed to remove directory {dir_path}: {e}")
    
    def create_subdir(self, name: str) -> Path:
        """
        Create subdirectory in test environment.
        
        Args:
            name: Directory name
            
        Returns:
            Path to created directory
        """
        subdir = self.test_dir / name
        subdir.mkdir(parents=True, exist_ok=True)
        self.created_dirs.append(subdir)
        return subdir
    
    def create_temp_file(self, content: str, suffix: str = ".tmp", 
                        encoding: str = "utf-8") -> Path:
        """
        Create temporary file with content.
        
        Args:
            content: File content
            suffix: File suffix
            encoding: File encoding
            
        Returns:
            Path to created file
        """
        temp_file = self.test_dir / f"temp_{len(self.created_files)}{suffix}"
        with open(temp_file, 'w', encoding=encoding) as f:
            f.write(content)
        self.created_files.append(temp_file)
        return temp_file
    
    def create_binary_file(self, data: bytes, suffix: str = ".bin") -> Path:
        """
        Create binary file with data.
        
        Args:
            data: Binary data
            suffix: File suffix
            
        Returns:
            Path to created file
        """
        temp_file = self.test_dir / f"binary_{len(self.created_files)}{suffix}"
        with open(temp_file, 'wb') as f:
            f.write(data)
        self.created_files.append(temp_file)
        return temp_file
    
    def add_cleanup(self, cleanup_func: Callable):
        """
        Add custom cleanup function.
        
        Args:
            cleanup_func: Function to call during cleanup
        """
        self.cleanup_functions.append(cleanup_func)
    
    def get_temp_path(self, filename: str) -> Path:
        """
        Get path within test directory.
        
        Args:
            filename: Filename
            
        Returns:
            Path within test directory
        """
        return self.test_dir / filename


# ============================================================================
# CSV AND DATA TESTING UTILITIES
# ============================================================================

class TestCSVHandler:
    """Handle CSV operations for tests."""
    
    @staticmethod
    def read_test_csv(csv_file: Optional[Union[str, Path]] = None) -> pd.DataFrame:
        """
        Read CSV file for testing.
        
        Args:
            csv_file: Path to CSV file (default: from config)
            
        Returns:
            DataFrame with CSV data
            
        Example:
            df = TestCSVHandler.read_test_csv("test_data.csv")
        """
        if not csv_file:
            # Try to get default CSV from config
            config = get_config()
            csv_file = config.get('csv.default_file', 'outputs/output.csv')
        
        csv_path = Path(csv_file)
        if not csv_path.exists():
            raise FileNotFoundError(f"Test CSV file not found: {csv_file}")
        
        return read_csv_safe(csv_file)
    
    @staticmethod
    def create_test_csv(data: List[Dict[str, Any]], output_path: Union[str, Path]) -> Path:
        """
        Create test CSV file.
        
        Args:
            data: List of row dictionaries
            output_path: Output file path
            
        Returns:
            Path to created CSV file
        """
        df = pd.DataFrame(data)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        write_csv_safe(df, output_path)
        return output_path
    
    @staticmethod
    def find_rows_with_pattern(df: pd.DataFrame, column: str, pattern: str, 
                             limit: int = 1) -> List[Dict[str, Any]]:
        """
        Find rows matching pattern in specified column.
        
        Args:
            df: DataFrame to search
            column: Column to search in
            pattern: Pattern to match
            limit: Maximum results to return
            
        Returns:
            List of matching row dictionaries
        """
        results = []
        for _, row in df.iterrows():
            value = str(row.get(column, ''))
            if pattern.lower() in value.lower():
                results.append(row.to_dict())
                if len(results) >= limit:
                    break
        return results
    
    @staticmethod
    def find_person_with_youtube(df: pd.DataFrame, limit: int = 1) -> List[Dict[str, Any]]:
        """
        Find people with YouTube links for testing.
        
        Args:
            df: DataFrame to search
            limit: Maximum results
            
        Returns:
            List of people with YouTube links
        """
        results = []
        for _, row in df.iterrows():
            youtube_links = TestCSVHandler.extract_links(row, 'youtube_playlist')
            if youtube_links and any('watch?v=' in link or 'youtu.be/' in link for link in youtube_links):
                results.append({
                    'row': row.to_dict(),
                    'youtube_links': youtube_links,
                    'person_name': row.get('name', 'Unknown')
                })
                if len(results) >= limit:
                    break
        return results
    
    @staticmethod
    def find_person_with_drive(df: pd.DataFrame, limit: int = 1) -> List[Dict[str, Any]]:
        """
        Find people with Google Drive links for testing.
        
        Args:
            df: DataFrame to search
            limit: Maximum results
            
        Returns:
            List of people with Drive links
        """
        results = []
        for _, row in df.iterrows():
            drive_links = TestCSVHandler.extract_links(row, 'google_drive')
            if drive_links and any('/file/d/' in link or '/document/d/' in link for link in drive_links):
                results.append({
                    'row': row.to_dict(),
                    'drive_links': drive_links,
                    'person_name': row.get('name', 'Unknown')
                })
                if len(results) >= limit:
                    break
        return results
    
    @staticmethod
    def extract_links(row: Union[pd.Series, Dict[str, Any]], column: str) -> List[str]:
        """
        Extract links from CSV row.
        
        Args:
            row: Row data (Series or dict)
            column: Column containing links
            
        Returns:
            List of links
        """
        if isinstance(row, pd.Series):
            value = row.get(column, '')
        else:
            value = row.get(column, '')
        
        if pd.isna(value) or not value:
            return []
        
        links = str(value).split('|')
        return [link.strip() for link in links if link and link.strip() and link.strip() != 'nan']


# ============================================================================
# TEST DATA FACTORIES
# ============================================================================

class TestDataFactory:
    """Factory for creating test data."""
    
    @staticmethod
    def create_test_person(row_id: str = "100", name: str = "Test User", 
                          email: str = "test@example.com", 
                          personality_type: str = "Test-Type",
                          doc_link: str = "") -> Dict[str, Any]:
        """
        Create a test person record.
        
        Args:
            row_id: Person row ID
            name: Person name
            email: Email address
            personality_type: Personality type
            doc_link: Document link
            
        Returns:
            Test person dictionary
        """
        return {
            'row_id': row_id,
            'name': name,
            'email': email,
            'type': personality_type,
            'link': doc_link,
            'created_at': datetime.now().isoformat(),
            'test_record': True
        }
    
    @staticmethod
    def create_test_links(youtube_count: int = 2, drive_count: int = 1) -> Dict[str, List[str]]:
        """
        Create test link data.
        
        Args:
            youtube_count: Number of YouTube links
            drive_count: Number of Drive links
            
        Returns:
            Dictionary with link lists
        """
        youtube_links = [
            f"https://www.youtube.com/watch?v=test{i:011d}" 
            for i in range(youtube_count)
        ]
        drive_links = [
            f"https://drive.google.com/file/d/test{i:025d}/view" 
            for i in range(drive_count)
        ]
        
        return {
            'youtube': youtube_links,
            'drive_files': drive_links,
            'drive_folders': [],
            'all_links': youtube_links + drive_links
        }
    
    @staticmethod
    def create_test_document(content_length: int = 1000) -> Tuple[str, str]:
        """
        Create test document content.
        
        Args:
            content_length: Approximate content length
            
        Returns:
            Tuple of (html_content, text_content)
        """
        text_content = "Test document text content. " * (content_length // 30)
        html_content = f"<html><body><p>{text_content}</p></body></html>"
        return html_content, text_content
    
    @staticmethod
    def generate_test_batch(num_records: int = 100, 
                           with_links: bool = False) -> List[Dict[str, Any]]:
        """
        Generate batch of test records.
        
        Args:
            num_records: Number of records to generate
            with_links: Whether to include links
            
        Returns:
            List of test records
        """
        test_data = []
        for i in range(num_records):
            person = TestDataFactory.create_test_person(
                row_id=f"test_{i:04d}",
                name=f"Test Person {i}",
                email=f"test{i}@example.com",
                personality_type=f"Test-Type-{i % 5}",
                doc_link=f"https://example.com/doc_{i}" if i % 3 == 0 else ""
            )
            
            if with_links and i % 3 == 0:
                links = TestDataFactory.create_test_links(
                    youtube_count=i % 3 + 1,
                    drive_count=i % 2 + 1
                )
                person['youtube_playlist'] = '|'.join(links['youtube'])
                person['google_drive'] = '|'.join(links['drive_files'])
            
            test_data.append(person)
        
        return test_data


# ============================================================================
# EXTRACTION AND COMPARISON UTILITIES
# ============================================================================

def run_extraction_test(row_id: str, test_case: Dict[str, Any], 
                       extraction_func: Callable) -> Dict[str, Any]:
    """
    Standard extraction test runner.
    
    Consolidates extraction testing patterns from 12+ test files.
    
    Args:
        row_id: Row ID to test
        test_case: Test case information
        extraction_func: Function that performs extraction
        
    Returns:
        Dictionary with test results
        
    Example:
        result = run_extraction_test("123", {"name": "Test Person"}, extract_person_data)
        if result['success']:
            print(f"Extraction took {result['timing']:.2f}s")
    """
    TestReporter.print_test_header(f"Testing Row {row_id}: {test_case.get('name', 'Unknown')}")
    
    result = {
        'row_id': row_id,
        'test_case': test_case.get('name', 'Unknown'),
        'success': False,
        'extracted_data': None,
        'error': None,
        'timing': 0,
        'timestamp': datetime.now().isoformat()
    }
    
    try:
        start_time = time.time()
        extracted_data = extraction_func(row_id, test_case)
        end_time = time.time()
        
        result.update({
            'success': True,
            'extracted_data': extracted_data,
            'timing': end_time - start_time
        })
        
        TestReporter.print_test_result(
            f"Row {row_id} Extraction", 
            True, 
            f"Completed in {result['timing']:.2f}s"
        )
        
    except Exception as e:
        result['error'] = str(e)
        TestReporter.print_test_result(f"Row {row_id} Extraction", False, str(e))
    
    return result


def compare_extraction_results(expected: Dict[str, Any], 
                             actual: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare extraction results with expected values.
    
    Args:
        expected: Expected extraction results
        actual: Actual extraction results
        
    Returns:
        Dictionary with comparison results
        
    Example:
        comparison = compare_extraction_results(expected_data, actual_data)
        if comparison['match']:
            print(f"Perfect match! Similarity: {comparison['similarity_score']:.2f}")
    """
    comparison = {
        'match': True,
        'differences': [],
        'similarity_score': 0.0,
        'details': {
            'expected_keys': set(expected.keys()) if expected else set(),
            'actual_keys': set(actual.keys()) if actual else set(),
            'common_keys': set(),
            'missing_keys': set(),
            'extra_keys': set()
        }
    }
    
    # Handle None/empty cases
    if not expected and not actual:
        comparison['similarity_score'] = 1.0
        return comparison
    
    if not expected or not actual:
        comparison['match'] = False
        comparison['differences'].append("One of the results is None/empty")
        return comparison
    
    # Analyze key differences
    expected_keys = set(expected.keys())
    actual_keys = set(actual.keys())
    
    comparison['details']['common_keys'] = expected_keys & actual_keys
    comparison['details']['missing_keys'] = expected_keys - actual_keys
    comparison['details']['extra_keys'] = actual_keys - expected_keys
    
    # Check for missing or extra keys
    if comparison['details']['missing_keys']:
        comparison['match'] = False
        comparison['differences'].append(f"Missing keys: {list(comparison['details']['missing_keys'])}")
    
    if comparison['details']['extra_keys']:
        comparison['differences'].append(f"Extra keys: {list(comparison['details']['extra_keys'])}")
    
    # Compare values for common keys
    matches = 0
    total_comparisons = 0
    
    for key in comparison['details']['common_keys']:
        total_comparisons += 1
        expected_val = expected[key]
        actual_val = actual[key]
        
        if expected_val == actual_val:
            matches += 1
        else:
            comparison['match'] = False
            
            # Special handling for different data types
            if isinstance(expected_val, list) and isinstance(actual_val, list):
                list_similarity = _compare_lists(expected_val, actual_val)
                comparison['differences'].append(f"Key '{key}': list similarity {list_similarity:.2f}")
            elif isinstance(expected_val, str) and isinstance(actual_val, str):
                string_similarity = _compare_strings(expected_val, actual_val)
                comparison['differences'].append(f"Key '{key}': string similarity {string_similarity:.2f}")
            else:
                comparison['differences'].append(f"Key '{key}': expected {expected_val}, got {actual_val}")
    
    # Calculate similarity score
    if total_comparisons > 0:
        key_score = len(comparison['details']['common_keys']) / len(expected_keys)
        value_score = matches / total_comparisons
        comparison['similarity_score'] = (key_score + value_score) / 2
    
    return comparison


def _compare_lists(list1: List[Any], list2: List[Any]) -> float:
    """Compare two lists and return similarity score."""
    if not list1 and not list2:
        return 1.0
    
    if not list1 or not list2:
        return 0.0
    
    # Convert to sets for comparison
    set1 = set(str(item) for item in list1)
    set2 = set(str(item) for item in list2)
    
    intersection = set1 & set2
    union = set1 | set2
    
    return len(intersection) / len(union) if union else 0.0


def _compare_strings(str1: str, str2: str) -> float:
    """Compare two strings and return similarity score."""
    if str1 == str2:
        return 1.0
    
    if not str1 or not str2:
        return 0.0
    
    # Simple character-based similarity
    str1 = str1.lower().strip()
    str2 = str2.lower().strip()
    
    if str1 == str2:
        return 1.0
    
    # Check if one is contained in the other
    if str1 in str2 or str2 in str1:
        return 0.8
    
    # Basic word overlap
    words1 = set(str1.split())
    words2 = set(str2.split())
    
    if words1 and words2:
        intersection = words1 & words2
        union = words1 | words2
        return len(intersection) / len(union)
    
    return 0.0


# ============================================================================
# TEST SUITE UTILITIES
# ============================================================================

class TestSuite:
    """Base class for creating organized test suites."""
    
    def __init__(self, name: str):
        self.name = name
        self.tests = []
        self.setup_functions = []
        self.teardown_functions = []
        self.results = []
    
    def add_test(self, test_func: Callable, name: str, **kwargs):
        """Add test function to suite."""
        self.tests.append({
            'function': test_func,
            'name': name,
            'kwargs': kwargs
        })
    
    def add_setup(self, setup_func: Callable):
        """Add setup function."""
        self.setup_functions.append(setup_func)
    
    def add_teardown(self, teardown_func: Callable):
        """Add teardown function."""
        self.teardown_functions.append(teardown_func)
    
    def run(self) -> List[Dict[str, Any]]:
        """Run all tests in the suite."""
        TestReporter.print_test_header(f"Running Test Suite: {self.name}")
        
        # Run setup functions
        for setup_func in self.setup_functions:
            try:
                setup_func()
            except Exception as e:
                logger.error(f"Setup function failed: {e}")
        
        self.results = []
        
        # Run tests
        for i, test in enumerate(self.tests):
            TestReporter.print_progress(i, len(self.tests), f"Running {test['name']}")
            
            result = {
                'name': test['name'],
                'success': False,
                'error': None,
                'timing': 0,
                'timestamp': datetime.now().isoformat()
            }
            
            try:
                start_time = time.time()
                test_result = test['function'](**test['kwargs'])
                end_time = time.time()
                
                result.update({
                    'success': True,
                    'timing': end_time - start_time,
                    'result': test_result
                })
                
            except Exception as e:
                result['error'] = str(e)
            
            self.results.append(result)
            TestReporter.print_test_result(test['name'], result['success'], 
                                         result.get('error', f"Completed in {result['timing']:.2f}s"))
        
        # Run teardown functions
        for teardown_func in self.teardown_functions:
            try:
                teardown_func()
            except Exception as e:
                logger.error(f"Teardown function failed: {e}")
        
        # Print summary
        TestReporter.print_summary(self.results)
        
        return self.results


# ============================================================================
# FILESYSTEM AND MONITORING UTILITIES
# ============================================================================

class FilesystemMonitor:
    """Monitor filesystem for file creation during tests."""
    
    def __init__(self, watch_dir: Union[str, Path]):
        self.watch_dir = Path(watch_dir)
        self.before_files = set()
        self.start_monitoring()
    
    def start_monitoring(self):
        """Start monitoring the directory."""
        if self.watch_dir.exists():
            self.before_files = set(os.listdir(self.watch_dir))
        else:
            self.before_files = set()
    
    def get_new_files(self) -> List[str]:
        """Get list of new files created since monitoring started."""
        if not self.watch_dir.exists():
            return []
        
        current_files = set(os.listdir(self.watch_dir))
        new_files = current_files - self.before_files
        return list(new_files)
    
    def has_new_files(self) -> bool:
        """Check if any new files were created."""
        return len(self.get_new_files()) > 0
    
    def get_file_sizes(self) -> Dict[str, int]:
        """Get sizes of all files in directory."""
        sizes = {}
        if self.watch_dir.exists():
            for file_path in self.watch_dir.glob('*'):
                if file_path.is_file():
                    sizes[file_path.name] = file_path.stat().st_size
        return sizes


# ============================================================================
# PERFORMANCE TESTING UTILITIES
# ============================================================================

class PerformanceTimer:
    """Simple performance timing utility."""
    
    def __init__(self, name: str = "Operation"):
        self.name = name
        self.start_time = None
        self.end_time = None
        self.elapsed = 0
    
    def start(self):
        """Start timing."""
        self.start_time = time.time()
        return self
    
    def stop(self):
        """Stop timing."""
        self.end_time = time.time()
        self.elapsed = self.end_time - self.start_time
        return self.elapsed
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
        TestReporter.print_test_result(
            f"Performance: {self.name}",
            True,
            f"Completed in {self.elapsed:.3f}s"
        )


@contextmanager
def time_operation(name: str):
    """
    Context manager for timing operations.
    
    Args:
        name: Operation name
        
    Example:
        with time_operation("Database Query"):
            result = db.query("SELECT * FROM users")
    """
    timer = PerformanceTimer(name)
    yield timer.start()
    timer.stop()


# ============================================================================
# COMMAND EXECUTION UTILITIES
# ============================================================================

def run_command_test(command: str, description: str, 
                    timeout: int = 30, 
                    expected_returncode: int = 0) -> Dict[str, Any]:
    """
    Run command and return test result.
    
    Args:
        command: Command to run
        description: Test description
        timeout: Command timeout
        expected_returncode: Expected return code
        
    Returns:
        Test result dictionary
        
    Example:
        result = run_command_test("python --version", "Python Version Check")
    """
    result = {
        'command': command,
        'description': description,
        'success': False,
        'returncode': None,
        'stdout': '',
        'stderr': '',
        'elapsed': 0,
        'error': None
    }
    
    try:
        start_time = time.time()
        
        process = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        end_time = time.time()
        
        result.update({
            'returncode': process.returncode,
            'stdout': process.stdout,
            'stderr': process.stderr,
            'elapsed': end_time - start_time,
            'success': process.returncode == expected_returncode
        })
        
    except subprocess.TimeoutExpired:
        result['error'] = f"Command timed out after {timeout} seconds"
    except Exception as e:
        result['error'] = str(e)
    
    TestReporter.print_test_result(
        description,
        result['success'],
        result.get('error', f"Completed in {result['elapsed']:.2f}s (code: {result['returncode']})")
    )
    
    return result


# ============================================================================
# ASSERTION UTILITIES
# ============================================================================

class TestAssertions:
    """Extended assertion utilities for tests."""
    
    @staticmethod
    def assert_file_exists(file_path: Union[str, Path], message: str = ""):
        """Assert that file exists."""
        path = Path(file_path)
        if not path.exists():
            raise AssertionError(f"File does not exist: {path}. {message}")
    
    @staticmethod
    def assert_file_size(file_path: Union[str, Path], min_size: int = 0, 
                        max_size: Optional[int] = None):
        """Assert file size constraints."""
        path = Path(file_path)
        TestAssertions.assert_file_exists(path)
        
        size = path.stat().st_size
        if size < min_size:
            raise AssertionError(f"File too small: {size} < {min_size} bytes")
        
        if max_size and size > max_size:
            raise AssertionError(f"File too large: {size} > {max_size} bytes")
    
    @staticmethod
    def assert_csv_valid(csv_path: Union[str, Path], required_columns: Optional[List[str]] = None):
        """Assert CSV file is valid."""
        df = read_csv_safe(csv_path)
        
        if df.empty:
            raise AssertionError(f"CSV file is empty: {csv_path}")
        
        if required_columns:
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                raise AssertionError(f"Missing columns: {missing_columns}")
    
    @staticmethod
    def assert_url_accessible(url: str, timeout: int = 10):
        """Assert URL is accessible."""
        try:
            import requests
            response = requests.head(url, timeout=timeout)
            if not response.ok:
                raise AssertionError(f"URL not accessible: {url} (status: {response.status_code})")
        except Exception as e:
            raise AssertionError(f"Failed to access URL: {url}. Error: {e}")
    
    @staticmethod
    def assert_json_structure(data: Dict[str, Any], required_keys: List[str]):
        """Assert JSON has required structure."""
        missing_keys = set(required_keys) - set(data.keys())
        if missing_keys:
            raise AssertionError(f"Missing required keys: {missing_keys}")


# ============================================================================
# BATCH TESTING UTILITIES
# ============================================================================

def run_batch_tests(test_functions: List[Callable], 
                   test_names: Optional[List[str]] = None,
                   stop_on_failure: bool = False) -> List[Dict[str, Any]]:
    """
    Run multiple test functions in batch.
    
    Args:
        test_functions: List of test functions
        test_names: Optional list of test names
        stop_on_failure: Whether to stop on first failure
        
    Returns:
        List of test results
        
    Example:
        results = run_batch_tests([test_db, test_api, test_files])
    """
    if test_names is None:
        test_names = [f"Test {i+1}" for i in range(len(test_functions))]
    
    results = []
    
    for i, (test_func, test_name) in enumerate(zip(test_functions, test_names)):
        TestReporter.print_progress(i, len(test_functions), f"Running {test_name}")
        
        result = {
            'name': test_name,
            'success': False,
            'error': None,
            'timing': 0,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            start_time = time.time()
            test_result = test_func()
            end_time = time.time()
            
            result.update({
                'success': True,
                'timing': end_time - start_time,
                'result': test_result
            })
            
        except Exception as e:
            result['error'] = str(e)
            if stop_on_failure:
                results.append(result)
                break
        
        results.append(result)
        TestReporter.print_test_result(test_name, result['success'], 
                                     result.get('error', f"Completed in {result['timing']:.2f}s"))
    
    TestReporter.print_summary(results)
    return results


# ============================================================================
# MOCK AND FIXTURE UTILITIES
# ============================================================================

class MockResponse:
    """Simple mock HTTP response for testing."""
    
    def __init__(self, status_code: int = 200, content: str = "", 
                 headers: Optional[Dict[str, str]] = None):
        self.status_code = status_code
        self.content = content
        self.text = content
        self.headers = headers or {}
        self.ok = 200 <= status_code < 300
    
    def json(self):
        """Return JSON content."""
        return json.loads(self.content)
    
    def raise_for_status(self):
        """Raise exception for bad status codes."""
        if not self.ok:
            raise Exception(f"HTTP {self.status_code}")


# Example usage and testing
if __name__ == "__main__":
    # Test the test utilities
    TestReporter.print_test_header("Testing Test Utilities", "Self-test of utilities module")
    
    # Test environment
    with TestEnvironment("utility_test") as env:
        # Test file creation
        test_file = env.create_temp_file("test content", ".txt")
        TestAssertions.assert_file_exists(test_file)
        
        # Test CSV creation
        test_data = TestDataFactory.generate_test_batch(5)
        csv_file = TestCSVHandler.create_test_csv(test_data, env.test_dir / "test.csv")
        TestAssertions.assert_csv_valid(csv_file, ['row_id', 'name', 'email'])
        
        # Test performance timing
        with time_operation("Sleep Test"):
            time.sleep(0.1)
        
        # Test filesystem monitoring
        monitor = FilesystemMonitor(env.test_dir)
        new_file = env.create_temp_file("new content", ".log")
        assert monitor.has_new_files()
        
        TestReporter.print_test_result("Test Utilities", True, "All components working")
    
    print("\n✓ Test utilities module is ready!")