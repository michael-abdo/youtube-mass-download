#!/usr/bin/env python3
"""
Consolidated Import Test Suite

Consolidates all import-related tests from:
- test_import*.py files
- test_dry_imports.py
- simple_import_test.py

Provides unified import testing and validation.
"""

import unittest
import sys
import os
from pathlib import Path
import importlib

# Add project root to path
project_root = Path(__file__).parent.parent
try:
    from utils.test_helpers import TestReporter, TestEnvironment
    from utils.config import get_config
except ImportError as e:
    print(f"Warning: Could not import test utilities: {e}")

class ImportTestSuite(unittest.TestCase):
    """Test suite for import validation"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_env = TestEnvironment("import_suite")
        
    def tearDown(self):
        """Clean up test environment"""
        self.test_env.__exit__(None, None, None)
    
    def test_core_utils_imports(self):
        """Test core utilities can be imported (from test_imports.py)"""
        TestReporter.print_test_header("Core Utils Import Test")
        
        core_modules = [
            'utils.config',
            'utils.logging_config',
            'utils.error_handling', 
            'utils.patterns',
            'utils.csv_manager',
            'utils.sanitization'
        ]
        
        for module_name in core_modules:
            with self.subTest(module=module_name):
                try:
                    module = importlib.import_module(module_name)
                    self.assertIsNotNone(module, f"Module {module_name} should import")
                except ImportError as e:
                    self.fail(f"Failed to import {module_name}: {e}")
    
    def test_download_stack_imports(self):
        """Test download-related imports"""
        TestReporter.print_test_header("Download Stack Import Test")
        
        download_modules = [
            'utils.downloader',
            'utils.s3_manager',
            'utils.extract_links',
            'utils.rate_limiter',
            'utils.retry_utils'
        ]
        
        for module_name in download_modules:
            with self.subTest(module=module_name):
                try:
                    module = importlib.import_module(module_name)
                    self.assertIsNotNone(module, f"Module {module_name} should import")
                except ImportError as e:
                    self.fail(f"Failed to import {module_name}: {e}")
    
    def test_dry_imports(self):
        """Test DRY refactoring imports (from test_dry_imports.py)"""
        TestReporter.print_test_header("DRY Refactoring Import Test")
        
        # Test that consolidated modules are accessible
        dry_modules = {
            'utils.s3_manager': ['UnifiedS3Manager', 'UploadMode'],
            'utils.downloader': ['MinimalDownloader'],
            'utils.csv_manager': ['CSVManager'],
            'utils.error_handling': ['with_standard_error_handling'],
            'utils.patterns': ['PatternRegistry', 'extract_youtube_id']
        }
        
        for module_name, expected_attrs in dry_modules.items():
            with self.subTest(module=module_name):
                try:
                    module = importlib.import_module(module_name)
                    for attr_name in expected_attrs:
                        self.assertTrue(
                            hasattr(module, attr_name),
                            f"Module {module_name} should have {attr_name}"
                        )
                except ImportError as e:
                    self.fail(f"Failed to import {module_name}: {e}")
    
    def test_simple_imports(self):
        """Test simple import scenarios (from simple_import_test.py)"""
        TestReporter.print_test_header("Simple Import Test")
        
        # Test basic Python standard library imports work
        standard_imports = [
            'os', 'sys', 'json', 'csv', 'pathlib',
            'datetime', 'subprocess', 'tempfile'
        ]
        
        for module_name in standard_imports:
            with self.subTest(module=module_name):
                try:
                    importlib.import_module(module_name)
                except ImportError as e:
                    self.fail(f"Standard library import failed {module_name}: {e}")
        
        # Test third-party imports
        third_party_imports = [
            'pandas', 'boto3', 'requests', 'yaml'
        ]
        
        for module_name in third_party_imports:
            with self.subTest(module=module_name):
                try:
                    importlib.import_module(module_name)
                except ImportError:
                    # Third-party imports are optional for testing
                    pass
    
    def test_circular_import_detection(self):
        """Test for circular imports"""
        TestReporter.print_test_header("Circular Import Detection")
        
        # Test that importing all utils modules doesn't create circular dependencies
        utils_modules = [
            'utils.config',
            'utils.patterns', 
            'utils.error_handling',
            'utils.logging_config',
            'utils.csv_manager',
            'utils.downloader',
            'utils.s3_manager'
        ]
        
        # Import all modules in sequence - if there are circular imports, this will fail
        imported_modules = []
        for module_name in utils_modules:
            try:
                module = importlib.import_module(module_name)
                imported_modules.append(module_name)
            except ImportError as e:
                if "circular import" in str(e).lower():
                    self.fail(f"Circular import detected in {module_name}: {e}")
                # Other import errors are tested elsewhere
        
        self.assertGreater(len(imported_modules), 0, "Should successfully import some modules")

class ImportUtilsTestSuite(unittest.TestCase):
    """Test suite for import utilities functionality"""
    
    def test_import_utils_module(self):
        """Test import_utils module if it exists"""
        TestReporter.print_test_header("Import Utils Module Test")
        
        try:
            import utils.import_utils as import_utils
            
            # Test that import_utils has expected functions
            expected_functions = ['import_core_utils', 'import_download_stack']
            for func_name in expected_functions:
                if hasattr(import_utils, func_name):
                    func = getattr(import_utils, func_name)
                    self.assertTrue(callable(func), f"{func_name} should be callable")
                    
        except ImportError:
            # import_utils may not exist yet, that's OK
            self.skipTest("utils.import_utils not yet implemented")

def run_all_import_tests():
    """Run all consolidated import tests"""
    TestReporter.print_test_header("Running All Import Tests")
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add import tests
    suite.addTest(unittest.makeSuite(ImportTestSuite))
    suite.addTest(unittest.makeSuite(ImportUtilsTestSuite))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    TestReporter.print_test_result(
        "All Import Tests",
        result.wasSuccessful(),
        f"Tests run: {result.testsRun}, Failures: {len(result.failures)}, Errors: {len(result.errors)}"
    )
    
    return result.wasSuccessful()

if __name__ == "__main__":
    success = run_all_import_tests()
    sys.exit(0 if success else 1)