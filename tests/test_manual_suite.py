#!/usr/bin/env python3
"""
Consolidated Manual Test Suite

Consolidates all manual and simple tests from:
- manual_*.py files
- simple_*.py test files
- test_manual.py
- test_ensure_directory.py
- test_new_functions.py

Provides unified manual testing capabilities.
"""

import unittest
import sys
import os
from pathlib import Path
import tempfile
import shutil

# Add project root to path
project_root = Path(__file__).parent.parent
try:
    from utils.test_helpers import TestReporter, TestEnvironment
    from utils.config import get_config, ensure_directory
    from utils.csv_manager import CSVManager
except ImportError as e:
    print(f"Warning: Could not import test utilities: {e}")

class ManualTestSuite(unittest.TestCase):
    """Manual test suite for interactive testing scenarios"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_env = TestEnvironment("manual_suite")
        
    def tearDown(self):
        """Clean up test environment"""
        self.test_env.__exit__(None, None, None)
    
    def test_ensure_directory_functionality(self):
        """Test ensure_directory functionality (from test_ensure_directory.py)"""
        TestReporter.print_test_header("Ensure Directory Test")
        
        # Test directory creation
        test_dir = self.test_env.test_dir / "test_subdir"
        
        try:
            # Test that ensure_directory creates directory
            result_dir = ensure_directory(test_dir)
            self.assertTrue(result_dir.exists(), "Directory should be created")
            self.assertTrue(result_dir.is_dir(), "Should be a directory")
            
            # Test that ensure_directory is idempotent
            result_dir2 = ensure_directory(test_dir)
            self.assertEqual(result_dir, result_dir2, "Should return same directory")
            
        except Exception as e:
            self.fail(f"ensure_directory test failed: {e}")
    
    def test_csv_manager_basic_operations(self):
        """Test CSV manager basic operations"""
        TestReporter.print_test_header("CSV Manager Basic Test")
        
        try:
            # Create test CSV file
            test_csv = self.test_env.test_dir / "test.csv" 
            
            # Test CSV creation
            csv_manager = CSVManager(str(test_csv))
            
            # Basic smoke test - check that CSVManager can be instantiated
            self.assertIsNotNone(csv_manager, "CSVManager should be created")
            self.assertEqual(str(csv_manager.csv_file), str(test_csv), "CSV file path should match")
            
        except Exception as e:
            self.fail(f"CSV manager test failed: {e}")
    
    def test_config_access(self):
        """Test configuration access"""
        TestReporter.print_test_header("Config Access Test")
        
        try:
            # Test that config can be loaded
            config = get_config()
            self.assertIsNotNone(config, "Config should be loaded")
            
            # Test that config has expected structure
            # These are basic checks that don't require specific config values
            self.assertTrue(hasattr(config, 'get') or isinstance(config, dict), 
                          "Config should be dict-like")
                          
        except Exception as e:
            self.fail(f"Config access test failed: {e}")
    
    def test_new_functions_basic(self):
        """Test new functions basic functionality (from test_new_functions.py)"""
        TestReporter.print_test_header("New Functions Basic Test")
        
        # TODO: Consolidate logic from test_new_functions.py
        # For now, basic test that consolidated modules work
        try:
            from utils import patterns, config, error_handling
            
            # Test that key functions exist
            self.assertTrue(hasattr(patterns, 'extract_youtube_id'), 
                          "patterns should have extract_youtube_id")
            self.assertTrue(hasattr(config, 'get_config'), 
                          "config should have get_config")
            self.assertTrue(hasattr(error_handling, 'with_standard_error_handling'), 
                          "error_handling should have decorators")
                          
        except Exception as e:
            self.fail(f"New functions test failed: {e}")

class SimpleTestSuite(unittest.TestCase):
    """Simple test suite for basic functionality"""
    
    def test_simple_file_operations(self):
        """Test simple file operations"""
        TestReporter.print_test_header("Simple File Operations")
        
        with TestEnvironment("simple_ops") as test_env:
            # Test file creation and reading
            test_file = test_env.create_temp_file("Hello, World!", ".txt")
            
            self.assertTrue(test_file.exists(), "Test file should exist")
            
            content = test_file.read_text()
            self.assertEqual(content, "Hello, World!", "File content should match")
    
    def test_simple_import_verification(self):
        """Simple import verification test"""
        TestReporter.print_test_header("Simple Import Verification")
        
        # Test that critical modules can be imported without errors
        critical_imports = [
            'utils.config',
            'utils.patterns', 
            'utils.error_handling'
        ]
        
        for module_name in critical_imports:
            with self.subTest(module=module_name):
                try:
                    __import__(module_name)
                except ImportError as e:
                    self.fail(f"Critical import failed {module_name}: {e}")
    
    def test_simple_workflow_components(self):
        """Test simple workflow components"""
        TestReporter.print_test_header("Simple Workflow Components")
        
        # TODO: Consolidate from simple_workflow.py test scenarios
        # Basic test that workflow components exist
        try:
            # Test that main workflow script exists
            workflow_script = Path(project_root) / "simple_workflow.py"
            self.assertTrue(workflow_script.exists(), "simple_workflow.py should exist")
            
        except Exception as e:
            self.fail(f"Simple workflow test failed: {e}")

class ManualVerificationTestSuite(unittest.TestCase):
    """Manual verification test suite (from manual_test_verification.py)"""
    
    def test_manual_verification_setup(self):
        """Test manual verification setup"""
        TestReporter.print_test_header("Manual Verification Setup")
        
        # TODO: Consolidate logic from manual_test_verification.py
        # For now, basic verification that test infrastructure works
        try:
            # Test that TestReporter works
            TestReporter.print_test_result("Sample Test", True, "This is a test")
            
            # Test that TestEnvironment works
            with TestEnvironment("verification_test") as env:
                self.assertIsNotNone(env.test_dir, "Test environment should have test_dir")
                self.assertTrue(env.test_dir.exists(), "Test directory should exist")
            
        except Exception as e:
            self.fail(f"Manual verification setup failed: {e}")

def run_all_manual_tests():
    """Run all consolidated manual tests"""
    TestReporter.print_test_header("Running All Manual Tests")
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add manual tests
    suite.addTest(unittest.makeSuite(ManualTestSuite))
    suite.addTest(unittest.makeSuite(SimpleTestSuite))
    suite.addTest(unittest.makeSuite(ManualVerificationTestSuite))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    TestReporter.print_test_result(
        "All Manual Tests",
        result.wasSuccessful(),
        f"Tests run: {result.testsRun}, Failures: {len(result.failures)}, Errors: {len(result.errors)}"
    )
    
    return result.wasSuccessful()

if __name__ == "__main__":
    success = run_all_manual_tests()
    sys.exit(0 if success else 1)