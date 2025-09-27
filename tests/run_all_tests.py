#!/usr/bin/env python3
"""
Unified Test Runner

Runs all consolidated test suites and provides comprehensive reporting.
Replaces scattered test execution scripts with centralized runner.
"""

import sys
import os
from pathlib import Path
import unittest
import time

# Add project root to path
project_root = Path(__file__).parent.parent
try:
    from utils.test_helpers import TestReporter
except ImportError:
    # Fallback reporter if utils not available
    class TestReporter:
        @staticmethod
        def print_test_header(title, description=""):
            print(f"\n🧪 {title.upper()}")
            if description:
                print(f"📝 {description}")
            print("=" * 70)
        
        @staticmethod
        def print_test_result(test_name, success, details=""):
            status = "✅ PASSED" if success else "❌ FAILED"
            print(f"{status} {test_name}")
            if details:
                print(f"   {details}")

def run_test_suite(suite_name, module_name):
    """Run a specific test suite"""
    TestReporter.print_test_header(f"Running {suite_name}")
    
    try:
        # Import the test module
        module = __import__(f"tests.{module_name}", fromlist=[module_name])
        
        # Get the main test function
        if hasattr(module, f"run_all_{module_name.split('_')[1]}_tests"):
            test_function = getattr(module, f"run_all_{module_name.split('_')[1]}_tests")
            success = test_function()
        else:
            # Fallback to unittest discovery
            loader = unittest.TestLoader()
            suite = loader.loadTestsFromModule(module)
            runner = unittest.TextTestRunner(verbosity=1)
            result = runner.run(suite)
            success = result.wasSuccessful()
        
        TestReporter.print_test_result(suite_name, success)
        return success
        
    except Exception as e:
        TestReporter.print_test_result(suite_name, False, f"Error: {e}")
        return False

def run_existing_tests():
    """Run existing tests in tests/ directory"""
    TestReporter.print_test_header("Running Existing Tests")
    
    # Discover and run existing tests
    loader = unittest.TestLoader()
    start_dir = Path(__file__).parent
    
    # Load tests but exclude our new consolidated suites to avoid duplicates
    exclude_patterns = [
        'test_validation_suite.py',
        'test_extraction_suite.py', 
        'test_import_suite.py',
        'test_manual_suite.py',
        'run_all_tests.py'
    ]
    
    suite = unittest.TestSuite()
    
    for test_file in start_dir.glob('test_*.py'):
        if test_file.name not in exclude_patterns:
            try:
                module_name = test_file.stem
                module = __import__(f"tests.{module_name}", fromlist=[module_name])
                test_suite = loader.loadTestsFromModule(module)
                suite.addTest(test_suite)
            except Exception as e:
                print(f"Warning: Could not load {test_file.name}: {e}")
    
    if suite.countTestCases() > 0:
        runner = unittest.TextTestRunner(verbosity=1)
        result = runner.run(suite)
        success = result.wasSuccessful()
        TestReporter.print_test_result("Existing Tests", success, 
                                     f"Tests: {result.testsRun}, Failures: {len(result.failures)}, Errors: {len(result.errors)}")
        return success
    else:
        TestReporter.print_test_result("Existing Tests", True, "No existing tests to run")
        return True

def main():
    """Main test runner"""
    start_time = time.time()
    
    TestReporter.print_test_header("Unified Test Runner", "Running all consolidated test suites")
    
    # Test suites to run
    test_suites = [
        ("Validation Suite", "test_validation_suite"),
        ("Extraction Suite", "test_extraction_suite"),
        ("Import Suite", "test_import_suite"),
        ("Manual Suite", "test_manual_suite")
    ]
    
    results = []
    
    # Run consolidated test suites
    for suite_name, module_name in test_suites:
        success = run_test_suite(suite_name, module_name)
        results.append((suite_name, success))
    
    # Run existing tests
    existing_success = run_existing_tests()
    results.append(("Existing Tests", existing_success))
    
    # Print final summary
    end_time = time.time()
    total_time = end_time - start_time
    
    TestReporter.print_test_header("Final Test Summary")
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for suite_name, success in results:
        status = "✅" if success else "❌"
        print(f"{status} {suite_name}")
    
    print(f"\n📊 Overall Results:")
    print(f"✅ Passed: {passed}/{total}")
    print(f"❌ Failed: {total - passed}/{total}")
    print(f"⏱️ Total time: {total_time:.2f} seconds")
    print(f"📈 Success rate: {(passed/total)*100:.1f}%")
    
    # Exit with appropriate code
    all_passed = passed == total
    if all_passed:
        print("\n🎉 All test suites passed!")
    else:
        print(f"\n⚠️ {total - passed} test suite(s) failed")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)