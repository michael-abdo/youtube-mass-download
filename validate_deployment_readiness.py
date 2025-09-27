#!/usr/bin/env python3
"""
Deployment Readiness Validation Script
Phase 6.10: Final validation and deployment readiness check

This script performs comprehensive validation of the mass download feature
to ensure it's ready for production deployment.
"""
import os
import sys
import tempfile
import shutil
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Tuple

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Test imports
try:
    # Try to import the modules
    import mass_download.mass_coordinator
    import mass_download.input_handler
    import mass_download.channel_discovery
    import mass_download.database_operations_ext
    import mass_download.progress_monitor
    import mass_download.concurrent_processor
    import mass_download.error_recovery
    IMPORTS_SUCCESSFUL = True
    IMPORT_ERROR = None
except ImportError as e:
    IMPORTS_SUCCESSFUL = False
    IMPORT_ERROR = str(e)


class DeploymentValidator:
    """Comprehensive deployment readiness validation."""
    
    def __init__(self):
        self.results = []
        self.temp_dir = None
        self.start_time = datetime.now()
        
    def log_result(self, test_name: str, passed: bool, details: str = ""):
        """Log a test result."""
        status = "✓ PASS" if passed else "✗ FAIL"
        self.results.append({
            'test_name': test_name,
            'status': status,
            'passed': passed,
            'details': details,
            'timestamp': datetime.now().isoformat()
        })
        print(f"{status}: {test_name}")
        if details and not passed:
            print(f"   Details: {details}")
            
    def validate_imports(self) -> bool:
        """Validate all required imports are working."""
        print("\n=== Import Validation ===")
        
        if IMPORTS_SUCCESSFUL:
            self.log_result("Module imports", True, "All core modules imported successfully")
            return True
        else:
            self.log_result("Module imports", False, f"Import error: {IMPORT_ERROR}")
            return False
    
    def validate_examples(self) -> bool:
        """Validate example files exist and are valid."""
        print("\n=== Example Files Validation ===")
        
        examples_dir = Path(__file__).parent / "examples"
        required_files = [
            "channels.csv",
            "channels.json", 
            "channels.txt",
            "USAGE_EXAMPLES.md",
            "config_examples.yaml"
        ]
        
        all_valid = True
        
        for file_name in required_files:
            file_path = examples_dir / file_name
            if file_path.exists():
                try:
                    content = file_path.read_text()
                    if len(content.strip()) > 0:
                        self.log_result(f"Example file {file_name}", True, f"File exists and has content ({len(content)} chars)")
                    else:
                        self.log_result(f"Example file {file_name}", False, "File exists but is empty")
                        all_valid = False
                except Exception as e:
                    self.log_result(f"Example file {file_name}", False, f"Error reading file: {e}")
                    all_valid = False
            else:
                self.log_result(f"Example file {file_name}", False, "File does not exist")
                all_valid = False
        
        return all_valid
    
    def validate_documentation(self) -> bool:
        """Validate documentation files exist and are comprehensive."""
        print("\n=== Documentation Validation ===")
        
        base_dir = Path(__file__).parent
        doc_files = [
            "MASS_DOWNLOAD_README.md",
            "examples/USAGE_EXAMPLES.md"
        ]
        
        all_valid = True
        
        for doc_file in doc_files:
            file_path = base_dir / doc_file
            if file_path.exists():
                try:
                    content = file_path.read_text()
                    if len(content) > 1000:  # Substantial documentation
                        self.log_result(f"Documentation {doc_file}", True, f"Comprehensive documentation ({len(content)} chars)")
                    else:
                        self.log_result(f"Documentation {doc_file}", False, f"Documentation too brief ({len(content)} chars)")
                        all_valid = False
                except Exception as e:
                    self.log_result(f"Documentation {doc_file}", False, f"Error reading file: {e}")
                    all_valid = False
            else:
                self.log_result(f"Documentation {doc_file}", False, "Documentation file missing")
                all_valid = False
                
        return all_valid
    
    def validate_configuration(self) -> bool:
        """Validate configuration files and settings."""
        print("\n=== Configuration Validation ===")
        
        config_file = Path(__file__).parent / "config" / "config.yaml"
        
        if not config_file.exists():
            self.log_result("Configuration file", False, "config/config.yaml does not exist")
            return False
        
        try:
            import yaml
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            if 'mass_download' in config:
                mass_config = config['mass_download']
                required_keys = [
                    'max_concurrent_channels',
                    'max_concurrent_downloads',
                    'max_videos_per_channel',
                    'continue_on_error',
                    'download_videos'
                ]
                
                missing_keys = [key for key in required_keys if key not in mass_config]
                if missing_keys:
                    self.log_result("Configuration completeness", False, f"Missing keys: {missing_keys}")
                    return False
                else:
                    self.log_result("Configuration completeness", True, "All required configuration keys present")
                    return True
            else:
                self.log_result("Mass download configuration", False, "mass_download section missing from config")
                return False
                
        except Exception as e:
            self.log_result("Configuration parsing", False, f"Error parsing config: {e}")
            return False
    
    def validate_basic_functionality(self) -> bool:
        """Test basic functionality without external dependencies."""
        print("\n=== Basic Functionality Validation ===")
        
        if not IMPORTS_SUCCESSFUL:
            self.log_result("Basic functionality", False, "Cannot test due to import failures")
            return False
        
        try:
            # Test input handler
            from mass_download.input_handler import InputHandler
            handler = InputHandler()
            self.log_result("Input handler creation", True)
            
            # Test progress monitor
            from mass_download.progress_monitor import ProgressMonitor
            monitor = ProgressMonitor()
            self.log_result("Progress monitor creation", True)
            
            return True
            
        except Exception as e:
            self.log_result("Basic functionality", False, f"Error testing basic functionality: {e}")
            return False
    
    def validate_example_parsing(self) -> bool:
        """Test parsing of example input files."""
        print("\n=== Example File Parsing Validation ===")
        
        if not IMPORTS_SUCCESSFUL:
            self.log_result("Example parsing", False, "Cannot test due to import failures")
            return False
        
        examples_dir = Path(__file__).parent / "examples"
        
        try:
            from mass_download.input_handler import InputHandler
            handler = InputHandler()
        except ImportError as e:
            self.log_result("Example parsing", False, f"Cannot import InputHandler: {e}")
            return False
            
        all_valid = True
        
        # Test CSV parsing
        csv_file = examples_dir / "channels.csv"
        if csv_file.exists():
            try:
                channels = handler.parse_file(str(csv_file))
                if len(channels) > 0:
                    self.log_result("CSV parsing", True, f"Parsed {len(channels)} channels from CSV")
                else:
                    self.log_result("CSV parsing", False, "CSV file parsed but no channels found")
                    all_valid = False
            except Exception as e:
                self.log_result("CSV parsing", False, f"Error parsing CSV: {e}")
                all_valid = False
        else:
            self.log_result("CSV parsing", False, "CSV example file not found")
            all_valid = False
        
        # Test JSON parsing
        json_file = examples_dir / "channels.json"
        if json_file.exists():
            try:
                channels = handler.parse_file(str(json_file))
                if len(channels) > 0:
                    self.log_result("JSON parsing", True, f"Parsed {len(channels)} channels from JSON")
                else:
                    self.log_result("JSON parsing", False, "JSON file parsed but no channels found")
                    all_valid = False
            except Exception as e:
                self.log_result("JSON parsing", False, f"Error parsing JSON: {e}")
                all_valid = False
        else:
            self.log_result("JSON parsing", False, "JSON example file not found")
            all_valid = False
        
        # Test TXT parsing
        txt_file = examples_dir / "channels.txt"
        if txt_file.exists():
            try:
                channels = handler.parse_file(str(txt_file))
                if len(channels) > 0:
                    self.log_result("TXT parsing", True, f"Parsed {len(channels)} channels from TXT")
                else:
                    self.log_result("TXT parsing", False, "TXT file parsed but no channels found")
                    all_valid = False
            except Exception as e:
                self.log_result("TXT parsing", False, f"Error parsing TXT: {e}")
                all_valid = False
        else:
            self.log_result("TXT parsing", False, "TXT example file not found")
            all_valid = False
        
        return all_valid
    
    def validate_test_suite(self) -> bool:
        """Run key test files to validate functionality.""" 
        print("\n=== Test Suite Validation ===")
        
        test_files = [
            "mass_download/test_channel_discovery_basic.py",
            "mass_download/test_input_handler.py",
            "mass_download/test_performance.py"
        ]
        
        all_passed = True
        
        for test_file in test_files:
            test_path = Path(__file__).parent / test_file
            if test_path.exists():
                try:
                    # Run the test file
                    result = subprocess.run(
                        [sys.executable, str(test_path)],
                        capture_output=True,
                        text=True,
                        timeout=60
                    )
                    
                    if result.returncode == 0:
                        self.log_result(f"Test suite {test_file}", True, "All tests passed")
                    else:
                        self.log_result(f"Test suite {test_file}", False, f"Tests failed with return code {result.returncode}")
                        all_passed = False
                        
                except subprocess.TimeoutExpired:
                    self.log_result(f"Test suite {test_file}", False, "Test timed out")
                    all_passed = False
                except Exception as e:
                    self.log_result(f"Test suite {test_file}", False, f"Error running test: {e}")
                    all_passed = False
            else:
                self.log_result(f"Test suite {test_file}", False, "Test file not found")
                all_passed = False
        
        return all_passed
    
    def validate_cli_scripts(self) -> bool:
        """Validate CLI scripts are accessible and functional."""
        print("\n=== CLI Scripts Validation ===")
        
        scripts = [
            "run_mass_download.py",
            "mass_download_cli.py"
        ]
        
        all_valid = True
        
        for script in scripts:
            script_path = Path(__file__).parent / script
            if script_path.exists():
                try:
                    # Test script is parseable Python
                    with open(script_path, 'r') as f:
                        content = f.read()
                    
                    # Basic syntax check
                    compile(content, script_path, 'exec')
                    
                    self.log_result(f"CLI script {script}", True, "Script exists and is valid Python")
                    
                except SyntaxError as e:
                    self.log_result(f"CLI script {script}", False, f"Syntax error: {e}")
                    all_valid = False
                except Exception as e:
                    self.log_result(f"CLI script {script}", False, f"Error validating script: {e}")
                    all_valid = False
            else:
                self.log_result(f"CLI script {script}", False, "Script file not found")
                all_valid = False
        
        return all_valid
    
    def validate_dependencies(self) -> bool:
        """Validate required dependencies are available."""
        print("\n=== Dependencies Validation ===")
        
        required_deps = [
            'yaml',
            'psutil',
            'sqlite3'
        ]
        
        all_available = True
        
        for dep in required_deps:
            try:
                __import__(dep)
                self.log_result(f"Dependency {dep}", True, "Available")
            except ImportError:
                self.log_result(f"Dependency {dep}", False, "Not available")
                all_available = False
        
        # Check yt-dlp availability
        try:
            result = subprocess.run(['yt-dlp', '--version'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                version = result.stdout.strip()
                self.log_result("yt-dlp availability", True, f"Version: {version}")
            else:
                self.log_result("yt-dlp availability", False, "yt-dlp command failed")
                all_available = False
        except Exception as e:
            self.log_result("yt-dlp availability", False, f"Error checking yt-dlp: {e}")
            all_available = False
        
        return all_available
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive deployment readiness report."""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        passed_tests = [r for r in self.results if r['passed']]
        failed_tests = [r for r in self.results if not r['passed']]
        
        report = {
            'validation_summary': {
                'start_time': self.start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'total_tests': len(self.results),
                'passed_tests': len(passed_tests),
                'failed_tests': len(failed_tests),
                'success_rate': len(passed_tests) / len(self.results) * 100 if self.results else 0
            },
            'test_results': self.results,
            'deployment_ready': len(failed_tests) == 0,
            'recommendations': self._get_recommendations(failed_tests)
        }
        
        return report
    
    def _get_recommendations(self, failed_tests: List[Dict]) -> List[str]:
        """Get deployment recommendations based on failed tests."""
        recommendations = []
        
        if not failed_tests:
            recommendations.append("✓ All validation tests passed - feature is ready for deployment")
            recommendations.append("✓ Documentation and examples are comprehensive")
            recommendations.append("✓ Core functionality is working correctly")
            return recommendations
        
        # Categorize failures
        import_failures = [t for t in failed_tests if 'import' in t['test_name'].lower()]
        config_failures = [t for t in failed_tests if 'config' in t['test_name'].lower()]
        test_failures = [t for t in failed_tests if 'test' in t['test_name'].lower()]
        doc_failures = [t for t in failed_tests if 'doc' in t['test_name'].lower() or 'example' in t['test_name'].lower()]
        
        if import_failures:
            recommendations.append("⚠ Fix import issues before deployment")
            recommendations.append("  - Check Python path and module structure")
            recommendations.append("  - Ensure all required files are present")
        
        if config_failures:
            recommendations.append("⚠ Update configuration files")
            recommendations.append("  - Add missing configuration keys")
            recommendations.append("  - Validate YAML syntax")
        
        if test_failures:
            recommendations.append("⚠ Fix failing test suites")
            recommendations.append("  - Review test failures and fix underlying issues")
            recommendations.append("  - Ensure test environment is properly configured")
        
        if doc_failures:
            recommendations.append("⚠ Complete documentation and examples")
            recommendations.append("  - Add missing documentation files")
            recommendations.append("  - Ensure examples are valid and working")
        
        if len(failed_tests) > len(self.results) * 0.5:
            recommendations.append("🚨 MAJOR ISSUES: Over 50% of tests failed")
            recommendations.append("   - Significant work needed before deployment")
            recommendations.append("   - Consider staged rollout or additional testing")
        
        return recommendations
    
    def run_full_validation(self) -> Dict[str, Any]:
        """Run complete deployment readiness validation."""
        print("=" * 70)
        print("MASS DOWNLOAD FEATURE - DEPLOYMENT READINESS VALIDATION")
        print("=" * 70)
        print(f"Validation started at: {self.start_time}")
        
        # Run all validation checks
        self.validate_imports()
        self.validate_examples()
        self.validate_documentation()
        self.validate_configuration()
        self.validate_basic_functionality()
        self.validate_example_parsing()
        self.validate_test_suite()
        self.validate_cli_scripts()
        self.validate_dependencies()
        
        # Generate final report
        report = self.generate_report()
        
        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)
        
        summary = report['validation_summary']
        print(f"Total Tests: {summary['total_tests']}")
        print(f"Passed: {summary['passed_tests']}")
        print(f"Failed: {summary['failed_tests']}")
        print(f"Success Rate: {summary['success_rate']:.1f}%")
        print(f"Duration: {summary['duration_seconds']:.2f} seconds")
        
        print(f"\nDeployment Ready: {'✓ YES' if report['deployment_ready'] else '✗ NO'}")
        
        print("\nRecommendations:")
        for rec in report['recommendations']:
            print(f"  {rec}")
        
        return report


def main():
    """Main validation function."""
    validator = DeploymentValidator()
    
    try:
        report = validator.run_full_validation()
        
        # Save report to file
        report_file = Path(__file__).parent / "deployment_readiness_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\nDetailed report saved to: {report_file}")
        
        # Exit with appropriate code
        sys.exit(0 if report['deployment_ready'] else 1)
        
    except KeyboardInterrupt:
        print("\n\nValidation interrupted by user")
        sys.exit(2)
    except Exception as e:
        print(f"\n\nValidation failed with error: {e}")
        sys.exit(3)


if __name__ == "__main__":
    main()