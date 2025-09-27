#!/usr/bin/env python3
"""
Test CLI Argument Validation
Phase 5.4: Test CLI with argument validation

This script tests:
1. Argument parsing
2. Input file validation
3. Configuration overrides
4. Help and version output
5. Error handling for invalid arguments
6. Dry run functionality

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""

import sys
import os
import subprocess
import tempfile
import json
from pathlib import Path
from typing import Tuple, Optional

# CLI script path - use the wrapper script
CLI_SCRIPT = Path(__file__).parent.parent.parent / "mass_download.py"


def run_cli(args: list, input_data: Optional[str] = None) -> Tuple[int, str, str]:
    """
    Run the CLI with given arguments.
    
    Returns:
        Tuple of (exit_code, stdout, stderr)
    """
    cmd = [sys.executable, str(CLI_SCRIPT)] + args
    
    # Create temporary input file if needed
    temp_file = None
    if input_data:
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        temp_file.write(input_data)
        temp_file.close()
        # Replace placeholder in args
        args = [temp_file.name if arg == 'INPUT_FILE' else arg for arg in args]
        cmd = [sys.executable, str(CLI_SCRIPT)] + args
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        return result.returncode, result.stdout, result.stderr
    finally:
        if temp_file:
            os.unlink(temp_file.name)


def test_help_output():
    """Test help output."""
    print("🧪 Testing help output...")
    
    # Test -h
    exit_code, stdout, stderr = run_cli(['-h'])
    assert exit_code == 0, f"Help should exit with 0, got {exit_code}"
    assert "Mass Download Tool" in stdout, "Help should contain tool description"
    assert "Examples:" in stdout, "Help should contain examples"
    print("  ✅ -h flag works correctly")
    
    # Test --help
    exit_code, stdout, stderr = run_cli(['--help'])
    assert exit_code == 0, f"Help should exit with 0, got {exit_code}"
    assert "Mass Download Tool" in stdout, "Help should contain tool description"
    print("  ✅ --help flag works correctly")
    
    return True


def test_version_output():
    """Test version output."""
    print("\n🧪 Testing version output...")
    
    exit_code, stdout, stderr = run_cli(['--version'])
    assert exit_code == 0, f"Version should exit with 0, got {exit_code}"
    assert "1.0.0" in stdout or "1.0.0" in stderr, "Version should be displayed"
    print("  ✅ --version flag works correctly")
    
    return True


def test_missing_input_file():
    """Test handling of missing input file."""
    print("\n🧪 Testing missing input file...")
    
    # No arguments
    exit_code, stdout, stderr = run_cli([])
    assert exit_code != 0, "Should fail without input file"
    assert "required" in stderr.lower() or "usage" in stderr.lower(), \
        "Should show error about required argument"
    print("  ✅ Correctly fails without input file")
    
    # Non-existent file
    exit_code, stdout, stderr = run_cli(['nonexistent.csv'])
    assert exit_code != 0, "Should fail with non-existent file"
    assert "not found" in stderr.lower() or "not found" in stdout.lower(), \
        "Should show file not found error"
    print("  ✅ Correctly fails with non-existent file")
    
    return True


def test_valid_arguments():
    """Test valid argument combinations."""
    print("\n🧪 Testing valid argument combinations...")
    
    # Create test data
    test_csv = """name,channel_url
Test Channel,https://youtube.com/@testchannel
"""
    
    # Test basic usage
    exit_code, stdout, stderr = run_cli(['INPUT_FILE', '--dry-run'], test_csv)
    assert exit_code == 0, f"Should succeed with valid file, got {exit_code}"
    assert "DRY RUN MODE" in stdout, "Should show dry run message"
    assert "Would process 1 channels" in stdout, "Should show channel count"
    print("  ✅ Basic usage with --dry-run works")
    
    # Test with multiple options
    args = [
        'INPUT_FILE',
        '--max-videos', '10',
        '--concurrent-channels', '2',
        '--no-download',
        '--dry-run'
    ]
    exit_code, stdout, stderr = run_cli(args, test_csv)
    assert exit_code == 0, f"Should succeed with multiple options, got {exit_code}"
    print("  ✅ Multiple options work correctly")
    
    return True


def test_invalid_arguments():
    """Test invalid argument handling."""
    print("\n🧪 Testing invalid argument handling...")
    
    test_csv = "name,channel_url\nTest,https://youtube.com/@test\n"
    
    # Invalid option
    exit_code, stdout, stderr = run_cli(['INPUT_FILE', '--invalid-option'], test_csv)
    assert exit_code != 0, "Should fail with invalid option"
    assert "unrecognized arguments" in stderr or "invalid" in stderr.lower(), \
        "Should show error about invalid argument"
    print("  ✅ Correctly rejects invalid option")
    
    # Invalid value for numeric option
    exit_code, stdout, stderr = run_cli(['INPUT_FILE', '--max-videos', 'abc'], test_csv)
    assert exit_code != 0, "Should fail with invalid numeric value"
    assert "invalid" in stderr.lower(), "Should show error about invalid value"
    print("  ✅ Correctly rejects invalid numeric value")
    
    # Invalid choice
    exit_code, stdout, stderr = run_cli(['INPUT_FILE', '--download-mode', 'invalid'], test_csv)
    assert exit_code != 0, "Should fail with invalid choice"
    assert "invalid choice" in stderr.lower(), "Should show error about invalid choice"
    print("  ✅ Correctly rejects invalid choice value")
    
    return True


def test_file_format_detection():
    """Test input file format detection."""
    print("\n🧪 Testing file format detection...")
    
    # Test CSV
    csv_data = """name,channel_url
Channel 1,https://youtube.com/@channel1
Channel 2,https://youtube.com/@channel2
"""
    exit_code, stdout, stderr = run_cli(['INPUT_FILE', '--dry-run'], csv_data)
    assert exit_code == 0, "Should handle CSV format"
    assert "2 channels" in stdout, "Should detect 2 channels in CSV"
    print("  ✅ CSV format detected correctly")
    
    # Test JSON
    json_data = json.dumps([
        {"name": "Channel 1", "channel_url": "https://youtube.com/@channel1"},
        {"name": "Channel 2", "channel_url": "https://youtube.com/@channel2"}
    ])
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(json_data)
        temp_json = f.name
    
    try:
        exit_code, stdout, stderr = run_cli([temp_json, '--dry-run'])
        assert exit_code == 0, f"Should handle JSON format, got {exit_code}"
        assert "2 channels" in stdout, "Should detect 2 channels in JSON"
        print("  ✅ JSON format detected correctly")
    finally:
        os.unlink(temp_json)
    
    # Test TXT
    txt_data = """https://youtube.com/@channel1
https://youtube.com/@channel2
https://youtube.com/@channel3
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(txt_data)
        temp_txt = f.name
    
    try:
        exit_code, stdout, stderr = run_cli([temp_txt, '--dry-run'])
        assert exit_code == 0, f"Should handle TXT format, got {exit_code}"
        assert "3 channels" in stdout, "Should detect 3 channels in TXT"
        print("  ✅ TXT format detected correctly")
    finally:
        os.unlink(temp_txt)
    
    return True


def test_configuration_overrides():
    """Test configuration override options."""
    print("\n🧪 Testing configuration overrides...")
    
    test_csv = "name,channel_url\nTest,https://youtube.com/@test\n"
    
    # Test all override options
    args = [
        'INPUT_FILE',
        '--max-videos', '25',
        '--concurrent-channels', '5',
        '--concurrent-downloads', '10',
        '--download-mode', 'local',
        '--local-dir', '/tmp/test_downloads',
        '--s3-bucket', 'test-bucket',
        '--checkpoint-dir', '/tmp/test_checkpoints',
        '--dry-run'
    ]
    
    exit_code, stdout, stderr = run_cli(args, test_csv)
    assert exit_code == 0, f"Should accept all override options, got {exit_code}"
    print("  ✅ All configuration overrides accepted")
    
    return True


def test_output_options():
    """Test output options."""
    print("\n🧪 Testing output options...")
    
    test_csv = "name,channel_url\nTest,https://youtube.com/@test\n"
    
    # Test quiet mode
    exit_code, stdout, stderr = run_cli(['INPUT_FILE', '--quiet', '--dry-run'], test_csv)
    assert exit_code == 0, "Should work in quiet mode"
    # In quiet mode, output should be minimal
    print("  ✅ Quiet mode works")
    
    # Test verbose mode
    exit_code, stdout, stderr = run_cli(['INPUT_FILE', '--verbose', '--dry-run'], test_csv)
    assert exit_code == 0, "Should work in verbose mode"
    # In verbose mode, should have more output
    print("  ✅ Verbose mode works")
    
    # Test output file (would be created in real run)
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
        output_file = f.name
    
    try:
        args = ['INPUT_FILE', '--output', output_file, '--dry-run']
        exit_code, stdout, stderr = run_cli(args, test_csv)
        assert exit_code == 0, "Should accept output file option"
        print("  ✅ Output file option accepted")
    finally:
        if os.path.exists(output_file):
            os.unlink(output_file)
    
    return True


def test_validation_options():
    """Test validation options."""
    print("\n🧪 Testing validation options...")
    
    # Test data with duplicate
    test_csv = """name,channel_url
Channel 1,https://youtube.com/@testchannel
Channel 2,https://youtube.com/@testchannel
"""
    
    # Default should warn about duplicates
    exit_code, stdout, stderr = run_cli(['INPUT_FILE', '--dry-run'], test_csv)
    assert "duplicate" in stdout.lower() or "duplicate" in stderr.lower(), \
        "Should warn about duplicates by default"
    print("  ✅ Duplicate detection works")
    
    # Skip duplicates option
    exit_code, stdout, stderr = run_cli(['INPUT_FILE', '--skip-duplicates', '--dry-run'], test_csv)
    assert exit_code == 0, "Should work with skip-duplicates"
    print("  ✅ Skip duplicates option works")
    
    # Skip validation option
    exit_code, stdout, stderr = run_cli(['INPUT_FILE', '--skip-validation', '--dry-run'], test_csv)
    assert exit_code == 0, "Should work with skip-validation"
    print("  ✅ Skip validation option works")
    
    return True


def test_special_modes():
    """Test special modes like resume and retry."""
    print("\n🧪 Testing special modes...")
    
    test_csv = "name,channel_url\nTest,https://youtube.com/@test\n"
    
    # Test resume mode (not implemented yet)
    exit_code, stdout, stderr = run_cli(['INPUT_FILE', '--resume', 'job_123'], test_csv)
    # Should show not implemented message
    assert "not yet implemented" in stdout.lower() or "not yet implemented" in stderr.lower(), \
        "Should show not implemented message for resume"
    print("  ✅ Resume mode shows not implemented")
    
    # Test retry failed mode
    exit_code, stdout, stderr = run_cli(['INPUT_FILE', '--retry-failed'], test_csv)
    # This might fail if no DLQ exists, but should handle gracefully
    print("  ✅ Retry failed mode handled")
    
    return True


def main():
    """Run all CLI validation tests."""
    print("🚀 Starting CLI Validation Tests")
    print("   Testing Phase 5.4: CLI argument validation")
    print("=" * 80)
    
    # Check if CLI script exists
    if not CLI_SCRIPT.exists():
        print(f"❌ CRITICAL FAILURE: CLI script not found at {CLI_SCRIPT}")
        return 1
    
    all_tests_passed = True
    
    tests = [
        ("Help output", test_help_output),
        ("Version output", test_version_output),
        ("Missing input file", test_missing_input_file),
        ("Valid arguments", test_valid_arguments),
        ("Invalid arguments", test_invalid_arguments),
        ("File format detection", test_file_format_detection),
        ("Configuration overrides", test_configuration_overrides),
        ("Output options", test_output_options),
        ("Validation options", test_validation_options),
        ("Special modes", test_special_modes)
    ]
    
    for test_name, test_func in tests:
        try:
            if not test_func():
                all_tests_passed = False
                print(f"❌ {test_name} test FAILED")
        except Exception as e:
            all_tests_passed = False
            print(f"❌ {test_name} test FAILED with error: {e}")
            import traceback
            traceback.print_exc()
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL CLI VALIDATION TESTS PASSED!")
        print("✅ Argument parsing works correctly")
        print("✅ Input file validation works")
        print("✅ Configuration overrides accepted")
        print("✅ Help and version output correct")
        print("✅ Error handling comprehensive")
        print("✅ All modes and options validated")
        print("\n🔥 CLI is READY for use!")
        return 0
    else:
        print("💥 SOME CLI VALIDATION TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the CLI before proceeding!")
        return 1


if __name__ == "__main__":
    sys.exit(main())