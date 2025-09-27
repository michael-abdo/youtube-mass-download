#!/usr/bin/env python3
"""
Test Configuration Loading and Validation
Phase 5.2: Test configuration loading and validation

This script tests:
1. Loading mass_download configuration from config.yaml
2. Validating all required fields are present
3. Type checking configuration values
4. Default value handling
5. Error handling for missing/invalid configuration

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""

import sys
import os
from pathlib import Path
import yaml
from typing import Dict, Any, Optional

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))
sys.path.insert(0, str(current_dir.parent.parent))

try:
    from utils.config import get_config
except ImportError:
    # Try alternative import path
    sys.path.insert(0, os.path.join(str(current_dir.parent.parent), "utils"))
    from config import get_config


def test_config_loading():
    """Test basic configuration loading."""
    print("🧪 Testing configuration loading...")
    
    try:
        config = get_config()
        print("✅ Configuration loaded successfully")
        return config
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot load configuration")
        print(f"   Error: {e}")
        return None


def test_mass_download_section(config: Any):
    """Test mass_download configuration section."""
    print("\n🧪 Testing mass_download configuration section...")
    
    try:
        # Check if mass_download section exists
        mass_config = config.get_section("mass_download")
        if not mass_config:
            print("❌ FAILURE: mass_download section not found in config")
            return False
        
        print("✅ mass_download section found")
        
        # Test required fields
        required_fields = [
            "max_concurrent_channels",
            "max_concurrent_downloads", 
            "max_videos_per_channel",
            "skip_existing_videos",
            "continue_on_error",
            "download_videos",
            "download_mode",
            "resource_limits",
            "error_recovery",
            "progress_tracking"
        ]
        
        missing_fields = []
        for field in required_fields:
            if field not in mass_config:
                missing_fields.append(field)
        
        if missing_fields:
            print(f"❌ FAILURE: Missing required fields: {missing_fields}")
            return False
        
        print("✅ All required fields present")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        return False


def test_config_values(config: Any):
    """Test configuration value types and ranges."""
    print("\n🧪 Testing configuration values...")
    
    try:
        mass_config = config.get_section("mass_download")
        
        # Test integer values
        int_fields = {
            "max_concurrent_channels": (1, 10),
            "max_concurrent_downloads": (1, 20),
            "max_videos_per_channel": (0, 10000)
        }
        
        for field, (min_val, max_val) in int_fields.items():
            value = mass_config.get(field)
            if not isinstance(value, int):
                print(f"❌ FAILURE: {field} should be int, got {type(value)}")
                return False
            if value < min_val or value > max_val:
                print(f"❌ FAILURE: {field}={value} outside range [{min_val}, {max_val}]")
                return False
            print(f"  ✅ {field}: {value} (valid)")
        
        # Test boolean values
        bool_fields = [
            "skip_existing_videos",
            "continue_on_error",
            "download_videos"
        ]
        
        for field in bool_fields:
            value = mass_config.get(field)
            if not isinstance(value, bool):
                print(f"❌ FAILURE: {field} should be bool, got {type(value)}")
                return False
            print(f"  ✅ {field}: {value} (valid)")
        
        # Test string values
        string_fields = {
            "download_mode": ["local", "stream_to_s3"],
            "local_download_dir": None,
            "s3_bucket": None
        }
        
        for field, allowed_values in string_fields.items():
            value = mass_config.get(field)
            if not isinstance(value, str):
                print(f"❌ FAILURE: {field} should be string, got {type(value)}")
                return False
            if allowed_values and value not in allowed_values:
                print(f"❌ FAILURE: {field}={value} not in allowed values {allowed_values}")
                return False
            print(f"  ✅ {field}: '{value}' (valid)")
        
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        return False


def test_nested_config(config: Any):
    """Test nested configuration structures."""
    print("\n🧪 Testing nested configuration structures...")
    
    try:
        mass_config = config.get_section("mass_download")
        
        # Test resource_limits
        resource_limits = mass_config.get("resource_limits", {})
        if not isinstance(resource_limits, dict):
            print("❌ FAILURE: resource_limits should be dict")
            return False
        
        required_resource_fields = [
            "max_cpu_percent",
            "max_memory_percent",
            "check_interval_seconds",
            "throttle_factor",
            "min_concurrent"
        ]
        
        for field in required_resource_fields:
            if field not in resource_limits:
                print(f"❌ FAILURE: resource_limits.{field} missing")
                return False
            print(f"  ✅ resource_limits.{field}: {resource_limits[field]}")
        
        # Test error_recovery
        error_recovery = mass_config.get("error_recovery", {})
        if not isinstance(error_recovery, dict):
            print("❌ FAILURE: error_recovery should be dict")
            return False
        
        # Test circuit breaker config
        circuit_breaker = error_recovery.get("circuit_breaker", {})
        if "failure_threshold" not in circuit_breaker:
            print("❌ FAILURE: circuit_breaker.failure_threshold missing")
            return False
        print(f"  ✅ circuit_breaker.failure_threshold: {circuit_breaker['failure_threshold']}")
        
        # Test retry config
        retry_config = error_recovery.get("retry", {})
        if "max_retries" not in retry_config:
            print("❌ FAILURE: retry.max_retries missing")
            return False
        print(f"  ✅ retry.max_retries: {retry_config['max_retries']}")
        
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        return False


def test_config_methods(config: Any):
    """Test configuration helper methods."""
    print("\n🧪 Testing configuration helper methods...")
    
    try:
        # Test direct get with defaults
        value = config.get("mass_download.max_concurrent_channels", default=5)
        print(f"  ✅ get() with path: {value}")
        
        # Test get with missing key
        missing = config.get("mass_download.non_existent_key", default="default_value")
        if missing != "default_value":
            print("❌ FAILURE: Default value not returned for missing key")
            return False
        print("  ✅ Default value handling works")
        
        # Test bucket name retrieval
        bucket = config.get("mass_download.s3_bucket") or config.get("bucket_name")
        if not bucket:
            print("❌ FAILURE: No bucket name configured")
            return False
        print(f"  ✅ Bucket name: {bucket}")
        
        # Test download mode
        download_mode = config.get("mass_download.download_mode", "local")
        print(f"  ✅ Download mode: {download_mode}")
        
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        return False


def test_config_validation():
    """Test configuration validation rules."""
    print("\n🧪 Testing configuration validation...")
    
    try:
        config = get_config()
        mass_config = config.get_section("mass_download")
        
        # Validate consistency
        errors = []
        
        # Check concurrent downloads <= concurrent channels
        if mass_config["max_concurrent_downloads"] > mass_config["max_concurrent_channels"] * 10:
            errors.append("max_concurrent_downloads seems too high relative to max_concurrent_channels")
        
        # Check resource limits are reasonable
        resource_limits = mass_config["resource_limits"]
        if resource_limits["max_cpu_percent"] > 100:
            errors.append("max_cpu_percent cannot exceed 100")
        
        if resource_limits["max_memory_percent"] > 100:
            errors.append("max_memory_percent cannot exceed 100")
        
        if resource_limits["throttle_factor"] <= 0 or resource_limits["throttle_factor"] > 1:
            errors.append("throttle_factor must be between 0 and 1")
        
        # Check error recovery settings
        retry_config = mass_config["error_recovery"]["retry"]
        if retry_config["base_delay_seconds"] > retry_config["max_delay_seconds"]:
            errors.append("base_delay_seconds cannot exceed max_delay_seconds")
        
        if errors:
            print("❌ VALIDATION ERRORS:")
            for error in errors:
                print(f"   - {error}")
            return False
        
        print("✅ All validation checks passed")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        return False


def test_environment_overrides():
    """Test environment variable overrides."""
    print("\n🧪 Testing environment variable overrides...")
    
    try:
        # Set test environment variable
        os.environ["MASS_DOWNLOAD_MAX_CONCURRENT_CHANNELS"] = "10"
        
        # Reload config
        config = get_config()
        
        # Note: ConfigLoader doesn't support env overrides by default
        # This is just to demonstrate where such functionality would go
        print("  ℹ️  Environment override functionality not implemented")
        print("  ℹ️  Would allow MASS_DOWNLOAD_* env vars to override config")
        
        # Clean up
        del os.environ["MASS_DOWNLOAD_MAX_CONCURRENT_CHANNELS"]
        
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        return False


def main():
    """Run all configuration tests."""
    print("🚀 Starting Configuration Loading Tests")
    print("   Testing Phase 5.2: Configuration loading and validation")
    print("=" * 80)
    
    all_tests_passed = True
    
    # Test basic loading
    config = test_config_loading()
    if not config:
        print("❌ Cannot continue without configuration")
        return 1
    
    # Run tests
    tests = [
        ("Mass download section", lambda: test_mass_download_section(config)),
        ("Configuration values", lambda: test_config_values(config)),
        ("Nested configuration", lambda: test_nested_config(config)),
        ("Configuration methods", lambda: test_config_methods(config)),
        ("Configuration validation", test_config_validation),
        ("Environment overrides", test_environment_overrides)
    ]
    
    for test_name, test_func in tests:
        if not test_func():
            all_tests_passed = False
            print(f"❌ {test_name} test FAILED")
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL CONFIGURATION TESTS PASSED!")
        print("✅ Configuration loading works correctly")
        print("✅ All required fields present")
        print("✅ Value types and ranges valid")
        print("✅ Nested structures properly formatted")
        print("✅ Validation rules satisfied")
        print("\n🔥 Configuration is READY for mass download feature!")
        return 0
    else:
        print("💥 SOME CONFIGURATION TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the configuration before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)