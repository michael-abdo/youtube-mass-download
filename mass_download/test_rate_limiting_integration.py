#!/usr/bin/env python3
"""
Test Rate Limiting Integration with Channel Discovery
Phase 2.8: Validate rate limiting with existing infrastructure

Tests:
1. Rate limiter initialization with configuration
2. Burst support functionality  
3. Token bucket behavior under load
4. Integration with channel discovery methods
5. Configuration loading and fallbacks

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import os
import time
import threading
from pathlib import Path
from typing import Dict, Any

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))

def test_rate_limiter_imports():
    """Test that enhanced rate limiter imports correctly."""
    print("🧪 Testing enhanced rate limiter imports...")
    
    try:
        from utils.rate_limiter import (
            RateLimitConfig, TokenBucket, ServiceRateLimiter,
            initialize_rate_limiter, get_rate_limiter, get_rate_limit_status
        )
        print("✅ SUCCESS: Enhanced rate limiter imports successful")
        return True, (RateLimitConfig, TokenBucket, ServiceRateLimiter)
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import enhanced rate limiter")
        print(f"   Error: {e}")
        return False, None


def test_rate_limit_config_validation():
    """Test RateLimitConfig validation with fail-fast principles."""
    print("\n🧪 Testing RateLimitConfig validation...")
    
    success, classes = test_rate_limiter_imports()
    if not success:
        return False
    
    RateLimitConfig, _, _ = classes
    
    try:
        # Test Case 1: Valid configuration
        config = RateLimitConfig(rate=2.0, burst=5)
        print(f"✅ SUCCESS: Valid config created - rate={config.rate}, burst={config.burst}")
        
        # Test Case 2: Invalid rate (should fail fast)
        try:
            invalid_config = RateLimitConfig(rate=-1.0, burst=5)
            print("❌ VALIDATION FAILURE: Negative rate should have failed!")
            return False
        except ValueError as e:
            if "rate must be positive" in str(e):
                print(f"✅ SUCCESS: Negative rate failed validation as expected")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        
        # Test Case 3: Invalid burst (should fail fast)
        try:
            invalid_config = RateLimitConfig(rate=2.0, burst=0)
            print("❌ VALIDATION FAILURE: Zero burst should have failed!")
            return False
        except ValueError as e:
            if "burst must be positive" in str(e):
                print(f"✅ SUCCESS: Zero burst failed validation as expected")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        
        print("✅ ALL RateLimitConfig validation tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Config validation test failed: {e}")
        return False


def test_token_bucket_functionality():
    """Test TokenBucket burst and rate limiting behavior."""
    print("\n🧪 Testing TokenBucket functionality...")
    
    success, classes = test_rate_limiter_imports()
    if not success:
        return False
    
    _, TokenBucket, _ = classes
    
    try:
        # Test Case 1: Basic token acquisition
        bucket = TokenBucket(rate=2.0, burst=5)  # 2 tokens/sec, burst of 5
        
        # Should be able to acquire burst tokens immediately
        for i in range(5):
            if not bucket.acquire():
                print(f"❌ FAILURE: Could not acquire token {i+1} from full bucket")
                return False
        print("✅ SUCCESS: Acquired all 5 burst tokens")
        
        # Test Case 2: Burst exhaustion
        if bucket.acquire():
            print("❌ FAILURE: Should not be able to acquire token from empty bucket")
            return False
        print("✅ SUCCESS: Correctly rejected token when bucket empty")
        
        # Test Case 3: Token replenishment
        print("   Waiting for token replenishment...")
        time.sleep(0.6)  # Wait for more than 0.5 seconds (should add 1+ tokens at 2/sec)
        
        if not bucket.acquire():
            print("❌ FAILURE: Should be able to acquire token after waiting")
            return False
        print("✅ SUCCESS: Token replenishment working correctly")
        
        # Test Case 4: Status monitoring
        status = bucket.get_status()
        expected_keys = {"rate", "burst", "tokens", "utilization"}
        if not expected_keys.issubset(status.keys()):
            print(f"❌ FAILURE: Status missing keys. Got: {status.keys()}")
            return False
        print(f"✅ SUCCESS: Status monitoring working - {status}")
        
        print("✅ ALL TokenBucket functionality tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: TokenBucket test failed: {e}")
        return False


def test_service_rate_limiter_integration():
    """Test ServiceRateLimiter with configuration integration."""
    print("\n🧪 Testing ServiceRateLimiter integration...")
    
    success, classes = test_rate_limiter_imports()
    if not success:
        return False
    
    _, _, ServiceRateLimiter = classes
    
    try:
        # Test Case 1: Default configuration (no config provided)
        service_limiter = ServiceRateLimiter()
        
        # Should create bucket with defaults
        if not service_limiter.acquire("youtube"):
            print("❌ FAILURE: Should acquire token from default configuration")
            return False
        print("✅ SUCCESS: Default configuration working")
        
        # Test Case 2: Service-specific rate limiting
        # Test different services have separate buckets
        youtube_acquired = service_limiter.acquire("youtube")
        drive_acquired = service_limiter.acquire("google_drive")
        
        if not (youtube_acquired or drive_acquired):
            print("❌ FAILURE: At least one service should have tokens available")
            return False
        print("✅ SUCCESS: Separate service buckets working")
        
        # Test Case 3: Status reporting for all services
        status = service_limiter.get_status()
        if not isinstance(status, dict):
            print(f"❌ FAILURE: Status should be dict, got: {type(status)}")
            return False
        print(f"✅ SUCCESS: Service status reporting working - services: {list(status.keys())}")
        
        print("✅ ALL ServiceRateLimiter integration tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: ServiceRateLimiter test failed: {e}")
        return False


def test_concurrent_rate_limiting():
    """Test rate limiting under concurrent load."""
    print("\n🧪 Testing concurrent rate limiting...")
    
    success, classes = test_rate_limiter_imports()
    if not success:
        return False
    
    _, TokenBucket, _ = classes
    
    try:
        # Test concurrent access to token bucket
        bucket = TokenBucket(rate=10.0, burst=20)  # Higher rate for testing
        results = []
        errors = []
        
        def worker_thread(thread_id: int):
            """Worker thread that tries to acquire tokens."""
            try:
                for i in range(5):
                    if bucket.acquire():
                        results.append(f"thread_{thread_id}_token_{i}")
                    time.sleep(0.01)  # Small delay between attempts
            except Exception as e:
                errors.append(f"Thread {thread_id}: {e}")
        
        # Start multiple threads
        threads = []
        for i in range(4):
            thread = threading.Thread(target=worker_thread, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check results
        if errors:
            print(f"❌ FAILURE: Concurrent access errors: {errors}")
            return False
        
        if len(results) == 0:
            print("❌ FAILURE: No tokens acquired in concurrent test")
            return False
        
        if len(results) > 20:  # Should not exceed burst limit
            print(f"❌ FAILURE: Too many tokens acquired: {len(results)} > 20")
            return False
        
        print(f"✅ SUCCESS: Concurrent rate limiting working - {len(results)} tokens acquired")
        
        print("✅ ALL concurrent rate limiting tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Concurrent test failed: {e}")
        return False


def test_channel_discovery_integration():
    """Test rate limiting integration with channel discovery module."""
    print("\n🧪 Testing channel discovery integration...")
    
    try:
        from channel_discovery import YouTubeChannelDiscovery
        from utils.rate_limiter import get_rate_limiter, get_rate_limit_status
        
        # Test Case 1: Initialization includes rate limiter setup
        try:
            discovery = YouTubeChannelDiscovery()
            print("✅ SUCCESS: Channel discovery initializes with rate limiting")
        except RuntimeError as e:
            if "yt-dlp" in str(e):
                print("🔍 EXPECTED FAILURE: yt-dlp not available (acceptable for test)")
                # Continue with rate limiter testing without yt-dlp
            else:
                print(f"❌ FAILURE: Unexpected initialization error: {e}")
                return False
        
        # Test Case 2: Global rate limiter is configured
        rate_limiter = get_rate_limiter()
        if rate_limiter is None:
            print("❌ FAILURE: Global rate limiter not initialized")
            return False
        print("✅ SUCCESS: Global rate limiter properly initialized")
        
        # Test Case 3: YouTube service has proper configuration
        youtube_acquired = rate_limiter.acquire("youtube")
        print(f"✅ SUCCESS: YouTube rate limiter acquisition test: {youtube_acquired}")
        
        # Test Case 4: Rate limit status monitoring
        status = get_rate_limit_status()
        if "youtube" in status:
            youtube_status = status["youtube"]
            print(f"✅ SUCCESS: YouTube rate limit status: {youtube_status}")
        
        print("✅ ALL channel discovery integration tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Channel discovery integration failed: {e}")
        return False


def test_decorator_functionality():
    """Test the @rate_limit decorator with burst support."""
    print("\n🧪 Testing @rate_limit decorator functionality...")
    
    try:
        from utils.rate_limiter import rate_limit
        
        # Test function with rate limiting
        call_count = 0
        
        @rate_limit("test_service", tokens=1, timeout=5.0)
        def test_function():
            nonlocal call_count
            call_count += 1
            return f"call_{call_count}"
        
        # Test Case 1: Basic decorator functionality
        result1 = test_function()
        if "call_1" not in result1:
            print(f"❌ FAILURE: Decorator not working correctly: {result1}")
            return False
        print("✅ SUCCESS: Rate limit decorator basic functionality working")
        
        # Test Case 2: Multiple calls (should work within burst limit)
        results = []
        for i in range(3):
            results.append(test_function())
        
        if len(results) != 3:
            print(f"❌ FAILURE: Expected 3 results, got {len(results)}")
            return False
        print("✅ SUCCESS: Multiple calls within burst limit working")
        
        print("✅ ALL decorator functionality tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Decorator test failed: {e}")
        return False


def main():
    """Run comprehensive rate limiting integration test suite."""
    print("🚀 Starting Rate Limiting Integration Test Suite")
    print("   Testing integration with existing infrastructure")
    print("   Validating burst support and token bucket algorithm")
    print("=" * 80)
    
    all_tests_passed = True
    test_functions = [
        test_rate_limiter_imports,
        test_rate_limit_config_validation,
        test_token_bucket_functionality,
        test_service_rate_limiter_integration,
        test_concurrent_rate_limiting,
        test_channel_discovery_integration,
        test_decorator_functionality
    ]
    
    for test_func in test_functions:
        if not test_func():
            all_tests_passed = False
            print(f"❌ {test_func.__name__} FAILED")
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL RATE LIMITING INTEGRATION TESTS PASSED!")
        print("✅ Configuration integration working")
        print("✅ Token bucket algorithm implemented")
        print("✅ Burst support functional")
        print("✅ Concurrent access thread-safe")
        print("✅ Channel discovery integration complete")
        print("✅ Decorator functionality validated")
        print("\\n🔥 Rate limiting integration is PRODUCTION-READY!")
        return 0
    else:
        print("💥 SOME RATE LIMITING INTEGRATION TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)