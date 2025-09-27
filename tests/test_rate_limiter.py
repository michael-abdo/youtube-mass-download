#!/usr/bin/env python3
"""
Unit tests for rate limiting module - prevents service blocking.
"""
import unittest
import time
import threading
from unittest.mock import patch, MagicMock

# Use centralized path setup instead of manual sys.path manipulation
from utils.path_setup import init_project_imports
init_project_imports()

from utils.rate_limiter import RateLimiter, ServiceRateLimiter, rate_limit, URLRateLimiter


class TestRateLimiter(unittest.TestCase):
    """Test basic rate limiter functionality"""
    
    def test_rate_limiting(self):
        """Test rate limiting enforces request rate"""
        limiter = RateLimiter(rate=5.0)  # 5 requests per second
        
        start_time = time.time()
        
        # Should be able to make 5 requests immediately (burst)
        for i in range(5):
            acquired = limiter.acquire(blocking=False)
            self.assertTrue(acquired)
        
        # 6th request should fail immediately
        acquired = limiter.acquire(blocking=False)
        self.assertFalse(acquired)
        
        # Wait for tokens to refill
        time.sleep(0.3)  # Should get 1-2 tokens back
        
        # Should be able to make another request
        acquired = limiter.acquire(blocking=False)
        self.assertTrue(acquired)
    
    def test_burst_capacity(self):
        """Test burst capacity limits"""
        limiter = RateLimiter(rate=2.0, burst=3)
        
        # Can make 3 requests in burst
        for i in range(3):
            self.assertTrue(limiter.acquire(blocking=False))
        
        # 4th fails
        self.assertFalse(limiter.acquire(blocking=False))
    
    def test_blocking_acquire(self):
        """Test blocking acquire waits for tokens"""
        limiter = RateLimiter(rate=10.0)  # 10 per second
        
        # Exhaust tokens
        for i in range(10):
            limiter.acquire()
        
        # Next acquire should block
        start_time = time.time()
        acquired = limiter.acquire(blocking=True)
        elapsed = time.time() - start_time
        
        self.assertTrue(acquired)
        self.assertGreater(elapsed, 0.05)  # Should have waited
    
    def test_concurrent_access(self):
        """Test thread-safe concurrent access"""
        limiter = RateLimiter(rate=10.0)
        results = []
        
        def make_requests(thread_id):
            for i in range(3):
                if limiter.acquire(blocking=False):
                    results.append((thread_id, time.time()))
        
        # Multiple threads trying to acquire
        threads = []
        for i in range(3):
            t = threading.Thread(target=make_requests, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Should have at most 10 successful acquisitions
        self.assertLessEqual(len(results), 10)


class TestServiceRateLimiter(unittest.TestCase):
    """Test service-specific rate limiting"""
    
    def test_service_limits(self):
        """Test different services have different limits"""
        limiter = ServiceRateLimiter()
        
        # YouTube should be limited to 2 per second
        youtube_limiter = limiter.get_limiter('youtube')
        self.assertEqual(youtube_limiter.rate, 2.0)
        
        # Google Drive should be 3 per second
        drive_limiter = limiter.get_limiter('google_drive')
        self.assertEqual(drive_limiter.rate, 3.0)
    
    def test_unknown_service_default(self):
        """Test unknown services get default limiter"""
        limiter = ServiceRateLimiter()
        
        unknown_limiter = limiter.get_limiter('unknown_service')
        self.assertIsNotNone(unknown_limiter)
        self.assertEqual(unknown_limiter.rate, 2.0)  # Default rate
    
    def test_service_isolation(self):
        """Test services don't interfere with each other"""
        limiter = ServiceRateLimiter()
        
        # Exhaust YouTube tokens
        for i in range(5):
            limiter.acquire('youtube', blocking=False)
        
        # Should still be able to use Google Drive
        acquired = limiter.acquire('google_drive', blocking=False)
        self.assertTrue(acquired)


class TestRateLimitDecorator(unittest.TestCase):
    """Test rate limit decorator"""
    
    @patch('utils.rate_limiter.get_service_limiter')
    def test_decorator_calls_limiter(self, mock_get_limiter):
        """Test decorator acquires tokens before function call"""
        mock_limiter = MagicMock()
        mock_get_limiter.return_value = mock_limiter
        
        @rate_limit('test_service')
        def test_function():
            return "success"
        
        result = test_function()
        
        self.assertEqual(result, "success")
        mock_limiter.wait.assert_called_once_with('test_service', 1)
    
    def test_decorator_with_real_limiter(self):
        """Test decorator with real rate limiter"""
        call_times = []
        
        @rate_limit('youtube')
        def download_video():
            call_times.append(time.time())
            return True
        
        # Make enough calls to exceed burst capacity (5 for YouTube)
        for i in range(6):
            download_video()
        
        # Check timing - the 6th call should be rate limited
        if len(call_times) >= 6:
            # First 5 calls can be in burst
            # 6th call should wait
            gap = call_times[5] - call_times[4]
            
            # Should have waited about 0.5s (rate is 2/s)
            self.assertGreater(gap, 0.3)


class TestURLRateLimiter(unittest.TestCase):
    """Test URL-based rate limiting"""
    
    def test_domain_detection(self):
        """Test domain extraction from URLs"""
        limiter = URLRateLimiter()
        
        self.assertEqual(limiter.get_domain("https://www.youtube.com/watch?v=123"), "youtube")
        self.assertEqual(limiter.get_domain("https://youtu.be/123"), "youtube")
        self.assertEqual(limiter.get_domain("https://drive.google.com/file/d/123"), "google_drive")
        self.assertEqual(limiter.get_domain("https://docs.google.com/document/d/123"), "google_docs")
        self.assertEqual(limiter.get_domain("https://example.com/page"), "example.com")
    
    def test_url_rate_limiting(self):
        """Test URLs are rate limited by domain"""
        limiter = URLRateLimiter()
        
        # Multiple YouTube URLs should share rate limit
        youtube_urls = [
            "https://www.youtube.com/watch?v=1",
            "https://www.youtube.com/watch?v=2",
            "https://youtu.be/3"
        ]
        
        # Should respect YouTube rate limit
        acquired_count = 0
        for url in youtube_urls * 3:  # Try 9 times
            if limiter.acquire(url, blocking=False):
                acquired_count += 1
        
        # Should get burst limit (5) but not all 9
        self.assertLessEqual(acquired_count, 5)
        self.assertGreater(acquired_count, 0)


if __name__ == "__main__":
    unittest.main()