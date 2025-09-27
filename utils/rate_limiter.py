#!/usr/bin/env python3
"""
Comprehensive Rate Limiting System with Token Bucket Algorithm
Integrates with existing configuration system and supports burst limits.

Implements fail-fast, fail-loud, fail-safely principles:
- Fail Fast: Immediate validation of configuration and parameters
- Fail Loud: Detailed error messages for misconfigurations  
- Fail Safely: Graceful fallback to default rates if configuration unavailable
"""
import time
import threading
from functools import wraps
from typing import Dict, Optional, Any
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """Configuration for a rate limit service."""
    rate: float  # requests per second
    burst: int   # maximum burst size
    
    def __post_init__(self):
        """Validate rate limit configuration with fail-fast principles."""
        if not isinstance(self.rate, (int, float)) or self.rate <= 0:
            raise ValueError(
                f"RATE_LIMIT CONFIG ERROR: rate must be positive number. "
                f"Got: {self.rate} (type: {type(self.rate)})"
            )
        
        if not isinstance(self.burst, int) or self.burst <= 0:
            raise ValueError(
                f"RATE_LIMIT CONFIG ERROR: burst must be positive integer. "
                f"Got: {self.burst} (type: {type(self.burst)})"
            )
        
        if self.burst < 1:
            raise ValueError(
                f"RATE_LIMIT CONFIG ERROR: burst must be at least 1. "
                f"Got: {self.burst}"
            )


class TokenBucket:
    """
    Token bucket rate limiter with burst support.
    
    Implements a token bucket algorithm where:
    - Tokens are added at a steady rate (rate per second)
    - Up to 'burst' tokens can be stored
    - Each request consumes one token
    - Requests block if no tokens available
    """
    
    def __init__(self, rate: float, burst: int):
        """
        Initialize token bucket with fail-fast validation.
        
        Args:
            rate: Tokens added per second (requests per second)
            burst: Maximum tokens that can be stored (burst limit)
        """
        if rate <= 0:
            raise ValueError(f"Token bucket rate must be positive, got: {rate}")
        if burst <= 0:
            raise ValueError(f"Token bucket burst must be positive, got: {burst}")
        
        self.rate = float(rate)
        self.burst = int(burst)
        self.tokens = float(burst)  # Start with full bucket
        self.last_update = time.time()
        self.lock = threading.RLock()  # Thread-safe operations
        
        logger.debug(f"TokenBucket initialized: rate={self.rate}/sec, burst={self.burst}")
    
    def acquire(self, tokens: int = 1) -> bool:
        """
        Acquire tokens from bucket (non-blocking).
        
        Args:
            tokens: Number of tokens to acquire
            
        Returns:
            True if tokens acquired, False if insufficient tokens
        """
        with self.lock:
            self._add_tokens()
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                logger.debug(f"Acquired {tokens} tokens, {self.tokens:.1f} remaining")
                return True
            else:
                logger.debug(f"Insufficient tokens: need {tokens}, have {self.tokens:.1f}")
                return False
    
    def wait_for_tokens(self, tokens: int = 1, timeout: float = 60.0) -> bool:
        """
        Wait for tokens to become available (blocking).
        
        Args:
            tokens: Number of tokens needed
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if tokens acquired, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if self.acquire(tokens):
                return True
            
            # Calculate how long to wait for next token
            with self.lock:
                if self.tokens < tokens:
                    tokens_needed = tokens - self.tokens
                    wait_time = tokens_needed / self.rate
                    wait_time = min(wait_time, 1.0)  # Don't wait more than 1 second at a time
                    
                    logger.debug(f"Waiting {wait_time:.2f}s for {tokens_needed:.1f} tokens")
                    time.sleep(wait_time)
        
        logger.warning(f"Timeout waiting for {tokens} tokens after {timeout}s")
        return False
    
    def _add_tokens(self):
        """Add tokens based on elapsed time (thread-safe)."""
        now = time.time()
        elapsed = now - self.last_update
        self.last_update = now
        
        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self.rate
        self.tokens = min(self.burst, self.tokens + tokens_to_add)
    
    def get_status(self) -> Dict[str, Any]:
        """Get current bucket status for monitoring."""
        with self.lock:
            self._add_tokens()
            return {
                "rate": self.rate,
                "burst": self.burst,
                "tokens": round(self.tokens, 2),
                "utilization": round((self.burst - self.tokens) / self.burst * 100, 1)
            }


class ServiceRateLimiter:
    """
    Per-service rate limiter with configuration integration.
    
    Manages rate limits for different services (youtube, google_drive, etc.)
    using token bucket algorithm with burst support.
    """
    
    def __init__(self, config: Optional[Any] = None):
        """
        Initialize service rate limiter with configuration.
        
        Args:
            config: Configuration object with rate_limiting section
        """
        self.config = config
        self.buckets: Dict[str, TokenBucket] = {}
        self.lock = threading.RLock()
        
        # Default rate limits (fail-safely)
        self.default_config = RateLimitConfig(rate=2.0, burst=5)
        
        logger.info("ServiceRateLimiter initialized with configuration integration")
    
    def get_service_config(self, service: str) -> RateLimitConfig:
        """
        Get rate limit configuration for a service with fail-safe fallbacks.
        
        Args:
            service: Service name (e.g., "youtube", "google_drive")
            
        Returns:
            RateLimitConfig with rate and burst limits
        """
        try:
            if self.config:
                # Try to get service-specific configuration
                service_config = self.config.get(f"rate_limiting.services.{service}")
                if service_config:
                    rate = service_config.get("rate", self.default_config.rate)
                    burst = service_config.get("burst", self.default_config.burst)
                    
                    # Validate and return configuration
                    config = RateLimitConfig(rate=rate, burst=burst)
                    logger.debug(f"Loaded config for {service}: rate={rate}/sec, burst={burst}")
                    return config
            
            # Fallback to default configuration
            logger.debug(f"Using default config for {service}: rate={self.default_config.rate}/sec, burst={self.default_config.burst}")
            return self.default_config
            
        except Exception as e:
            logger.warning(f"Failed to load rate limit config for {service}: {e}")
            logger.info(f"Falling back to default rate limit for {service}")
            return self.default_config
    
    def get_bucket(self, service: str) -> TokenBucket:
        """
        Get or create token bucket for a service.
        
        Args:
            service: Service name
            
        Returns:
            TokenBucket for the service
        """
        with self.lock:
            if service not in self.buckets:
                config = self.get_service_config(service)
                self.buckets[service] = TokenBucket(config.rate, config.burst)
                logger.info(f"Created token bucket for {service}: {config.rate}/sec, burst={config.burst}")
            
            return self.buckets[service]
    
    def acquire(self, service: str, tokens: int = 1) -> bool:
        """Acquire tokens for a service (non-blocking)."""
        bucket = self.get_bucket(service)
        return bucket.acquire(tokens)
    
    def wait_for_rate_limit(self, service: str, tokens: int = 1, timeout: float = 60.0) -> bool:
        """Wait for rate limit availability (blocking)."""
        bucket = self.get_bucket(service)
        return bucket.wait_for_tokens(tokens, timeout)
    
    def get_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all service rate limiters."""
        status = {}
        with self.lock:
            for service, bucket in self.buckets.items():
                status[service] = bucket.get_status()
        return status


# Global service rate limiter instance
_service_rate_limiter: Optional[ServiceRateLimiter] = None
_rate_limiter_lock = threading.RLock()


def initialize_rate_limiter(config: Optional[Any] = None):
    """Initialize global rate limiter with configuration."""
    global _service_rate_limiter
    with _rate_limiter_lock:
        if _service_rate_limiter is None:
            _service_rate_limiter = ServiceRateLimiter(config)
            logger.info("Global rate limiter initialized")


def get_rate_limiter() -> ServiceRateLimiter:
    """Get global rate limiter instance (creates with defaults if needed)."""
    global _service_rate_limiter
    with _rate_limiter_lock:
        if _service_rate_limiter is None:
            _service_rate_limiter = ServiceRateLimiter()
            logger.info("Created default global rate limiter")
        return _service_rate_limiter


def rate_limit(service: str, tokens: int = 1, timeout: float = 60.0):
    """
    Rate limiting decorator with burst support and token bucket algorithm.
    
    Args:
        service: Service name for rate limiting
        tokens: Number of tokens to consume
        timeout: Maximum time to wait for tokens
        
    Usage:
        @rate_limit("youtube")
        def download_video():
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            rate_limiter = get_rate_limiter()
            
            # Try non-blocking acquire first for burst scenarios
            if rate_limiter.acquire(service, tokens):
                logger.debug(f"Burst token acquired for {service}")
                return func(*args, **kwargs)
            
            # Wait for tokens if burst not available
            logger.debug(f"Waiting for rate limit: {service} ({tokens} tokens)")
            if rate_limiter.wait_for_rate_limit(service, tokens, timeout):
                logger.debug(f"Rate limit acquired for {service}")
                return func(*args, **kwargs)
            else:
                raise RuntimeError(
                    f"RATE_LIMIT ERROR: Timeout waiting for rate limit after {timeout}s. "
                    f"Service: {service}, tokens: {tokens}. "
                    f"This indicates the service may be overloaded or misconfigured."
                )
        
        return wrapper
    return decorator


def wait_for_rate_limit(service: str, tokens: int = 1, timeout: float = 60.0) -> bool:
    """
    Wait for rate limit availability (function interface).
    
    Args:
        service: Service name
        tokens: Number of tokens needed
        timeout: Maximum wait time
        
    Returns:
        True if tokens acquired, False if timeout
    """
    rate_limiter = get_rate_limiter()
    return rate_limiter.wait_for_rate_limit(service, tokens, timeout)


def get_rate_limit_status() -> Dict[str, Dict[str, Any]]:
    """Get status of all rate limiters for monitoring."""
    rate_limiter = get_rate_limiter()
    return rate_limiter.get_status()


# Backward compatibility functions
def rate_limit_simple(service: str):
    """Simple rate limiting with just a delay (legacy compatibility)."""
    return rate_limit(service, tokens=1, timeout=10.0)