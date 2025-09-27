"""
Centralized Retry Utilities (DRY Refactoring Phase 4.11)

This module provides standardized retry decorators and utilities to replace
custom retry logic scattered throughout the codebase. All retry patterns
are centralized here for consistency and maintainability.

Enhanced with comprehensive retry strategies, better config integration,
and specialized decorators for different operation types.
"""
import time
import random
import functools
from typing import Callable, Any, Optional, Tuple, Type, Union, Dict
from dataclasses import dataclass
from enum import Enum
import subprocess
import requests

try:
    from config import get_timeout, get_config
    from logging_config import get_logger
except ImportError:
    from .config import get_timeout, get_config
    from .logging_config import get_logger

# Module logger
logger = get_logger(__name__)

# Get configuration
config = get_config()


class RetryError(Exception):
    """Raised when all retry attempts are exhausted"""
    pass


class RetryStrategy(Enum):
    """Retry strategy types"""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"
    FIBONACCI = "fibonacci"


@dataclass
class RetryConfig:
    """Enhanced configuration for retry operations"""
    max_attempts: int = None
    base_delay: float = None
    max_delay: float = None
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    jitter: bool = None
    
    def __post_init__(self):
        """Initialize from config if values are None"""
        if self.max_attempts is None:
            self.max_attempts = config.get('retry.max_attempts', 3)
        if self.base_delay is None:
            self.base_delay = config.get('retry.base_delay', 1.0)
        if self.max_delay is None:
            self.max_delay = config.get('retry.max_delay', 60.0)
        if self.jitter is None:
            self.jitter = config.get('retry.jitter', True)


# Specialized exception types for different retry scenarios
class NetworkRetryableError(Exception):
    """Network-related errors that should be retried"""
    pass


class FileRetryableError(Exception):
    """File operation errors that should be retried"""
    pass


class SubprocessRetryableError(Exception):
    """Subprocess errors that should be retried"""
    pass


# === ENHANCED RETRY DECORATORS ===

def network_retry(
    max_attempts: int = None,
    base_delay: float = None,
    operation_name: str = None
):
    """
    Decorator specifically for network operations.
    Uses config-based retry settings for downloads.
    """
    if max_attempts is None:
        max_attempts = config.get('retry.download.max_attempts', 3)
    if base_delay is None:
        base_delay = config.get('retry.download.base_delay', 5.0)
    
    network_exceptions = (
        requests.exceptions.RequestException,
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError,
        NetworkRetryableError,
        OSError,
        ConnectionResetError,
        ConnectionRefusedError,
        TimeoutError
    )
    
    return retry_with_backoff(
        max_attempts=max_attempts,
        base_delay=base_delay,
        exceptions=network_exceptions,
        logger=logger
    )


def subprocess_retry_decorator(
    max_attempts: int = None,
    base_delay: float = None,
    operation_name: str = None
):
    """
    Decorator specifically for subprocess operations.
    Uses config-based retry settings for subprocess operations.
    """
    if max_attempts is None:
        max_attempts = config.get('retry.subprocess.max_attempts', 3)
    if base_delay is None:
        base_delay = config.get('retry.subprocess.base_delay', 2.0)
    
    subprocess_exceptions = (
        subprocess.SubprocessError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        SubprocessRetryableError,
        OSError
    )
    
    return retry_with_backoff(
        max_attempts=max_attempts,
        base_delay=base_delay,
        exceptions=subprocess_exceptions,
        logger=logger
    )


def file_operation_retry(
    max_attempts: int = None,
    base_delay: float = None,
    operation_name: str = None
):
    """
    Decorator specifically for file operations.
    Uses shorter delays suitable for file I/O.
    """
    file_exceptions = (
        IOError,
        OSError,
        PermissionError,
        FileRetryableError
    )
    
    return retry_with_backoff(
        max_attempts=max_attempts or 3,
        base_delay=base_delay or 0.5,
        exceptions=file_exceptions,
        logger=logger
    )


def calculate_delay_with_strategy(
    attempt: int,
    base_delay: float,
    strategy: RetryStrategy,
    max_delay: float,
    jitter: bool = True
) -> float:
    """
    Calculate delay based on retry strategy.
    Enhanced version that supports multiple backoff strategies.
    """
    if strategy == RetryStrategy.EXPONENTIAL:
        delay = base_delay * (2 ** attempt)
    elif strategy == RetryStrategy.LINEAR:
        delay = base_delay * (attempt + 1)
    elif strategy == RetryStrategy.FIXED:
        delay = base_delay
    elif strategy == RetryStrategy.FIBONACCI:
        delay = base_delay * _fibonacci(attempt + 1)
    else:
        delay = base_delay * (2 ** attempt)  # Default to exponential
    
    # Apply maximum delay cap
    delay = min(delay, max_delay)
    
    # Add jitter if enabled
    if jitter:
        jitter_amount = delay * 0.1  # 10% jitter
        delay += random.uniform(-jitter_amount, jitter_amount)
        delay = max(0, delay)  # Ensure delay is not negative
    
    return delay


def _fibonacci(n: int) -> int:
    """Calculate nth Fibonacci number for Fibonacci backoff"""
    if n <= 1:
        return n
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b


def exponential_backoff(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: Optional[float] = None,
    jitter: bool = True
) -> float:
    """
    Calculate exponential backoff delay
    
    Args:
        attempt: Current attempt number (0-indexed)
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds (from config if None)
        jitter: Add random jitter to prevent thundering herd
    
    Returns:
        Delay in seconds
    """
    # Get max_delay from config if not provided
    if max_delay is None:
        max_delay = get_timeout('retry_max')
    
    # Calculate exponential delay: base * 2^attempt
    delay = min(base_delay * (2 ** attempt), max_delay)
    
    if jitter:
        # Add random jitter between 0-25% of delay
        delay = delay * (1 + random.random() * 0.25)
    
    return delay


def retry_with_backoff(
    func: Optional[Callable] = None,
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: Optional[float] = None,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None,
    logger: Optional[Any] = None
):
    """
    Decorator to retry a function with exponential backoff
    
    Args:
        func: Function to retry (used when called without arguments)
        max_attempts: Maximum number of attempts
        base_delay: Base delay between retries in seconds
        max_delay: Maximum delay between retries
        exceptions: Tuple of exceptions to catch and retry
        on_retry: Optional callback called on each retry with (exception, attempt)
        logger: Optional logger instance
    
    Usage:
        @retry_with_backoff(max_attempts=5, base_delay=2.0)
        def download_file(url):
            return requests.get(url)
        
        @retry_with_backoff  # Uses defaults
        def simple_download(url):
            return requests.get(url)
    """
    def decorator(f: Callable) -> Callable:
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            # Get max_delay from config if not provided
            actual_max_delay = max_delay if max_delay is not None else get_timeout('retry_max')
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts - 1:
                        # Last attempt failed
                        if logger:
                            logger.error(f"All {max_attempts} attempts failed for {f.__name__}: {e}")
                        raise RetryError(f"Failed after {max_attempts} attempts: {e}") from e
                    
                    # Calculate backoff delay
                    delay = exponential_backoff(attempt, base_delay, actual_max_delay)
                    
                    # Log retry
                    if logger:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_attempts} failed for {f.__name__}: {e}. "
                            f"Retrying in {delay:.1f} seconds..."
                        )
                    
                    # Call retry callback if provided
                    if on_retry:
                        on_retry(e, attempt)
                    
                    # Wait before retry
                    time.sleep(delay)
            
            # Should never reach here
            raise last_exception
        
        return wrapper
    
    # Handle decorator usage with or without arguments
    if func is None:
        # Called with arguments: @retry_with_backoff(max_attempts=5)
        return decorator
    else:
        # Called without arguments: @retry_with_backoff
        return decorator(func)


def retry_subprocess(
    cmd: list,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    logger: Optional[Any] = None,
    **subprocess_kwargs
) -> subprocess.CompletedProcess:
    """
    Run a subprocess command with retry logic
    
    Args:
        cmd: Command to run as list
        max_attempts: Maximum number of attempts
        base_delay: Base delay between retries
        logger: Optional logger instance
        **subprocess_kwargs: Additional arguments for subprocess.run
    
    Returns:
        CompletedProcess instance
    
    Raises:
        RetryError: If all attempts fail
    """
    @retry_with_backoff(
        max_attempts=max_attempts,
        base_delay=base_delay,
        exceptions=(subprocess.CalledProcessError,),
        logger=logger
    )
    def run_command():
        return subprocess.run(cmd, check=True, **subprocess_kwargs)
    
    return run_command()


def retry_request(
    method: str,
    url: str,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    logger: Optional[Any] = None,
    **request_kwargs
) -> requests.Response:
    """
    Make an HTTP request with retry logic
    
    Args:
        method: HTTP method (GET, POST, etc.)
        url: URL to request
        max_attempts: Maximum number of attempts
        base_delay: Base delay between retries
        logger: Optional logger instance
        **request_kwargs: Additional arguments for requests
    
    Returns:
        Response object
    
    Raises:
        RetryError: If all attempts fail
    """
    # Define retryable status codes
    retryable_status_codes = {408, 429, 500, 502, 503, 504}
    
    @retry_with_backoff(
        max_attempts=max_attempts,
        base_delay=base_delay,
        exceptions=(requests.RequestException,),
        logger=logger
    )
    def make_request():
        response = requests.request(method, url, **request_kwargs)
        
        # Check if we should retry based on status code
        if response.status_code in retryable_status_codes:
            raise requests.RequestException(
                f"Retryable status code: {response.status_code}"
            )
        
        # Raise for other bad status codes
        response.raise_for_status()
        return response
    
    return make_request()


# Convenience functions for common HTTP methods
def get_with_retry(url: str, **kwargs) -> requests.Response:
    """GET request with retry"""
    return retry_request('GET', url, **kwargs)


def post_with_retry(url: str, **kwargs) -> requests.Response:
    """POST request with retry"""
    return retry_request('POST', url, **kwargs)


# Example usage and tests
if __name__ == "__main__":
    print("Testing retry utilities...")
    
    # Test exponential backoff calculation
    print("\nExponential backoff delays:")
    for i in range(5):
        delay = exponential_backoff(i, base_delay=1.0, max_delay=30.0)
        print(f"  Attempt {i + 1}: {delay:.2f} seconds")
    
    # Test retry decorator
    @retry_with_backoff(max_attempts=3, base_delay=0.5)
    def flaky_function(success_on_attempt=3):
        """Simulates a flaky function that fails initially"""
        if not hasattr(flaky_function, 'attempts'):
            flaky_function.attempts = 0
        flaky_function.attempts += 1
        
        print(f"  Attempt {flaky_function.attempts}")
        
        if flaky_function.attempts < success_on_attempt:
            raise ValueError(f"Failed on attempt {flaky_function.attempts}")
        
        return f"Success on attempt {flaky_function.attempts}"
    
    print("\nTesting retry decorator:")
    try:
        # Reset attempts counter
        if hasattr(flaky_function, 'attempts'):
            delattr(flaky_function, 'attempts')
        
        result = flaky_function(success_on_attempt=2)
        print(f"  Result: {result}")
    except RetryError as e:
        print(f"  Failed: {e}")
    
    print("\nRetry utilities ready for use!")