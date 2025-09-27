"""
HTTP Utilities and Connection Pooling (DRY Phase 6)

Consolidates HTTP request patterns found throughout the codebase:
- Connection pooling with retry logic
- Standardized request handling
- Error handling and timeouts
- Response validation patterns
- Progress tracking for downloads
"""
import os
import time
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Optional, Dict, Any, Callable, Union, Tuple
import logging
from io import BytesIO
from pathlib import Path

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

from utils.config import get_config, is_ssl_verify_enabled, Constants
from utils.logging_config import get_logger
from utils.error_handling import handle_network_operations, ErrorMessages, network_error

# Get configuration
config = get_config()

# Global session instance
_session = None

class HTTPPool:
    """HTTP connection pool with retry logic and configuration."""
    
    def __init__(self, 
                 pool_connections: int = 10,
                 pool_maxsize: int = 10,
                 max_retries: int = 3,
                 backoff_factor: float = 1.0,
                 status_forcelist: Optional[list] = None,
                 verify_ssl: Optional[bool] = None):
        """
        Initialize HTTP connection pool.
        
        Args:
            pool_connections: Number of connection pools to cache
            pool_maxsize: Maximum number of connections to save in the pool
            max_retries: Maximum number of retry attempts
            backoff_factor: Backoff factor for retries
            status_forcelist: HTTP status codes to retry
            verify_ssl: Whether to verify SSL certificates
        """
        self.session = requests.Session()
        
        # Get retry configuration from config
        retry_config = config.get_section('retry')
        self.max_retries = max_retries or retry_config.get('max_attempts', 3)
        self.backoff_factor = backoff_factor or retry_config.get('base_delay', 1.0)
        
        # Default status codes to retry
        if status_forcelist is None:
            status_forcelist = [408, 429, 500, 502, 503, 504]
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "TRACE"],
            raise_on_status=False
        )
        
        # Configure connection pooling
        adapter = HTTPAdapter(
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
            max_retries=retry_strategy
        )
        
        # Mount adapter for both http and https
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        headers = {
            'User-Agent': config.get('web_scraping.user_agent', 
                                   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'),
            'Accept': config.get('web_scraping.accept_header', 
                               'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'),
            'Accept-Language': config.get('web_scraping.accept_language', 'en-US,en;q=0.5'),
        }
        self.session.headers.update(headers)
        
        # SSL verification
        if verify_ssl is None:
            verify_ssl = is_ssl_verify_enabled()
        self.session.verify = verify_ssl
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """Make GET request with connection pooling."""
        timeout = kwargs.pop('timeout', config.get('timeouts.http_request', 60.0))
        return self.session.get(url, timeout=timeout, **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """Make POST request with connection pooling."""
        timeout = kwargs.pop('timeout', config.get('timeouts.http_request', 60.0))
        return self.session.post(url, timeout=timeout, **kwargs)
    
    def put(self, url: str, **kwargs) -> requests.Response:
        """Make PUT request with connection pooling."""
        timeout = kwargs.pop('timeout', config.get('timeouts.http_request', 60.0))
        return self.session.put(url, timeout=timeout, **kwargs)
    
    def delete(self, url: str, **kwargs) -> requests.Response:
        """Make DELETE request with connection pooling."""
        timeout = kwargs.pop('timeout', config.get('timeouts.http_request', 60.0))
        return self.session.delete(url, timeout=timeout, **kwargs)
    
    def head(self, url: str, **kwargs) -> requests.Response:
        """Make HEAD request with connection pooling."""
        timeout = kwargs.pop('timeout', config.get('timeouts.http_request', 60.0))
        return self.session.head(url, timeout=timeout, **kwargs)
    
    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make generic request with connection pooling."""
        timeout = kwargs.pop('timeout', config.get('timeouts.http_request', 60.0))
        return self.session.request(method, url, timeout=timeout, **kwargs)
    
    def close(self):
        """Close the session and all connections."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def get_http_pool() -> HTTPPool:
    """
    Get singleton HTTP pool instance.
    
    Returns:
        HTTPPool instance
    """
    global _session
    
    if _session is None:
        _session = HTTPPool()
    
    return _session


def close_http_pool():
    """Close the global HTTP pool."""
    global _session
    
    if _session is not None:
        _session.close()
        _session = None


# Convenience functions that use the global pool
def get(url: str, **kwargs) -> requests.Response:
    """Make GET request using global connection pool."""
    return get_http_pool().get(url, **kwargs)


def post(url: str, **kwargs) -> requests.Response:
    """Make POST request using global connection pool."""
    return get_http_pool().post(url, **kwargs)


def put(url: str, **kwargs) -> requests.Response:
    """Make PUT request using global connection pool."""
    return get_http_pool().put(url, **kwargs)


def delete(url: str, **kwargs) -> requests.Response:
    """Make DELETE request using global connection pool."""
    return get_http_pool().delete(url, **kwargs)


def head(url: str, **kwargs) -> requests.Response:
    """Make HEAD request using global connection pool."""
    return get_http_pool().head(url, **kwargs)


# ============================================================================
# DRY PHASE 6: CONSOLIDATED HTTP UTILITIES
# ============================================================================

@handle_network_operations("HTTP GET request", return_on_error=None, retry_count=2)
def http_get(url: str, 
             timeout: Optional[float] = None,
             headers: Optional[Dict[str, str]] = None,
             params: Optional[Dict[str, Any]] = None,
             stream: bool = False,
             validate_response: bool = True,
             **kwargs) -> Optional[requests.Response]:
    """
    Consolidated GET request function with standardized error handling.
    
    Eliminates the repeated pattern found in 30+ files:
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response
        except Exception as e:
            logger.error(f"HTTP error: {e}")
            return None
    
    Args:
        url: URL to request
        timeout: Request timeout (uses config default if None)
        headers: Additional headers
        params: Query parameters
        stream: Whether to stream the response
        validate_response: Whether to validate HTTP status
        **kwargs: Additional arguments passed to requests
        
    Returns:
        Response object or None on error
        
    Example:
        response = http_get('https://api.example.com/data', timeout=60)
        if response:
            data = response.json()
    """
    if timeout is None:
        timeout = config.get('timeouts.http_request', 60.0)
    
    # Merge headers
    request_headers = {}
    if headers:
        request_headers.update(headers)
    
    # Make request using connection pool
    response = get_http_pool().get(
        url, 
        timeout=timeout,
        headers=request_headers,
        params=params,
        stream=stream,
        **kwargs
    )
    
    # Validate response if requested
    if validate_response:
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise ValueError(network_error('HTTP_ERROR', status=response.status_code, message=str(e)))
    
    return response


@handle_network_operations("HTTP POST request", return_on_error=None, retry_count=2)
def http_post(url: str,
              data: Optional[Union[Dict[str, Any], str, bytes]] = None,
              json: Optional[Dict[str, Any]] = None,
              timeout: Optional[float] = None,
              headers: Optional[Dict[str, str]] = None,
              **kwargs) -> Optional[requests.Response]:
    """
    Consolidated POST request function with standardized error handling.
    
    Args:
        url: URL to request
        data: Form data to send
        json: JSON data to send
        timeout: Request timeout
        headers: Additional headers
        **kwargs: Additional arguments
        
    Returns:
        Response object or None on error
        
    Example:
        response = http_post('https://api.example.com/submit', json={'key': 'value'})
    """
    if timeout is None:
        timeout = config.get('timeouts.http_request', 60.0)
    
    # Merge headers
    request_headers = {}
    if headers:
        request_headers.update(headers)
    
    response = get_http_pool().post(
        url,
        data=data,
        json=json,
        timeout=timeout,
        headers=request_headers,
        **kwargs
    )
    
    response.raise_for_status()
    return response


def download_with_progress(url: str,
                          output_path: Optional[Union[str, Path]] = None,
                          chunk_size: int = 8192,
                          progress_callback: Optional[Callable[[int, int], None]] = None,
                          timeout: Optional[float] = None,
                          headers: Optional[Dict[str, str]] = None) -> Union[Path, bytes]:
    """
    Download file with progress tracking.
    
    Consolidates download patterns found in 10+ files.
    
    Args:
        url: URL to download
        output_path: Output file path (returns bytes if None)
        chunk_size: Download chunk size
        progress_callback: Callback function(downloaded, total)
        timeout: Request timeout
        headers: Additional headers
        
    Returns:
        Path to downloaded file or bytes if output_path is None
        
    Example:
        def progress(downloaded, total):
            print(f"Downloaded {downloaded}/{total} bytes")
        
        file_path = download_with_progress(url, 'output.zip', progress_callback=progress)
    """
    logger = get_logger(__name__)
    
    if timeout is None:
        timeout = config.get('timeouts.file_download', 300.0)
    
    response = http_get(url, timeout=timeout, headers=headers, stream=True)
    if not response:
        raise ValueError(f"Failed to start download from {url}")
    
    # Get content length for progress tracking
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    
    # Prepare output
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        file_obj = open(output_path, 'wb')
    else:
        file_obj = BytesIO()
    
    try:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:  # Filter out keep-alive chunks
                file_obj.write(chunk)
                downloaded += len(chunk)
                
                # Call progress callback
                if progress_callback:
                    progress_callback(downloaded, total_size)
        
        if output_path:
            file_obj.close()
            logger.info(f"Downloaded {downloaded} bytes to {output_path}")
            return output_path
        else:
            content = file_obj.getvalue()
            file_obj.close()
            logger.info(f"Downloaded {downloaded} bytes to memory")
            return content
            
    except Exception as e:
        file_obj.close()
        if output_path and output_path.exists():
            output_path.unlink()  # Clean up partial download
        raise


def validate_http_response(response: requests.Response,
                          min_size: Optional[int] = None,
                          max_size: Optional[int] = None,
                          required_content_type: Optional[str] = None,
                          forbidden_patterns: Optional[list] = None) -> Tuple[bool, Optional[str]]:
    """
    Validate HTTP response against criteria.
    
    Consolidates response validation patterns found in 15+ files.
    
    Args:
        response: HTTP response to validate
        min_size: Minimum content size in bytes
        max_size: Maximum content size in bytes
        required_content_type: Required content type prefix
        forbidden_patterns: List of forbidden text patterns in content
        
    Returns:
        Tuple of (is_valid, error_message)
        
    Example:
        is_valid, error = validate_http_response(
            response, 
            min_size=100, 
            required_content_type='application/json'
        )
        if not is_valid:
            logger.error(f"Invalid response: {error}")
    """
    # Check status code
    if not response.ok:
        return False, f"HTTP {response.status_code}: {response.reason}"
    
    # Check content size
    content_length = len(response.content)
    
    if min_size and content_length < min_size:
        return False, f"Response too small: {content_length} < {min_size} bytes"
    
    if max_size and content_length > max_size:
        return False, f"Response too large: {content_length} > {max_size} bytes"
    
    # Check content type
    if required_content_type:
        content_type = response.headers.get('content-type', '').lower()
        if not content_type.startswith(required_content_type.lower()):
            return False, f"Wrong content type: got '{content_type}', expected '{required_content_type}'"
    
    # Check for forbidden patterns
    if forbidden_patterns:
        content_text = response.text.lower()
        for pattern in forbidden_patterns:
            if pattern.lower() in content_text:
                return False, f"Forbidden pattern found: '{pattern}'"
    
    return True, None


def check_url_availability(url: str, 
                          timeout: Optional[float] = None,
                          method: str = 'HEAD') -> bool:
    """
    Check if URL is available without downloading content.
    
    Consolidates URL checking patterns.
    
    Args:
        url: URL to check
        timeout: Request timeout
        method: HTTP method ('HEAD' or 'GET')
        
    Returns:
        True if URL is available
        
    Example:
        if check_url_availability('https://example.com/file.zip'):
            download_file(url)
    """
    try:
        if method.upper() == 'HEAD':
            response = head(url, timeout=timeout)
        else:
            response = http_get(url, timeout=timeout, stream=True)
            
        return response.ok if response else False
        
    except Exception:
        return False


def extract_filename_from_response(response: requests.Response,
                                  fallback_url: Optional[str] = None) -> str:
    """
    Extract filename from HTTP response headers or URL.
    
    Consolidates filename extraction patterns.
    
    Args:
        response: HTTP response
        fallback_url: URL to extract filename from if headers don't provide one
        
    Returns:
        Extracted filename
        
    Example:
        filename = extract_filename_from_response(response, url)
        output_path = downloads_dir / filename
    """
    # Try Content-Disposition header first
    content_disposition = response.headers.get('content-disposition', '')
    if 'filename=' in content_disposition:
        # Extract filename from header
        import re
        filename_match = re.search(r'filename[*]?=(?:UTF-8\'\')?["\']?([^"\';]+)["\']?', content_disposition)
        if filename_match:
            return filename_match.group(1).strip()
    
    # Fall back to URL
    if fallback_url:
        from urllib.parse import urlparse, unquote
        parsed = urlparse(fallback_url)
        filename = unquote(parsed.path.split('/')[-1])
        if filename and '.' in filename:
            return filename
    
    # Final fallback
    return f"download_{int(time.time())}"


def get_content_size(url: str, timeout: Optional[float] = None) -> Optional[int]:
    """
    Get content size without downloading the file.
    
    Consolidates size checking patterns.
    
    Args:
        url: URL to check
        timeout: Request timeout
        
    Returns:
        Content size in bytes or None if unavailable
        
    Example:
        size = get_content_size(url)
        if size and size > MAX_FILE_SIZE:
            logger.warning("File too large to download")
    """
    try:
        response = head(url, timeout=timeout)
        if response and response.ok:
            return int(response.headers.get('content-length', 0))
    except Exception:
        pass
    
    return None


def make_request_with_fallback(urls: list,
                              method: str = 'GET',
                              timeout: Optional[float] = None,
                              **kwargs) -> Optional[requests.Response]:
    """
    Try multiple URLs in order until one succeeds.
    
    Consolidates fallback URL patterns.
    
    Args:
        urls: List of URLs to try
        method: HTTP method
        timeout: Request timeout
        **kwargs: Additional request arguments
        
    Returns:
        First successful response or None
        
    Example:
        response = make_request_with_fallback([
            'https://primary.example.com/api',
            'https://backup.example.com/api',
            'https://fallback.example.com/api'
        ])
    """
    logger = get_logger(__name__)
    
    for i, url in enumerate(urls):
        try:
            if method.upper() == 'GET':
                response = http_get(url, timeout=timeout, **kwargs)
            elif method.upper() == 'POST':
                response = http_post(url, timeout=timeout, **kwargs)
            else:
                response = get_http_pool().request(method, url, timeout=timeout, **kwargs)
            
            if response and response.ok:
                if i > 0:
                    logger.info(f"Succeeded with fallback URL #{i+1}: {url}")
                return response
                
        except Exception as e:
            logger.warning(f"URL #{i+1} failed ({url}): {e}")
            continue
    
    logger.error(f"All {len(urls)} URLs failed")
    return None


def create_session_with_config(pool_connections: int = 10,
                              pool_maxsize: int = 10,
                              max_retries: int = 3,
                              custom_headers: Optional[Dict[str, str]] = None) -> requests.Session:
    """
    Create configured requests session.
    
    Consolidates session creation patterns.
    
    Args:
        pool_connections: Number of connection pools
        pool_maxsize: Maximum connections per pool
        max_retries: Retry attempts
        custom_headers: Additional headers
        
    Returns:
        Configured session
        
    Example:
        session = create_session_with_config(max_retries=5)
        response = session.get('https://api.example.com')
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=1.0,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "TRACE"]
    )
    
    # Configure adapter
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=retry_strategy
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set headers
    default_headers = {
        'User-Agent': config.get('web_scraping.user_agent', 
                               'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    }
    
    if custom_headers:
        default_headers.update(custom_headers)
    
    session.headers.update(default_headers)
    session.verify = is_ssl_verify_enabled()
    
    return session


# Backwards compatibility aliases
http_get_with_retry = http_get
download_file_with_progress = download_with_progress


# Register cleanup on exit
import atexit
atexit.register(close_http_pool)


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Testing HTTP connection pooling...")
    
    # Test with global pool
    response = get("https://httpbin.org/get")
    print(f"Status: {response.status_code}")
    print(f"Headers: {dict(response.headers)}")
    
    # Test with context manager
    with HTTPPool() as pool:
        response = pool.get("https://httpbin.org/delay/1")
        print(f"\nDelayed response status: {response.status_code}")
        
        # Test multiple requests (connection reuse)
        for i in range(3):
            response = pool.get(f"https://httpbin.org/get?request={i}")
            print(f"Request {i+1} status: {response.status_code}")
    
    print("\n✓ HTTP connection pooling test complete!")