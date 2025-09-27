#!/usr/bin/env python3
"""
Centralized Regex Pattern Registry (DRY)
Consolidates scattered regex patterns throughout the codebase for consistency
Also includes Selenium helpers for consistent web automation
"""

import re
import time
from typing import Pattern, Dict, List
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
# DRY CONSOLIDATION - Step 1: Import centralized URL patterns
from .constants import URLPatterns


class PatternRegistry:
    """Central registry for all regex patterns used across the application"""
    
    # URL Patterns
    YOUTUBE_VIDEO_ID = re.compile(r'[a-zA-Z0-9_-]{11}')
    YOUTUBE_VIDEO_URL = re.compile(r'https?://(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})')
    YOUTUBE_SHORT_URL = re.compile(r'https?://youtu\.be/([a-zA-Z0-9_-]{11})')
    YOUTUBE_PLAYLIST_URL = re.compile(r'https?://(?:www\.)?youtube\.com/playlist\?list=([a-zA-Z0-9_-]+)')
    
    # Google Drive Patterns
    DRIVE_FILE_ID = re.compile(r'[a-zA-Z0-9_-]{25,}')
    DRIVE_FILE_URL = re.compile(r'https://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)')
    DRIVE_OPEN_URL = re.compile(r'https://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)')
    DRIVE_FOLDER_URL = re.compile(r'https://drive\.google\.com/drive/folders/([a-zA-Z0-9_-]+)')
    
    # Google Docs Patterns
    GOOGLE_DOC_ID = re.compile(r'[a-zA-Z0-9_-]{25,}')
    GOOGLE_DOC_URL = re.compile(r'https://docs\.google\.com/document/d/([a-zA-Z0-9_-]+)')
    GOOGLE_DOC_EDIT_URL = re.compile(r'https://docs\.google\.com/document/d/([a-zA-Z0-9_-]+)/edit')
    GOOGLE_DOC_VIEW_URL = re.compile(r'https://docs\.google\.com/document/d/([a-zA-Z0-9_-]+)/(?:view|preview)')
    GOOGLE_DOC_PUB_URL = re.compile(r'https://docs\.google\.com/document/d/([a-zA-Z0-9_-]+)/pub')
    GOOGLE_DOC_EXPORT_URL = re.compile(r'https://docs\.google\.com/document/d/([a-zA-Z0-9_-]+)/export\?format=(\w+)')
    
    # Enhanced URL extraction patterns
    YOUTUBE_VIDEO_FULL = re.compile(r'https?://(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})[^\s<>"]*')
    YOUTUBE_SHORT_FULL = re.compile(r'https?://youtu\.be/([a-zA-Z0-9_-]{11})[^\s<>"]*')
    YOUTUBE_PLAYLIST_FULL = re.compile(r'https?://(?:www\.)?youtube\.com/playlist\?list=([a-zA-Z0-9_-]+)[^\s<>"]*')
    DRIVE_FILE_FULL = re.compile(r'https://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)[^\s<>"]*')
    DRIVE_OPEN_FULL = re.compile(r'https://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)[^\s<>"]*')
    DRIVE_FOLDER_FULL = re.compile(r'https://drive\.google\.com/drive/folders/([a-zA-Z0-9_-]+)[^\s<>"]*')
    
    # Generic URL patterns  
    HTTP_URL = re.compile(r'https?://[^\s<>"{}\\|^\[\]`]+[^\s<>"{}\\|^\[\]`.,;:!?\)\]]')
    
    # Text cleaning patterns
    WHITESPACE_CLEANUP = re.compile(r'\s+')
    URL_TRAILING_PUNCTUATION = re.compile(r'[.,;:!?\)\]\}"\'>]+$')
    
    # YouTube parameter extraction
    YOUTUBE_VIDEO_PARAM = re.compile(r'v=([a-zA-Z0-9_-]{11})')
    YOUTUBE_LIST_PARAM = re.compile(r'[&?]list=([a-zA-Z0-9_-]+)')


# Pattern groups for related operations
YOUTUBE_PATTERNS = {
    'video': PatternRegistry.YOUTUBE_VIDEO_URL,
    'short': PatternRegistry.YOUTUBE_SHORT_URL,
    'playlist': PatternRegistry.YOUTUBE_PLAYLIST_URL,
    'video_full': PatternRegistry.YOUTUBE_VIDEO_FULL,
    'short_full': PatternRegistry.YOUTUBE_SHORT_FULL,
    'playlist_full': PatternRegistry.YOUTUBE_PLAYLIST_FULL,
}

DRIVE_PATTERNS = {
    'file': PatternRegistry.DRIVE_FILE_URL,
    'open': PatternRegistry.DRIVE_OPEN_URL,
    'folder': PatternRegistry.DRIVE_FOLDER_URL,
    'file_full': PatternRegistry.DRIVE_FILE_FULL,
    'open_full': PatternRegistry.DRIVE_OPEN_FULL,
    'folder_full': PatternRegistry.DRIVE_FOLDER_FULL,
}

GOOGLE_DOC_PATTERNS = {
    'doc': PatternRegistry.GOOGLE_DOC_URL,
    'edit': PatternRegistry.GOOGLE_DOC_EDIT_URL,
    'view': PatternRegistry.GOOGLE_DOC_VIEW_URL,
    'pub': PatternRegistry.GOOGLE_DOC_PUB_URL,
    'export': PatternRegistry.GOOGLE_DOC_EXPORT_URL,
}

CLEANING_PATTERNS = {
    'whitespace': PatternRegistry.WHITESPACE_CLEANUP,
    'trailing_punctuation': PatternRegistry.URL_TRAILING_PUNCTUATION,
}


# DRY CONSOLIDATION - Step 2: Import centralized extraction functions
try:
    from .url_utils import extract_youtube_id as _extract_youtube_id
    from .url_utils import extract_drive_id as _extract_drive_id
except ImportError:
    from url_utils import extract_youtube_id as _extract_youtube_id
    from url_utils import extract_drive_id as _extract_drive_id

# Convenience functions for common operations
def extract_youtube_id(url: str) -> str:
    """Extract YouTube video ID from URL (DRY CONSOLIDATION - Step 2)"""
    # Use centralized extraction logic from url_utils
    result = _extract_youtube_id(url)
    return result if result else ""


def extract_drive_id(url: str) -> str:
    """Extract Google Drive file ID from URL (DRY CONSOLIDATION - Step 2)"""
    # Use centralized extraction logic from url_utils
    result = _extract_drive_id(url)
    return result if result else ""


def extract_google_doc_id(url: str) -> str:
    """Extract Google Doc ID from any URL format"""
    for pattern in GOOGLE_DOC_PATTERNS.values():
        match = pattern.search(url)
        if match:
            return match.group(1)
    return ""


def is_google_doc_url(url: str) -> bool:
    """Check if URL is a Google Doc URL"""
    return any(pattern.search(url) for pattern in GOOGLE_DOC_PATTERNS.values())


def generate_doc_export_url(doc_id: str, format_type: str = "txt") -> str:
    """Generate export URL for a Google Doc"""
    return f"https://docs.google.com/document/d/{doc_id}/export?format={format_type}"


# DRY CONSOLIDATION: File Extension Detection Patterns
MEDIA_EXTENSIONS = {
    'video': ['.mp4', '.avi', '.mov', '.mkv', '.webm', '.flv', '.wmv', '.m4v'],
    'audio': ['.mp3', '.wav', '.m4a', '.aac', '.flac', '.ogg', '.wma'],
    'image': ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.tiff', '.svg'],
    'document': ['.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt'],
    'archive': ['.zip', '.rar', '.7z', '.tar', '.gz', '.bz2'],
    'data': ['.csv', '.json', '.xml', '.xlsx', '.xls']
}

def get_file_type(filename: str) -> str:
    """
    Standardized file type detection (DRY consolidation).
    
    Args:
        filename: Name of the file
        
    Returns:
        File type category ('video', 'audio', 'image', 'document', 'archive', 'data', 'unknown')
    """
    from pathlib import Path
    ext = Path(filename).suffix.lower()
    
    for file_type, extensions in MEDIA_EXTENSIONS.items():
        if ext in extensions:
            return file_type
    return 'unknown'

def is_media_file(filename: str) -> bool:
    """Check if file is a media file (video or audio)"""
    file_type = get_file_type(filename)
    return file_type in ['video', 'audio']

def get_file_extensions_by_type(file_type: str) -> List[str]:
    """Get list of extensions for a specific file type"""
    return MEDIA_EXTENSIONS.get(file_type, [])

def clean_url(url: str) -> str:
    """Clean URL using centralized patterns"""
    if not url:
        return url
    
    # Remove trailing punctuation
    url = PatternRegistry.URL_TRAILING_PUNCTUATION.sub('', url)
    
    return url.strip()


def normalize_whitespace(text: str) -> str:
    """Normalize whitespace using centralized pattern"""
    return PatternRegistry.WHITESPACE_CLEANUP.sub(' ', text).strip()


def is_youtube_url(url: str) -> bool:
    """Check if URL is a YouTube URL"""
    return any(pattern.search(url) for pattern in YOUTUBE_PATTERNS.values())


def is_drive_url(url: str) -> bool:
    """Check if URL is a Google Drive URL"""
    return any(pattern.search(url) for pattern in DRIVE_PATTERNS.values())


# Selenium helper functions (DRY)
def get_chrome_options() -> Options:
    """Get standardized Chrome options for Selenium WebDriver (DRY)"""
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # Use new headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    
    # Additional options to fix Chrome crashes
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Use temporary directory in /tmp for Chrome user data
    import tempfile
    import os
    
    # Handle running as root/sudo by setting HOME to /tmp
    if os.geteuid() == 0:  # Running as root
        os.environ['HOME'] = '/tmp'
    
    temp_dir = tempfile.mkdtemp(prefix="chrome_temp_", dir="/tmp")
    # Ensure directory has proper permissions
    os.chmod(temp_dir, 0o755)
    
    chrome_options.add_argument(f"--user-data-dir={temp_dir}")
    chrome_options.add_argument(f"--crash-dumps-dir={temp_dir}")
    
    # Try to find Chrome binary
    chrome_paths = [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/snap/bin/chromium",
        "/usr/local/bin/google-chrome"
    ]
    
    for path in chrome_paths:
        if os.path.exists(path):
            chrome_options.binary_location = path
            break
    
    # Additional stability options
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_experimental_option('prefs', {
        'profile.default_content_setting_values.notifications': 2,
        'profile.default_content_settings.popups': 0
    })
    
    return chrome_options


def wait_and_scroll_page(driver, wait_timeout: int = 30, scroll_delay: float = 0.1) -> None:
    """Wait for page load and scroll to ensure all content is loaded (DRY)
    
    Args:
        driver: Selenium WebDriver instance
        wait_timeout: Timeout in seconds for page load
        scroll_delay: Delay in seconds between scroll steps
    """
    # Wait for page to load
    WebDriverWait(driver, wait_timeout).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    
    # Extra wait for dynamic content (e.g., Google Docs)
    time.sleep(3)
    
    # Scroll to ensure all content is loaded
    height = driver.execute_script("return document.body.scrollHeight")
    for i in range(0, height, 300):
        driver.execute_script(f"window.scrollTo(0, {i});")
        time.sleep(scroll_delay)
    
    # Scroll back to top
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(1)


# Global selenium driver with enhanced management
import atexit
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

# Try to import webdriver_manager, but make it optional
try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False

# Initialize logger and error handling
try:
    from logging_config import get_logger
    from error_handling import with_standard_error_handling
    logger = get_logger(__name__)
except ImportError:
    try:
        from .logging_config import get_logger
        from .error_handling import with_standard_error_handling
        logger = get_logger(__name__)
    except ImportError:
        import logging
        logger = logging.getLogger(__name__)
        # Fallback decorator if error_handling not available
        def with_standard_error_handling(operation_name, return_on_error):
            def decorator(func):
                return func
            return decorator

_driver = None

@with_standard_error_handling("Selenium driver initialization", None)
def get_selenium_driver():
    """Get initialized Selenium WebDriver with standardized options and enhanced error handling (DRY)"""
    global _driver
    if _driver is None:
        logger.info("Initializing Selenium Chrome driver...")
        chrome_options = get_chrome_options()
        
        try:
            # Try direct Chrome driver first (requires chromedriver in PATH)
            _driver = webdriver.Chrome(options=chrome_options)
        except Exception as e1:
            if HAS_WEBDRIVER_MANAGER:
                # Try with webdriver_manager if available
                try:
                    _driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
                except Exception as e2:
                    logger.error(f"Error with webdriver_manager: {str(e2)}")
                    _driver = None
            else:
                logger.error(f"Error initializing Chrome driver: {str(e1)}")
                logger.error("Install chromedriver and ensure it's in PATH, or install webdriver-manager")
                _driver = None
    
    # Ensure driver is still alive
    try:
        _driver.title  # Simple check to see if driver is responsive
    except Exception:
        logger.warning("Driver was closed, reinitializing...")
        _driver = None
        return get_selenium_driver()
    
    return _driver

@with_standard_error_handling("Selenium driver cleanup", None)
def cleanup_selenium_driver():
    """Cleanup global Selenium driver with enhanced error handling (DRY)"""
    global _driver
    if _driver is not None:
        try:
            _driver.quit()
            _driver = None
            logger.info("Selenium driver cleaned up successfully")
        except Exception as e:
            logger.error(f"Error cleaning up Selenium driver: {e}")

# Register cleanup function to run on exit
atexit.register(cleanup_selenium_driver)


# Example usage
if __name__ == "__main__":
    # Test pattern registry
    test_urls = [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtu.be/dQw4w9WgXcQ",
        "https://drive.google.com/file/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/view",
        "https://drive.google.com/drive/folders/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    ]
    
    for url in test_urls:
        print(f"URL: {url}")
        print(f"  YouTube ID: {extract_youtube_id(url)}")
        print(f"  Drive ID: {extract_drive_id(url)}")
        print(f"  Is YouTube: {is_youtube_url(url)}")
        print(f"  Is Drive: {is_drive_url(url)}")
        print()


# === DRY PHASE 2: CONSOLIDATED URL PROCESSING PATTERNS ===

def normalize_url_for_comparison(url: str) -> str:
    """
    Normalize URLs for comparison purposes.
    
    Consolidates the repeated pattern found in 10+ files:
        def normalize_url(url):
            url = url.split('?')[0] if '?' in url else url
            url = url.rstrip('/')
            return url.strip()
    
    Args:
        url: URL to normalize
        
    Returns:
        Normalized URL string
        
    Example:
        normalized = normalize_url_for_comparison('https://youtu.be/abc123?t=10')
        # Returns: 'https://www.youtube.com/watch?v=abc123'
    """
    if not url or url in ['nan', 'None', '']:
        return ''
    
    # Clean the URL first
    url = clean_url(url)
    
    # Remove query parameters
    url = url.split('?')[0] if '?' in url else url
    
    # Remove trailing slashes
    url = url.rstrip('/')
    
    # Normalize common YouTube URL formats
    if 'youtube.com' in url or 'youtu.be' in url:
        # Extract video ID and normalize
        video_id = extract_youtube_video_id(url)
        if video_id:
            # DRY CONSOLIDATION - Step 1: Use centralized URL construction
            return URLPatterns.youtube_watch_url(video_id)
    
    # Normalize Google Drive URLs
    if 'drive.google.com' in url:
        drive_id = extract_drive_id(url)
        if drive_id:
            if '/folders/' in url:
                # DRY CONSOLIDATION - Step 1: Use centralized URL construction
                return URLPatterns.drive_folder_url(drive_id)
            else:
                # DRY CONSOLIDATION - Step 1: Use centralized URL construction
                return URLPatterns.drive_file_url(drive_id, view=False)
    
    return url.strip()


def normalize_url_for_truth_comparison(url: str) -> str:
    """
    Enhanced URL normalization for truth source comparison.
    
    Consolidates the specific pattern from test_all_30_rows.py and test_all_truth_source.py:
        def normalize_url_for_comparison(url):
            url = re.sub(r'[?&](si|usp|feature)=[^&\\s]*', '', url)
            url = url.rstrip('?&')
            
            if 'youtu.be/' in url:
                video_id = url.split('youtu.be/')[-1].split('?')[0]
                # DRY CONSOLIDATION - Step 1: Use centralized URL construction
                return URLPatterns.youtube_watch_url(video_id)
            
            if 'drive.google.com/file/d/' in url and not url.endswith('/view'):
                url = url.rstrip('/') + '/view'
            
            return url.rstrip('/')
    
    Args:
        url: URL to normalize for truth source comparison
        
    Returns:
        Normalized URL string suitable for truth source comparison
        
    Example:
        normalized = normalize_url_for_truth_comparison('https://youtu.be/abc123?si=tracking')
        # Returns: 'https://www.youtube.com/watch?v=abc123'
    """
    if not url or url in ['nan', 'None', '']:
        return ''
    
    # Remove tracking parameters specific to truth source comparison
    url = re.sub(r'[?&](si|usp|feature)=[^&\\s]*', '', url)
    url = url.rstrip('?&')
    
    # Normalize YouTube short URLs to standard format
    if 'youtu.be/' in url:
        video_id = url.split('youtu.be/')[-1].split('?')[0]
        # DRY CONSOLIDATION - Step 1: Use centralized URL construction
        return URLPatterns.youtube_watch_url(video_id)
    
    # Ensure Drive URLs end with /view for consistent comparison
    if 'drive.google.com/file/d/' in url and not url.endswith('/view'):
        url = url.rstrip('/') + '/view'
    
    return url.rstrip('/')


def compare_urls_for_truth(url1: str, url2: str) -> bool:
    """
    Compare URLs using truth source normalization rules.
    
    Consolidates the compare_urls function from test files:
        def compare_urls(found_url, expected_url):
            return normalize_url_for_comparison(found_url) == normalize_url_for_comparison(expected_url)
    
    Args:
        url1: First URL to compare
        url2: Second URL to compare
        
    Returns:
        True if URLs are equivalent after normalization
        
    Example:
        match = compare_urls_for_truth('https://youtu.be/abc123', 'https://www.youtube.com/watch?v=abc123')
        # Returns: True
    """
    return normalize_url_for_truth_comparison(url1) == normalize_url_for_truth_comparison(url2)


def extract_youtube_video_id(url: str) -> str:
    """
    Extract YouTube video ID from various URL formats.
    
    Consolidates the repeated pattern found in 10+ files with different regex patterns.
    
    Args:
        url: YouTube URL in any format
        
    Returns:
        11-character YouTube video ID or empty string if not found
        
    Example:
        video_id = extract_youtube_video_id('https://youtu.be/abc123?t=10')
        # Returns: 'abc123'
    """
    if not url:
        return ''
    
    # Use existing function for consistency
    return extract_youtube_id(url)


def extract_all_urls_from_text(text: str) -> Dict[str, list]:
    """
    Extract all URLs from text and categorize them.
    
    Consolidates URL extraction patterns used throughout the codebase.
    
    Args:
        text: Text to extract URLs from
        
    Returns:
        Dictionary with categorized URLs
        
    Example:
        urls = extract_all_urls_from_text(document_text)
        youtube_urls = urls['youtube']
        drive_urls = urls['drive']
        other_urls = urls['other']
    """
    result = {
        'youtube': [],
        'drive': [],
        'other': [],
        'all': []
    }
    
    # Find all HTTP URLs
    all_urls = PatternRegistry.HTTP_URL.findall(text)
    
    for url in all_urls:
        url = clean_url(url)
        if not url:
            continue
        
        result['all'].append(url)
        
        if is_youtube_url(url):
            result['youtube'].append(url)
        elif is_drive_url(url):
            result['drive'].append(url)
        else:
            result['other'].append(url)
    
    return result


def filter_meaningful_urls(urls: list, exclude_patterns: list = None) -> list:
    """
    Filter URLs to remove noise and infrastructure links.
    
    Consolidates meaningful link filtering patterns used in link extraction.
    
    Args:
        urls: List of URLs to filter
        exclude_patterns: Additional patterns to exclude
        
    Returns:
        List of filtered URLs
        
    Example:
        meaningful_urls = filter_meaningful_urls(all_urls)
        # Removes things like login pages, error pages, etc.
    """
    if exclude_patterns is None:
        exclude_patterns = []
    
    # Common noise patterns to exclude
    default_exclude_patterns = [
        'accounts.google.com',
        'login',
        'signin',
        'error',
        '404',
        'redirect',
        'auth',
        'oauth',
        'support.google.com',
        'policies.google.com',
        'privacy',
        'terms',
        'help',
        'about',
        'contact',
        'feedback',
        'report',
        'abuse',
        'developers.google.com',
        'cloud.google.com',
        'workspace.google.com',
        'gsuite.google.com',
        'admin.google.com',
        'myaccount.google.com',
        'google.com/search',
        'google.com/intl',
        'google.com/policies',
        'google.com/chrome',
        'google.com/gmail',
        'google.com/maps',
        'google.com/news',
        'google.com/shopping',
        'google.com/images',
        'google.com/translate',
        'google.com/calendar',
        'google.com/drive/help',
        'google.com/drive/apps',
        'docs.google.com/document/u/0',
        'docs.google.com/spreadsheets/u/0',
        'docs.google.com/presentation/u/0',
        'docs.google.com/forms/u/0',
        'drive.google.com/drive/u/0',
        'drive.google.com/drive/my-drive',
        'drive.google.com/drive/shared-with-me',
        'drive.google.com/drive/recent',
        'drive.google.com/drive/starred',
        'drive.google.com/drive/trash',
        'drive.google.com/drive/activity',
        'drive.google.com/drive/settings',
        'www.youtube.com/feed',
        'www.youtube.com/channel',
        'www.youtube.com/user',
        'www.youtube.com/results',
        'www.youtube.com/playlist',
        'music.youtube.com',
        'youtube.com/shorts',
        'youtube.com/live',
        'youtube.com/gaming',
        'youtube.com/trending',
        'youtube.com/subscriptions',
        'youtube.com/history',
        'youtube.com/library',
        'youtube.com/account',
        'youtube.com/upload',
        'youtube.com/create',
        'youtube.com/studio',
        'youtube.com/analytics',
        'youtube.com/ads',
        'youtube.com/premium',
        'youtube.com/music',
        'youtube.com/tv',
        'youtube.com/kids',
        'youtube.com/howyoutubeworks',
        'youtube.com/about',
        'youtube.com/press',
        'youtube.com/copyright',
        'youtube.com/policies',
        'youtube.com/safety',
        'youtube.com/creators',
        'youtube.com/advertise',
        'youtube.com/developer',
        'youtube.com/terms',
        'youtube.com/privacy',
        'youtube.com/community',
        'youtube.com/intl'
    ]
    
    all_exclude_patterns = default_exclude_patterns + exclude_patterns
    
    filtered_urls = []
    for url in urls:
        if not url or url in ['nan', 'None', '']:
            continue
        
        # Check if URL contains any exclude pattern
        url_lower = url.lower()
        if any(pattern.lower() in url_lower for pattern in all_exclude_patterns):
            continue
        
        # Additional checks for meaningful content
        if is_youtube_url(url):
            # Only keep video URLs, not channel/playlist/etc
            if extract_youtube_video_id(url):
                filtered_urls.append(url)
        elif is_drive_url(url):
            # Keep all drive URLs (files and folders)
            filtered_urls.append(url)
        else:
            # Keep other URLs if they don't match exclude patterns
            filtered_urls.append(url)
    
    return filtered_urls


def validate_url_format(url: str, url_type: str = 'any') -> bool:
    """
    Validate URL format for specific types.
    
    Consolidates URL validation patterns used throughout the codebase.
    
    Args:
        url: URL to validate
        url_type: Type to validate ('youtube', 'drive', 'any')
        
    Returns:
        True if URL is valid for the specified type
        
    Example:
        is_valid = validate_url_format(url, 'youtube')
        if is_valid:
            process_youtube_url(url)
    """
    if not url or url in ['nan', 'None', '']:
        return False
    
    if url_type == 'youtube':
        return is_youtube_url(url) and bool(extract_youtube_video_id(url))
    elif url_type == 'drive':
        return is_drive_url(url) and bool(extract_drive_id(url))
    elif url_type == 'any':
        return bool(PatternRegistry.HTTP_URL.match(url))
    else:
        return False


def standardize_url_format(url: str) -> str:
    """
    Standardize URL format for storage and comparison.
    
    Consolidates URL standardization patterns used in CSV and database storage.
    
    Args:
        url: URL to standardize
        
    Returns:
        Standardized URL string
        
    Example:
        standard_url = standardize_url_format('HTTPS://YOUTU.BE/ABC123?T=10')
        # Returns: 'https://www.youtube.com/watch?v=ABC123'
    """
    if not url or url in ['nan', 'None', '']:
        return ''
    
    # Convert to lowercase and normalize
    url = url.lower().strip()
    
    # Use normalize_url_for_comparison for consistency
    return normalize_url_for_comparison(url)