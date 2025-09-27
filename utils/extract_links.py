from bs4 import BeautifulSoup
import re
import os
import json
import time
import atexit
import urllib.parse
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    from http_pool import get as http_get
    from config import get_config
    from logging_config import get_logger
    from rate_limiter import rate_limit, wait_for_rate_limit
    from patterns import clean_url, get_selenium_driver, cleanup_selenium_driver, is_google_doc_url
    from error_handling import with_standard_error_handling
    from google_docs_http import extract_google_doc_with_http_fallback
    HAS_HTTP_EXTRACTION = True
except ImportError:
    try:
        from .http_pool import get as http_get
        from .config import get_config
        from .logging_config import get_logger
        from .rate_limiter import rate_limit, wait_for_rate_limit
        from .patterns import clean_url, get_selenium_driver, cleanup_selenium_driver, is_google_doc_url
        from .error_handling import with_standard_error_handling
        from .google_docs_http import extract_google_doc_with_http_fallback
        HAS_HTTP_EXTRACTION = True
    except ImportError:
        from .http_pool import get as http_get
        from .config import get_config
        from .logging_config import get_logger
        from .rate_limiter import rate_limit, wait_for_rate_limit
        from .patterns import clean_url, get_selenium_driver, cleanup_selenium_driver
        from .error_handling import with_standard_error_handling
        HAS_HTTP_EXTRACTION = False

# Setup module logger
logger = get_logger(__name__)

# Get configuration
config = get_config()

# Base directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(BASE_DIR, "cache")

# Only cache Google Sheets HTML
GOOGLE_SHEET_CACHE_FILE = os.path.join(CACHE_DIR, "google_sheet_cache.html")

# Selenium driver functions are now imported from patterns.py (DRY consolidation)

@rate_limit('selenium')
@with_standard_error_handling("Selenium HTML extraction", "")
def get_html_with_selenium(url, debug=False):
    """Get HTML using Selenium for JavaScript rendering"""
    driver = get_selenium_driver()
    if not driver:
        logger.error("Failed to initialize Selenium driver")
        return ""
    
    logger.info(f"Loading {url} with Selenium...")
    try:
        driver.get(url)
        # Wait for page to load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        # Extra wait for Google Docs to render
        time.sleep(5)
        
        # For Google Docs, try scrolling to ensure all content is loaded
        if "docs.google.com/document" in url:
            # Scroll down in small increments to ensure content loads
            height = driver.execute_script("return document.body.scrollHeight")
            for i in range(0, height, 300):
                driver.execute_script(f"window.scrollTo(0, {i});")
                time.sleep(0.1)
            # Scroll back to top
            driver.execute_script("window.scrollTo(0, 0);")
        
        html = driver.page_source
        
        # For debugging, save the HTML content
        if debug:
            # DRY CONSOLIDATION: Use path_utils for directory creation
            from .path_utils import ensure_directory
            ensure_directory(CACHE_DIR)
                
            debug_file = os.path.join(CACHE_DIR, f"selenium_debug_{url.replace('://', '_').replace('/', '_').replace('?', '_').replace('=', '_')}.html")
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html)
            logger.debug(f"Saved Selenium debug HTML to {debug_file}")
        
        return html
    except Exception as e:
        logger.error(f"Error loading {url} with Selenium: {str(e)}")
        return ""

@with_standard_error_handling("Google Doc text extraction", "")
def extract_google_doc_text(url, driver=None, prefer_http=True):
    """Enhanced Google Doc text extraction with HTTP-first approach and Selenium fallback
    
    Consolidates extraction logic with new HTTP export method for better performance.
    Tries HTTP export first (fast), falls back to Selenium (robust) if needed.
    
    Args:
        url (str): Google Doc URL to extract text from
        driver: Optional existing Selenium driver instance
        prefer_http (bool): Whether to try HTTP extraction first (default: True)
        
    Returns:
        str: Extracted text content from the document
    """
    
    # Try HTTP extraction first if enabled and preferred for Google Docs
    if prefer_http and HAS_HTTP_EXTRACTION and is_google_doc_url(url):
        try:
            logger.info(f"Attempting HTTP-first extraction for Google Doc: {url}")
            content, error = extract_google_doc_with_http_fallback(url)
            if not error and content and len(content.strip()) > 0:
                logger.info(f"HTTP extraction successful: {len(content)} characters")
                return content
            else:
                logger.warning(f"HTTP extraction failed: {error}, falling back to Selenium")
        except Exception as e:
            logger.warning(f"HTTP extraction error: {str(e)}, falling back to Selenium")
    
    # Existing Selenium implementation continues here...
    # Use provided driver or get the shared one
    if driver is None:
        driver = get_selenium_driver()
    if not driver:
        logger.error("Failed to initialize Selenium driver")
        return ""
    
    logger.info(f"Loading Google Doc with enhanced extraction: {url}")
    start_time = time.time()
    driver.get(url)
    
    # Wait for page to load
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    load_time = time.time() - start_time
    logger.info(f"Page loaded in {load_time:.2f} seconds")
    
    # Dynamic wait for content to stabilize
    logger.info("Waiting for content to stabilize...")
    previous_content_length = 0
    stable_checks = 0
    max_wait = 30
    start_wait = time.time()
    
    while time.time() - start_wait < max_wait:
        try:
            current_content_length = driver.execute_script("""
                var content = document.body.innerText || '';
                var editables = document.querySelectorAll('[contenteditable="true"]');
                for (var i = 0; i < editables.length; i++) {
                    content += editables[i].innerText || '';
                }
                return content.length;
            """)
            
            if current_content_length > previous_content_length:
                previous_content_length = current_content_length
                stable_checks = 0
                time.sleep(2)
            elif current_content_length == previous_content_length and current_content_length > 100:
                stable_checks += 1
                if stable_checks >= 2:
                    logger.info(f"Content stabilized at {current_content_length} chars")
                    break
                time.sleep(1)
            else:
                time.sleep(2)
        except:
            time.sleep(2)
    
    # Enhanced JavaScript-based extraction
    logger.info("Extracting content with JavaScript...")
    extraction_result = driver.execute_script("""
            var content = '';
            
            // Method 1: Extract from contenteditable areas (main document content)
            var editables = document.querySelectorAll('[contenteditable="true"]');
            for (var i = 0; i < editables.length; i++) {
                var text = editables[i].innerText || editables[i].textContent || '';
                if (text.length > 20) {
                    content += text + '\\n';
                }
            }
            
            // Method 2: Extract from document/textbox roles
            var docElements = document.querySelectorAll('[role="document"], [role="textbox"]');
            for (var i = 0; i < docElements.length; i++) {
                var text = docElements[i].innerText || docElements[i].textContent || '';
                if (text.length > 20 && content.indexOf(text.substring(0, 50)) === -1) {
                    content += text + '\\n';
                }
            }
            
            // Method 3: Look for Google Docs specific content areas
            var kixElements = document.querySelectorAll('[class*="kix"]');
            for (var i = 0; i < kixElements.length; i++) {
                var text = kixElements[i].innerText || kixElements[i].textContent || '';
                if (text.length > 50 && content.indexOf(text.substring(0, 50)) === -1) {
                    content += text + '\\n';
                }
            }
            
            // Method 4: Try iframe content if accessible
            var iframes = document.querySelectorAll('iframe');
            for (var i = 0; i < iframes.length; i++) {
                try {
                    var iframeDoc = iframes[i].contentDocument || iframes[i].contentWindow.document;
                    if (iframeDoc && iframeDoc.body) {
                        var text = iframeDoc.body.innerText || iframeDoc.body.textContent || '';
                        if (text.length > 50 && content.indexOf(text.substring(0, 50)) === -1) {
                            content += text + '\\n';
                        }
                    }
                } catch (e) {
                    // Cross-origin iframe, skip
                }
            }
            
            // Fallback: get all meaningful text
            if (content.length < 100) {
                content = document.body.innerText || document.body.textContent || '';
            }
            
            return content.trim();
    """)
    
    # Clean up the extracted content
    text_content = re.sub(r'\s+', ' ', extraction_result).strip()
    
    # Additional verification - ensure we got meaningful content
    if len(text_content) < 50:
        logger.warning("Low content extraction, trying fallback...")
        # Fallback to BeautifulSoup extraction
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        body = soup.find('body')
        if body:
            fallback_text = body.get_text(separator=' ', strip=True)
            if len(fallback_text) > len(text_content):
                text_content = fallback_text
    
    logger.info(f"Enhanced extraction: {len(text_content)} characters")
    
    # Quality assessment
    if len(text_content) > 200:
        logger.debug("Quality: High content volume")
    elif len(text_content) > 50:
        logger.debug("Quality: Moderate content volume")
    else:
        logger.warning("Quality: Low content volume - may need manual review")
    
    return text_content
        # Note: Error handling now provided by @with_standard_error_handling decorator (DRY)

@with_standard_error_handling("Document text extraction with retry", ("", "Failed to extract text"))
def extract_text_with_retry(doc_url, max_attempts=None):
    """Extract text from document with retry logic (DRY consolidation)"""
    if max_attempts is None:
        max_attempts = config.get("retry.max_attempts", 3)
    
    for attempt in range(max_attempts):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_attempts}: Extracting text from {doc_url}")
            text = extract_google_doc_text(doc_url)
            if text and len(text.strip()) > 0:
                logger.info(f"Successfully extracted {len(text)} characters")
                return text, None
            else:
                logger.warning(f"No text extracted on attempt {attempt + 1}")
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Attempt {attempt + 1} failed: {error_msg}")
            if attempt < max_attempts - 1:
                retry_delay = config.get("retry.base_delay", 2.0)
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
    
    return "", f"Failed after {max_attempts} attempts"

def extract_actual_url(google_url):
    """Extract the actual URL from a Google redirect URL
    
    Consolidates URL extraction logic from multiple duplicate implementations.
    
    Args:
        google_url (str): Google redirect URL (starts with https://www.google.com/url?q=)
        
    Returns:
        str: The actual URL extracted from the Google redirect, or original URL if not a redirect
    """
    if not google_url.startswith("https://www.google.com/url?q="):
        return google_url
    
    # Extract the 'q' parameter which contains the actual URL
    start_idx = google_url.find("q=") + 2
    end_idx = google_url.find("&", start_idx)
    if end_idx == -1:
        actual_url = google_url[start_idx:]
    else:
        actual_url = google_url[start_idx:end_idx]
    
    # URL decode
    return urllib.parse.unquote(actual_url)

@with_standard_error_handling("HTML download", "")
def get_html(url, debug=False):
    """Get HTML from the web without caching (except for Google Sheets)"""
    # Use Selenium for Google Docs to handle JavaScript rendering
    if "docs.google.com/document" in url:
        return get_html_with_selenium(url, debug)
    
    logger.info(f"Downloading HTML for {url}")
    try:
        # Headers are already configured in http_pool
        # Just use streaming
        response = http_get(url, stream=True)
        response.raise_for_status()
        
        # Stream HTML content to avoid loading large pages into memory
        html = ""
        chunk_size = 8192
        for chunk in response.iter_content(chunk_size=chunk_size, decode_unicode=True):
            if chunk:
                html += chunk
        
        # For debugging, save the HTML content
        if debug:
            # DRY CONSOLIDATION: Use path_utils for directory creation
            from .path_utils import ensure_directory
            ensure_directory(CACHE_DIR)
                
            debug_file = os.path.join(CACHE_DIR, f"debug_{url.replace('://', '_').replace('/', '_').replace('?', '_').replace('=', '_')}.html")
            with open(debug_file, 'w', encoding='utf-8') as f:
                # Write HTML in chunks if it's very large
                if len(html) > 1024 * 1024:  # 1MB
                    for i in range(0, len(html), chunk_size):
                        f.write(html[i:i+chunk_size])
                else:
                    f.write(html)
            logger.debug(f"Saved debug HTML to {debug_file}")
        
        # Only cache Google Sheets
        if "docs.google.com/spreadsheets" in url and html:
            with open(GOOGLE_SHEET_CACHE_FILE, 'w', encoding='utf-8') as f:
                # Write in chunks for large Google Sheets
                if len(html) > 1024 * 1024:  # 1MB
                    for i in range(0, len(html), chunk_size):
                        f.write(html[i:i+chunk_size])
                else:
                    f.write(html)
            logger.info(f"Cached Google Sheet HTML to {GOOGLE_SHEET_CACHE_FILE}")
        
        # Add a small delay to ensure the page has time to render
        time.sleep(1)
        
        return html
    except Exception as e:
        # Log error but don't fail completely - some URLs might be temporarily unavailable
        logger.warning(f"Error downloading {url}: {str(e)}")
        # Return empty string to allow processing to continue
        # Caller should check for empty response
        return ""

def extract_links(url, limit=1, debug=False):
    """
    Extract links from a given URL. Limit parameter restricts the number of links returned.
    For Google Docs, we use Selenium to handle JavaScript rendering.
    
    Returns a tuple of (all_links, drive_links) where drive_links is a list of Google Drive URLs.
    """
    # Get the HTML content with the appropriate method
    html = get_html(url, debug=debug)
    if not html:
        return []
    
    if debug:
        logger.debug(f"Downloaded HTML for debugging purposes ({len(html)} bytes)")
    
    # Google Docs special handling
    if "docs.google.com/document" in url:
        logger.info(f"Google document detected: {url}")
        # Add original URL to results
        result = [url]
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract links from anchor tags
        doc_links = set()
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            if href:
                # Decode unicode escapes in href
                try:
                    if '\\u' in href:
                        href = href.encode('utf-8').decode('unicode_escape')
                except:
                    pass
                doc_links.add(clean_url(href))
        
        # Get links appearing in plain text (emails, URLs)
        # First decode unicode escapes in the HTML before extracting
        # Google Docs often encodes URL characters as unicode escapes
        decoded_html = html
        try:
            # Replace common unicode escapes for URL characters
            # \u003d is =, \u0026 is &, \u003f is ?
            decoded_html = decoded_html.replace('\\u003d', '=')
            decoded_html = decoded_html.replace('\\u0026', '&')
            decoded_html = decoded_html.replace('\\u003f', '?')
        except:
            pass
        
        # Updated regex to properly terminate URLs at common boundaries
        # Explicitly exclude all control characters (ASCII 0-31 and 127-159)
        raw_text_links = re.findall(r'https?://[^\s<>"{}\\|\^\[\]`\x00-\x1f\x7f-\x9f]+(?:[.,;:!?\)\]]*(?=[\s<>"{}\\|\^\[\]`\x00-\x1f\x7f-\x9f]|$))', decoded_html)
        text_links = {clean_url(link) for link in raw_text_links if link}
        email_links = set(re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', html))
        
        # Parse meta tags for content links
        meta_links = set()
        for meta in soup.find_all('meta', property=lambda x: x and ('og:description' in x or 'description' in x)):
            content = meta.get('content', '')
            if content:
                # Decode unicode escapes in meta content
                try:
                    content = content.replace('\\u003d', '=').replace('\\u0026', '&').replace('\\u003f', '?')
                except:
                    pass
                # Find URLs in content
                raw_meta_links = re.findall(r'https?://[^\s<>"{}\\|\^\[\]`\x00-\x1f\x7f-\x9f]+(?:[.,;:!?\)\]]*(?=[\s<>"{}\\|\^\[\]`\x00-\x1f\x7f-\x9f]|$))', content)
                meta_links.update(clean_url(link) for link in raw_meta_links if link)
                # Find emails in content
                meta_links.update([f"mailto:{email}" for email in 
                                 re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', content)])
        
        # Add email links with mailto: prefix
        email_links = {f"mailto:{email}" for email in email_links}
        
        # Combine all links
        all_links = doc_links.union(text_links).union(email_links).union(meta_links)
        
        # Filter out infrastructure links and keep only content links
        filtered_links = set()
        
        # Always include the original document URL
        filtered_links.add(url)
        
        # Include emails and specific content links
        for link in all_links:
            # Keep email links
            if link.startswith('mailto:'):
                filtered_links.add(link)
                continue
                
            # Keep drive links that aren't infrastructure
            if 'drive.google.com/drive' in link and 'folder' in link:
                # Clean up Google Drive links
                if '?usp' in link:
                    clean_link = link.split('?usp')[0]
                    filtered_links.add(clean_link)
                else:
                    filtered_links.add(link)
                continue
                
            # Check if it's a content link and not an infrastructure link
            is_infrastructure = any([
                'gstatic.com' in link,
                'apis.google.com' in link,
                'script.google.com' in link,
                'chrome.google.com' in link,
                'clients6.google.com' in link,
                '/static/' in link,
                'accounts.google.com' in link,
                'docs.google.com/picker' in link,
                'docs.google.com/relay.html' in link,
                'contacts.google.com' in link,
                'lh7-rt.googleusercontent.com' in link,
                'googleusercontent.com/docs' in link,
                'schema.org' in link,
                'w3.org' in link,
                '#' in link,
                link.endswith('.js'),
                link.endswith('.css'),
                link.endswith('.png'),
                link.endswith('.gif'),
                'docs.google.com/static' in link,
                'docs.google.com/preview' in link,
                'docs.google.com?usp=' in link,
                '",s-blob-v1-IMAGE-' in link,
                '"' in link,
                'support.google.com' in link,
                "}.config['csfu']" in link
            ])
            
            if not is_infrastructure:
                # Clean up URL if needed (remove trailing quotes or parentheses, etc.)
                link = re.sub(r'[\"\'\)]$', '', link)
                
                # Skip links that have JSON/code markers or font references
                if any([
                    '{' in link,
                    '}' in link,
                    '[' in link,
                    ']' in link,
                    'si:' in link,
                    'ei:' in link,
                    'sm:' in link,
                    'spi:' in link,
                    'docs/fonts' in link,
                    '.woff' in link
                ]):
                    continue
                    
                # Skip Google Doc internal links that aren't really content
                if link.startswith('https://docs.google.com') and link != url:
                    if any([
                        'usp\u003d' in link,
                        '/preview' in link,
                        '/edit?' in link and url.split('/edit')[0] in link
                    ]):
                        continue
                
                filtered_links.add(link)
        
        # Remove duplicates and return
        result = list(filtered_links)
        return result[:limit] if limit > 0 else result
    
    # Regular drive.google.com links
    elif "drive.google.com" in url:
        logger.info(f"Google Drive detected: {url}")
        # Add original URL to results
        result = [url]
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract links from anchor tags
        drive_links = {a.get('href') for a in soup.find_all('a', href=True)}
        
        # Get links appearing in plain text
        text_links = set(re.findall(r'https?://\S+', html))
        
        # Combine all links
        all_links = drive_links.union(text_links)
        
        # Filter out empty or None links
        all_links = {link for link in all_links if link and not link.startswith('javascript:')}
        
        # Add them to result
        result.extend(all_links)
        
        # Remove duplicates and return
        result = list(set(result))
        return result[:limit] if limit > 0 else result
    
    # For other sites, regular link extraction
    soup = BeautifulSoup(html, 'html.parser')
    # Get links from anchor tags
    links = {a.get('href') for a in soup.find_all('a', href=True)}
    # Get links appearing in plain text
    text_links = set(re.findall(r'https?://\S+', html))
    result = list(links.union(text_links))
    
    # Filter out empty or None links
    result = [link for link in result if link]
    
    # Return only the requested number of links
    return result[:limit] if limit > 0 else result


from urllib.parse import urlparse, parse_qs

def extract_drive_links_from_html(html):
    """Extract Google Drive links directly from HTML content"""
    import re
    
    # DRY CONSOLIDATION - Step 2: Use centralized patterns and URL construction
    try:
        from .patterns import DRIVE_PATTERNS, extract_drive_id
        from .constants import URLPatterns
    except ImportError:
        from patterns import DRIVE_PATTERNS, extract_drive_id
        from constants import URLPatterns
    
    # Use centralized patterns for extraction
    drive_urls = []
    for pattern_name, pattern in DRIVE_PATTERNS.items():
        if pattern_name.endswith('_full'):  # Use full patterns for HTML extraction
            matches = pattern.findall(html)
            drive_urls.extend(matches)
    
    # Process found URLs and extract IDs
    seen_ids = set()
    unique_links = []
    
    for url in drive_urls:
        drive_id = extract_drive_id(url)
        if drive_id and drive_id not in seen_ids:
            seen_ids.add(drive_id)
            # Determine if it's a folder or file
            if '/folders/' in url or 'folders/' in url:
                unique_links.append(URLPatterns.drive_folder_url(drive_id))
            else:
                unique_links.append(URLPatterns.drive_file_url(drive_id, view=True))
    
    return unique_links

def extract_drive_links(links, html=None):
    """Extract Google Drive links from URLs and optionally from HTML content"""
    drive_urls = []
    
    # First extract from regular links
    for link in links:
        try:
            # Skip non-URL links or invalid URLs
            if not link.startswith('http'):
                continue
                
            # Match Google Drive file links
            if ('drive.google.com/file/d/' in link or 
                'drive.google.com/open?id=' in link or
                'drive.google.com/drive/folders/' in link):
                drive_urls.append(link)
        except Exception as e:
            print(f"Error parsing drive link {link}: {str(e)}")
            continue
    
    # If HTML content is provided, also extract links from there
    if html:
        html_drive_links = extract_drive_links_from_html(html)
        drive_urls.extend([link for link in html_drive_links if link not in drive_urls])
    
    return drive_urls

def extract_youtube_ids(links):
    # DRY CONSOLIDATION - Step 2: Use centralized YouTube ID extraction
    try:
        from .patterns import extract_youtube_id
    except ImportError:
        from patterns import extract_youtube_id
    
    yt_ids = set()
    for link in links:
        try:
            # Skip non-URL links like mailto: or invalid URLs
            if not link.startswith('http'):
                continue
            
            # Use centralized extraction function
            video_id = extract_youtube_id(link)
            if video_id:
                yt_ids.add(video_id)
        except Exception as e:
            print(f"Error parsing link {link}: {str(e)}")
            continue
    return list(yt_ids)

def extract_youtube_playlists(links):
    """Extract YouTube playlist URLs from a list of links.
    
    This function specifically looks for actual YouTube playlists (not synthetic ones)
    and returns clean playlist URLs with just the list parameter.
    
    Args:
        links: List of URLs to search for playlists
        
    Returns:
        List of clean YouTube playlist URLs
    """
    playlists = []
    for link in links:
        try:
            # Skip non-URL links
            if not link.startswith('http'):
                continue
                
            parsed = urlparse(link)
            if "youtube.com" in parsed.netloc and "/playlist" in parsed.path:
                qs = parse_qs(parsed.query)
                if "list" in qs and qs["list"]:
                    # Reconstruct clean playlist URL with only the list parameter
                    playlist_id = qs["list"][0]
                    playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
                    if playlist_url not in playlists:
                        playlists.append(playlist_url)
        except Exception as e:
            logger.error(f"Error parsing playlist link {link}: {str(e)}")
            continue
    return playlists

def build_youtube_playlist_url(yt_ids):
    # DRY CONSOLIDATION - Step 2: Use centralized URL pattern
    try:
        from .constants import URLPatterns
    except ImportError:
        from constants import URLPatterns
    
    if yt_ids:
        return URLPatterns.YOUTUBE_WATCH_VIDEOS + ",".join(yt_ids)
    return None

def process_url(url, limit=1, debug=False):
    """
    Process a URL to extract links, YouTube playlists, and Google Drive links.
    
    Args:
        url (str): The URL to process
        limit (int): Maximum number of links to return. Default is 1 to only return the primary link.
        debug (bool): Whether to save HTML content for debugging purposes
    
    Returns:
        tuple: (list of links, YouTube playlist URL or None, list of Google Drive links or None)
    """
    # Check if this is a YouTube video URL (not a playlist)
    if url and ('youtube.com/watch' in url or 'youtu.be/' in url):
        # Skip processing individual YouTube videos to avoid large data extraction
        logger.info(f"Skipping link extraction for YouTube video: {url}")
        return [], None, None
    
    # First get the HTML content
    html = ""
    if "docs.google.com/document" in url:
        html = get_html_with_selenium(url, debug=debug)
    elif "drive.google.com" in url:
        html = get_html(url, debug=debug)
    
    # Extract regular links
    links = extract_links(url, limit, debug=debug)
    
    # Process links to extract YouTube content and Drive links
    if links:
        # First check for actual YouTube playlists
        youtube_playlists = extract_youtube_playlists(links)
        
        if youtube_playlists:
            # If we found actual playlists, use them (join with | for multiple)
            yt_playlist_url = "|".join(youtube_playlists)
        else:
            # Fall back to synthetic playlist from individual videos
            yt_ids = extract_youtube_ids(links)
            yt_playlist_url = build_youtube_playlist_url(yt_ids)
        
        # Extract Drive links from both regular links and HTML content
        drive_links = extract_drive_links(links, html=html)
        
        # Return None for empty values
        if not yt_playlist_url:
            yt_playlist_url = None
        if not drive_links:
            drive_links = None
        
        return links, yt_playlist_url, drive_links
    
    # Even if no regular links were found, try to extract Drive links from HTML
    elif html:
        drive_links = extract_drive_links([], html=html)
        
        # Return None for empty values
        if not drive_links:
            drive_links = None
                
        return [], None, drive_links
    
    # Return None for empty values
    return [], None, None
    
# ============================================================================
# EXTRACTION STRATEGY CONSOLIDATION (DRY Refactoring Phase 1.4)
# ============================================================================

# Consolidates functionality from:
# - extract_single_doc.py (Selenium-based)
# - extract_doc_simple.py (HTTP requests-based)  
# - extract_chromium.py (Chromium subprocess-based)

import subprocess
import tempfile
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

class ExtractionStrategy:
    """Base class for document extraction strategies"""
    
    def extract_content(self, url: str) -> str:
        """Extract text content from URL. Subclasses must implement this."""
        raise NotImplementedError("Subclasses must implement extract_content")
    
    def is_suitable_for(self, url: str) -> bool:
        """Check if this strategy is suitable for the given URL"""
        return True  # Default: suitable for all URLs

class SeleniumExtractionStrategy(ExtractionStrategy):
    """Selenium-based extraction strategy (consolidates extract_single_doc.py)"""
    
    def extract_content(self, url: str) -> str:
        """Extract content using Selenium WebDriver"""
        logger.info(f"Using Selenium strategy for: {url}")
        
        # Enhanced Chrome options for root user (from extract_single_doc.py)
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument("--user-data-dir=/tmp/chrome-user-data")
        chrome_options.add_argument("--data-path=/tmp/chrome-data")
        chrome_options.add_argument("--disk-cache-dir=/tmp/chrome-cache")
        chrome_options.add_argument("--homedir=/tmp")
        chrome_options.add_argument("--window-size=1920,1080")
        
        try:
            # DRY CONSOLIDATION: Use centralized Selenium driver
            driver = get_selenium_driver()
            logger.info("Loading document...")
            driver.get(url)
            
            # Wait for document to load
            wait = WebDriverWait(driver, 30)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Try multiple selectors for Google Docs content
            selectors = [
                '[role="document"]',
                '[contenteditable="true"]',
                '.kix-page',
                '.doc-content',
                'body'
            ]
            
            content = ""
            for selector in selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        for element in elements:
                            text = element.text.strip()
                            if len(text) > len(content):
                                content = text
                        if content:
                            break
                except Exception:
                    continue
            
            driver.quit()
            return content
            
        except Exception as e:
            logger.error(f"Selenium extraction failed: {str(e)}")
            return ""

class HttpExtractionStrategy(ExtractionStrategy):
    """HTTP requests-based extraction strategy (consolidates extract_doc_simple.py)"""
    
    def extract_content(self, url: str) -> str:
        """Extract content using HTTP requests"""
        logger.info(f"Using HTTP strategy for: {url}")
        
        # Extract document ID (from extract_doc_simple.py)
        doc_id_match = re.search(r'/document/d/([a-zA-Z0-9-_]+)', url)
        if not doc_id_match:
            logger.warning("Could not extract document ID for HTTP strategy")
            return ""
        
        doc_id = doc_id_match.group(1)
        
        # Try different URL formats (from extract_doc_simple.py)
        urls_to_try = [
            f"https://docs.google.com/document/d/{doc_id}/pub",
            f"https://docs.google.com/document/d/{doc_id}/preview", 
            f"https://docs.google.com/document/d/{doc_id}/edit",
            url
        ]
        
        # DRY CONSOLIDATION - Step 5: Use centralized HTTP header configuration
        from .config import get_config
        config = get_config()
        headers = {
            'User-Agent': config.get('web_scraping.user_agent', 
                                   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        }
        
        for test_url in urls_to_try:
            try:
                logger.debug(f"Trying URL: {test_url}")
                response = requests.get(test_url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    # Extract text content
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Remove script and style elements
                    for script in soup(["script", "style"]):
                        script.decompose()
                    
                    # Get text and clean up
                    text = soup.get_text()
                    lines = (line.strip() for line in text.splitlines())
                    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                    text = ' '.join(chunk for chunk in chunks if chunk)
                    
                    if len(text.strip()) > 100:  # Only return if we got substantial content
                        logger.info(f"HTTP extraction successful: {len(text)} characters")
                        return text
                        
            except Exception as e:
                logger.debug(f"HTTP attempt failed for {test_url}: {str(e)}")
                continue
        
        logger.warning("All HTTP extraction attempts failed")
        return ""

class ChromiumExtractionStrategy(ExtractionStrategy):
    """Chromium subprocess-based extraction strategy (consolidates extract_chromium.py)"""
    
    def is_suitable_for(self, url: str) -> bool:
        """Check if Chromium is available on the system"""
        try:
            subprocess.run(['/usr/bin/chromium-browser', '--version'], 
                         capture_output=True, timeout=5)
            return True
        except Exception:
            return False
    
    def extract_content(self, url: str) -> str:
        """Extract content using Chromium subprocess"""
        logger.info(f"Using Chromium strategy for: {url}")
        
        try:
            # Run chromium in headless mode (from extract_chromium.py)
            cmd = [
                '/usr/bin/chromium-browser',
                '--headless',
                '--no-sandbox', 
                '--disable-gpu',
                '--disable-dev-shm-usage',
                '--disable-extensions',
                '--virtual-time-budget=10000',
                '--dump-dom',
                '--user-data-dir=/tmp/chromium-data',
                url
            ]
            
            logger.debug("Running Chromium...")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                # Extract text from HTML
                html_content = result.stdout
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()
                
                text = soup.get_text()
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = ' '.join(chunk for chunk in chunks if chunk)
                
                logger.info(f"Chromium extraction successful: {len(text)} characters")
                return text
            else:
                logger.warning(f"Chromium process failed: {result.stderr}")
                return ""
                
        except Exception as e:
            logger.error(f"Chromium extraction failed: {str(e)}")
            return ""

class ExtractionContext:
    """Context class that manages extraction strategies"""
    
    def __init__(self):
        self.strategies = [
            SeleniumExtractionStrategy(),
            HttpExtractionStrategy(),
            ChromiumExtractionStrategy()
        ]
    
    def extract_with_strategy(self, url: str, strategy_name: str = None) -> str:
        """Extract content using a specific strategy or auto-select best one"""
        
        if strategy_name:
            # Use specified strategy
            strategy_map = {
                'selenium': SeleniumExtractionStrategy(),
                'http': HttpExtractionStrategy(), 
                'chromium': ChromiumExtractionStrategy()
            }
            
            if strategy_name in strategy_map:
                strategy = strategy_map[strategy_name]
                if strategy.is_suitable_for(url):
                    return strategy.extract_content(url)
                else:
                    logger.warning(f"Strategy '{strategy_name}' not suitable for URL")
                    return ""
            else:
                logger.error(f"Unknown strategy: {strategy_name}")
                return ""
        
        # Auto-select best strategy
        for strategy in self.strategies:
            if strategy.is_suitable_for(url):
                try:
                    content = strategy.extract_content(url)
                    if content.strip():  # If we got content, return it
                        return content
                except Exception as e:
                    logger.warning(f"Strategy {strategy.__class__.__name__} failed: {str(e)}")
                    continue
        
        logger.error("All extraction strategies failed")
        return ""

# Global extraction context instance
extraction_context = ExtractionContext()

def extract_content_with_strategy(url: str, strategy: str = None) -> str:
    """
    Extract content from URL using specified strategy or auto-selection.
    
    Consolidates functionality from multiple extraction scripts into a unified interface.
    
    Args:
        url: URL to extract content from
        strategy: Strategy to use ('selenium', 'http', 'chromium') or None for auto-select
        
    Returns:
        Extracted text content
    """
    return extraction_context.extract_with_strategy(url, strategy)

# If the script is run directly, test with a sample URL
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract content from URLs using various strategies')
    parser.add_argument('url', help='URL to extract content from')
    parser.add_argument('--strategy', choices=['selenium', 'http', 'chromium'], 
                       help='Extraction strategy to use (default: auto-select)')
    parser.add_argument('--links', action='store_true', 
                       help='Extract links instead of content')
    parser.add_argument('--limit', type=int, default=10,
                       help='Limit number of links to extract')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug output')
    
    args = parser.parse_args()
    
    if args.links:
        # Original link extraction functionality
        links, playlist, drive_links = process_url(args.url, limit=args.limit, debug=args.debug)
        print(f"Extracted links from {args.url}:")
        for link in links:
            print(f"  - {link}")
        
        print(f"\nYouTube playlist: {playlist if playlist else 'None'}")
        
        if drive_links:
            print(f"\nGoogle Drive links ({len(drive_links)}):")
            for drive_link in drive_links:
                print(f"  - {drive_link}")
    else:
        # New content extraction functionality
        content = extract_content_with_strategy(args.url, args.strategy)
        print(f"Extracted content from {args.url}:")
        print("=" * 50)
        print(content[:1000] + ("..." if len(content) > 1000 else ""))
        print("=" * 50)
        print(f"Total length: {len(content)} characters")
    
    # Clean up the driver
    cleanup_selenium_driver()