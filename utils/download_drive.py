import os
import re
import sys
import json
import time
import argparse
import requests
from pathlib import Path
from urllib.parse import urlparse, parse_qs
try:
    from logging_config import get_logger
    from validation import validate_google_drive_url, validate_file_path, ValidationError
    from retry_utils import retry_request, get_with_retry, retry_with_backoff
    from file_lock import file_lock, safe_file_operation
    from rate_limiter import rate_limit, wait_for_rate_limit
    from row_context import RowContext, DownloadResult
    from sanitization import sanitize_error_message, SafeDownloadError, validate_csv_field_safety
    from config import get_drive_downloads_dir, create_download_dir, Constants
    from download_utils import download_file_with_progress
    # DRY CONSOLIDATION - Step 1: Import centralized URL patterns
    from constants import URLPatterns
except ImportError:
    from .logging_config import get_logger
    from .validation import validate_google_drive_url, validate_file_path, ValidationError
    from .retry_utils import retry_request, get_with_retry, retry_with_backoff
    from .file_lock import file_lock, safe_file_operation
    from .rate_limiter import rate_limit, wait_for_rate_limit
    from .row_context import RowContext, DownloadResult
    from .sanitization import sanitize_error_message, SafeDownloadError, validate_csv_field_safety
    from .config import get_drive_downloads_dir, create_download_dir, Constants
    from .download_utils import download_file_with_progress
    # DRY CONSOLIDATION - Step 1: Import centralized URL patterns
    from .constants import URLPatterns

# Setup module logger
logger = get_logger(__name__)

# Directory to save downloaded files (from config)
DOWNLOADS_DIR = get_drive_downloads_dir()

def extract_file_id(url):
    """Extract Google Drive file ID from URL"""
    # Pattern for different Google Drive URL formats
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',  # /file/d/{fileId}
        r'id=([a-zA-Z0-9_-]+)',       # id={fileId}
        r'drive.google.com/open\?id=([a-zA-Z0-9_-]+)' # open?id={fileId}
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    # Try parsing the URL query parameters
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    
    # Check various possible parameter names
    for param in ['id', 'file_id', 'fileId', 'docid']:
        if param in query_params:
            return query_params[param][0]
    
    return None


def extract_folder_id(url):
    """Extract Google Drive folder ID from URL"""
    # Pattern for folder URLs
    folder_patterns = [
        r'/drive/folders/([a-zA-Z0-9_-]+)',  # /drive/folders/{folderId}
        r'folders/([a-zA-Z0-9_-]+)',         # folders/{folderId}
    ]
    
    for pattern in folder_patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None


def _validate_folder_response(response, logger=None):
    """
    Validate that HTTP response contains folder data, not HTML/JavaScript.
    
    Prevents CSV corruption by detecting Google Drive HTML pages that contain
    massive JavaScript configuration objects instead of folder content.
    
    Args:
        response: HTTP response object
        logger: Logger instance for error reporting
        
    Returns:
        tuple: (is_valid, error_message)
               is_valid=True if response contains folder data
               is_valid=False if response contains HTML/JavaScript
    """
    if not logger:
        logger = globals()['logger']  # Use module-level logger
    
    # Check response size - allow larger responses for legitimate folders
    content_length = len(response.text)
    if content_length > 1000000:  # 1MB limit - only reject extremely large responses
        error_msg = f"Response too large ({content_length} bytes) - likely corrupted or malicious"
        logger.warning(sanitize_error_message(error_msg))
        return False, error_msg
    
    # Check Content-Type header - HTML is expected for Google Drive folders
    content_type = response.headers.get('Content-Type', '').lower()
    if 'text/html' not in content_type:
        error_msg = f"Unexpected Content-Type: {content_type} - expected HTML for folder page"
        logger.warning(sanitize_error_message(error_msg))
        return False, error_msg
    
    html_content = response.text
    
    # Check for legitimate folder content vs error pages
    # Look for signs this is actually a folder page with files
    folder_content_indicators = [
        'drive.google.com/file/d/',
        'data-id=',
        '/file/d/',
    ]
    
    has_file_content = any(indicator in html_content for indicator in folder_content_indicators)
    
    # Check for error/login page indicators
    error_indicators = [
        'Sign in - Google Accounts',
        'access denied',
        'permission denied',
        'folder is empty',
        'no files',
        'Error 404',
        'Error 403',
    ]
    
    has_error_content = any(indicator.lower() in html_content.lower() for indicator in error_indicators)
    
    if has_error_content and not has_file_content:
        error_msg = "Response appears to be an error or login page - folder likely private or inaccessible"
        logger.warning(sanitize_error_message(error_msg))
        return False, error_msg
    
    # If we reach here, the response looks legitimate
    # It has file content indicators and no error indicators
    if not has_file_content:
        error_msg = "Response does not contain recognizable file patterns - folder may be empty"
        logger.info(sanitize_error_message(error_msg))
        # Don't return False - empty folders are valid
    
    return True, None


def is_folder_url(url):
    """Check if URL is a Google Drive folder"""
    return '/drive/folders/' in url or 'folders/' in url


def list_folder_files(folder_url, logger=None):
    """
    List files in a Google Drive folder by scraping the public folder page
    Returns list of file dictionaries with id and name
    """
    if not logger:
        logger = globals()['logger']  # Use module-level logger
    
    folder_id = extract_folder_id(folder_url)
    if not folder_id:
        logger.error(f"Could not extract folder ID from URL: {folder_url}")
        return []
    
    try:
        # Import here to avoid circular imports
        try:
            from http_pool import get as http_get
        except ImportError:
            from .http_pool import get as http_get
        
        # Try to access the folder page
        # DRY CONSOLIDATION - Step 1: Use centralized URL construction
        folder_page_url = URLPatterns.drive_folder_url(folder_id)
        logger.info(f"Attempting to list files in folder: {folder_id}")
        
        response = http_get(folder_page_url, timeout=30)
        
        if response.status_code != 200:
            if response.status_code == 404:
                logger.error(f"Folder not found or not publicly accessible (HTTP 404)")
            elif response.status_code == 403:
                logger.error(f"Permission denied - folder is private (HTTP 403)")
            else:
                logger.error(f"Failed to access folder page: HTTP {response.status_code}")
            return []
        
        # Validate response to prevent CSV corruption from HTML/JavaScript content
        is_valid, validation_error = _validate_folder_response(response, logger)
        if not is_valid:
            safe_error = sanitize_error_message(validation_error)
            logger.error(f"Invalid folder response: {safe_error}")
            return []
        
        html_content = response.text
        
        # Parse the HTML to find file references
        files = []
        
        # Look for file patterns in the HTML
        # Google Drive embeds file information in JSON-like structures
        import re
        
        # Extract file IDs using simpler, more reliable patterns
        file_id_patterns = [
            r'/file/d/([a-zA-Z0-9_-]+)',  # /file/d/{fileId}
            r'data-id="([a-zA-Z0-9_-]+)"',  # data-id="{fileId}"
        ]
        
        found_file_ids = set()
        for pattern in file_id_patterns:
            matches = re.findall(pattern, html_content)
            for file_id in matches:
                # Filter out invalid IDs (too short, system IDs, etc.)
                if len(file_id) > 10 and file_id not in ['_gd', '_folder']:
                    found_file_ids.add(file_id)
        
        # For each file ID, try to extract a name from surrounding context
        for file_id in found_file_ids:
            # Try to find the file name near the file ID
            name = f"file_{file_id[:8]}"  # Default name
            
            # Look for aria-label or title attributes near this file ID
            context_patterns = [
                rf'data-id="{re.escape(file_id)}"[^>]*aria-label="([^"]+)"',
                rf'/file/d/{re.escape(file_id)}[^"]*"[^>]*title="([^"]+)"',
                rf'/file/d/{re.escape(file_id)}[^"]*"[^>]*>([^<]+)<',
            ]
            
            for context_pattern in context_patterns:
                context_match = re.search(context_pattern, html_content)
                if context_match:
                    potential_name = context_match.group(1).strip()
                    if potential_name and len(potential_name) > 1:
                        name = potential_name
                        break
            
            files.append({
                'id': file_id,
                'name': name,
                # DRY CONSOLIDATION - Step 1: Use centralized URL construction
                'url': URLPatterns.drive_file_url(file_id, view=True)
            })
        
        # Remove duplicates based on file ID
        seen_ids = set()
        unique_files = []
        for file_info in files:
            if file_info['id'] not in seen_ids:
                seen_ids.add(file_info['id'])
                unique_files.append(file_info)
        
        logger.info(f"Found {len(unique_files)} files in folder")
        return unique_files
        
    except Exception as e:
        safe_error = sanitize_error_message(str(e))
        logger.error(f"Error listing folder contents: {safe_error}")
        return []


def download_folder_files(folder_url, row_context, logger=None):
    """
    Download all files from a Google Drive folder
    Returns DownloadResult with all downloaded files
    """
    if not logger:
        logger = globals()['logger']  # Use module-level logger
    
    logger.info(f"Starting Google Drive folder download for {row_context.name} (Row {row_context.row_id})")
    
    downloaded_files = []
    metadata_files = []
    errors = []
    
    try:
        # List files in the folder
        folder_files = list_folder_files(folder_url, logger)
        
        if not folder_files:
            logger.warning("No files found in folder or folder is not publicly accessible")
            return DownloadResult(
                success=False,
                files_downloaded=[],
                media_id=extract_folder_id(folder_url),
                error_message="No files found in folder or folder is not publicly accessible",
                metadata_file=None,
                row_context=row_context,
                download_type='drive'
            )
        
        logger.info(f"Found {len(folder_files)} files to download from folder")
        
        # Download each file
        for file_info in folder_files:
            try:
                logger.info(f"Downloading file: {file_info['name']} (ID: {file_info['id']})")
                
                # Download individual file (avoid recursion by calling internal function)
                result = _download_individual_file_with_context(file_info['url'], row_context, logger)
                
                if result.success:
                    downloaded_files.extend(result.files_downloaded)
                    if result.metadata_file:
                        metadata_files.append(result.metadata_file)
                else:
                    errors.append(f"Failed to download {file_info['name']}: {result.error_message}")
                    
            except Exception as e:
                error_msg = f"Error downloading {file_info['name']}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        # Create combined metadata for the folder download
        folder_id = extract_folder_id(folder_url)
        if folder_id and downloaded_files:
            suffix = row_context.to_filename_suffix()
            combined_metadata_filename = f"folder_{folder_id}{suffix}_metadata.json"
        else:
            combined_metadata_filename = None
        
        success = len(downloaded_files) > 0
        error_msg = "; ".join(errors) if errors else None
        
        result = DownloadResult(
            success=success,
            files_downloaded=downloaded_files,
            media_id=folder_id,
            error_message=error_msg,
            metadata_file=combined_metadata_filename,
            row_context=row_context,
            download_type='drive'
        )
        
        # Save combined metadata
        if combined_metadata_filename and downloaded_files:
            folder_metadata = {
                'folder_id': folder_id,
                'folder_url': folder_url,
                'files_in_folder': len(folder_files),
                'files_downloaded': len(downloaded_files),
                'individual_metadata_files': metadata_files,
                'download_errors': errors
            }
            
            # Save the metadata
            metadata_path = os.path.join(DOWNLOADS_DIR, combined_metadata_filename)
            folder_metadata.update(row_context.to_metadata_dict())
            
            try:
                with open(metadata_path, 'w') as f:
                    json.dump(folder_metadata, f, indent=2)
                logger.info(f"Saved folder metadata to {combined_metadata_filename}")
            except Exception as e:
                logger.warning(f"Could not save folder metadata: {e}")
        
        logger.info(f"Folder download completed for {row_context.name}: {len(downloaded_files)} files downloaded")
        return result
        
    except Exception as e:
        logger.error(f"Folder download failed for {row_context.name} (Row {row_context.row_id}): {str(e)}")
        return DownloadResult(
            success=False,
            files_downloaded=[],
            media_id=extract_folder_id(folder_url),
            error_message=str(e),
            metadata_file=None,
            row_context=row_context,
            download_type='drive'
        )

def get_filename_from_url(url):
    """Try to extract the filename from Drive URL or Content-Disposition header"""
    # First check if the filename is in the URL
    match = re.search(r'/([^/]+)$', url)
    if match:
        filename = match.group(1)
        # Clean up any URL parameters
        if '?' in filename:
            filename = filename.split('?')[0]
        if filename and filename != 'view' and not filename.startswith('d/'):
            return filename
    
    return None

def get_filename_from_response(response):
    """Extract filename from Content-Disposition header or content-type"""
    # Try Content-Disposition header first
    if 'Content-Disposition' in response.headers:
        content_disposition = response.headers['Content-Disposition']
        match = re.search(r'filename="?([^";]+)"?', content_disposition)
        if match:
            return match.group(1)
    
    # If no filename found, use the file ID with appropriate extension
    content_type = response.headers.get('Content-Type', '')
    
    # Map common MIME types to file extensions
    mime_to_ext = {
        'application/pdf': '.pdf',
        'application/msword': '.doc',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
        'application/vnd.ms-excel': '.xls',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
        'application/vnd.ms-powerpoint': '.ppt',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation': '.pptx',
        'text/plain': '.txt',
        'text/csv': '.csv',
        'image/jpeg': '.jpg',
        'image/png': '.png',
        'image/gif': '.gif',
        'application/zip': '.zip',
        'application/x-rar-compressed': '.rar',
        'application/x-tar': '.tar',
        'application/x-gzip': '.gz',
        'audio/mpeg': '.mp3',
        'video/mp4': '.mp4',
        'application/json': '.json'
    }
    
    extension = mime_to_ext.get(content_type, '')
    if not extension and '/' in content_type:
        # Use the subtype as extension for unrecognized types
        extension = '.' + content_type.split('/')[1].split(';')[0]
    
    return extension

# Duplicate functions removed - using canonical versions defined earlier in file

def get_folder_contents(folder_id, logger=None):
    """Not implemented - Google Drive API requires authentication"""
    if logger:
        logger.warning("Folder downloading is not supported without API keys.")
        logger.info(f"Please access the folder directly at: https://drive.google.com/drive/folders/{folder_id}")
    else:
        logger.warning("Folder downloading is not supported without API keys.")
        logger.info(f"Please access the folder directly at: https://drive.google.com/drive/folders/{folder_id}")
    return []

@retry_with_backoff(
    max_attempts=3,
    base_delay=5.0,
    exceptions=(requests.RequestException, IOError)
)
@rate_limit('google_drive')
def download_drive_file(file_id, output_filename=None, logger=None):
    """Download a file from Google Drive using file ID"""
    if not logger:
        logger = globals()['logger']  # Use module-level logger
    
    # Validate file ID
    if not file_id or not re.match(r'^[a-zA-Z0-9_-]+$', file_id):
        logger.error(f"Invalid Google Drive file ID: {file_id}")
        return None
    
    # Direct download URL format
    # DRY CONSOLIDATION - Step 1: Use centralized URL construction
    download_url = URLPatterns.drive_download_url(file_id)
    
    # For large files, Google Drive shows a confirmation page
    # We need to handle this case properly
    
    session = requests.Session()
    
    logger.info(f"Downloading file with ID: {file_id}")
    
    # First request to get cookies and confirmation page for large files
    response = session.get(download_url, stream=True, timeout=30)
    
    # Check if we got the download confirmation page
    content_type = response.headers.get('Content-Type', '')
    if 'text/html' in content_type and 'virus scan warning' in response.text.lower():
        # This is a virus scan warning page - we need to parse it
        confirm_match = re.search(r'name="confirm" value="([^"]*)"', response.text)
        uuid_match = re.search(r'name="uuid" value="([^"]*)"', response.text)
        
        if confirm_match:
            confirm_code = confirm_match.group(1)
            logger.info("Large file detected with virus scan warning, bypassing confirmation...")
            
            # Build the proper download URL with all parameters
            download_params = {
                'id': file_id,
                'export': 'download',
                'confirm': confirm_code
            }
            
            if uuid_match:
                download_params['uuid'] = uuid_match.group(1)
            
            # Use drive.usercontent.google.com for direct downloads
            direct_download_url = "https://drive.usercontent.google.com/download"
            
            # Make the download request with all parameters
            response = session.get(direct_download_url, params=download_params, stream=True, timeout=30)
    
    # Also check if the initial URL was already a direct download link
    elif 'drive.usercontent.google.com' in download_url and response.headers.get('Content-Type', '') == 'text/html':
        # We might have been given a direct download URL but still got HTML
        # Just retry the same URL - it should work on second attempt
        logger.info("Retrying direct download URL...")
        response = session.get(download_url, stream=True, timeout=30)
    
    # Check response
    if response.status_code != 200:
        logger.error(f"Error downloading file: HTTP status {response.status_code}")
        return None
    
    # Get filename if not provided
    if not output_filename:
        # Try to get from Content-Disposition header
        filename_extension = get_filename_from_response(response)
        if filename_extension:
            if filename_extension.startswith('.'):
                # It's just an extension
                output_filename = f"{file_id}{filename_extension}"
            else:
                # It's a full filename
                output_filename = filename_extension
        else:
            # Default filename based on file ID
            output_filename = f"{file_id}.bin"
    
    output_path = os.path.join(DOWNLOADS_DIR, output_filename)
    lock_file = Path(DOWNLOADS_DIR) / f".{file_id}.lock"
    
    # First check with shared lock if file exists
    with file_lock(lock_file, exclusive=False, timeout=30.0, logger=logger):
        if os.path.exists(output_path):
            logger.info(f"File already exists: {output_path}")
            return output_path
    
    # Now acquire exclusive lock for download
    with file_lock(lock_file, exclusive=True, timeout=300.0, logger=logger):  # 5 min timeout
        # Double-check after acquiring exclusive lock
        if os.path.exists(output_path):
            logger.info(f"File already exists: {output_path}")
            return output_path
        
        # Save file
        try:
            total_size = int(response.headers.get('content-length', 0))
            
            if total_size == 0:
                logger.warning("Could not determine file size")
            else:
                logger.info(f"File size: {total_size / Constants.BYTES_PER_MB:.2f} MB")
            
            # Download to a temporary file first
            temp_path = f"{output_path}.tmp"
            
            # Use centralized download function (DRY consolidation)
            success = download_file_with_progress(response, temp_path, total_size, logger)
            if not success:
                raise Exception("Download failed")
            
            # Move to final location
            os.replace(temp_path, output_path)
            
        except Exception as e:
            # Clean up temp file on error
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            logger.error(f"Error saving file: {str(e)}")
            return None

def save_metadata(file_id, url, metadata, logger=None):
    """Save file metadata to a JSON file"""
    if not logger:
        logger = globals()['logger']  # Use module-level logger
    
    metadata_file = os.path.join(DOWNLOADS_DIR, f"{file_id}_metadata.json")
    
    # Add URL to metadata
    metadata['url'] = url
    metadata['file_id'] = file_id
    metadata['downloaded_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
    
    # Use file locking for metadata writes
    lock_file = Path(DOWNLOADS_DIR) / f".{file_id}_metadata.lock"
    
    with file_lock(lock_file, exclusive=True, timeout=30.0, logger=logger):
        # Write to temp file first
        temp_file = f"{metadata_file}.tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        
        # Atomic rename
        os.replace(temp_file, metadata_file)
    
    logger.info(f"Saved metadata to {metadata_file}")
    return metadata_file

def process_direct_download_url(url, output_filename=None, logger=None):
    """Process a direct drive.usercontent.google.com download URL"""
    if not logger:
        logger = globals()['logger']  # Use module-level logger
    
    # Extract parameters from URL
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    
    file_id = query_params.get('id', [None])[0]
    if not file_id:
        logger.error("Could not extract file ID from direct download URL")
        return None
    
    # For direct download URLs, we just download directly
    create_download_dir(DOWNLOADS_DIR, logger)
    
    # Create a session to handle the download
    session = requests.Session()
    
    # Make the download request
    response = session.get(url, stream=True, timeout=30)
    
    # Handle virus scan warning if present
    content_type = response.headers.get('Content-Type', '')
    if 'text/html' in content_type:
        # Extract filename from HTML if available
        html_content = response.text
        filename_match = re.search(r'>([^<]+\.[^<]+)</a>', html_content)
        suggested_filename = filename_match.group(1) if filename_match else None
        
        # Retry the same URL - it should work on second attempt
        logger.info("Retrying direct download URL after virus scan page...")
        response = session.get(url, stream=True, timeout=30)
    else:
        suggested_filename = None
    
    if response.status_code != 200:
        logger.error(f"Error downloading file: HTTP status {response.status_code}")
        return None
    
    # Determine filename
    if not output_filename:
        # Try Content-Disposition header
        cd = response.headers.get('Content-Disposition', '')
        filename_match = re.search(r'filename="?([^";]+)"?', cd)
        if filename_match:
            output_filename = filename_match.group(1)
        elif suggested_filename:
            output_filename = suggested_filename
        else:
            output_filename = f"{file_id}.bin"
    
    output_path = os.path.join(DOWNLOADS_DIR, output_filename)
    lock_file = Path(DOWNLOADS_DIR) / f".{file_id}.lock"
    
    # Check if file exists
    with file_lock(lock_file, exclusive=False, timeout=30.0, logger=logger):
        if os.path.exists(output_path):
            logger.info(f"File already exists: {output_path}")
            return output_path
    
    # Download with exclusive lock
    with file_lock(lock_file, exclusive=True, timeout=300.0, logger=logger):
        if os.path.exists(output_path):
            logger.info(f"File already exists: {output_path}")
            return output_path
        
        # Download to temp file
        temp_path = f"{output_path}.tmp"
        total_size = int(response.headers.get('Content-Length', 0))
        
        if total_size > 0:
            logger.info(f"File size: {total_size / Constants.BYTES_PER_MB:.2f} MB")
        
        try:
            # Use centralized download function (DRY consolidation)
            success = download_file_with_progress(response, temp_path, total_size, logger)
            if not success:
                raise Exception("Download failed")
            
            # Move to final location
            os.replace(temp_path, output_path)
            logger.success(f"Downloaded file to {output_path}")
            return output_path
            
        except Exception as e:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            logger.error(f"Error saving file: {str(e)}")
            return None


def _download_individual_file_with_context(url: str, row_context: RowContext, logger) -> DownloadResult:
    """Download a single Google Drive file with context tracking (internal function)"""
    try:
        # Extract file ID for individual files
        file_id = extract_file_id(url)
        if not file_id:
            raise ValueError(f"Could not extract file ID from URL: {url}")
        
        # Download using existing functionality  
        downloaded_path, metadata_path = process_drive_url(
            url, output_filename=None, save_metadata_flag=True, logger=logger
        )
        
        # Collect downloaded files
        downloaded_files = []
        if downloaded_path and os.path.exists(downloaded_path):
            downloaded_files.append(os.path.basename(downloaded_path))
        
        # Create context-aware metadata file
        suffix = row_context.to_filename_suffix()
        context_metadata_filename = f"{file_id}{suffix}_metadata.json"
        
        success = len(downloaded_files) > 0
        error_msg = None if success else f"Failed to download file {file_id}"
        
        result = DownloadResult(
            success=success,
            files_downloaded=downloaded_files,
            media_id=file_id,
            error_message=error_msg,
            metadata_file=context_metadata_filename,
            row_context=row_context,
            download_type='drive'
        )
        
        # Save metadata with row context
        if success:
            result.save_metadata(DOWNLOADS_DIR)
            
        return result
        
    except Exception as e:
        return DownloadResult(
            success=False,
            files_downloaded=[],
            media_id=extract_file_id(url),
            error_message=str(e),
            metadata_file=None,
            row_context=row_context,
            download_type='drive'
        )


def download_drive_with_context(url: str, row_context: RowContext) -> DownloadResult:
    """Download Google Drive file or folder with full row context tracking"""
    logger = globals()['logger']  # Use module-level logger
    
    # Check for null/empty URL first
    if not url or url == 'None' or url == 'nan' or url.strip() == '':
        logger.info(f"No Google Drive URL provided for {row_context.name} (Row {row_context.row_id}) - skipping")
        return DownloadResult(
            success=True,  # Not an error, just no URL
            files_downloaded=[],
            media_id=None,
            error_message='No Google Drive URL provided',
            metadata_file=None,
            row_context=row_context,
            download_type='drive'
        )
    
    logger.info(f"Starting Google Drive download for {row_context.name} (Row {row_context.row_id}, Type: {row_context.type})")
    
    try:
        # Check if this is a folder URL
        if is_folder_url(url):
            logger.info(f"Detected folder URL, downloading all files in folder")
            return download_folder_files(url, row_context, logger)
        else:
            # Handle individual file download
            result = _download_individual_file_with_context(url, row_context, logger)
            logger.info(f"Google Drive download completed for {row_context.name}: {len(result.files_downloaded)} files")
            return result
        
    except Exception as e:
        logger.error(f"Google Drive download failed for {row_context.name} (Row {row_context.row_id}): {str(e)}")
        return DownloadResult(
            success=False,
            files_downloaded=[],
            media_id=None,
            error_message=str(e),
            metadata_file=None,
            row_context=row_context,
            download_type='drive'
        )


def process_drive_url(url, output_filename=None, save_metadata_flag=False, logger=None):
    """Process a Google Drive URL (file or folder)"""
    if not logger:
        logger = globals()['logger']  # Use module-level logger
    
    # Check if this is a direct download URL
    if 'drive.usercontent.google.com/download' in url:
        logger.info("Processing direct download URL...")
        downloaded_path = process_direct_download_url(url, output_filename, logger)
        
        # Handle metadata if requested
        metadata_path = None
        if downloaded_path and save_metadata_flag:
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            file_id = query_params.get('id', [None])[0]
            
            if file_id:
                metadata = {
                    'file_id': file_id,
                    'url': url,
                    'filename': os.path.basename(downloaded_path),
                    'file_size_bytes': os.path.getsize(downloaded_path)
                }
                metadata_path = save_metadata(file_id, url, metadata, logger)
        
        return downloaded_path, metadata_path
    
    create_download_dir(DOWNLOADS_DIR, logger)
    
    # Check if it's a folder BEFORE validating as a file URL
    if is_folder_url(url):
        folder_id = extract_folder_id(url)
        if not folder_id:
            logger.error(f"Could not extract folder ID from URL: {url}")
            return None, None
        
        logger.info(f"Folder ID: {folder_id}")
        logger.warning("Note: Folder downloading requires Google Drive API authentication")
        logger.info("Try accessing the folder directly in your browser")
        get_folder_contents(folder_id, logger)
        return None, None
    
    # Validate URL for regular Drive file URLs
    try:
        url, file_id = validate_google_drive_url(url)
    except ValidationError as e:
        logger.error(f"Invalid Google Drive URL: {e}")
        return None, None
    
    # Process single file
    file_id = extract_file_id(url)
    if not file_id:
        logger.error(f"Could not extract file ID from URL: {url}")
        return None, None
    
    logger.info(f"File ID: {file_id}")
    
    # Check if file already exists
    if output_filename:
        file_path = os.path.join(DOWNLOADS_DIR, output_filename)
    else:
        # Try to derive filename, otherwise we'll use the file ID
        filename_from_url = get_filename_from_url(url)
        if filename_from_url:
            file_path = os.path.join(DOWNLOADS_DIR, filename_from_url)
        else:
            # We'll determine this after the request
            file_path = None
    
    if file_path and os.path.exists(file_path):
        logger.info(f"File already exists at {file_path}")
        
        # Check for existing metadata
        metadata_path = os.path.join(DOWNLOADS_DIR, f"{file_id}_metadata.json")
        if os.path.exists(metadata_path):
            logger.info(f"Metadata already exists at {metadata_path}")
            return file_path, metadata_path
        
        # If metadata requested but doesn't exist, we'll create it
        if save_metadata_flag:
            metadata = {
                'file_id': file_id,
                'url': url,
                'filename': os.path.basename(file_path),
                'downloaded_at': 'previously',
                'file_size_bytes': os.path.getsize(file_path)
            }
            metadata_path = save_metadata(file_id, url, metadata, logger)
            return file_path, metadata_path
        
        return file_path, None
    
    # Download file
    downloaded_path = download_drive_file(file_id, output_filename, logger)
    
    # Save metadata if requested
    metadata_path = None
    if downloaded_path and save_metadata_flag:
        metadata = {
            'file_id': file_id,
            'url': url,
            'filename': os.path.basename(downloaded_path),
            'file_size_bytes': os.path.getsize(downloaded_path)
        }
        metadata_path = save_metadata(file_id, url, metadata, logger)
    
    return downloaded_path, metadata_path

def main():
    # Setup logging
    logger = globals()['logger']  # Use module-level logger
    
    parser = argparse.ArgumentParser(description='Download Google Drive files')
    parser.add_argument('url', help='Google Drive file or folder URL')
    parser.add_argument('--filename', help='Output filename (optional)')
    parser.add_argument('--metadata', action='store_true',
                      help='Save file metadata to a JSON file')
    
    args = parser.parse_args()
    
    create_download_dir(logger)
    
    # Process the URL
    file_path, metadata_path = process_drive_url(
        args.url, 
        args.filename,
        args.metadata,
        logger
    )
    
    if file_path:
        logger.success(f"Download complete: {file_path}")
        if metadata_path:
            logger.info(f"Metadata saved: {metadata_path}")
    else:
        logger.error("Download failed.")

if __name__ == "__main__":
    main()