#!/usr/bin/env python3
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "utils"))
from download_utils import download_file_with_progress
from config import Constants
"""
Download large Google Drive files that show virus scan warnings.
Handles direct drive.usercontent.google.com URLs properly.
"""
import os
import sys
import re
import requests
import time
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logging_config import get_logger
from utils.validation import validate_url, ValidationError
from utils.file_lock import file_lock

logger = get_logger(__name__)

DOWNLOADS_DIR = "drive_downloads"

def extract_file_info_from_url(url):
    """Extract file ID and other parameters from the URL"""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    
    file_id = params.get('id', [None])[0]
    confirm = params.get('confirm', [None])[0]
    uuid = params.get('uuid', [None])[0]
    
    return {
        'file_id': file_id,
        'confirm': confirm,
        'uuid': uuid,
        'base_url': f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    }

def download_large_drive_file(url, output_dir=DOWNLOADS_DIR):
    """Download a large Google Drive file that requires virus scan confirmation"""
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract file information
    file_info = extract_file_info_from_url(url)
    file_id = file_info['file_id']
    
    if not file_id:
        logger.error("Could not extract file ID from URL")
        return None
    
    logger.info(f"File ID: {file_id}")
    
    # Create a session to handle cookies
    session = requests.Session()
    
    # First, make a request to get the actual download URL with confirmation
    # This is needed because Google Drive requires confirmation for large files
    response = session.get(url, stream=True, timeout=30)
    
    if response.status_code != 200:
        logger.error(f"Failed to access file: HTTP {response.status_code}")
        return None
    
    # Check if we got an HTML page (virus scan warning)
    content_type = response.headers.get('Content-Type', '')
    if 'text/html' in content_type:
        # Parse the HTML to get file information
        html_content = response.text
        
        # Extract filename from HTML
        filename_match = re.search(r'>([^<]+\.mp4)</a>', html_content)
        if not filename_match:
            filename_match = re.search(r'>([^<]+\.[^<]+)</a>', html_content)
        
        filename = filename_match.group(1) if filename_match else f"{file_id}.bin"
        
        # Extract file size
        size_match = re.search(r'\(([0-9.]+[GMK])\)', html_content)
        file_size = size_match.group(1) if size_match else 'unknown'
        
        logger.info(f"File: {filename} ({file_size})")
        logger.info("File requires virus scan confirmation, proceeding with download...")
        
        # The URL already has all necessary parameters, just need to make the request again
        # but this time we'll handle the actual file download
        response = session.get(url, stream=True, timeout=30)
    else:
        # We got the actual file on first try
        # Try to get filename from Content-Disposition header
        cd = response.headers.get('Content-Disposition', '')
        filename_match = re.search(r'filename="?([^";]+)"?', cd)
        filename = filename_match.group(1) if filename_match else f"{file_id}.bin"
    
    # Set up file paths
    output_path = os.path.join(output_dir, filename)
    temp_path = f"{output_path}.tmp"
    lock_file = Path(output_dir) / f".{file_id}.lock"
    
    # Check if file already exists
    if os.path.exists(output_path):
        logger.info(f"File already exists: {output_path}")
        return output_path
    
    # Acquire lock for download
    with file_lock(lock_file, exclusive=True, timeout=300.0, logger=logger):
        # Double-check after acquiring lock
        if os.path.exists(output_path):
            logger.info(f"File already exists: {output_path}")
            return output_path
        
        # Get file size if available
        total_size = int(response.headers.get('Content-Length', 0))
        if total_size > 0:
            logger.info(f"File size: {total_size / Constants.BYTES_PER_MB:.2f} MB")
        
        # Download file with progress
        try:
            # Use centralized download function (DRY consolidation)
            success = download_file_with_progress(response, temp_path, total_size, logger)
            if not success:
                logger.error("Download failed")
                return None
            
            # Move to final location
            os.replace(temp_path, output_path)
            
            logger.info(f"✅ Download completed")
            logger.info(f"Saved to: {output_path}")
            
            return output_path
            
        except Exception as e:
            # Clean up on error
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            logger.error(f"Download failed: {str(e)}")
            return None

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Download large Google Drive files with virus scan warnings')
    parser.add_argument('url', help='Google Drive download URL (drive.usercontent.google.com)')
    parser.add_argument('--output-dir', default=DOWNLOADS_DIR, help='Output directory')
    
    args = parser.parse_args()
    
    # Validate URL allows drive.usercontent.google.com
    try:
        url = validate_url(args.url, allowed_domains=['drive.google.com', 'drive.usercontent.google.com'])
    except ValidationError as e:
        logger.error(f"Invalid URL: {e}")
        sys.exit(1)
    
    # Download the file
    result = download_large_drive_file(url, args.output_dir)
    
    if result:
        logger.info(f"✅ Success! File saved to: {result}")
    else:
        logger.error("❌ Download failed")
        sys.exit(1)

if __name__ == "__main__":
    main()