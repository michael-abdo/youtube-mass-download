#!/usr/bin/env python3
"""
DRY URL Processing Utilities
Consolidates URL parsing and processing patterns.
"""

import re
from typing import Optional, Tuple, List
from urllib.parse import urlparse, parse_qs
# DRY CONSOLIDATION - Step 2: Import centralized patterns
from .constants import URLPatterns

def extract_youtube_id(url: str) -> Optional[str]:
    """
    Extract YouTube video ID from various URL formats (DRY CONSOLIDATION - Step 2).
    
    Uses centralized regex pattern from URLPatterns for consistency.
    
    Args:
        url: YouTube URL
        
    Returns:
        YouTube video ID or None if not found
    """
    # Use centralized pattern which handles all YouTube URL formats
    match = URLPatterns.YOUTUBE_VIDEO_ID.search(url)
    if match:
        return match.group(1)
    
    # Handle edge cases not covered by main pattern
    # Extract from query parameters for complex URLs
    parsed = urlparse(url)
    if parsed.hostname and 'youtube' in parsed.hostname:
        query_params = parse_qs(parsed.query)
        if 'v' in query_params:
            return query_params['v'][0]
    
    return None

def extract_drive_id(url: str) -> Optional[str]:
    """
    Extract Google Drive file ID from various URL formats (DRY CONSOLIDATION - Step 2).
    
    Uses centralized regex pattern from URLPatterns for consistency.
    
    Args:
        url: Google Drive URL
        
    Returns:
        Drive file ID or None if not found
    """
    # Use centralized pattern for file IDs
    match = URLPatterns.DRIVE_FILE_ID.search(url)
    if match:
        return match.group(1)
    
    # Also check for folder IDs
    folder_match = URLPatterns.DRIVE_FOLDER_ID.search(url)
    if folder_match:
        return folder_match.group(1)
    
    # Handle edge cases with query parameters
    parsed = urlparse(url)
    if parsed.hostname and 'drive' in parsed.hostname:
        query_params = parse_qs(parsed.query)
        if 'id' in query_params:
            return query_params['id'][0]
    
    return None

def validate_youtube_url(url: str) -> Tuple[bool, Optional[str]]:
    """
    Validate YouTube URL and extract video ID.
    
    Args:
        url: YouTube URL to validate
        
    Returns:
        Tuple of (is_valid, video_id)
    """
    video_id = extract_youtube_id(url)
    return (video_id is not None, video_id)

def validate_drive_url(url: str) -> Tuple[bool, Optional[str]]:
    """
    Validate Google Drive URL and extract file ID.
    
    Args:
        url: Google Drive URL to validate
        
    Returns:
        Tuple of (is_valid, file_id)
    """
    file_id = extract_drive_id(url)
    return (file_id is not None, file_id)

def normalize_youtube_url(url: str) -> Optional[str]:
    """
    Normalize YouTube URL to standard format.
    
    Args:
        url: YouTube URL
        
    Returns:
        Normalized YouTube URL or None if invalid
    """
    video_id = extract_youtube_id(url)
    if video_id:
        return f"https://www.youtube.com/watch?v={video_id}"
    return None

def normalize_drive_url(url: str) -> Optional[str]:
    """
    Normalize Google Drive URL to standard format.
    
    Args:
        url: Google Drive URL
        
    Returns:
        Normalized Drive URL or None if invalid
    """
    file_id = extract_drive_id(url)
    if file_id:
        return f"https://drive.google.com/file/d/{file_id}/view"
    return None

def is_youtube_url(url: str) -> bool:
    """Check if URL is a YouTube URL."""
    return 'youtube.com' in url or 'youtu.be' in url

def is_drive_url(url: str) -> bool:
    """Check if URL is a Google Drive URL."""
    return 'drive.google.com' in url

def parse_url_links(text: str, separator: str = '|') -> List[str]:
    """
    Parse pipe-separated URLs from text.
    
    Args:
        text: Text containing URLs
        separator: URL separator character
        
    Returns:
        List of clean URLs
    """
    if not text or text == 'nan':
        return []
    
    links = text.split(separator)
    return [link.strip() for link in links if link.strip() and link.strip() != 'nan']

def create_drive_download_url(file_id: str) -> str:
    """
    Create Google Drive download URL from file ID.
    
    Args:
        file_id: Google Drive file ID
        
    Returns:
        Download URL
    """
    return f"https://drive.google.com/uc?id={file_id}&export=download"

def create_drive_usercontent_url(file_id: str, confirm_code: str, uuid: Optional[str] = None) -> str:
    """
    Create Google Drive usercontent download URL with confirmation.
    
    Args:
        file_id: Google Drive file ID
        confirm_code: Virus scan confirmation code
        uuid: Optional UUID parameter
        
    Returns:
        Usercontent download URL
    """
    url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm={confirm_code}"
    if uuid:
        url += f"&uuid={uuid}"
    return url