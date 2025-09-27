#!/usr/bin/env python3
"""
Unified Metadata Extraction and Management (DRY ITERATION 3 - Step 5)

Consolidates metadata extraction patterns found throughout the codebase:
- File metadata (size, type, timestamps)
- YouTube metadata extraction
- Google Drive metadata extraction
- Consistent timestamp formatting
- Standardized metadata schemas

BUSINESS IMPACT: Prevents inconsistent metadata storage and enables reliable tracking
"""

import os
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union
import json

# Standardized imports
try:
    from .logging_config import get_logger
    from .patterns import extract_youtube_id, extract_drive_id
    from .validation import UnifiedValidator
except ImportError:
    from logging_config import get_logger
    from patterns import extract_youtube_id, extract_drive_id
    from validation import UnifiedValidator

logger = get_logger(__name__)


# ============================================================================
# STANDARDIZED TIMESTAMP FORMATTING
# ============================================================================

def format_timestamp(dt: Optional[datetime] = None, format_type: str = 'iso') -> str:
    """
    Standardized timestamp formatting across the system.
    
    CONSOLIDATES PATTERNS:
    - time.strftime('%Y-%m-%d %H:%M:%S')
    - datetime.now().isoformat()
    - Various other timestamp formats
    
    Args:
        dt: Datetime object (uses current time if None)
        format_type: 'iso', 'readable', 'filename', 'compact'
        
    Returns:
        Formatted timestamp string
        
    Example:
        ts = format_timestamp()  # '2024-01-15T10:30:45.123456'
        ts = format_timestamp(format_type='readable')  # '2024-01-15 10:30:45'
    """
    if dt is None:
        dt = datetime.now()
    
    if format_type == 'iso':
        return dt.isoformat()
    elif format_type == 'readable':
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    elif format_type == 'filename':
        return dt.strftime('%Y%m%d_%H%M%S')
    elif format_type == 'compact':
        return dt.strftime('%Y%m%d%H%M%S')
    else:
        return dt.isoformat()


# ============================================================================
# FILE METADATA EXTRACTION
# ============================================================================

def get_file_metadata(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Extract comprehensive file metadata.
    
    CONSOLIDATES PATTERNS:
    - os.path.getsize(downloaded_path)
    - file_path.stat().st_size
    - Multiple stat() calls for different attributes
    
    Args:
        file_path: Path to file
        
    Returns:
        Dictionary with file metadata
        
    Example:
        meta = get_file_metadata('video.mp4')
        # Returns: {
        #   'filename': 'video.mp4',
        #   'file_size_bytes': 1048576,
        #   'file_size_mb': 1.0,
        #   'extension': '.mp4',
        #   'mime_type': 'video/mp4',
        #   'created_at': '2024-01-15T10:30:45',
        #   'modified_at': '2024-01-15T10:30:45',
        #   'exists': True
        # }
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        return {
            'filename': file_path.name,
            'exists': False,
            'error': 'File not found'
        }
    
    try:
        stat = file_path.stat()
        
        # Get file size
        size_bytes = stat.st_size
        size_mb = round(size_bytes / (1024 * 1024), 2)
        size_gb = round(size_bytes / (1024 * 1024 * 1024), 2) if size_bytes > 1e9 else None
        
        # Get extension and MIME type
        valid, extension = UnifiedValidator.validate_file_extension(file_path.name)
        mime_type, _ = mimetypes.guess_type(str(file_path))
        
        metadata = {
            'filename': file_path.name,
            'file_path': str(file_path.absolute()),
            'file_size_bytes': size_bytes,
            'file_size_mb': size_mb,
            'file_size_gb': size_gb,
            'extension': extension,
            'mime_type': mime_type or 'application/octet-stream',
            'created_at': format_timestamp(datetime.fromtimestamp(stat.st_ctime)),
            'modified_at': format_timestamp(datetime.fromtimestamp(stat.st_mtime)),
            'exists': True,
            'is_media': extension in UnifiedValidator.FILE_EXTENSIONS.get('video', set()) | 
                       UnifiedValidator.FILE_EXTENSIONS.get('audio', set())
        }
        
        return metadata
        
    except Exception as e:
        logger.error(f"Error extracting metadata from {file_path}: {e}")
        return {
            'filename': file_path.name,
            'exists': True,
            'error': str(e)
        }


# ============================================================================
# DOWNLOAD METADATA SCHEMAS
# ============================================================================

def create_download_metadata(url: str, file_path: Union[str, Path], 
                           file_id: Optional[str] = None,
                           source_type: str = 'generic') -> Dict[str, Any]:
    """
    Create standardized download metadata.
    
    CONSOLIDATES PATTERNS from download_drive.py lines 810-815, 873-879, 891-896
    
    Args:
        url: Source URL
        file_path: Downloaded file path
        file_id: Optional file ID (for Drive files)
        source_type: 'youtube', 'drive', 'generic'
        
    Returns:
        Standardized metadata dictionary
    """
    file_metadata = get_file_metadata(file_path)
    
    metadata = {
        'source_type': source_type,
        'source_url': url,
        'downloaded_at': format_timestamp(),
        'download_timestamp_readable': format_timestamp(format_type='readable'),
        **file_metadata  # Include all file metadata
    }
    
    # Add source-specific fields
    if file_id:
        metadata['file_id'] = file_id
    
    if source_type == 'youtube':
        video_id = extract_youtube_id(url)
        if video_id:
            metadata['youtube_video_id'] = video_id
            
    elif source_type == 'drive':
        if not file_id:
            file_id = extract_drive_id(url)
        if file_id:
            metadata['drive_file_id'] = file_id
    
    return metadata


def create_upload_metadata(file_path: Union[str, Path], s3_key: str,
                         bucket_name: Optional[str] = None,
                         person_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Create standardized upload metadata.
    
    CONSOLIDATES S3 upload metadata patterns from s3_manager.py
    
    Args:
        file_path: Local file path
        s3_key: S3 object key
        bucket_name: S3 bucket name
        person_name: Associated person name
        
    Returns:
        Standardized metadata dictionary
    """
    file_metadata = get_file_metadata(file_path)
    
    metadata = {
        'uploaded_at': format_timestamp(),
        'upload_timestamp_readable': format_timestamp(format_type='readable'),
        'source': 'typing-clients-ingestion',
        's3_key': s3_key,
        'original_filename': Path(file_path).name,
        **file_metadata  # Include all file metadata
    }
    
    if bucket_name:
        metadata['s3_bucket'] = bucket_name
        metadata['s3_url'] = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
    
    if person_name:
        metadata['person_name'] = person_name
    
    return metadata


# ============================================================================
# YOUTUBE METADATA EXTRACTION
# ============================================================================

def extract_youtube_metadata(url: str, additional_info: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Extract YouTube-specific metadata.
    
    NEW FUNCTION: Consolidates YouTube metadata extraction patterns
    
    Args:
        url: YouTube URL
        additional_info: Optional additional metadata (e.g., from yt-dlp)
        
    Returns:
        YouTube metadata dictionary
    """
    video_id = extract_youtube_id(url)
    
    metadata = {
        'platform': 'youtube',
        'url': url,
        'video_id': video_id,
        'extracted_at': format_timestamp()
    }
    
    # Construct standard YouTube URLs
    if video_id:
        metadata['watch_url'] = f"https://www.youtube.com/watch?v={video_id}"
        metadata['embed_url'] = f"https://www.youtube.com/embed/{video_id}"
        metadata['thumbnail_url'] = f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg"
    
    # Merge additional info if provided (e.g., from yt-dlp)
    if additional_info:
        # Extract commonly needed fields
        if 'title' in additional_info:
            metadata['title'] = additional_info['title']
        if 'duration' in additional_info:
            metadata['duration_seconds'] = additional_info['duration']
            metadata['duration_formatted'] = format_duration(additional_info['duration'])
        if 'uploader' in additional_info:
            metadata['channel_name'] = additional_info['uploader']
        if 'upload_date' in additional_info:
            metadata['upload_date'] = additional_info['upload_date']
    
    return metadata


# ============================================================================
# GOOGLE DRIVE METADATA EXTRACTION
# ============================================================================

def extract_drive_metadata(url: str, file_path: Optional[Union[str, Path]] = None,
                         file_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Extract Google Drive-specific metadata.
    
    NEW FUNCTION: Consolidates Drive metadata extraction patterns
    
    Args:
        url: Google Drive URL
        file_path: Optional downloaded file path
        file_id: Optional Drive file ID
        
    Returns:
        Drive metadata dictionary
    """
    if not file_id:
        file_id = extract_drive_id(url)
    
    metadata = {
        'platform': 'google_drive',
        'url': url,
        'file_id': file_id,
        'extracted_at': format_timestamp()
    }
    
    # Construct standard Drive URLs
    if file_id:
        metadata['view_url'] = f"https://drive.google.com/file/d/{file_id}/view"
        metadata['download_url'] = f"https://drive.google.com/uc?export=download&id={file_id}"
        metadata['embed_url'] = f"https://drive.google.com/file/d/{file_id}/preview"
    
    # Add file metadata if path provided
    if file_path:
        file_meta = get_file_metadata(file_path)
        metadata.update(file_meta)
    
    return metadata


# ============================================================================
# METADATA PERSISTENCE
# ============================================================================

def save_metadata_json(metadata: Dict[str, Any], output_path: Union[str, Path]) -> bool:
    """
    Save metadata to JSON file with consistent formatting.
    
    Args:
        metadata: Metadata dictionary
        output_path: Path for JSON file
        
    Returns:
        True if successful
    """
    try:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
        
        return True
    except Exception as e:
        logger.error(f"Failed to save metadata to {output_path}: {e}")
        return False


def load_metadata_json(json_path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """
    Load metadata from JSON file.
    
    Args:
        json_path: Path to JSON file
        
    Returns:
        Metadata dictionary or None if error
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load metadata from {json_path}: {e}")
        return None


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def format_duration(seconds: Union[int, float]) -> str:
    """
    Format duration in seconds to human-readable format.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string like "1:23:45" or "45:30"
    """
    if not seconds or seconds < 0:
        return "0:00"
    
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    else:
        return f"{minutes}:{secs:02d}"


def merge_metadata(*metadata_dicts: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge multiple metadata dictionaries, with later dicts overriding earlier ones.
    
    Args:
        *metadata_dicts: Variable number of metadata dictionaries
        
    Returns:
        Merged metadata dictionary
    """
    result = {}
    for metadata in metadata_dicts:
        if metadata:
            result.update(metadata)
    return result


# ============================================================================
# METADATA SCHEMAS
# ============================================================================

class MetadataSchema:
    """Standard metadata schemas for different contexts"""
    
    FILE_DOWNLOAD = {
        'source_url': str,
        'source_type': str,  # 'youtube', 'drive', 'generic'
        'downloaded_at': str,
        'filename': str,
        'file_size_bytes': int,
        'extension': str,
        'mime_type': str
    }
    
    S3_UPLOAD = {
        'uploaded_at': str,
        's3_key': str,
        's3_bucket': str,
        's3_url': str,
        'original_filename': str,
        'file_size_bytes': int,
        'upload_duration_seconds': float
    }
    
    MEDIA_FILE = {
        'duration_seconds': int,
        'duration_formatted': str,
        'codec': str,
        'bitrate': int,
        'resolution': str,
        'has_audio': bool,
        'has_video': bool
    }