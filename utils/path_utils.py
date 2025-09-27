#!/usr/bin/env python3
"""
DRY Path Utilities
Consolidates file path creation and management patterns.
"""

import os
from pathlib import Path
from typing import Union, Optional

def ensure_directory(path: Union[str, Path], exist_ok: bool = True) -> Path:
    """
    Create directory if it doesn't exist.
    
    Args:
        path: Directory path to create
        exist_ok: Don't raise error if directory already exists
    
    Returns:
        Path object of the created directory
    """
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=exist_ok)
    return path_obj

def create_download_directory(row_id: int, person_name: str, base_dir: str = "downloads") -> Path:
    """
    Create standardized download directory for a person.
    
    Args:
        row_id: Row identifier
        person_name: Person's name
        base_dir: Base downloads directory
    
    Returns:
        Path to the created directory
    """
    clean_name = person_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
    dir_name = f"{row_id}_{clean_name}"
    full_path = Path(base_dir) / dir_name
    return ensure_directory(full_path)

def create_download_path(row_id: int, person_name: str, content_type: str) -> Path:
    """
    Create standardized download path for all content types (DRY consolidation).
    
    Args:
        row_id: Row identifier
        person_name: Person's name  
        content_type: Content type ('youtube', 'drive', or 'general')
    
    Returns:
        Path to the created directory
    """
    clean_name = person_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
    
    if content_type == 'youtube':
        base = 'youtube_downloads'
    elif content_type == 'drive':
        base = 'drive_downloads'
    else:
        base = 'downloads'
    
    dir_name = f"row_{row_id}_{clean_name}"
    return ensure_directory(Path(base) / dir_name)

def create_cache_directory(cache_name: str = "cache") -> Path:
    """
    Create cache directory in standard location.
    
    Args:
        cache_name: Name of the cache directory
    
    Returns:
        Path to the cache directory
    """
    return ensure_directory(cache_name)

def safe_filename(filename: str) -> str:
    """
    Clean filename to be filesystem-safe.
    
    Args:
        filename: Original filename
    
    Returns:
        Safe filename string
    """
    # Remove/replace unsafe characters
    unsafe_chars = '<>:"/\\|?*'
    safe_name = filename
    for char in unsafe_chars:
        safe_name = safe_name.replace(char, '_')
    
    # Limit length
    if len(safe_name) > 255:
        name, ext = os.path.splitext(safe_name)
        safe_name = name[:255-len(ext)] + ext
    
    return safe_name

def get_file_extension(file_path: Union[str, Path]) -> str:
    """
    Get file extension in lowercase.
    
    Args:
        file_path: Path to file
    
    Returns:
        File extension (with dot) in lowercase
    """
    return Path(file_path).suffix.lower()

def extract_extension(filename: str, preserve_case: bool = False, include_dot: bool = True) -> str:
    """
    Extract file extension using standard method (DRY CONSOLIDATION - Step 2).
    
    Centralizes extension extraction to replace:
    - file_ext = '.' + file_name.split('.')[-1].lower()
    - _, ext = os.path.splitext(filename)
    - Various conditional extension handling
    
    Args:
        filename: File name or path
        preserve_case: If False, convert to lowercase (default: False)
        include_dot: If True, include leading dot (default: True)
        
    Returns:
        Extension string (e.g., '.mp3' or 'mp3')
        
    Example:
        ext = extract_extension("video.MP4")  # Returns: '.mp4'
        ext = extract_extension("video.MP4", preserve_case=True)  # Returns: '.MP4'
        ext = extract_extension("video.mp4", include_dot=False)  # Returns: 'mp4'
        ext = extract_extension("no_extension")  # Returns: ''
    """
    _, ext = os.path.splitext(filename)
    
    if not preserve_case and ext:
        ext = ext.lower()
    
    if not include_dot and ext.startswith('.'):
        ext = ext[1:]
    
    return ext

def split_filename(filename: str) -> tuple[str, str]:
    """
    Split filename into name and extension parts (DRY CONSOLIDATION - Step 2).
    
    Args:
        filename: File name or path
        
    Returns:
        Tuple of (name_without_extension, extension_with_dot)
        
    Example:
        name, ext = split_filename("document.pdf")  # Returns: ("document", ".pdf")
        name, ext = split_filename("archive.tar.gz")  # Returns: ("archive.tar", ".gz")
        name, ext = split_filename("no_extension")  # Returns: ("no_extension", "")
    """
    return os.path.splitext(filename)

def normalize_extension(ext: str) -> str:
    """
    Normalize file extension to standard format (DRY CONSOLIDATION - Step 2).
    
    Args:
        ext: Extension string (with or without dot)
        
    Returns:
        Normalized extension with leading dot and lowercase
        
    Example:
        norm = normalize_extension("MP3")  # Returns: '.mp3'
        norm = normalize_extension(".PDF")  # Returns: '.pdf'
        norm = normalize_extension("jpg")  # Returns: '.jpg'
    """
    if not ext:
        return ''
    
    # Ensure leading dot
    if not ext.startswith('.'):
        ext = '.' + ext
    
    # Convert to lowercase
    return ext.lower()

def get_extension_or_default(filename: str, default: str = '.bin') -> str:
    """
    Get file extension or return default if none exists (DRY CONSOLIDATION - Step 2).
    
    Replaces the common pattern:
        if '.' in file_name:
            file_ext = '.' + file_name.split('.')[-1].lower()
        else:
            file_ext = '.bin'
    
    Args:
        filename: File name or path
        default: Default extension if none found (default: '.bin')
        
    Returns:
        Extension string or default
        
    Example:
        ext = get_extension_or_default("video.mp4")  # Returns: '.mp4'
        ext = get_extension_or_default("unknown")  # Returns: '.bin'
        ext = get_extension_or_default("data", ".dat")  # Returns: '.dat'
    """
    ext = extract_extension(filename)
    return ext if ext else normalize_extension(default)

def create_timestamped_filename(base_name: str, extension: str, timestamp: Optional[str] = None) -> str:
    """
    Create filename with timestamp.
    
    Args:
        base_name: Base name for file
        extension: File extension (with or without dot)
        timestamp: Optional timestamp string (defaults to current time)
    
    Returns:
        Timestamped filename
    """
    if timestamp is None:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if not extension.startswith('.'):
        extension = '.' + extension
    
    return f"{base_name}_{timestamp}{extension}"


# ============================================================================
# DRY CONSOLIDATION - STEP 3: UNIFIED FILE PATH VALIDATION
# ============================================================================

def validate_and_resolve_path(path: Union[str, Path], 
                             must_exist: bool = False,
                             create_parent: bool = False,
                             check_writable: bool = False,
                             max_length: int = 260) -> Path:
    """
    Validate and resolve file path with comprehensive checks (DRY CONSOLIDATION - Step 3).
    
    Consolidates path validation patterns found across 12+ files:
    - path = Path(file_path).resolve()
    - if not path.exists(): raise FileNotFoundError
    - path.parent.mkdir(parents=True, exist_ok=True)
    
    Args:
        path: File or directory path
        must_exist: Whether path must already exist
        create_parent: Whether to create parent directories
        check_writable: Whether to check write permissions
        max_length: Maximum path length
        
    Returns:
        Resolved Path object
        
    Raises:
        FileNotFoundError: If must_exist=True and path doesn't exist
        PermissionError: If check_writable=True and path not writable
        ValueError: If path is invalid
        
    Example:
        path = validate_and_resolve_path('/path/to/file.txt', create_parent=True)
        output_path = validate_and_resolve_path('outputs/data.csv', must_exist=False, create_parent=True)
    """
    if not path:
        raise ValueError("Path cannot be empty")
    
    # Convert to Path and resolve
    try:
        path_obj = Path(path).resolve()
    except Exception as e:
        raise ValueError(f"Invalid path format: {path} ({e})")
    
    # Check path length
    if len(str(path_obj)) > max_length:
        raise ValueError(f"Path too long: {len(str(path_obj))} > {max_length}")
    
    # Create parent directories if requested
    if create_parent:
        ensure_directory(path_obj.parent)
    
    # Check existence
    if must_exist and not path_obj.exists():
        raise FileNotFoundError(f"Path does not exist: {path_obj}")
    
    # Check write permissions
    if check_writable:
        # Check if we can write to the file or its parent directory
        check_path = path_obj if path_obj.exists() else path_obj.parent
        if not os.access(check_path, os.W_OK):
            raise PermissionError(f"Path not writable: {path_obj}")
    
    return path_obj


def safe_file_operation(file_path: Union[str, Path], operation: str = "read") -> Path:
    """
    Safely prepare file path for operations (DRY CONSOLIDATION - Step 3).
    
    Consolidates safe file operation patterns:
    - Check if file exists before reading
    - Create parent directories before writing
    - Validate file permissions
    
    Args:
        file_path: Path to file
        operation: Type of operation ('read', 'write', 'append')
        
    Returns:
        Validated Path object
        
    Raises:
        FileNotFoundError: If read operation and file doesn't exist
        PermissionError: If insufficient permissions
        
    Example:
        input_path = safe_file_operation('data/input.csv', 'read')
        output_path = safe_file_operation('outputs/result.csv', 'write')
    """
    path_obj = Path(file_path).resolve()
    
    if operation == "read":
        if not path_obj.exists():
            raise FileNotFoundError(f"File not found for reading: {path_obj}")
        if not path_obj.is_file():
            raise ValueError(f"Path is not a file: {path_obj}")
        if not os.access(path_obj, os.R_OK):
            raise PermissionError(f"File not readable: {path_obj}")
            
    elif operation in ["write", "append"]:
        # Create parent directory if needed
        ensure_directory(path_obj.parent)
        
        # Check write permissions on file or parent directory
        check_path = path_obj if path_obj.exists() else path_obj.parent
        if not os.access(check_path, os.W_OK):
            raise PermissionError(f"Location not writable: {path_obj}")
    
    return path_obj


def resolve_project_path(*path_parts: str, base_dir: Optional[Union[str, Path]] = None) -> Path:
    """
    Resolve path relative to project base directory (DRY CONSOLIDATION - Step 3).
    
    Consolidates project path resolution patterns found in 8+ files:
    - BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    - full_path = os.path.join(BASE_DIR, "some", "path")
    
    Args:
        *path_parts: Path components to join
        base_dir: Base directory (defaults to project root)
        
    Returns:
        Resolved Path object
        
    Example:
        config_path = resolve_project_path('config', 'settings.yaml')
        cache_dir = resolve_project_path('cache')
        data_file = resolve_project_path('outputs', 'data.csv')
    """
    if base_dir is None:
        # Default to project root (two levels up from utils directory)
        base_dir = Path(__file__).parent.parent
    
    base_path = Path(base_dir).resolve()
    return base_path.joinpath(*path_parts)


def create_backup_path(original_path: Union[str, Path], 
                      backup_suffix: str = "backup",
                      timestamp: bool = True) -> Path:
    """
    Create backup file path (DRY CONSOLIDATION - Step 3).
    
    Consolidates backup path creation patterns found in 6+ files:
    - backup_file = original_file + '.backup'
    - backup_file = f"{original_file}.backup_{timestamp}"
    
    Args:
        original_path: Original file path
        backup_suffix: Backup suffix to add
        timestamp: Whether to include timestamp
        
    Returns:
        Backup file path
        
    Example:
        backup = create_backup_path('data.csv')  
        # Returns: 'data.backup_20250127_143022.csv'
        
        backup = create_backup_path('config.json', 'old', False)
        # Returns: 'config.old.json'
    """
    path_obj = Path(original_path)
    name, ext = split_filename(path_obj.name)
    
    if timestamp:
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{name}.{backup_suffix}_{ts}{ext}"
    else:
        backup_name = f"{name}.{backup_suffix}{ext}"
    
    return path_obj.parent / backup_name


def find_available_filename(file_path: Union[str, Path], max_attempts: int = 1000) -> Path:
    """
    Find available filename by adding suffix if file exists (DRY CONSOLIDATION - Step 3).
    
    Consolidates filename availability patterns:
    - while os.path.exists(file_path): file_path = add_suffix(file_path)
    
    Args:
        file_path: Desired file path
        max_attempts: Maximum number of attempts
        
    Returns:
        Available file path
        
    Raises:
        RuntimeError: If no available filename found after max_attempts
        
    Example:
        path = find_available_filename('output.csv')
        # If 'output.csv' exists, returns 'output_1.csv', 'output_2.csv', etc.
    """
    path_obj = Path(file_path)
    
    if not path_obj.exists():
        return path_obj
    
    name, ext = split_filename(path_obj.name)
    parent = path_obj.parent
    
    for i in range(1, max_attempts + 1):
        candidate = parent / f"{name}_{i}{ext}"
        if not candidate.exists():
            return candidate
    
    raise RuntimeError(f"Could not find available filename after {max_attempts} attempts")


def normalize_path_separators(path: Union[str, Path], target_os: str = "current") -> str:
    """
    Normalize path separators for target OS (DRY CONSOLIDATION - Step 3).
    
    Consolidates path separator handling patterns.
    
    Args:
        path: Path to normalize
        target_os: Target OS ('windows', 'posix', 'current')
        
    Returns:
        Normalized path string
        
    Example:
        norm = normalize_path_separators('path\\to\\file', 'posix')
        # Returns: 'path/to/file'
    """
    path_str = str(path)
    
    if target_os == "windows":
        return path_str.replace('/', '\\')
    elif target_os == "posix":
        return path_str.replace('\\', '/')
    else:  # current
        return str(Path(path_str))


def get_relative_path(path: Union[str, Path], base: Union[str, Path]) -> Path:
    """
    Get relative path from base directory (DRY CONSOLIDATION - Step 3).
    
    Args:
        path: Target path
        base: Base directory
        
    Returns:
        Relative path
        
    Example:
        rel = get_relative_path('/project/data/file.csv', '/project')
        # Returns: Path('data/file.csv')
    """
    path_obj = Path(path).resolve()
    base_obj = Path(base).resolve()
    
    try:
        return path_obj.relative_to(base_obj)
    except ValueError:
        # Paths are not relative, return absolute path
        return path_obj