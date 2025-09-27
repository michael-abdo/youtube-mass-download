#!/usr/bin/env python3
"""
Common Validation Patterns Module (DRY Phase 8)

Consolidates validation patterns found throughout the codebase:
- URL validation and sanitization
- File path validation
- Data format validation
- Input sanitization
- Schema validation
- Security checks
"""

import re
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from urllib.parse import urlparse, parse_qs
import mimetypes

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

from utils.logging_config import get_logger
from utils.error_handling import validation_error
from utils.patterns import PatternRegistry, is_youtube_url, is_drive_url, extract_youtube_id, extract_drive_id

logger = get_logger(__name__)


# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================

class ValidationError(Exception):
    """Raised when input validation fails."""
    pass


class SecurityValidationError(ValidationError):
    """Raised when input fails security validation."""
    pass


class FormatValidationError(ValidationError):
    """Raised when input fails format validation."""
    pass


# ============================================================================
# URL VALIDATION PATTERNS
# ============================================================================

def validate_url(url: str, 
                 allowed_domains: Optional[List[str]] = None,
                 allowed_schemes: Optional[List[str]] = None,
                 check_security: bool = True) -> str:
    """
    Validate and sanitize URL with security checks.
    
    Consolidates URL validation patterns found in 15+ files.
    
    Args:
        url: URL to validate
        allowed_domains: List of allowed domains
        allowed_schemes: List of allowed schemes (default: ['http', 'https'])
        check_security: Whether to perform security validation
        
    Returns:
        Sanitized URL
        
    Raises:
        ValidationError: If URL is invalid
        SecurityValidationError: If URL fails security checks
        
    Example:
        clean_url = validate_url('https://youtube.com/watch?v=abc123')
        youtube_url = validate_url(url, allowed_domains=['youtube.com', 'youtu.be'])
    """
    if not url or not isinstance(url, str):
        raise ValidationError("URL must be a non-empty string")
    
    # Remove control characters and normalize
    url = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', url).strip()
    
    if not url:
        raise ValidationError("URL cannot be empty after sanitization")
    
    # Parse URL
    try:
        parsed = urlparse(url)
    except Exception as e:
        raise ValidationError(f"Invalid URL format: {url} ({e})")
    
    # Validate scheme and netloc
    if not parsed.scheme or not parsed.netloc:
        raise ValidationError(f"URL must include protocol and domain: {url}")
    
    # Check allowed schemes
    if allowed_schemes is None:
        allowed_schemes = ['http', 'https']
    
    if parsed.scheme.lower() not in [s.lower() for s in allowed_schemes]:
        raise ValidationError(f"URL scheme '{parsed.scheme}' not in allowed schemes: {allowed_schemes}")
    
    # Security validation
    if check_security:
        _validate_url_security(url)
    
    # Domain validation
    if allowed_domains:
        _validate_url_domain(parsed.netloc, allowed_domains)
    
    return url


def _validate_url_security(url: str) -> None:
    """Validate URL for security threats."""
    # Check for suspicious patterns
    suspicious_patterns = [
        (r'[;|`$]', 'Command separators'),
        (r'\.\./', 'Path traversal'),
        (r'%00', 'Null byte'),
        (r'\$\(', 'Command substitution'),
        (r'\{.*\}', 'Variable expansion'),
        (r"'.*&&.*'", 'Command injection'),
        (r'".*&&.*"', 'Command injection'),
        (r'javascript:', 'JavaScript protocol'),
        (r'data:', 'Data protocol'),
        (r'file:', 'File protocol'),
        (r'ftp:', 'FTP protocol'),
    ]
    
    for pattern, description in suspicious_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            raise SecurityValidationError(f"URL contains {description}: {url}")


def _validate_url_domain(netloc: str, allowed_domains: List[str]) -> None:
    """Validate URL domain against allowed list."""
    domain = netloc.lower().replace('www.', '')
    
    # Remove port if present
    domain = domain.split(':')[0]
    
    allowed = False
    for allowed_domain in allowed_domains:
        allowed_domain = allowed_domain.lower().replace('www.', '')
        if domain == allowed_domain or domain.endswith('.' + allowed_domain):
            allowed = True
            break
    
    if not allowed:
        raise ValidationError(f"Domain '{domain}' not in allowed list: {allowed_domains}")


def validate_youtube_url(url: str) -> Tuple[str, str]:
    """
    Validate YouTube URL and extract video ID (DRY CONSOLIDATION - Step 1).
    
    Consolidates URL validation from url_utils.py with enhanced validation logic.
    
    Args:
        url: YouTube URL
        
    Returns:
        Tuple of (validated_url, video_id)
        
    Raises:
        ValidationError: If URL is invalid
        
    Example:
        url, video_id = validate_youtube_url('https://youtu.be/abc123')
    """
    # Clean and validate URL first
    url = validate_url(url, allowed_domains=['youtube.com', 'youtu.be'])
    
    if not is_youtube_url(url):
        raise ValidationError(f"Not a valid YouTube URL: {url}")
    
    video_id = extract_youtube_id(url)
    if not video_id:
        raise ValidationError(f"Could not extract video ID from YouTube URL: {url}")
    
    # Validate video ID format
    if not re.match(r'^[a-zA-Z0-9_-]{11}$', video_id):
        raise ValidationError(f"Invalid YouTube video ID format: {video_id}")
    
    return url, video_id


def normalize_youtube_url(url: str) -> str:
    """
    Normalize YouTube URL to standard format (DRY CONSOLIDATION - Step 1).
    
    Consolidates functionality from url_utils.py into validation module.
    
    Args:
        url: YouTube URL to normalize
        
    Returns:
        Normalized YouTube URL
        
    Raises:
        ValidationError: If URL is invalid
        
    Example:
        normalized = normalize_youtube_url('https://youtu.be/abc123')
        # Returns: 'https://www.youtube.com/watch?v=abc123'
    """
    _, video_id = validate_youtube_url(url)
    return f"https://www.youtube.com/watch?v={video_id}"


def validate_drive_url(url: str) -> Tuple[str, str]:
    """
    Validate Google Drive URL and extract file ID (DRY CONSOLIDATION - Step 1).
    
    Consolidates URL validation from url_utils.py with enhanced validation logic.
    
    Args:
        url: Google Drive URL
        
    Returns:
        Tuple of (validated_url, file_id)
        
    Raises:
        ValidationError: If URL is invalid
        
    Example:
        url, file_id = validate_drive_url('https://drive.google.com/file/d/abc123/view')
    """
    # Clean and validate URL first  
    url = validate_url(url, allowed_domains=['drive.google.com', 'docs.google.com'])
    
    if not is_drive_url(url):
        raise ValidationError(f"Not a valid Google Drive URL: {url}")
    
    file_id = extract_drive_id(url)
    if not file_id:
        raise ValidationError(f"Could not extract file ID from Drive URL: {url}")
    
    # Validate file ID format (Google Drive IDs are typically 25+ characters)
    if not re.match(r'^[a-zA-Z0-9_-]{25,}$', file_id):
        raise ValidationError(f"Invalid Google Drive file ID format: {file_id}")
    
    return url, file_id


def normalize_drive_url(url: str) -> str:
    """
    Normalize Google Drive URL to standard format (DRY CONSOLIDATION - Step 1).
    
    Consolidates functionality from url_utils.py into validation module.
    
    Args:
        url: Google Drive URL to normalize
        
    Returns:
        Normalized Google Drive URL
        
    Raises:
        ValidationError: If URL is invalid
        
    Example:
        normalized = normalize_drive_url('https://drive.google.com/file/d/abc123/edit')
        # Returns: 'https://drive.google.com/file/d/abc123/view'
    """
    _, file_id = validate_drive_url(url)
    return f"https://drive.google.com/file/d/{file_id}/view"


def parse_url_links(text: str, separator: str = '|') -> List[str]:
    """
    Parse pipe-separated URLs from text (DRY CONSOLIDATION - Step 1).
    
    Consolidates URL parsing logic from url_utils.py into validation module.
    
    Args:
        text: Text containing URLs
        separator: URL separator character
        
    Returns:
        List of clean URLs
        
    Example:
        urls = parse_url_links('url1|url2|url3')
        # Returns: ['url1', 'url2', 'url3']
    """
    if not text or text == 'nan':
        return []
    
    # Handle pandas NaN values
    try:
        import pandas as pd
        if pd.isna(text):
            return []
    except ImportError:
        pass
    
    links = str(text).split(separator)
    return [link.strip() for link in links if link.strip() and link.strip() != 'nan']


# ============================================================================
# FILE PATH VALIDATION
# ============================================================================

def validate_file_path(file_path: Union[str, Path], 
                       must_exist: bool = False,
                       allowed_extensions: Optional[List[str]] = None,
                       max_length: int = 260,
                       check_security: bool = True) -> Path:
    """
    Validate file path with security checks.
    
    Consolidates file path validation patterns.
    
    Args:
        file_path: File path to validate
        must_exist: Whether file must exist
        allowed_extensions: List of allowed file extensions
        max_length: Maximum path length
        check_security: Whether to perform security validation
        
    Returns:
        Validated Path object
        
    Raises:
        ValidationError: If path is invalid
        
    Example:
        path = validate_file_path('downloads/video.mp4', allowed_extensions=['.mp4', '.mp3'])
    """
    if not file_path:
        raise ValidationError("File path cannot be empty")
    
    # Convert to Path object
    path = Path(file_path)
    
    # Validate path length
    if len(str(path)) > max_length:
        raise ValidationError(f"File path too long: {len(str(path))} > {max_length}")
    
    # Security validation
    if check_security:
        _validate_path_security(str(path))
    
    # Check if file exists when required
    if must_exist and not path.exists():
        raise ValidationError(f"File does not exist: {path}")
    
    # Validate file extension
    if allowed_extensions:
        extension = path.suffix.lower()
        allowed_lower = [ext.lower() for ext in allowed_extensions]
        if extension not in allowed_lower:
            raise ValidationError(f"File extension '{extension}' not in allowed list: {allowed_extensions}")
    
    return path


def _validate_path_security(path: str) -> None:
    """Validate path for security threats."""
    # Check for path traversal
    if '..' in path:
        raise SecurityValidationError(f"Path traversal detected: {path}")
    
    # Check for suspicious characters
    suspicious_chars = ['|', ';', '&', '$', '`', '<', '>', '"', "'"]
    for char in suspicious_chars:
        if char in path:
            raise SecurityValidationError(f"Suspicious character '{char}' in path: {path}")
    
    # Check for control characters
    if re.search(r'[\x00-\x1f\x7f-\x9f]', path):
        raise SecurityValidationError(f"Control characters detected in path: {path}")


def validate_filename(filename: str, 
                     max_length: int = 255,
                     allowed_chars: Optional[str] = None) -> str:
    """
    Validate filename for filesystem compatibility.
    
    Args:
        filename: Filename to validate
        max_length: Maximum filename length
        allowed_chars: Regex pattern for allowed characters
        
    Returns:
        Validated filename
        
    Example:
        clean_name = validate_filename('video (1).mp4')
    """
    if not filename:
        raise ValidationError("Filename cannot be empty")
    
    # Check length
    if len(filename) > max_length:
        raise ValidationError(f"Filename too long: {len(filename)} > {max_length}")
    
    # Check for invalid filesystem characters
    invalid_chars = r'[<>:"/\\|?*\x00-\x1f]'
    if re.search(invalid_chars, filename):
        raise ValidationError(f"Invalid characters in filename: {filename}")
    
    # Check for reserved names (Windows)
    reserved_names = {
        'CON', 'PRN', 'AUX', 'NUL', 
        'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
        'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
    }
    
    # DRY CONSOLIDATION - Step 2: Use centralized extension handling
    from .path_utils import split_filename
    name_without_ext, _ = split_filename(filename)
    name_without_ext = name_without_ext.upper()
    if name_without_ext in reserved_names:
        raise ValidationError(f"Reserved filename: {filename}")
    
    # Custom character validation
    if allowed_chars and not re.match(allowed_chars, filename):
        raise ValidationError(f"Filename contains disallowed characters: {filename}")
    
    return filename


def sanitize_filename(filename: str, replacement: str = '_') -> str:
    """
    Sanitize filename by replacing invalid characters.
    
    Args:
        filename: Filename to sanitize
        replacement: Character to replace invalid chars with
        
    Returns:
        Sanitized filename
        
    Example:
        clean_name = sanitize_filename('video<>name.mp4')  # Returns 'video__name.mp4'
    """
    if not filename:
        return 'unnamed'
    
    # Replace invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', replacement, filename)
    
    # Remove leading/trailing dots and spaces
    sanitized = sanitized.strip('. ')
    
    # Ensure not empty
    if not sanitized:
        sanitized = 'unnamed'
    
    # Truncate if too long
    if len(sanitized) > 255:
        name, ext = split_filename(sanitized)
        max_name_len = 255 - len(ext)
        sanitized = name[:max_name_len] + ext
    
    return sanitized


# ============================================================================
# DATA FORMAT VALIDATION
# ============================================================================

def validate_email(email: str) -> str:
    """
    Validate email address format.
    
    Args:
        email: Email address to validate
        
    Returns:
        Validated email address
        
    Example:
        email = validate_email('user@example.com')
    """
    if not email or not isinstance(email, str):
        raise ValidationError("Email must be a non-empty string")
    
    email = email.strip().lower()
    
    # Basic email regex (RFC 5322 compliant)
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    if not re.match(email_pattern, email):
        raise ValidationError(f"Invalid email format: {email}")
    
    # Additional checks
    if len(email) > 254:  # RFC 5321 limit
        raise ValidationError("Email address too long")
    
    local, domain = email.split('@')
    if len(local) > 64:  # RFC 5321 limit
        raise ValidationError("Email local part too long")
    
    return email


def validate_phone(phone: str, country_code: Optional[str] = None) -> str:
    """
    Validate phone number format.
    
    Args:
        phone: Phone number to validate
        country_code: Optional country code for validation
        
    Returns:
        Normalized phone number
        
    Example:
        phone = validate_phone('+1-555-123-4567')
    """
    if not phone or not isinstance(phone, str):
        raise ValidationError("Phone number must be a non-empty string")
    
    # Remove common formatting
    cleaned = re.sub(r'[^\d+]', '', phone)
    
    # Basic validation - must have at least 7 digits
    if len(re.sub(r'[^\d]', '', cleaned)) < 7:
        raise ValidationError("Phone number too short")
    
    # International format validation
    if cleaned.startswith('+'):
        if not re.match(r'^\+\d{7,15}$', cleaned):
            raise ValidationError("Invalid international phone format")
    else:
        if not re.match(r'^\d{7,15}$', cleaned):
            raise ValidationError("Invalid phone format")
    
    return cleaned


def validate_json(json_string: str) -> Dict[str, Any]:
    """
    Validate JSON string format.
    
    Args:
        json_string: JSON string to validate
        
    Returns:
        Parsed JSON object
        
    Example:
        data = validate_json('{"key": "value"}')
    """
    if not json_string or not isinstance(json_string, str):
        raise ValidationError("JSON must be a non-empty string")
    
    try:
        return json.loads(json_string)
    except json.JSONDecodeError as e:
        raise FormatValidationError(f"Invalid JSON format: {e}")


def validate_csv_row(row: Dict[str, Any], 
                    required_columns: List[str],
                    column_validators: Optional[Dict[str, Callable]] = None) -> Dict[str, Any]:
    """
    Validate CSV row data.
    
    Args:
        row: Row data as dictionary
        required_columns: List of required column names
        column_validators: Dictionary of column name -> validator function
        
    Returns:
        Validated row data
        
    Example:
        row = validate_csv_row(
            {'name': 'John', 'email': 'john@example.com'},
            required_columns=['name', 'email'],
            column_validators={'email': validate_email}
        )
    """
    if not isinstance(row, dict):
        raise ValidationError("Row must be a dictionary")
    
    # Check required columns
    missing = set(required_columns) - set(row.keys())
    if missing:
        raise ValidationError(f"Missing required columns: {missing}")
    
    # Validate individual columns
    if column_validators:
        validated_row = row.copy()
        for column, validator in column_validators.items():
            if column in row and row[column] is not None:
                try:
                    validated_row[column] = validator(row[column])
                except Exception as e:
                    raise ValidationError(f"Validation failed for column '{column}': {e}")
        return validated_row
    
    return row


# ============================================================================
# INPUT SANITIZATION
# ============================================================================

def sanitize_string(text: str, 
                   max_length: Optional[int] = None,
                   allowed_chars: Optional[str] = None,
                   remove_html: bool = True) -> str:
    """
    Sanitize string input for safe processing.
    
    Args:
        text: Text to sanitize
        max_length: Maximum allowed length
        allowed_chars: Regex pattern for allowed characters
        remove_html: Whether to remove HTML tags
        
    Returns:
        Sanitized text
        
    Example:
        clean_text = sanitize_string('<script>alert("xss")</script>Hello', remove_html=True)
    """
    if not isinstance(text, str):
        text = str(text) if text is not None else ''
    
    # Remove HTML tags if requested
    if remove_html:
        text = re.sub(r'<[^>]*>', '', text)
    
    # Remove control characters
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
    
    # Apply character whitelist
    if allowed_chars:
        text = re.sub(f'[^{allowed_chars}]', '', text)
    
    # Truncate if too long
    if max_length and len(text) > max_length:
        text = text[:max_length]
    
    return text.strip()


def sanitize_sql_identifier(identifier: str) -> str:
    """
    Sanitize SQL identifier (table/column name).
    
    Args:
        identifier: SQL identifier to sanitize
        
    Returns:
        Sanitized identifier
        
    Example:
        table_name = sanitize_sql_identifier('user_data')
    """
    if not identifier:
        raise ValidationError("SQL identifier cannot be empty")
    
    # Only allow alphanumeric and underscore
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValidationError(f"Invalid SQL identifier: {identifier}")
    
    # Check for SQL keywords (basic list)
    sql_keywords = {
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER',
        'TABLE', 'DATABASE', 'INDEX', 'VIEW', 'PROCEDURE', 'FUNCTION',
        'TRIGGER', 'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK', 'TRANSACTION'
    }
    
    if identifier.upper() in sql_keywords:
        raise ValidationError(f"SQL keyword cannot be used as identifier: {identifier}")
    
    return identifier


# ============================================================================
# CONTENT VALIDATION
# ============================================================================

def validate_file_content(file_path: Union[str, Path],
                         expected_mime_type: Optional[str] = None,
                         max_size: Optional[int] = None,
                         check_encoding: bool = True) -> bool:
    """
    Validate file content and properties.
    
    Args:
        file_path: Path to file
        expected_mime_type: Expected MIME type
        max_size: Maximum file size in bytes
        check_encoding: Whether to check text encoding
        
    Returns:
        True if valid
        
    Example:
        is_valid = validate_file_content('data.csv', expected_mime_type='text/csv')
    """
    path = Path(file_path)
    
    if not path.exists():
        raise ValidationError(f"File does not exist: {path}")
    
    # Check file size
    file_size = path.stat().st_size
    if max_size and file_size > max_size:
        raise ValidationError(f"File too large: {file_size} > {max_size} bytes")
    
    # Check MIME type
    if expected_mime_type:
        detected_type, _ = mimetypes.guess_type(str(path))
        if detected_type != expected_mime_type:
            raise ValidationError(f"Wrong file type: expected {expected_mime_type}, got {detected_type}")
    
    # Check encoding for text files
    if check_encoding and expected_mime_type and expected_mime_type.startswith('text/'):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                f.read(1024)  # Read small sample
        except UnicodeDecodeError:
            raise ValidationError(f"File encoding is not UTF-8: {path}")
    
    return True


def validate_url_list(urls: List[str], 
                     allowed_domains: Optional[List[str]] = None) -> List[str]:
    """
    Validate list of URLs.
    
    Args:
        urls: List of URLs to validate
        allowed_domains: List of allowed domains
        
    Returns:
        List of validated URLs
        
    Example:
        clean_urls = validate_url_list(['https://youtube.com/watch?v=abc', 'invalid-url'])
    """
    if not isinstance(urls, list):
        raise ValidationError("URLs must be provided as a list")
    
    validated_urls = []
    errors = []
    
    for i, url in enumerate(urls):
        try:
            validated_url = validate_url(url, allowed_domains=allowed_domains)
            validated_urls.append(validated_url)
        except ValidationError as e:
            errors.append(f"URL {i+1}: {e}")
    
    if errors:
        raise ValidationError(f"URL validation errors: {'; '.join(errors)}")
    
    return validated_urls


# ============================================================================
# VALIDATION DECORATORS
# ============================================================================

def validate_input(**validators):
    """
    Decorator to validate function inputs.
    
    Args:
        **validators: Keyword arguments mapping parameter names to validator functions
        
    Example:
        @validate_input(email=validate_email, age=lambda x: int(x) if x > 0 else None)
        def create_user(email, age):
            return {'email': email, 'age': age}
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Get function parameter names
            import inspect
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            # Validate each parameter
            for param_name, validator in validators.items():
                if param_name in bound_args.arguments:
                    value = bound_args.arguments[param_name]
                    if value is not None:  # Skip None values
                        try:
                            validated_value = validator(value)
                            bound_args.arguments[param_name] = validated_value
                        except Exception as e:
                            raise ValidationError(f"Validation failed for parameter '{param_name}': {e}")
            
            return func(*bound_args.args, **bound_args.kwargs)
        return wrapper
    return decorator


# ============================================================================
# BATCH VALIDATION
# ============================================================================

def validate_batch(items: List[Any], 
                  validator: Callable,
                  stop_on_first_error: bool = False) -> Tuple[List[Any], List[str]]:
    """
    Validate a batch of items.
    
    Args:
        items: List of items to validate
        validator: Validator function
        stop_on_first_error: Whether to stop on first error
        
    Returns:
        Tuple of (validated_items, error_messages)
        
    Example:
        emails = ['user1@example.com', 'invalid-email', 'user2@example.com']
        valid_emails, errors = validate_batch(emails, validate_email)
    """
    validated_items = []
    errors = []
    
    for i, item in enumerate(items):
        try:
            validated_item = validator(item)
            validated_items.append(validated_item)
        except Exception as e:
            error_msg = f"Item {i+1}: {e}"
            errors.append(error_msg)
            
            if stop_on_first_error:
                break
    
    return validated_items, errors


# Example usage
if __name__ == "__main__":
    # Test URL validation
    try:
        url = validate_url('https://youtube.com/watch?v=abc123')
        print(f"Valid URL: {url}")
        
        url, video_id = validate_youtube_url('https://youtu.be/abc123def45')
        print(f"YouTube URL: {url}, Video ID: {video_id}")
        
    except ValidationError as e:
        print(f"Validation error: {e}")
    
    # Test email validation
    try:
        email = validate_email('user@example.com')
        print(f"Valid email: {email}")
    except ValidationError as e:
        print(f"Email validation error: {e}")
    
    # Test batch validation
    urls = [
        'https://youtube.com/watch?v=abc123',
        'invalid-url',
        'https://drive.google.com/file/d/123456789'
    ]
    
    valid_urls, errors = validate_batch(urls, validate_url)
    print(f"Valid URLs: {len(valid_urls)}, Errors: {len(errors)}")
    
    print("✓ Validation patterns test complete!")


# ============================================================================
# ADDITIONAL VALIDATION FUNCTIONS FOR DRY TEST COMPATIBILITY
# ============================================================================
# DRY CONSOLIDATION - Step 2: Duplicate functions removed
# validate_google_drive_url and validate_youtube_url are already defined above
# using centralized extraction patterns from utils.patterns
# See lines 160-218 for the canonical implementations

# Backward compatibility aliases
validate_google_drive_url = validate_drive_url


def validate_file_path(path: str, must_exist: bool = False) -> str:
    """
    Validate file path.
    
    Args:
        path: File path to validate
        must_exist: Whether file must exist
        
    Returns:
        Validated path
    """
    if not path or not isinstance(path, str):
        raise ValidationError("Path must be a non-empty string")
    
    path = path.strip()
    
    # Check for path traversal attempts
    if '..' in path:
        raise ValidationError("Path traversal not allowed")
    
    # Check for invalid characters
    invalid_chars = ['<', '>', ':', '"', '|', '?', '*']
    if any(char in path for char in invalid_chars):
        raise ValidationError(f"Path contains invalid characters: {invalid_chars}")
    
    if must_exist and not os.path.exists(path):
        raise ValidationError(f"Path does not exist: {path}")
    
    return path


def is_valid_youtube_url(url: str) -> bool:
    """Check if URL is a valid YouTube URL."""
    try:
        validate_youtube_url(url)
        return True
    except ValidationError:
        return False


def is_valid_drive_url(url: str) -> bool:
    """Check if URL is a valid Google Drive URL."""
    try:
        validate_google_drive_url(url)
        return True
    except ValidationError:
        return False


def get_url_type(url: str) -> str:
    """Get the type of URL (youtube, drive, or unknown)."""
    if is_valid_youtube_url(url):
        return 'youtube'
    elif is_valid_drive_url(url):
        return 'drive'
    else:
        return 'unknown'


def run_validation():
    """Run validation tests."""
    print("Running validation tests...")
    
    # Test YouTube URLs
    youtube_urls = [
        'https://youtube.com/watch?v=dQw4w9WgXcQ',
        'https://youtu.be/dQw4w9WgXcQ',
        'invalid_url'
    ]
    
    for url in youtube_urls:
        print(f"YouTube URL {url}: {is_valid_youtube_url(url)}")
    
    # Test Drive URLs
    drive_urls = [
        'https://drive.google.com/file/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',
        'https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',
        'invalid_url'
    ]
    
    for url in drive_urls:
        print(f"Drive URL {url}: {is_valid_drive_url(url)}")
    
    print("Validation tests complete.")


def validate_and_extract_media_url(url: str, expected_type: str = 'any') -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Single source of truth for all URL validation and ID extraction (DRY CONSOLIDATION).
    
    Eliminates 3+ different URL validation implementations that cause security vulnerabilities
    and data corruption from inconsistent URL handling.
    
    Args:
        url: URL to validate and process
        expected_type: Expected URL type ('youtube', 'drive', 'any')
        
    Returns:
        Tuple of (is_valid, extracted_id, normalized_url)
        - is_valid: True if URL is valid for expected type
        - extracted_id: Extracted video/file ID (None for generic URLs)
        - normalized_url: Canonicalized URL for consistent storage
        
    Example:
        valid, video_id, canonical = validate_and_extract_media_url(
            'https://youtu.be/abc123?t=10', 'youtube'
        )
        # Returns: (True, 'abc123', 'https://www.youtube.com/watch?v=abc123')
    """
    if not url or url.strip() in ['', 'nan', 'None', 'null']:
        return False, None, None
    
    # Use existing patterns from utils.patterns but centralize logic here
    from .patterns import (
        extract_youtube_id, extract_drive_id, clean_url,
        is_youtube_url, is_drive_url, validate_url_format
    )
    
    cleaned_url = clean_url(url.strip())
    
    # YouTube URL validation with security checks
    if expected_type in ['youtube', 'any'] and is_youtube_url(cleaned_url):
        video_id = extract_youtube_id(cleaned_url)
        if video_id and len(video_id) == 11 and re.match(r'^[a-zA-Z0-9_-]{11}$', video_id):
            # Security: Ensure video ID is alphanumeric only
            normalized = f"https://www.youtube.com/watch?v={video_id}"
            return True, video_id, normalized
    
    # Google Drive URL validation with security checks  
    if expected_type in ['drive', 'any'] and is_drive_url(cleaned_url):
        file_id = extract_drive_id(cleaned_url)
        if file_id and len(file_id) >= 25 and re.match(r'^[a-zA-Z0-9_-]{25,}$', file_id):
            # Security: Ensure file ID is alphanumeric only
            normalized = f"https://drive.google.com/file/d/{file_id}/view"
            return True, file_id, normalized
    
    # Generic URL validation
    if expected_type == 'any' and validate_url_format(cleaned_url):
        return True, None, cleaned_url
    
    return False, None, None


def validate_row_id(row_id: Any) -> Tuple[bool, Optional[int]]:
    """
    Standardized row ID validation across all modules (DRY consolidation).
    
    Args:
        row_id: Row ID to validate (can be str, int, float, or None)
        
    Returns:
        Tuple of (is_valid, validated_id)
        - is_valid: True if row ID is valid, False otherwise
        - validated_id: Integer row ID if valid, None if invalid
        
    Business Rules:
        - Row IDs must be positive integers
        - Empty/None values are invalid
        - String representations like "123.0" are converted to integers
        - Values <= 0 are invalid
    """
    from typing import Any, Optional, Tuple
    
    if row_id is None or row_id == '':
        return False, None
    
    try:
        # Handle string representations
        if isinstance(row_id, str):
            row_id = row_id.strip()
            if not row_id or row_id.lower() in ['nan', 'none', 'null']:
                return False, None
        
        # Convert to integer (handles "123.0" strings and floats)
        valid_id = int(float(str(row_id)))
        
        # Business rule: Row IDs must be positive
        if valid_id <= 0:
            return False, None
            
        return True, valid_id
        
    except (ValueError, TypeError, OverflowError):
        return False, None


# ============================================================================
# UNIFIED VALIDATION SYSTEM (DRY ITERATION 3 - Step 3)
# ============================================================================

class UnifiedValidator:
    """
    Unified validation system to eliminate duplication across codebase.
    
    CONSOLIDATES PATTERNS FROM:
    - 15+ files with different file extension validation
    - 10+ files with different CSV field validation
    - Multiple URL validation implementations
    - Inconsistent data type validation
    
    BUSINESS IMPACT: Prevents data corruption from inconsistent validation
    """
    
    # CSV field size limits (consistent across system)
    CSV_FIELD_MAX_LENGTH = 32000  # Standard limit
    CSV_FIELD_SAFE_LENGTH = 30000  # Conservative limit with buffer
    
    # Common file extensions grouped by type
    FILE_EXTENSIONS = {
        'video': {'.mp4', '.avi', '.mov', '.mkv', '.webm', '.flv', '.m4v', '.wmv'},
        'audio': {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a', '.wma'},
        'image': {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp'},
        'document': {'.pdf', '.doc', '.docx', '.txt', '.odt', '.rtf'},
        'archive': {'.zip', '.rar', '.7z', '.tar', '.gz', '.bz2'},
        'data': {'.csv', '.json', '.xml', '.yaml', '.yml'},
    }
    
    @classmethod
    def validate_file_extension(cls, filename: str, allowed_types: Union[str, List[str]] = 'all') -> Tuple[bool, str]:
        """
        Unified file extension validation.
        
        ELIMINATES PATTERNS:
        - ext = os.path.splitext(filename)[1]
        - file_ext = Path(file_path).suffix.lower()
        - extension = filename.split('.')[-1] if '.' in filename else ''
        
        Args:
            filename: Filename to validate
            allowed_types: 'all', specific type ('video', 'audio'), or list of extensions
            
        Returns:
            Tuple of (is_valid, normalized_extension)
            
        Example:
            valid, ext = UnifiedValidator.validate_file_extension('video.MP4', 'video')
            # Returns: (True, '.mp4')
        """
        if not filename:
            return False, ''
        
        # Normalize and extract extension
        filename = str(filename).strip()
        parts = filename.rsplit('.', 1)
        
        if len(parts) < 2 or not parts[1]:
            return False, ''
        
        # Normalize extension
        extension = f".{parts[1].lower()}"
        
        # Check against allowed types
        if allowed_types == 'all':
            all_extensions = set()
            for ext_set in cls.FILE_EXTENSIONS.values():
                all_extensions.update(ext_set)
            return extension in all_extensions, extension
        
        elif isinstance(allowed_types, str):
            if allowed_types in cls.FILE_EXTENSIONS:
                return extension in cls.FILE_EXTENSIONS[allowed_types], extension
            else:
                return False, extension
                
        elif isinstance(allowed_types, list):
            normalized_allowed = {ext.lower() if ext.startswith('.') else f'.{ext.lower()}' 
                                for ext in allowed_types}
            return extension in normalized_allowed, extension
        
        return False, extension
    
    @classmethod
    def validate_csv_field(cls, value: Any, field_name: str = 'field',
                          max_length: Optional[int] = None,
                          sanitize: bool = True) -> Tuple[bool, str, Optional[str]]:
        """
        Unified CSV field validation with consistent rules.
        
        CONSOLIDATES PATTERNS:
        - if len(value) > 32000:  # Some files
        - if len(str(value)) > 65535:  # Other files  
        - if '\\x00' in value:  # Security checks in some places only
        
        Args:
            value: Value to validate
            field_name: Name of field for error messages
            max_length: Maximum allowed length (default: CSV_FIELD_MAX_LENGTH)
            sanitize: Whether to sanitize the value
            
        Returns:
            Tuple of (is_valid, sanitized_value, error_message)
            
        Example:
            valid, clean_val, err = UnifiedValidator.validate_csv_field(user_input, 'description')
        """
        max_length = max_length or cls.CSV_FIELD_MAX_LENGTH
        
        # Convert to string
        if value is None:
            return True, '', None
        
        str_value = str(value)
        
        # Security validation - check for null bytes
        if '\x00' in str_value:
            return False, '', f"{field_name} contains null bytes (security risk)"
        
        # Check for CSV injection patterns
        dangerous_starts = ('=', '+', '-', '@', '\t', '\r')
        if str_value.strip() and str_value.strip()[0] in dangerous_starts:
            if sanitize:
                str_value = '_' + str_value.strip()
            else:
                return False, str_value, f"{field_name} starts with potentially dangerous character"
        
        # Length validation
        if len(str_value) > max_length:
            if sanitize:
                str_value = str_value[:max_length - 3] + '...'
            else:
                return False, str_value, f"{field_name} exceeds maximum length of {max_length}"
        
        # Remove problematic characters if sanitizing
        if sanitize:
            # Remove control characters
            str_value = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', str_value)
            # Normalize whitespace
            str_value = ' '.join(str_value.split())
        
        return True, str_value, None
    
    @classmethod
    def validate_s3_key(cls, key: str, check_uuid: bool = True) -> Tuple[bool, Optional[str]]:
        """
        Unified S3 key validation.
        
        CONSOLIDATES PATTERN: f"files/{uuid}{ext}" repeated in 10+ files
        
        Args:
            key: S3 key to validate
            check_uuid: Whether to validate UUID format
            
        Returns:
            Tuple of (is_valid, error_message)
            
        Example:
            valid, err = UnifiedValidator.validate_s3_key('files/123e4567-e89b.mp4')
        """
        if not key or not isinstance(key, str):
            return False, "S3 key must be a non-empty string"
        
        # Check basic format
        if not key.startswith('files/'):
            return False, "S3 key must start with 'files/'"
        
        # Extract filename part
        filename = key[6:]  # Remove 'files/' prefix
        if not filename:
            return False, "S3 key missing filename"
        
        # Validate extension
        valid_ext, _ = cls.validate_file_extension(filename, 'all')
        if not valid_ext:
            return False, f"Invalid file extension in S3 key: {filename}"
        
        # Validate UUID if requested
        if check_uuid:
            # Extract UUID part (before extension)
            uuid_part = filename.rsplit('.', 1)[0]
            # Basic UUID format check (simplified)
            if not re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', 
                          uuid_part, re.IGNORECASE):
                return False, f"Invalid UUID format in S3 key: {uuid_part}"
        
        # Check for dangerous characters
        if any(char in key for char in ['..', '//', '\\', '\x00', ' ', '\t', '\n', '\r']):
            return False, "S3 key contains invalid characters"
        
        return True, None


def validate_and_sanitize_input(value: Any, input_type: str = 'text',
                              max_length: int = 1000,
                              allowed_patterns: Optional[List[str]] = None) -> Tuple[bool, Any, Optional[str]]:
    """
    Universal input validation and sanitization function.
    
    CONSOLIDATES validation logic scattered across the codebase.
    
    Args:
        value: Input value to validate
        input_type: Type of input ('text', 'url', 'email', 'number', 'json', 'csv_field')
        max_length: Maximum allowed length
        allowed_patterns: Optional list of regex patterns to match
        
    Returns:
        Tuple of (is_valid, sanitized_value, error_message)
        
    Example:
        valid, clean, err = validate_and_sanitize_input(user_input, 'url')
        valid, clean, err = validate_and_sanitize_input(data, 'json')
    """
    if value is None:
        return True, None, None
    
    try:
        if input_type == 'text':
            str_val = str(value).strip()
            if len(str_val) > max_length:
                return False, str_val[:max_length], f"Text exceeds {max_length} characters"
            # Remove control characters
            clean_val = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', str_val)
            return True, clean_val, None
            
        elif input_type == 'url':
            try:
                return True, validate_url(str(value)), None
            except ValidationError as e:
                return False, str(value), str(e)
                
        elif input_type == 'email':
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            str_val = str(value).strip().lower()
            if re.match(email_pattern, str_val):
                return True, str_val, None
            return False, str_val, "Invalid email format"
            
        elif input_type == 'number':
            try:
                if isinstance(value, (int, float)):
                    return True, value, None
                num_val = float(str(value))
                return True, num_val, None
            except (ValueError, TypeError):
                return False, value, "Invalid number format"
                
        elif input_type == 'json':
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    return True, parsed, None
                except json.JSONDecodeError as e:
                    return False, value, f"Invalid JSON: {str(e)}"
            # Already a dict/list
            return True, value, None
            
        elif input_type == 'csv_field':
            return UnifiedValidator.validate_csv_field(value, max_length=max_length)
            
        else:
            return False, value, f"Unknown input type: {input_type}"
            
    except Exception as e:
        return False, value, f"Validation error: {str(e)}"


# ============================================================================
# MIGRATION HELPERS
# ============================================================================

def get_file_extension(filename: str) -> str:
    """
    DEPRECATED: Use UnifiedValidator.validate_file_extension() instead.
    
    Backward compatibility wrapper.
    """
    valid, ext = UnifiedValidator.validate_file_extension(filename)
    return ext if valid else ''


def is_valid_csv_field(value: Any, max_length: int = 32000) -> bool:
    """
    DEPRECATED: Use UnifiedValidator.validate_csv_field() instead.
    
    Backward compatibility wrapper.
    """
    valid, _, _ = UnifiedValidator.validate_csv_field(value, max_length=max_length)
    return valid