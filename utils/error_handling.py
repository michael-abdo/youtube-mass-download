#!/usr/bin/env python3
"""
Unified Error Handling System (DRY Consolidation Phase 2.6)

Consolidates all error handling functionality:
- Error categories and severity levels
- Error message templates (from error_messages.py)
- Error handling decorators (from error_decorators.py)
- ErrorHandler class and ErrorContext dataclass
- Helper functions for formatting errors

This is now the single source of truth for all error handling in the system.
"""

import os
import re
import sys
import functools
import traceback
from typing import Optional, Dict, Any, List, Callable, Type, Union, Tuple
from datetime import datetime
from enum import Enum
from dataclasses import dataclass
from pathlib import Path

try:
    from logging_config import get_logger
except ImportError:
    from .logging_config import get_logger


# ============================================================================
# ERROR SEVERITY AND CATEGORIES
# ============================================================================

class ErrorSeverity(Enum):
    """Error severity levels"""
    INFO = "info"
    WARNING = "warning" 
    ERROR = "error"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error categories for classification"""
    NETWORK = "network"
    FILE_IO = "file_io"
    VALIDATION = "validation"
    PERMISSION = "permission"
    QUOTA = "quota"
    RATE_LIMIT = "rate_limit"
    SYSTEM = "system"
    DATA_CORRUPTION = "data_corruption"


# ============================================================================
# ERROR MESSAGE TEMPLATES (Consolidated from error_messages.py)
# ============================================================================

class ErrorMessages:
    """Centralized error message templates with consistent formatting"""
    
    # Network-related errors
    NETWORK = {
        'CONNECTION_TIMEOUT': "Connection timeout after {timeout}s for {resource}",
        'NETWORK_ERROR': "Network error accessing {resource}: {details}",
        'HTTP_ERROR': "HTTP {status} error: {message}",
        'DNS_ERROR': "DNS resolution failed for {domain}: {details}",
        'SSL_ERROR': "SSL/TLS error connecting to {host}: {details}",
        'RATE_LIMITED': "Rate limit exceeded for {service}: {details}"
    }
    
    # File operation errors  
    FILE_OPERATIONS = {
        'FILE_NOT_FOUND': "File not found: {path}",
        'PERMISSION_DENIED': "Permission denied accessing {path}",
        'ACCESS_DENIED': "Access denied to {resource}",
        'DISK_FULL': "Insufficient disk space for {operation}",
        'FILE_LOCKED': "File is locked and cannot be accessed: {path}",
        'INVALID_PATH': "Invalid file path: {path}",
        'COPY_FAILED': "Failed to copy {source} to {destination}: {error}",
        'DELETE_FAILED': "Failed to delete {path}: {error}",
        'WRITE_FAILED': "Failed to write to {path}: {error}",
        'READ_FAILED': "Failed to read from {path}: {error}"
    }
    
    # Validation errors
    VALIDATION = {
        'INVALID_URL': "Invalid URL format: {url}",
        'INVALID_FORMAT': "Invalid {type} format: {value}",
        'VALIDATION_FAILED': "Validation failed for {field}: {reason}",
        'MISSING_REQUIRED': "Required field missing: {field}",
        'VALUE_OUT_OF_RANGE': "{field} value {value} is out of valid range ({min_val}-{max_val})",
        'INVALID_CSV_FORMAT': "Invalid CSV format in {file}: {details}",
        'INVALID_JSON': "Invalid JSON in {file}: {details}",
        'SCHEMA_VALIDATION_FAILED': "Schema validation failed: {details}"
    }
    
    # Download-specific errors (DRY CONSOLIDATION STEP 4 - Enhanced templates)
    DOWNLOAD = {
        'DOWNLOAD_FAILED': "Download failed for {file_type} '{identifier}': {reason}",
        'YOUTUBE_ERROR': "YouTube download error for {video_id}: {details}",
        'DRIVE_ERROR': "Google Drive download error for {file_id}: {details}",
        'QUOTA_EXCEEDED': "Download quota exceeded for {service}",
        'VIDEO_UNAVAILABLE': "Video unavailable: {video_id}",
        'PLAYLIST_EMPTY': "Playlist is empty or inaccessible: {playlist_url}",
        'SUBTITLE_UNAVAILABLE': "Subtitles not available for {video_id} in language {lang}",
        'FORMAT_UNAVAILABLE': "Requested format {format} not available for {video_id}"
    }
    
    # CSV processing errors
    CSV_PROCESSING = {
        'CSV_READ_ERROR': "Failed to read CSV file {path}: {error}",
        'CSV_WRITE_ERROR': "Failed to write CSV file {path}: {error}",
        'CSV_CORRUPTION': "CSV file corruption detected in {path}: {details}",
        'CSV_BACKUP_FAILED': "Failed to create CSV backup for {path}: {error}",
        'CSV_VALIDATION_FAILED': "CSV validation failed for {path}: {details}",
        'ROW_NOT_FOUND': "Row {row_id} not found in CSV",
        'COLUMN_MISSING': "Required column {column} not found in CSV",
        'DUPLICATE_ROW_ID': "Duplicate row_id {row_id} found in CSV"
    }
    
    # System and process errors
    SYSTEM = {
        'MEMORY_ERROR': "Insufficient memory for {operation}",
        'PROCESS_FAILED': "Process {process} failed with exit code {code}",
        'TIMEOUT_ERROR': "Operation {operation} timed out after {timeout}s",
        'RESOURCE_UNAVAILABLE': "Required resource {resource} is unavailable",
        'DEPENDENCY_MISSING': "Required dependency {dependency} is not installed",
        'CONFIG_ERROR': "Configuration error: {details}",
        'INITIALIZATION_FAILED': "Failed to initialize {component}: {error}"
    }
    
    # Persistence errors
    PERSISTENCE = {
        'BACKUP_FAILED': "Backup operation failed: {details}",
        'RESTORE_FAILED': "Restore operation failed: {details}",
        'SYNC_ERROR': "Synchronization error: {details}",
        'LOCK_TIMEOUT': "Failed to acquire lock for {resource} within {timeout}s"
    }
    
    # Authentication and authorization
    AUTH = {
        'AUTH_FAILED': "Authentication failed for {service}: {details}",
        'TOKEN_EXPIRED': "Access token expired for {service}",
        'INVALID_CREDENTIALS': "Invalid credentials for {service}",
        'PERMISSION_DENIED': "Permission denied for {operation}",
        'QUOTA_EXCEEDED': "API quota exceeded for {service}"
    }
    
    @staticmethod
    def format_error(category: str, error_type: str, **kwargs) -> str:
        """
        Format error message with provided context (DRY CONSOLIDATION STEP 4).
        
        ELIMINATES: Inconsistent error messages across modules
        STANDARDIZES: Error message length and format consistency
        
        Args:
            category: Error category (NETWORK, FILE_OPERATIONS, etc.)
            error_type: Specific error type within category
            **kwargs: Template variables for string formatting
            
        Returns:
            Formatted error message with length limit applied
        """
        category_dict = getattr(ErrorMessages, category.upper(), {})
        template = category_dict.get(error_type.upper())
        
        if template is None:
            return f"Unknown error: {category}.{error_type}"
        
        try:
            message = template.format(**kwargs)
            
            # Apply length limit from CoreDefaults (DRY CONSOLIDATION)
            try:
                from .config import CoreDefaults
                max_length = CoreDefaults.MAX_ERROR_MESSAGE_LENGTH
                if len(message) > max_length:
                    message = message[:max_length-3] + "..."
            except ImportError:
                # Fallback if config not available
                if len(message) > 500:
                    message = message[:497] + "..."
                    
            return message
        except KeyError as e:
            return f"Error message template missing variable {e}: {template}"


# ============================================================================
# ERROR CONTEXT AND HANDLER
# ============================================================================

@dataclass
class ErrorContext:
    """Complete error context for tracking and debugging"""
    error_id: str
    severity: ErrorSeverity
    category: ErrorCategory
    message: str
    details: Optional[str] = None
    row_id: Optional[str] = None
    download_type: Optional[str] = None
    url: Optional[str] = None
    file_path: Optional[str] = None
    timestamp: str = None
    traceback: Optional[str] = None
    recovery_attempts: int = 0
    recovery_suggestions: List[str] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
        if self.recovery_suggestions is None:
            self.recovery_suggestions = []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'error_id': self.error_id,
            'severity': self.severity.value,
            'category': self.category.value,
            'message': self.message,
            'details': self.details,
            'row_id': self.row_id,
            'download_type': self.download_type,
            'url': self.url,
            'file_path': self.file_path,
            'timestamp': self.timestamp,
            'traceback': self.traceback,
            'recovery_attempts': self.recovery_attempts,
            'recovery_suggestions': self.recovery_suggestions
        }
    
    def to_user_message(self) -> str:
        """Generate user-friendly error message"""
        base_msg = f"[{self.severity.value.upper()}] {self.message}"
        if self.recovery_suggestions:
            base_msg += f"\nSuggestions: {', '.join(self.recovery_suggestions)}"
        return base_msg


class ErrorHandler:
    """Centralized error handling with context tracking"""
    
    def __init__(self, logger: Any):
        self.logger = logger
        self.error_counts: Dict[str, int] = {}
        self.recent_errors: List[ErrorContext] = []
        
    def handle_error(self, error: Exception, context: Optional[Dict[str, Any]] = None,
                    raise_after_handling: bool = False) -> ErrorContext:
        """
        Handle errors with full context tracking
        
        Args:
            error: The exception that occurred
            context: Additional context information
            raise_after_handling: Whether to re-raise the error after handling
            
        Returns:
            ErrorContext object with full error details
        """
        error_id = f"ERR_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{id(error)}"
        
        # Classify error
        category, severity = self._classify_error(error)
        
        # Extract details
        details = self._extract_error_details(error)
        
        # Build error context
        error_context = ErrorContext(
            error_id=error_id,
            severity=severity,
            category=category,
            message=str(error),
            details=details,
            traceback=traceback.format_exc() if severity in [ErrorSeverity.ERROR, ErrorSeverity.CRITICAL] else None,
            **context if context else {}
        )
        
        # Add recovery suggestions
        error_context.recovery_suggestions = self._generate_recovery_suggestions(error_context)
        
        # Log the error
        self._log_error(error_context)
        
        # Track the error
        self._track_error_pattern(error_context)
        
        if raise_after_handling:
            raise
            
        return error_context
    
    def _classify_error(self, error: Exception) -> Tuple[ErrorCategory, ErrorSeverity]:
        """Classify error into category and severity"""
        error_type = type(error).__name__
        error_msg = str(error).lower()
        
        # File system errors
        if isinstance(error, (FileNotFoundError, NotADirectoryError, IsADirectoryError)):
            return ErrorCategory.FILE_IO, ErrorSeverity.WARNING
            
        if isinstance(error, PermissionError):
            return ErrorCategory.PERMISSION, ErrorSeverity.ERROR
            
        # Network errors
        if isinstance(error, (ConnectionError, TimeoutError)):
            return ErrorCategory.NETWORK, ErrorSeverity.WARNING
            
        if any(keyword in error_msg for keyword in ['network', 'connection', 'timeout', 'dns']):
            return ErrorCategory.NETWORK, ErrorSeverity.WARNING
            
        # Rate limiting
        if any(keyword in error_msg for keyword in ['rate limit', '429', 'too many requests']):
            return ErrorCategory.RATE_LIMIT, ErrorSeverity.INFO
            
        # Quota errors
        if any(keyword in error_msg for keyword in ['quota', 'limit exceeded', 'storage full']):
            return ErrorCategory.QUOTA, ErrorSeverity.WARNING
            
        # Validation errors
        if any(keyword in error_msg for keyword in ['invalid', 'malformed', 'validation']):
            return ErrorCategory.VALIDATION, ErrorSeverity.INFO
            
        # YouTube/Google API specific errors
        if any(keyword in error_msg for keyword in ['video unavailable', 'private video', 'deleted']):
            return ErrorCategory.VALIDATION, ErrorSeverity.INFO
            
        # Drive file errors
        if any(keyword in error_msg for keyword in ['file not found', 'access denied', 'sharing']):
            return ErrorCategory.PERMISSION, ErrorSeverity.WARNING
            
        # Data corruption
        if any(keyword in error_msg for keyword in ['corrupt', 'malformed', 'invalid format']):
            return ErrorCategory.DATA_CORRUPTION, ErrorSeverity.ERROR
            
        # System errors
        if error_type in ['OSError', 'SystemError', 'MemoryError']:
            return ErrorCategory.SYSTEM, ErrorSeverity.CRITICAL
            
        # Default classification
        return ErrorCategory.SYSTEM, ErrorSeverity.ERROR
    
    def _extract_error_details(self, error: Exception) -> Optional[str]:
        """Extract additional error details for debugging"""
        error_msg = str(error)
        
        # Extract YouTube video ID from error
        youtube_id_match = re.search(r'[a-zA-Z0-9_-]{11}', error_msg)
        if youtube_id_match:
            return f"YouTube video ID: {youtube_id_match.group()}"
            
        # Extract Google Drive file ID
        drive_id_match = re.search(r'[a-zA-Z0-9_-]{25,}', error_msg)
        if drive_id_match:
            return f"Google Drive file ID: {drive_id_match.group()}"
            
        # Extract HTTP status codes
        http_match = re.search(r'HTTP (\d{3})', error_msg)
        if http_match:
            return f"HTTP status: {http_match.group(1)}"
            
        return None
    
    def _generate_recovery_suggestions(self, error_context: ErrorContext) -> List[str]:
        """Generate recovery suggestions based on error type"""
        suggestions = []
        
        if error_context.category == ErrorCategory.NETWORK:
            suggestions.extend([
                "Check your internet connection",
                "Retry the operation in a few minutes",
                "Verify the URL is accessible"
            ])
        elif error_context.category == ErrorCategory.PERMISSION:
            suggestions.extend([
                "Check file permissions",
                "Ensure you have access to the resource",
                "Try running with appropriate privileges"
            ])
        elif error_context.category == ErrorCategory.RATE_LIMIT:
            suggestions.extend([
                "Wait before retrying",
                "Reduce request frequency",
                "Check API quota limits"
            ])
        elif error_context.category == ErrorCategory.FILE_IO:
            suggestions.extend([
                "Verify the file path exists",
                "Check available disk space",
                "Ensure the file is not locked by another process"
            ])
        elif error_context.category == ErrorCategory.VALIDATION:
            suggestions.extend([
                "Verify input format is correct",
                "Check URL or ID validity",
                "Ensure required fields are provided"
            ])
            
        return suggestions
    
    def _log_error(self, error_context: ErrorContext):
        """Log error with appropriate level"""
        log_msg = error_context.to_user_message()
        
        if error_context.severity == ErrorSeverity.CRITICAL:
            self.logger.critical(log_msg)
        elif error_context.severity == ErrorSeverity.ERROR:
            self.logger.error(log_msg)
        elif error_context.severity == ErrorSeverity.WARNING:
            self.logger.warning(log_msg)
        else:
            self.logger.info(log_msg)
    
    def _track_error_pattern(self, error_context: ErrorContext):
        """Track error patterns for monitoring"""
        category_key = error_context.category.value
        self.error_counts[category_key] = self.error_counts.get(category_key, 0) + 1
        
        # Keep recent errors for analysis
        self.recent_errors.append(error_context)
        if len(self.recent_errors) > 100:  # Keep last 100 errors
            self.recent_errors.pop(0)
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get error summary for monitoring"""
        return {
            'total_errors': sum(self.error_counts.values()),
            'error_counts_by_category': self.error_counts.copy(),
            'recent_error_count': len(self.recent_errors),
            'critical_errors': len([e for e in self.recent_errors if e.severity == ErrorSeverity.CRITICAL]),
            'last_error': self.recent_errors[-1].to_dict() if self.recent_errors else None
        }
    
    def should_retry(self, error_context: ErrorContext, attempt_count: int, max_attempts: int = 3) -> bool:
        """Determine if operation should be retried based on error type"""
        if attempt_count >= max_attempts:
            return False
            
        # Retry network errors
        if error_context.category == ErrorCategory.NETWORK:
            return True
            
        # Retry rate limit errors with backoff
        if error_context.category == ErrorCategory.RATE_LIMIT:
            return True
            
        # Don't retry validation errors or permission errors
        if error_context.category in [ErrorCategory.VALIDATION, ErrorCategory.PERMISSION]:
            return False
            
        # Don't retry critical system errors
        if error_context.severity == ErrorSeverity.CRITICAL:
            return False
            
        # Retry other errors up to limit
        return True


# ============================================================================
# ERROR HANDLING DECORATORS (Consolidated from error_decorators.py)
# ============================================================================

def with_standard_error_handling(operation_name: str, return_on_error: Any = None,
                               log_errors: bool = True, raise_after_handling: bool = False):
    """
    Standard error handling decorator for any operation
    
    Args:
        operation_name: Name of the operation for error messages
        return_on_error: Value to return if error occurs
        log_errors: Whether to log errors
        raise_after_handling: Whether to re-raise error after handling
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            error_handler = ErrorHandler(logger)
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                context = {
                    'details': f"Operation: {operation_name}, Function: {func.__name__}, Module: {func.__module__}"
                }
                
                error_handler.handle_error(e, context, raise_after_handling)
                return return_on_error
                
        return wrapper
    return decorator


def handle_file_operations(operation_name: str, return_on_error: Any = None, 
                         log_errors: bool = True, context: Optional[Dict] = None):
    """
    Decorator for standardized file operation error handling
    
    Args:
        operation_name: Name of the file operation for error messages
        return_on_error: Value to return if error occurs (default: None)
        log_errors: Whether to log errors (default: True)
        context: Additional context for error handling
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            error_handler = ErrorHandler(logger)
            
            try:
                return func(*args, **kwargs)
            except FileNotFoundError as e:
                error_msg = ErrorMessages.format_error('FILE_OPERATIONS', 'FILE_NOT_FOUND', path=str(e))
                if log_errors:
                    logger.error(f"{operation_name} failed: {error_msg}")
                
                error_context = {
                    'operation': operation_name,
                    'file_path': str(e),
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except PermissionError as e:
                error_msg = ErrorMessages.format_error('FILE_OPERATIONS', 'PERMISSION_DENIED', path=str(e))
                if log_errors:
                    logger.error(f"{operation_name} failed: {error_msg}")
                
                error_context = {
                    'operation': operation_name,
                    'file_path': str(e),
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except OSError as e:
                # Disk full or other OS errors
                if e.errno == 28:  # ENOSPC - No space left on device
                    error_msg = ErrorMessages.format_error('FILE_OPERATIONS', 'DISK_FULL', operation=operation_name)
                else:
                    error_msg = f"OS error in {operation_name}: {str(e)}"
                
                if log_errors:
                    logger.error(error_msg)
                
                error_context = {
                    'operation': operation_name,
                    'error_code': e.errno,
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except Exception as e:
                error_msg = f"Unexpected error in {operation_name}: {str(e)}"
                if log_errors:
                    logger.error(error_msg)
                
                error_context = {
                    'operation': operation_name,
                    'error_type': type(e).__name__,
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
        return wrapper
    return decorator


def handle_network_operations(operation_name: str, return_on_error: Any = None,
                            retry_count: int = 0, log_errors: bool = True,
                            context: Optional[Dict] = None):
    """
    Decorator for standardized network operation error handling
    
    This decorator now delegates to the centralized retry_utils module
    for consistent retry behavior across the codebase.
    
    Args:
        operation_name: Name of the network operation for error messages
        return_on_error: Value to return if error occurs
        retry_count: Number of retries for network errors (default: 0)
        log_errors: Whether to log errors
        context: Additional context for error handling
    """
    # Import centralized retry utilities
    try:
        from retry_utils import network_retry
    except ImportError:
        from .retry_utils import network_retry
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            error_handler = ErrorHandler(logger)
            
            # Apply centralized network retry logic
            if retry_count > 0:
                retry_decorator = network_retry(
                    max_attempts=retry_count + 1,
                    operation_name=operation_name
                )
                retried_func = retry_decorator(func)
                
                try:
                    return retried_func(*args, **kwargs)
                except Exception as e:
                    # Handle the final error after all retries exhausted
                    if isinstance(e, (ConnectionError, TimeoutError)):
                        error_msg = ErrorMessages.format_error('NETWORK', 'CONNECTION_TIMEOUT', 
                                                             timeout=30, resource=operation_name)
                    else:
                        error_msg = f"Network operation {operation_name} failed: {str(e)}"
                    
                    if log_errors:
                        logger.error(f"{operation_name} failed after {retry_count + 1} attempts: {error_msg}")
                    
                    error_context = {
                        'operation': operation_name,
                        'attempts': retry_count + 1,
                        **(context or {})
                    }
                    error_handler.handle_error(e, error_context)
                    return return_on_error
            else:
                # No retries, execute once
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if log_errors:
                        logger.error(f"{operation_name} failed: {str(e)}")
                    
                    error_context = {
                        'operation': operation_name,
                        'attempts': 1,
                        **(context or {})
                    }
                    error_handler.handle_error(e, error_context)
                    return return_on_error
                    
        return wrapper
    return decorator


def handle_csv_operations(operation_name: str, return_on_error: Any = None,
                         backup_on_write: bool = True, validate_result: bool = True,
                         log_errors: bool = True, context: Optional[Dict] = None):
    """
    Decorator for standardized CSV operation error handling
    
    Args:
        operation_name: Name of the CSV operation for error messages
        return_on_error: Value to return if error occurs
        backup_on_write: Whether to create backup before write operations
        validate_result: Whether to validate CSV after operations
        log_errors: Whether to log errors
        context: Additional context for error handling
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            error_handler = ErrorHandler(logger)
            
            try:
                result = func(*args, **kwargs)
                
                # Validate CSV if requested and result looks like a file path
                if validate_result and isinstance(result, (str, Path)):
                    try:
                        import pandas as pd
                        df = pd.read_csv(result)
                        if len(df.columns) == 0:
                            raise ValueError("CSV has no columns")
                    except Exception as validation_error:
                        error_msg = ErrorMessages.format_error('CSV_PROCESSING', 'CSV_VALIDATION_FAILED',
                                                             path=str(result), details=str(validation_error))
                        if log_errors:
                            logger.warning(f"CSV validation warning: {error_msg}")
                
                return result
                
            except pd.errors.EmptyDataError:
                error_msg = ErrorMessages.format_error('CSV_PROCESSING', 'CSV_CORRUPTION',
                                                     path='unknown', details='Empty data')
                if log_errors:
                    logger.error(f"{operation_name} failed: {error_msg}")
                
                error_context = {
                    'operation': operation_name,
                    'error_type': 'EmptyDataError',
                    **(context or {})
                }
                error_handler.handle_error(Exception(error_msg), error_context)
                return return_on_error
                
            except pd.errors.ParserError as e:
                error_msg = ErrorMessages.format_error('CSV_PROCESSING', 'CSV_CORRUPTION',
                                                     path='unknown', details=str(e))
                if log_errors:
                    logger.error(f"{operation_name} failed: {error_msg}")
                
                error_context = {
                    'operation': operation_name,
                    'error_type': 'ParserError',
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except Exception as e:
                error_msg = f"Unexpected error in CSV operation {operation_name}: {str(e)}"
                if log_errors:
                    logger.error(error_msg)
                
                error_context = {
                    'operation': operation_name,
                    'error_type': type(e).__name__,
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
        return wrapper
    return decorator


def handle_download_operations(operation_name: str, return_on_error: Any = None,
                             retry_count: int = 2, log_errors: bool = True,
                             context: Optional[Dict] = None):
    """
    Decorator for standardized download operation error handling
    
    This decorator now delegates to the centralized retry_utils module
    for consistent retry behavior across the codebase.
    
    Args:
        operation_name: Name of the download operation for error messages
        return_on_error: Value to return if error occurs
        retry_count: Number of retries for download errors (default: 2)
        log_errors: Whether to log errors
        context: Additional context for error handling
    """
    # Import centralized retry utilities
    try:
        from retry_utils import retry_with_backoff
    except ImportError:
        from .retry_utils import retry_with_backoff
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            error_handler = ErrorHandler(logger)
            
            # Apply centralized retry logic for downloads
            if retry_count > 0:
                retry_decorator = retry_with_backoff(
                    max_attempts=retry_count + 1,
                    base_delay=2.0,  # Appropriate for download operations
                    exceptions=(Exception,),
                    logger=logger
                )
                retried_func = retry_decorator(func)
                
                try:
                    return retried_func(*args, **kwargs)
                except Exception as e:
                    # Handle the final error after all retries exhausted
                    error_str = str(e).lower()
                    
                    # Classify download error
                    if any(keyword in error_str for keyword in ['quota', 'limit exceeded']):
                        error_msg = ErrorMessages.format_error('DOWNLOAD', 'QUOTA_EXCEEDED',
                                                             service=operation_name)
                    elif any(keyword in error_str for keyword in ['unavailable', 'not found', 'deleted']):
                        error_msg = ErrorMessages.format_error('DOWNLOAD', 'VIDEO_UNAVAILABLE',
                                                             video_id='unknown')
                    else:
                        error_msg = ErrorMessages.format_error('DOWNLOAD', 'DOWNLOAD_FAILED',
                                                             url='unknown', error=str(e))
                    
                    if log_errors:
                        logger.error(f"{operation_name} failed after {retry_count + 1} attempts: {error_msg}")
                    
                    error_context = {
                        'operation': operation_name,
                        'attempts': retry_count + 1,
                        'download_type': context.get('download_type', 'unknown') if context else 'unknown',
                        **(context or {})
                    }
                    error_handler.handle_error(e, error_context)
                    return return_on_error
            else:
                # No retries, execute once
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if log_errors:
                        logger.error(f"{operation_name} failed: {str(e)}")
                    
                    error_context = {
                        'operation': operation_name,
                        'attempts': 1,
                        'download_type': context.get('download_type', 'unknown') if context else 'unknown',
                        **(context or {})
                    }
                    error_handler.handle_error(e, error_context)
                    return return_on_error
                        
        return wrapper
    return decorator


def handle_validation_errors(operation_name: str, return_on_error: Any = None,
                           log_errors: bool = True, context: Optional[Dict] = None):
    """
    Decorator for standardized validation error handling
    
    Args:
        operation_name: Name of the validation operation for error messages
        return_on_error: Value to return if error occurs
        log_errors: Whether to log errors
        context: Additional context for error handling
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            error_handler = ErrorHandler(logger)
            
            try:
                return func(*args, **kwargs)
                
            except ValueError as e:
                error_msg = ErrorMessages.format_error('VALIDATION', 'VALIDATION_FAILED',
                                                     field=operation_name, reason=str(e))
                if log_errors:
                    logger.error(error_msg)
                
                error_context = {
                    'operation': operation_name,
                    'validation_type': 'value_error',
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except TypeError as e:
                error_msg = ErrorMessages.format_error('VALIDATION', 'INVALID_FORMAT',
                                                     type=operation_name, value='unknown')
                if log_errors:
                    logger.error(error_msg)
                
                error_context = {
                    'operation': operation_name,
                    'validation_type': 'type_error',
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except Exception as e:
                error_msg = f"Unexpected validation error in {operation_name}: {str(e)}"
                if log_errors:
                    logger.error(error_msg)
                
                error_context = {
                    'operation': operation_name,
                    'error_type': type(e).__name__,
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
        return wrapper
    return decorator


# ============================================================================
# DRY ERROR HANDLING DECORATORS WITH TRACEBACK
# ============================================================================

def with_traceback_logging(operation_name: str = None, logger_name: str = None):
    """
    Decorator to automatically log exceptions with traceback.
    
    Args:
        operation_name: Name of the operation for logging context
        logger_name: Logger name to use (defaults to module logger)
    
    Example:
        @with_traceback_logging("process_file")
        def process_file(filename):
            # code that might raise exceptions
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import logging
            logger = logging.getLogger(logger_name or func.__module__)
            op_name = operation_name or func.__name__
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error in {op_name}: {str(e)}")
                logger.error(f"Traceback:\n{traceback.format_exc()}")
                raise
        return wrapper
    return decorator

def try_except_log(default_return=None, logger_name: str = None, 
                  reraise: bool = False, log_level: str = 'error'):
    """
    Decorator for standardized try/except with logging.
    
    Args:
        default_return: Value to return on exception
        logger_name: Logger name to use
        reraise: Whether to re-raise the exception after logging
        log_level: Logging level ('error', 'warning', 'info')
    
    Example:
        @try_except_log(default_return=[], reraise=False)
        def get_files():
            # code that might fail
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import logging
            logger = logging.getLogger(logger_name or func.__module__)
            log_func = getattr(logger, log_level, logger.error)
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                context = f"{func.__name__}"
                if args:
                    # Add first arg to context if it's a simple type
                    first_arg = args[0]
                    if isinstance(first_arg, (str, int, float)):
                        context += f"({first_arg})"
                
                log_func(f"Error in {context}: {str(e)}")
                if log_level == 'error':
                    log_func(f"Traceback:\n{traceback.format_exc()}")
                
                if reraise:
                    raise
                return default_return
        return wrapper
    return decorator

def retry_with_traceback(max_attempts: int = 3, delay: float = 1.0, 
                        backoff: float = 2.0, exceptions=(Exception,)):
    """
    Decorator to retry operations with traceback logging on failures.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier for delay
        exceptions: Tuple of exceptions to catch and retry
    
    Example:
        @retry_with_traceback(max_attempts=3, exceptions=(ConnectionError, TimeoutError))
        def download_file(url):
            # code that might fail transiently
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import logging
            import time
            logger = logging.getLogger(func.__module__)
            
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    if attempt > 0:
                        logger.info(f"Retry attempt {attempt}/{max_attempts - 1} for {func.__name__}")
                    return func(*args, **kwargs)
                    
                except exceptions as e:
                    last_exception = e
                    
                    if attempt < max_attempts - 1:
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}")
                        logger.debug(f"Traceback:\n{traceback.format_exc()}")
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
                        logger.error(f"Final error: {str(e)}")
                        logger.error(f"Traceback:\n{traceback.format_exc()}")
            
            raise last_exception
        return wrapper
    return decorator

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def file_error(error_type: str, **kwargs) -> str:
    """Helper function for file operation errors"""
    return ErrorMessages.format_error('FILE_OPERATIONS', error_type, **kwargs)


def network_error(error_type: str, **kwargs) -> str:
    """Helper function for network errors"""
    return ErrorMessages.format_error('NETWORK', error_type, **kwargs)


def validation_error(error_type: str, **kwargs) -> str:
    """Helper function for validation errors"""
    return ErrorMessages.format_error('VALIDATION', error_type, **kwargs)


def download_error(error_type: str, **kwargs) -> str:
    """Helper function for download errors"""
    return ErrorMessages.format_error('DOWNLOAD', error_type, **kwargs)


def csv_error(error_type: str, **kwargs) -> str:
    """Helper function for CSV processing errors"""
    return ErrorMessages.format_error('CSV_PROCESSING', error_type, **kwargs)


def system_error(error_type: str, **kwargs) -> str:
    """Helper function for system errors"""
    return ErrorMessages.format_error('SYSTEM', error_type, **kwargs)


# ============================================================================
# UNIFIED ERROR HANDLING PATTERNS (DRY ITERATION 3 - Step 2)
# ============================================================================

def try_operation(func: Callable, *args, operation_name: str = None, 
                 default_return: Any = None, log_errors: bool = True,
                 reraise: bool = False, **kwargs) -> Any:
    """
    Unified try/except pattern to eliminate 189+ duplicate try/except blocks.
    
    ELIMINATES PATTERNS LIKE:
    - try: result = func() except Exception as e: logger.error(f"Error: {e}"); return None
    - try: do_something() except Exception as e: logger.error(f"Failed: {e}"); raise
    
    Args:
        func: Function to execute
        *args: Arguments for function
        operation_name: Name of operation for error messages
        default_return: Value to return on error (None, False, [], etc.)
        log_errors: Whether to log errors
        reraise: Whether to re-raise exceptions
        **kwargs: Keyword arguments for function
        
    Returns:
        Function result or default_return on error
        
    Example:
        # Replace this:
        try:
            result = s3_client.upload_file(local_path, bucket, key)
            return result
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return False
            
        # With this:
        return try_operation(s3_client.upload_file, local_path, bucket, key,
                           operation_name="S3 upload", default_return=False)
    """
    operation_name = operation_name or func.__name__
    
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if log_errors:
            logger = get_logger(__name__)
            error_msg = f"{operation_name} failed: {str(e)}"
            
            # Add context if available
            if hasattr(e, '__traceback__'):
                tb_str = ''.join(traceback.format_tb(e.__traceback__))
                logger.error(f"{error_msg}\nTraceback:\n{tb_str}")
            else:
                logger.error(error_msg)
        
        if reraise:
            raise
            
        return default_return


def safe_execute(operation_name: str, default_return: Any = None, 
                log_level: str = "error", capture_traceback: bool = True):
    """
    Decorator for unified error handling across operations.
    
    CONSOLIDATES 189+ TRY/EXCEPT PATTERNS into a single decorator.
    
    Args:
        operation_name: Name of the operation for error messages
        default_return: Value to return on error
        log_level: Logging level ('error', 'warning', 'info')
        capture_traceback: Whether to capture and log full traceback
        
    Example:
        @safe_execute("Download file", default_return=False)
        def download_file(url, path):
            # No need for try/except - decorator handles it
            response = requests.get(url)
            response.raise_for_status()
            with open(path, 'wb') as f:
                f.write(response.content)
            return True
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_msg = f"{operation_name} failed: {str(e)}"
                
                if capture_traceback:
                    tb_str = ''.join(traceback.format_tb(e.__traceback__))
                    error_msg += f"\nTraceback:\n{tb_str}"
                
                # Log at appropriate level
                log_func = getattr(logger, log_level, logger.error)
                log_func(error_msg)
                
                # Optional: Store error for monitoring
                error_handler = ErrorHandler()
                context = error_handler.create_error_context(
                    e, operation=operation_name, 
                    severity=ErrorSeverity.ERROR if log_level == "error" else ErrorSeverity.WARNING
                )
                error_handler.handle_error(context)
                
                return default_return
                
        return wrapper
    return decorator


class UnifiedRetryHandler:
    """
    Unified retry handler to eliminate 20+ duplicate retry implementations.
    
    CONSOLIDATES PATTERNS:
    - for attempt in range(max_retries): try: ... except: if attempt < max_retries - 1: time.sleep(delay)
    - Various @retry decorators with different strategies
    
    BUSINESS IMPACT: Consistent retry behavior across all operations
    """
    
    def __init__(self, max_attempts: int = 3, initial_delay: float = 1.0,
                 backoff_factor: float = 2.0, max_delay: float = 60.0,
                 retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)):
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self.retryable_exceptions = retryable_exceptions
    
    def retry_operation(self, func: Callable, *args, operation_name: str = None, **kwargs) -> Any:
        """
        Execute operation with retry logic.
        
        Example:
            retry_handler = UnifiedRetryHandler(max_attempts=3)
            result = retry_handler.retry_operation(
                s3_client.upload_file, local_path, bucket, key,
                operation_name="S3 upload"
            )
        """
        operation_name = operation_name or func.__name__
        delay = self.initial_delay
        last_exception = None
        
        for attempt in range(self.max_attempts):
            try:
                return func(*args, **kwargs)
            except self.retryable_exceptions as e:
                last_exception = e
                
                if attempt < self.max_attempts - 1:
                    logger = get_logger(__name__)
                    logger.warning(f"{operation_name} failed (attempt {attempt + 1}/{self.max_attempts}): {str(e)}")
                    
                    # Exponential backoff with jitter
                    import random
                    jittered_delay = delay * (0.5 + random.random())
                    actual_delay = min(jittered_delay, self.max_delay)
                    
                    logger.info(f"Retrying in {actual_delay:.1f} seconds...")
                    import time
                    time.sleep(actual_delay)
                    
                    delay *= self.backoff_factor
        
        # All retries exhausted
        raise RuntimeError(f"{operation_name} failed after {self.max_attempts} attempts") from last_exception
    
    def as_decorator(self, operation_name: str = None):
        """
        Use as a decorator for automatic retry.
        
        Example:
            @UnifiedRetryHandler(max_attempts=3).as_decorator("API call")
            def call_api(endpoint):
                response = requests.get(endpoint)
                response.raise_for_status()
                return response.json()
        """
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                op_name = operation_name or func.__name__
                return self.retry_operation(func, *args, operation_name=op_name, **kwargs)
            return wrapper
        return decorator


# Global retry handlers for common patterns
network_retry = UnifiedRetryHandler(
    max_attempts=3,
    initial_delay=1.0,
    retryable_exceptions=(ConnectionError, TimeoutError, OSError)
)

s3_retry = UnifiedRetryHandler(
    max_attempts=5,
    initial_delay=0.5,
    backoff_factor=2.0,
    retryable_exceptions=(Exception,)  # S3 can throw various exceptions
)


# ============================================================================
# BACKWARDS COMPATIBILITY
# ============================================================================

# For backwards compatibility, expose commonly used imports at module level
__all__ = [
    'ErrorSeverity', 'ErrorCategory', 'ErrorContext', 'ErrorHandler', 'ErrorMessages',
    'with_standard_error_handling', 'handle_file_operations', 'handle_network_operations',
    'handle_csv_operations', 'handle_download_operations', 'handle_validation_errors',
    'file_error', 'network_error', 'validation_error', 'download_error', 'csv_error', 'system_error',
    # DRY error handling decorators
    'with_traceback_logging', 'try_except_log', 'retry_with_traceback',
    # DRY ITERATION 3 - Unified patterns
    'try_operation', 'safe_execute', 'UnifiedRetryHandler', 'network_retry', 's3_retry'
]