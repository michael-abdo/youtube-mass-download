#!/usr/bin/env python3
"""
Logging setup for mass download feature.
Self-contained logging configuration without external dependencies.
"""
import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


def setup_mass_download_logging(
    log_dir: Optional[Path] = None,
    log_level: str = "INFO",
    console_level: str = "INFO",
    file_level: str = "DEBUG"
) -> None:
    """
    Set up comprehensive logging for the mass download feature.
    
    Args:
        log_dir: Directory for log files (defaults to logs/mass_download)
        log_level: Root logging level
        console_level: Console handler level
        file_level: File handler level
    """
    # Default log directory
    if log_dir is None:
        log_dir = Path("logs/mass_download")
    
    # Ensure log directory exists
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Set up root logger for mass download
    root_logger = logging.getLogger("mass_download")
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler with clean format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, console_level.upper()))
    console_formatter = logging.Formatter(
        '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # Main log file with rotation
    main_log_file = log_dir / "mass_download.log"
    main_file_handler = logging.handlers.RotatingFileHandler(
        str(main_log_file),
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s'
    )
    main_file_handler.setFormatter(file_formatter)
    main_file_handler.setLevel(getattr(logging, file_level.upper()))
    root_logger.addHandler(main_file_handler)
    
    # Error-only log file
    error_log_file = log_dir / "mass_download_errors.log"
    error_handler = logging.FileHandler(str(error_log_file))
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - [%(levelname)s] - [%(filename)s:%(lineno)d] - %(message)s'
    )
    error_handler.setFormatter(error_formatter)
    root_logger.addHandler(error_handler)
    
    # Suppress noisy third-party libraries
    noisy_libraries = [
        "urllib3",
        "requests", 
        "botocore",
        "s3transfer",
        "googleapiclient",
        "yt_dlp",  # yt-dlp can be very verbose
        "urllib3.connectionpool"
    ]
    for lib in noisy_libraries:
        logging.getLogger(lib).setLevel(logging.WARNING)
    
    # Log startup
    root_logger.info("="*80)
    root_logger.info("Mass Download Logging Initialized")
    root_logger.info(f"Log directory: {log_dir.absolute()}")
    root_logger.info(f"Console level: {console_level}")
    root_logger.info(f"File level: {file_level}")
    root_logger.info("="*80)


def get_mass_download_logger(module_name: str) -> logging.Logger:
    """
    Get a logger for a mass download module.
    
    Args:
        module_name: Module name (e.g., 'mass_coordinator', 'input_handler')
        
    Returns:
        Logger instance
    """
    # Extract just the module name if it's a full path
    if '.' in module_name:
        module_name = module_name.split('.')[-1]
    
    # Create logger with mass_download namespace
    logger_name = f"mass_download.{module_name}"
    return logging.getLogger(logger_name)


def log_operation_start(logger: logging.Logger, operation: str, **kwargs) -> None:
    """
    Log the start of a major operation.
    
    Args:
        logger: Logger instance
        operation: Operation name
        **kwargs: Additional context to log
    """
    logger.info(f"Starting {operation}")
    if kwargs:
        for key, value in kwargs.items():
            logger.debug(f"  {key}: {value}")


def log_operation_complete(
    logger: logging.Logger, 
    operation: str, 
    success: bool = True,
    duration: Optional[float] = None,
    **kwargs
) -> None:
    """
    Log the completion of a major operation.
    
    Args:
        logger: Logger instance
        operation: Operation name
        success: Whether operation succeeded
        duration: Operation duration in seconds
        **kwargs: Additional results to log
    """
    status = "completed successfully" if success else "failed"
    duration_str = f" in {duration:.2f}s" if duration else ""
    
    log_func = logger.info if success else logger.error
    log_func(f"{operation} {status}{duration_str}")
    
    if kwargs:
        for key, value in kwargs.items():
            logger.debug(f"  {key}: {value}")


def log_progress(
    logger: logging.Logger,
    current: int,
    total: int,
    operation: str = "Processing",
    interval: int = 10
) -> None:
    """
    Log progress at regular intervals.
    
    Args:
        logger: Logger instance
        current: Current progress
        total: Total items
        operation: Operation name
        interval: Log every N items
    """
    if current % interval == 0 or current == total:
        percent = (current / total * 100) if total > 0 else 0
        logger.info(f"{operation}: {current}/{total} ({percent:.1f}%)")


class StructuredLogger:
    """
    Structured logging wrapper for consistent log formatting.
    """
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.context = {}
    
    def set_context(self, **kwargs):
        """Set persistent context for all log messages."""
        self.context.update(kwargs)
    
    def clear_context(self):
        """Clear the persistent context."""
        self.context.clear()
    
    def _format_message(self, message: str, **kwargs) -> str:
        """Format message with context."""
        # Merge persistent context with call-specific context
        all_context = {**self.context, **kwargs}
        
        if not all_context:
            return message
        
        # Format context as key=value pairs
        context_str = " ".join(f"{k}={v}" for k, v in all_context.items())
        return f"{message} [{context_str}]"
    
    def debug(self, message: str, **kwargs):
        self.logger.debug(self._format_message(message, **kwargs))
    
    def info(self, message: str, **kwargs):
        self.logger.info(self._format_message(message, **kwargs))
    
    def warning(self, message: str, **kwargs):
        self.logger.warning(self._format_message(message, **kwargs))
    
    def error(self, message: str, **kwargs):
        self.logger.error(self._format_message(message, **kwargs))
    
    def exception(self, message: str, **kwargs):
        self.logger.exception(self._format_message(message, **kwargs))


def create_operation_logger(operation_name: str, **context) -> StructuredLogger:
    """
    Create a structured logger for a specific operation.
    
    Args:
        operation_name: Name of the operation
        **context: Initial context for the logger
        
    Returns:
        StructuredLogger instance
    """
    logger = get_mass_download_logger(operation_name)
    structured = StructuredLogger(logger)
    structured.set_context(**context)
    return structured


# Example usage in modules:
"""
# In mass_coordinator.py:
from mass_download.logging_setup import get_mass_download_logger, log_operation_start

logger = get_mass_download_logger(__name__)

def process_channels(self):
    log_operation_start(logger, "channel processing", 
                       total_channels=len(self.channels))
    # ... processing logic ...
"""