#!/usr/bin/env python3
"""
Mass Download Feature Module

Provides YouTube channel bulk download functionality with:
- Person/Video database schema
- Channel discovery and enumeration  
- Mass download coordination
- S3 streaming integration
"""

__version__ = "1.0.0"
__author__ = "Mass Download Team"

# Fail-fast imports - validate critical dependencies immediately
try:
    import sys
    import os
    from pathlib import Path
    
    # Add parent directory to path for relative imports
    current_dir = Path(__file__).parent
    parent_dir = current_dir.parent
    if str(parent_dir) not in sys.path:
        sys.path.insert(0, str(parent_dir))
        
except ImportError as e:
    raise ImportError(
        f"CRITICAL: Failed to import required system modules. "
        f"Mass download feature cannot initialize. Error: {e}"
    ) from e

# Validate Python version (fail-fast)
if sys.version_info < (3, 8):
    raise RuntimeError(
        f"CRITICAL: Python 3.8+ required for mass download feature. "
        f"Current version: {sys.version_info.major}.{sys.version_info.minor}"
    )

# Key class imports for package-level access
try:
    from .channel_discovery import YouTubeChannelDiscovery, ChannelInfo
    from .input_handler import InputHandler
    from .database_schema import PersonRecord, VideoRecord
    
    # Mark import success for core modules
    _IMPORTS_SUCCESSFUL = True
    
except ImportError as e:
    # Allow package to load even if some imports fail (graceful degradation)
    _IMPORTS_SUCCESSFUL = False
    print(f"Warning: Some mass_download imports failed: {e}")

# Try to import additional modules that may have issues
try:
    from .mass_coordinator import MassDownloadCoordinator
    from .progress_monitor import ProgressMonitor  
    from .database_operations_ext import MassDownloadDatabaseOperations
except ImportError as e:
    print(f"Info: Advanced modules not available: {e}")

# Module exports - key classes and functions
__all__ = [
    "YouTubeChannelDiscovery",
    "InputHandler", 
    "ChannelInfo",
    "MassDownloadCoordinator",
    "ProgressMonitor",
    "MassDownloadDatabaseOperations",
    "Person",
    "Video",
    "validate_environment",
    "get_config_path",
    "setup_logging"
]

# Validation flag for testing
_VALIDATION_PASSED = True

def get_config_path():
    """Get path to configuration file."""
    config_path = current_dir.parent / "config" / "config.yaml"
    return config_path

def setup_logging():
    """Setup basic logging configuration."""
    import logging
    
    # Basic logging setup
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create mass_download logger
    logger = logging.getLogger('mass_download')
    logger.setLevel(logging.INFO)
    
    return logger

def validate_environment():
    """
    Fail-fast environment validation.
    
    Raises:
        RuntimeError: If environment validation fails
    """
    global _VALIDATION_PASSED
    
    try:
        # Check required directories exist
        required_dirs = ['utils', 'config', 'docs']
        for dir_name in required_dirs:
            dir_path = parent_dir / dir_name
            if not dir_path.exists():
                raise FileNotFoundError(f"Required directory missing: {dir_path}")
        
        _VALIDATION_PASSED = True
        return True
        
    except Exception as e:
        _VALIDATION_PASSED = False
        raise RuntimeError(
            f"CRITICAL: Environment validation failed. "
            f"Mass download feature cannot operate safely. Error: {e}"
        ) from e

# Run validation on import (fail-fast)
validate_environment()