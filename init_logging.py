#!/usr/bin/env python3
"""
Initialize logging for mass download feature.
This script ensures logging is properly configured before running the feature.
"""
import sys
from pathlib import Path

# Add mass_download to path
sys.path.insert(0, str(Path(__file__).parent / "mass_download"))

from mass_download.logging_setup import setup_mass_download_logging

def initialize_mass_download_logging(debug: bool = False):
    """
    Initialize the mass download logging system.
    
    Args:
        debug: Enable debug logging if True
    """
    log_level = "DEBUG" if debug else "INFO"
    file_level = "DEBUG"  # Always keep detailed logs in files
    
    # Set up the logging
    setup_mass_download_logging(
        log_level=log_level,
        console_level=log_level,
        file_level=file_level
    )

if __name__ == "__main__":
    # Check for debug flag
    debug_mode = "--debug" in sys.argv or "-d" in sys.argv
    initialize_mass_download_logging(debug=debug_mode)