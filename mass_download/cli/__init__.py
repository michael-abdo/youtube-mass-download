"""
Mass Download CLI Package

Command-line interface for the mass YouTube channel download system.
"""

__version__ = "1.0.0"

# CLI module exports
__all__ = [
    "main"
]

# Import main CLI function
try:
    from .main import main
except ImportError:
    # Allow package to load even if main isn't available yet
    pass