#!/usr/bin/env python3
"""
Mass Download Command Line Interface
Simplified CLI using proper package imports

This script provides a command-line interface for the mass download feature.
It supports various input formats and processing options.

Usage:
    mass-download [options] <input_file>

Examples:
    mass-download channels.csv
    mass-download --max-videos 50 channels.json
    mass-download --job-id custom_job channels.csv
    mass-download --resume custom_job channels.csv

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""

import sys
import argparse
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

# Import mass download modules using proper package imports  
from mass_download import setup_logging, get_config_path

# Optional imports (may not be available if modules have import issues)
try:
    from mass_download.input_handler import InputHandler
    from mass_download.mass_coordinator import MassDownloadCoordinator
    from mass_download.progress_monitor import ProgressMonitor
    _ADVANCED_FEATURES = True
    print("Info: Advanced mass download features are available")
except ImportError as e:
    print(f"Warning: Advanced features not available: {e}")
    print("CLI will run in basic mode with limited functionality")
    _ADVANCED_FEATURES = False
    InputHandler = None
    MassDownloadCoordinator = None  
    ProgressMonitor = None


def setup_argument_parser() -> argparse.ArgumentParser:
    """Set up command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Mass Download Tool - Download YouTube channels at scale",
        epilog="""
Examples:
  %(prog)s channels.csv                    # Process channels from CSV file
  %(prog)s --max-videos 50 channels.json  # Limit videos per channel
  %(prog)s --job-id my_job channels.txt   # Use custom job ID
  %(prog)s --resume my_job channels.csv   # Resume interrupted job
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Positional arguments
    parser.add_argument(
        'input_file',
        help='Input file containing channel information (CSV, JSON, or TXT format)'
    )

    # Optional arguments
    parser.add_argument(
        '--job-id',
        type=str,
        help='Unique identifier for this download job (default: auto-generated)'
    )

    parser.add_argument(
        '--max-videos',
        type=int,
        help='Maximum number of videos to download per channel (overrides config)'
    )

    parser.add_argument(
        '--max-channels',
        type=int,
        help='Maximum number of concurrent channels to process (overrides config)'
    )

    parser.add_argument(
        '--max-downloads',
        type=int,
        help='Maximum number of concurrent downloads per channel (overrides config)'
    )

    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume a previously interrupted job (requires --job-id)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate input and configuration without starting downloads'
    )

    parser.add_argument(
        '--output',
        type=str,
        help='Output file for results/reports (JSON format)'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )

    parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration file (default: config/config.yaml)'
    )

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 1.0.0'
    )

    return parser


def validate_arguments(args: argparse.Namespace) -> None:
    """Validate command line arguments."""
    # Check if input file exists
    input_path = Path(args.input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {args.input_file}")

    # Validate input file format
    allowed_extensions = {'.csv', '.json', '.txt'}
    if input_path.suffix.lower() not in allowed_extensions:
        raise ValueError(f"Unsupported file format. Allowed: {', '.join(allowed_extensions)}")

    # Validate resume option
    if args.resume and not args.job_id:
        raise ValueError("--resume requires --job-id to specify which job to resume")

    # Validate numeric arguments
    if args.max_videos is not None and args.max_videos < 1:
        raise ValueError("--max-videos must be a positive integer")

    if args.max_channels is not None and args.max_channels < 1:
        raise ValueError("--max-channels must be a positive integer")

    if args.max_downloads is not None and args.max_downloads < 1:
        raise ValueError("--max-downloads must be a positive integer")


def setup_logging_from_args(args: argparse.Namespace) -> logging.Logger:
    """Set up logging based on command line arguments."""
    logger = setup_logging()
    
    # Set log level from arguments
    log_level = getattr(logging, args.log_level.upper())
    logger.setLevel(log_level)
    
    # Also set root logger level
    logging.getLogger().setLevel(log_level)
    
    return logger


def load_configuration(args: argparse.Namespace) -> Dict[str, Any]:
    """Load configuration from file."""
    try:
        import yaml
        
        # Determine config file path
        if args.config:
            config_path = Path(args.config)
        else:
            config_path = get_config_path()
            
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
        # Load YAML configuration
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            
        # Override config with command line arguments
        if 'mass_download' not in config:
            config['mass_download'] = {}
            
        mass_config = config['mass_download']
        
        if args.max_videos is not None:
            mass_config['max_videos_per_channel'] = args.max_videos
            
        if args.max_channels is not None:
            mass_config['max_concurrent_channels'] = args.max_channels
            
        if args.max_downloads is not None:
            mass_config['max_concurrent_downloads'] = args.max_downloads
            
        return config
        
    except Exception as e:
        raise RuntimeError(f"Failed to load configuration: {e}")


def main() -> int:
    """Main CLI entry point."""
    try:
        # Parse command line arguments
        parser = setup_argument_parser()
        args = parser.parse_args()

        # Validate arguments
        validate_arguments(args)

        # Set up logging
        logger = setup_logging_from_args(args)
        logger.info(f"Starting mass download CLI - version 1.0.0")
        logger.info(f"Input file: {args.input_file}")
        
        # Load configuration
        config = load_configuration(args)
        logger.info("Configuration loaded successfully")

        # Generate job ID if not provided
        if not args.job_id:
            args.job_id = f"mass_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Job ID: {args.job_id}")

        # Parse input file for validation
        if not _ADVANCED_FEATURES:
            logger.error("Input parsing functionality not available due to import issues")
            logger.error("Cannot proceed without InputHandler - please fix import issues")
            return 1
            
        logger.info("Parsing input file...")
        input_handler = InputHandler()
        channels = input_handler.parse_input_file(args.input_file)
        logger.info(f"Successfully parsed {len(channels)} channels")

        # Dry run mode
        if args.dry_run:
            logger.info("DRY RUN MODE - Validation complete, no downloads will be performed")
            if args.output:
                dry_run_results = {
                    "job_id": args.job_id,
                    "input_file": args.input_file,
                    "channels_found": len(channels),
                    "channels": [{"name": ch.name, "url": ch.channel_url} for ch in channels],
                    "config": config.get('mass_download', {}),
                    "dry_run": True,
                    "timestamp": datetime.now().isoformat()
                }
                with open(args.output, 'w') as f:
                    json.dump(dry_run_results, f, indent=2)
                logger.info(f"Dry run results saved to: {args.output}")
            return 0

        # Check if advanced features are available
        if not _ADVANCED_FEATURES:
            logger.error("Mass download functionality not available due to import issues")
            logger.error("Please check that all dependencies are installed and import issues are resolved")
            return 1

        # Initialize mass coordinator
        logger.info("Initializing mass download coordinator...")
        coordinator = MassDownloadCoordinator(config=config)

        # Start processing
        logger.info("Starting mass download processing...")
        
        # First, parse and validate the input file
        person_channel_pairs = coordinator.process_input_file(args.input_file, job_id=args.job_id)
        logger.info(f"Found {len(person_channel_pairs)} channels to process")
        
        # Now actually download the channels
        logger.info("Starting downloads...")
        results = coordinator.process_channels_with_downloads(person_channel_pairs)
        
        # Output results
        if args.output:
            output_data = {
                "job_id": args.job_id,
                "results": results,
                "timestamp": datetime.now().isoformat()
            }
            with open(args.output, 'w') as f:
                json.dump(output_data, f, indent=2)
            logger.info(f"Results saved to: {args.output}")
        
        logger.info("Mass download completed successfully")
        return 0

    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        return 1
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Mass download failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())