#!/usr/bin/env python3
"""
Mass Download Command Line Interface
Phase 5.3: Create command line interface entry point

This script provides a command-line interface for the mass download feature.
It supports various input formats and processing options.

Usage:
    python mass_download_cli.py [options] <input_file>

Examples:
    python mass_download_cli.py channels.csv
    python mass_download_cli.py --max-videos 50 channels.json
    python mass_download_cli.py --no-download --output report.json channels.txt
    python mass_download_cli.py --resume job_123 channels.csv
    python mass_download_cli.py --dry-run channels.csv

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""

import sys
import os
import argparse
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

# Add the mass_download directory to Python path
script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir))
sys.path.insert(0, str(script_dir.parent))

# Import mass download modules
try:
    from mass_download.mass_coordinator import MassDownloadCoordinator
    from mass_download.input_handler import InputHandler
except ImportError:
    # Try alternative import path
    sys.path.insert(0, str(script_dir / 'mass_download'))
    try:
        from mass_coordinator import MassDownloadCoordinator
        from input_handler import InputHandler
    except ImportError as e:
        print(f"CRITICAL IMPORT ERROR: Failed to import required module: {e}")
        print("Ensure all dependencies are properly installed")
        sys.exit(1)

# Define InputValidationError if not available
try:
    from mass_download.input_handler import InputValidationError
except ImportError:
    try:
        from input_handler import InputValidationError
    except ImportError:
        # Define a simple validation error class
        class InputValidationError(Exception):
            """Input validation error."""
            pass

# Import utilities
try:
    from utils.config import get_config
    from utils.logging_config import setup_logging
except ImportError:
    # Try adding parent directory
    # The script is at mass-download/mass_download_cli.py
    # So we need to go up one level to get to the project root
    project_root = script_dir.parent
    utils_path = project_root / "utils"
    if utils_path.exists():
        sys.path.insert(0, str(utils_path.parent))
        try:
            from utils.config import get_config
            from utils.logging_config import setup_logging
        except ImportError:
            # If that fails, try direct import
            sys.path.insert(0, str(utils_path))
            try:
                from config import get_config
                from logging_config import setup_logging
            except ImportError as e:
                print(f"ERROR: Cannot import required utilities: {e}")
                print(f"Tried utils path: {utils_path}")
                print(f"sys.path: {sys.path[:5]}...")  # Show first 5 paths
                sys.exit(1)
    else:
        print("ERROR: Cannot find utils directory.")
        sys.exit(1)


def setup_argument_parser() -> argparse.ArgumentParser:
    """Set up command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Mass Download Tool - Download YouTube channels at scale",
        epilog="""
Examples:
  %(prog)s channels.csv                    # Process channels from CSV file
  %(prog)s --max-videos 50 channels.json   # Limit to 50 videos per channel
  %(prog)s --no-download channels.txt      # Extract metadata only
  %(prog)s --resume job_123 channels.csv   # Resume a previous job
  %(prog)s --dry-run channels.csv          # Preview what would be processed
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Positional arguments
    parser.add_argument(
        'input_file',
        type=str,
        help='Input file containing channel URLs (CSV, JSON, or TXT format)'
    )
    
    # Processing options
    processing_group = parser.add_argument_group('Processing Options')
    processing_group.add_argument(
        '--max-videos',
        type=int,
        default=None,
        help='Maximum videos to process per channel (default: from config)'
    )
    processing_group.add_argument(
        '--max-channels',
        type=int,
        default=None,
        help='Maximum number of channels to process'
    )
    processing_group.add_argument(
        '--concurrent-channels',
        type=int,
        default=None,
        help='Number of channels to process concurrently (default: from config)'
    )
    processing_group.add_argument(
        '--concurrent-downloads',
        type=int,
        default=None,
        help='Number of videos to download concurrently per channel (default: from config)'
    )
    
    # Download options
    download_group = parser.add_argument_group('Download Options')
    download_group.add_argument(
        '--no-download',
        action='store_true',
        help='Extract metadata only, do not download videos'
    )
    download_group.add_argument(
        '--download-mode',
        choices=['local', 'stream_to_s3'],
        default=None,
        help='Download mode (default: from config)'
    )
    download_group.add_argument(
        '--local-dir',
        type=str,
        default=None,
        help='Local directory for downloads (default: from config)'
    )
    download_group.add_argument(
        '--s3-bucket',
        type=str,
        default=None,
        help='S3 bucket for uploads (default: from config)'
    )
    
    # Resume and recovery options
    recovery_group = parser.add_argument_group('Resume and Recovery')
    recovery_group.add_argument(
        '--resume',
        type=str,
        metavar='JOB_ID',
        help='Resume a previous job by job ID'
    )
    recovery_group.add_argument(
        '--retry-failed',
        action='store_true',
        help='Retry failed items from dead letter queue'
    )
    recovery_group.add_argument(
        '--checkpoint-dir',
        type=str,
        default=None,
        help='Directory for checkpoints (default: from config)'
    )
    
    # Output options
    output_group = parser.add_argument_group('Output Options')
    output_group.add_argument(
        '--output',
        '-o',
        type=str,
        default=None,
        help='Output file for results (JSON format)'
    )
    output_group.add_argument(
        '--progress-interval',
        type=int,
        default=10,
        help='Progress update interval in seconds (default: 10)'
    )
    output_group.add_argument(
        '--quiet',
        '-q',
        action='store_true',
        help='Suppress progress output'
    )
    output_group.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    # Validation options
    validation_group = parser.add_argument_group('Validation Options')
    validation_group.add_argument(
        '--skip-validation',
        action='store_true',
        help='Skip input validation checks'
    )
    validation_group.add_argument(
        '--skip-duplicates',
        action='store_true',
        help='Skip duplicate channel checking'
    )
    validation_group.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue processing if a channel fails'
    )
    
    # Other options
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be processed without actually doing it'
    )
    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help='Path to custom config file'
    )
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 1.0.0'
    )
    
    return parser


def load_configuration(args: argparse.Namespace) -> Any:
    """Load configuration with CLI overrides."""
    # Load base configuration
    if args.config:
        # TODO: Implement custom config file loading
        print(f"Loading custom config from: {args.config}")
    
    config = get_config()
    
    # Apply CLI overrides
    overrides = {}
    
    if args.max_videos is not None:
        overrides['mass_download.max_videos_per_channel'] = args.max_videos
    
    if args.concurrent_channels is not None:
        overrides['mass_download.max_concurrent_channels'] = args.concurrent_channels
    
    if args.concurrent_downloads is not None:
        overrides['mass_download.max_concurrent_downloads'] = args.concurrent_downloads
    
    if args.no_download:
        overrides['mass_download.download_videos'] = False
    
    if args.download_mode:
        overrides['mass_download.download_mode'] = args.download_mode
    
    if args.local_dir:
        overrides['mass_download.local_download_dir'] = args.local_dir
    
    if args.s3_bucket:
        overrides['mass_download.s3_bucket'] = args.s3_bucket
    
    if args.checkpoint_dir:
        overrides['mass_download.error_recovery.checkpoint_dir'] = args.checkpoint_dir
    
    if args.continue_on_error:
        overrides['mass_download.continue_on_error'] = True
    
    # Apply overrides to config
    # Note: This is a simplified approach. In production, you'd want
    # a more sophisticated config override mechanism
    for key, value in overrides.items():
        parts = key.split('.')
        obj = config
        for part in parts[:-1]:
            if not hasattr(obj, part):
                setattr(obj, part, type('obj', (object,), {}))
            obj = getattr(obj, part)
        setattr(obj, parts[-1], value)
    
    return config


def setup_logging_cli(verbose: bool, quiet: bool):
    """Set up logging for CLI."""
    if quiet:
        log_level = logging.WARNING
    elif verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    
    # Set up basic logging
    logging.basicConfig(
        level=log_level,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Reduce noise from some modules
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('yt_dlp').setLevel(logging.WARNING)


def validate_input_file(file_path: str) -> Path:
    """Validate input file exists and is readable."""
    path = Path(file_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")
    
    if not path.is_file():
        raise ValueError(f"Input path is not a file: {file_path}")
    
    if not os.access(path, os.R_OK):
        raise PermissionError(f"Cannot read input file: {file_path}")
    
    return path


def print_summary(results: Dict[str, Any], output_file: Optional[str] = None):
    """Print processing summary."""
    print("\n" + "=" * 80)
    print("MASS DOWNLOAD SUMMARY")
    print("=" * 80)
    
    if 'job_id' in results:
        print(f"Job ID: {results['job_id']}")
    
    print(f"Start Time: {results.get('start_time', 'N/A')}")
    print(f"End Time: {results.get('end_time', 'N/A')}")
    print(f"Duration: {results.get('duration', 'N/A')}")
    
    print(f"\nChannels:")
    print(f"  Total: {results.get('total_channels', 0)}")
    print(f"  Processed: {results.get('channels_processed', 0)}")
    print(f"  Failed: {results.get('channels_failed', 0)}")
    print(f"  Skipped: {results.get('channels_skipped', 0)}")
    
    print(f"\nVideos:")
    print(f"  Total Found: {results.get('total_videos', 0)}")
    print(f"  Downloaded: {results.get('videos_downloaded', 0)}")
    print(f"  Failed: {results.get('videos_failed', 0)}")
    print(f"  Skipped: {results.get('videos_skipped', 0)}")
    
    if results.get('errors'):
        print(f"\nErrors: {len(results['errors'])}")
        for i, error in enumerate(results['errors'][:5]):
            print(f"  {i+1}. {error}")
        if len(results['errors']) > 5:
            print(f"  ... and {len(results['errors']) - 5} more")
    
    if output_file:
        print(f"\nDetailed results saved to: {output_file}")
    
    print("=" * 80)


def main():
    """Main CLI entry point."""
    # Parse arguments
    parser = setup_argument_parser()
    args = parser.parse_args()
    
    # Set up logging
    setup_logging_cli(args.verbose, args.quiet)
    logger = logging.getLogger(__name__)
    
    try:
        # Validate input file
        input_path = validate_input_file(args.input_file)
        logger.info(f"Processing input file: {input_path}")
        
        # Load configuration
        config = load_configuration(args)
        
        # Initialize components
        input_handler = InputHandler()
        coordinator = MassDownloadCoordinator(config)
        
        # Handle resume mode
        if args.resume:
            logger.info(f"Resuming job: {args.resume}")
            # TODO: Implement job resume functionality
            print("Resume functionality not yet implemented")
            return 1
        
        # Handle retry failed mode
        if args.retry_failed:
            logger.info("Retrying failed items from dead letter queue")
            results = coordinator.retry_failed_operations()
            print_summary(results, args.output)
            return 0
        
        # Parse input file
        if not args.skip_validation:
            logger.info("Validating input file...")
        
        try:
            person_channel_pairs = input_handler.parse_file(
                str(input_path),
                validate=not args.skip_validation
            )
        except Exception as e:
            # Check if it's a validation error
            if "validation" in str(e).lower() or "invalid" in str(e).lower():
                logger.error(f"Input validation failed: {e}")
            else:
                logger.error(f"Failed to parse input file: {e}")
            return 1
        
        logger.info(f"Found {len(person_channel_pairs)} channels to process")
        
        # Apply max channels limit
        if args.max_channels:
            person_channel_pairs = person_channel_pairs[:args.max_channels]
            logger.info(f"Limited to {len(person_channel_pairs)} channels")
        
        # Check for duplicates
        if not args.skip_duplicates:
            unique_urls = set()
            filtered_pairs = []
            for person, url in person_channel_pairs:
                if url not in unique_urls:
                    unique_urls.add(url)
                    filtered_pairs.append((person, url))
                else:
                    logger.warning(f"Skipping duplicate channel: {url}")
            person_channel_pairs = filtered_pairs
        
        # Dry run mode
        if args.dry_run:
            print("\nDRY RUN MODE - No actual processing will occur")
            print(f"\nWould process {len(person_channel_pairs)} channels:")
            for i, (person, url) in enumerate(person_channel_pairs[:10]):
                print(f"  {i+1}. {person.name}: {url}")
            if len(person_channel_pairs) > 10:
                print(f"  ... and {len(person_channel_pairs) - 10} more")
            return 0
        
        # Start processing
        start_time = datetime.now()
        logger.info("Starting mass download processing...")
        
        if not args.quiet:
            print(f"\nProcessing {len(person_channel_pairs)} channels...")
            print("Press Ctrl+C to stop gracefully\n")
        
        try:
            # Process channels
            results = coordinator.process_input_file(input_path)
            
            # Add timing information
            end_time = datetime.now()
            duration = end_time - start_time
            
            results['start_time'] = start_time.isoformat()
            results['end_time'] = end_time.isoformat()
            results['duration'] = str(duration)
            
            # Save results if output specified
            if args.output:
                output_path = Path(args.output)
                with open(output_path, 'w') as f:
                    json.dump(results, f, indent=2, default=str)
                logger.info(f"Results saved to: {output_path}")
            
            # Print summary
            if not args.quiet:
                print_summary(results, args.output)
            
            # Return appropriate exit code
            if results.get('channels_failed', 0) > 0:
                return 2  # Partial failure
            return 0  # Success
            
        except KeyboardInterrupt:
            logger.info("Processing interrupted by user")
            coordinator.shutdown()
            return 130  # Standard exit code for Ctrl+C
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=args.verbose)
        return 1


if __name__ == '__main__':
    sys.exit(main())