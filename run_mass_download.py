#!/usr/bin/env python3
"""
Simple runner script for mass download feature.
This script sets up the environment and runs the mass download process.

Usage:
    python run_mass_download.py <input_file> [options]

Examples:
    python run_mass_download.py channels.csv
    python run_mass_download.py channels.json --max-videos 50
    python run_mass_download.py channels.txt --dry-run
"""

import sys
import os
from pathlib import Path

# Add necessary paths
script_dir = Path(__file__).parent
project_root = script_dir.parent

# Add all necessary paths to sys.path
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(script_dir / "mass_download"))
sys.path.insert(0, str(project_root / "utils"))

# Initialize logging first
from init_logging import initialize_mass_download_logging

# Now we can import everything
def main():
    """Main entry point."""
    # Initialize logging based on debug flag
    debug_mode = '--debug' in sys.argv or '-d' in sys.argv
    initialize_mass_download_logging(debug=debug_mode)
    
    from mass_coordinator import MassDownloadCoordinator
    from input_handler import InputHandler
    
    # Simple argument parsing
    if len(sys.argv) < 2 or sys.argv[1] in ['-h', '--help']:
        print(__doc__)
        return 0
    
    input_file = sys.argv[1]
    
    # Check if file exists
    if not Path(input_file).exists():
        print(f"ERROR: Input file not found: {input_file}")
        return 1
    
    # Get options from arguments
    dry_run = '--dry-run' in sys.argv
    max_videos = None
    
    # Parse max-videos option
    for i, arg in enumerate(sys.argv):
        if arg == '--max-videos' and i + 1 < len(sys.argv):
            try:
                max_videos = int(sys.argv[i + 1])
            except ValueError:
                print(f"ERROR: Invalid value for --max-videos: {sys.argv[i + 1]}")
                return 1
    
    try:
        # Initialize components
        print(f"Loading configuration...")
        from utils.config import get_config, get_s3_bucket
        config = get_config()
        
        # Add bucket_name attribute if missing (compatibility fix)
        if not hasattr(config, 'bucket_name'):
            config.bucket_name = config.get('mass_download.s3_bucket') or get_s3_bucket()
        
        # Add other missing attributes
        if not hasattr(config, 'region'):
            config.region = config.get('aws_region', 'us-west-2')
        if not hasattr(config, 'downloads_dir'):
            config.downloads_dir = config.get('mass_download.local_download_dir', 'mass_downloads')
        if not hasattr(config, 'csv_file'):
            config.csv_file = config.get('paths.output_csv', 'outputs/output.csv')
        
        # Override config with command line options
        if max_videos is not None:
            # This is a simplified approach - in production you'd want proper config override
            pass
        
        print(f"Initializing mass download coordinator...")
        coordinator = MassDownloadCoordinator(config)
        
        print(f"Parsing input file: {input_file}")
        input_handler = InputHandler()
        person_channel_pairs = input_handler.parse_file(input_file)
        
        print(f"Found {len(person_channel_pairs)} channels to process")
        
        if dry_run:
            print("\nDRY RUN MODE - Would process:")
            for i, (person, url) in enumerate(person_channel_pairs[:5]):
                print(f"  {i+1}. {person.name}: {url}")
            if len(person_channel_pairs) > 5:
                print(f"  ... and {len(person_channel_pairs) - 5} more")
            return 0
        
        # Process the channels
        print(f"\nStarting mass download processing...")
        print("Press Ctrl+C to stop gracefully\n")
        
        results = coordinator.process_input_file(Path(input_file))
        
        # Print summary
        print("\n" + "=" * 80)
        print("MASS DOWNLOAD COMPLETE")
        print("=" * 80)
        print(f"Total channels: {results.get('total_channels', 0)}")
        print(f"Processed: {results.get('channels_processed', 0)}")
        print(f"Failed: {results.get('channels_failed', 0)}")
        print(f"Total videos: {results.get('total_videos', 0)}")
        print("=" * 80)
        
        return 0 if results.get('channels_failed', 0) == 0 else 1
        
    except KeyboardInterrupt:
        print("\nProcessing interrupted by user")
        return 130
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())