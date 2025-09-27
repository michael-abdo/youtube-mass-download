#!/usr/bin/env python3
import os
import sys
from pathlib import Path

def fix_transcript_extensions(directory="youtube_downloads", old_ext="mp4", new_ext="vtt"):
    """
    Fixes transcript files with incorrect extensions.
    
    Args:
        directory: Directory containing the transcript files
        old_ext: Current incorrect extension
        new_ext: Desired correct extension
    """
    directory_path = Path(directory)
    if not directory_path.exists() or not directory_path.is_dir():
        print(f"Error: Directory '{directory}' does not exist")
        return False
    
    # Find all transcript files with the old extension
    transcript_files = list(directory_path.glob(f"*_transcript.{old_ext}"))
    if not transcript_files:
        print(f"No transcript files with extension .{old_ext} found in {directory}")
        return False
    
    print(f"Found {len(transcript_files)} transcript files with incorrect extension .{old_ext}")
    
    # Rename all transcript files
    for file_path in transcript_files:
        new_path = file_path.with_suffix(f".{new_ext}")
        
        # Check if the target file already exists
        if new_path.exists():
            print(f"Skipping {file_path.name}: Target file {new_path.name} already exists")
            continue
        
        try:
            file_path.rename(new_path)
            print(f"Renamed: {file_path.name} -> {new_path.name}")
        except Exception as e:
            print(f"Error renaming {file_path.name}: {str(e)}")
    
    # Count successful renames
    remaining_files = list(directory_path.glob(f"*_transcript.{old_ext}"))
    fixed_count = len(transcript_files) - len(remaining_files)
    
    print(f"\nSummary: Fixed {fixed_count} of {len(transcript_files)} transcript files")
    return True

def main():
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Fix transcript file extensions")
    parser.add_argument("--directory", default="youtube_downloads", 
                        help="Directory containing transcript files (default: youtube_downloads)")
    parser.add_argument("--old-ext", default="mp4", 
                        help="Current incorrect extension (default: mp4)")
    parser.add_argument("--new-ext", default="vtt", 
                        help="Desired correct extension (default: vtt)")
    
    args = parser.parse_args()
    
    # Run the fix
    success = fix_transcript_extensions(args.directory, args.old_ext, args.new_ext)
    
    # Return status code
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())