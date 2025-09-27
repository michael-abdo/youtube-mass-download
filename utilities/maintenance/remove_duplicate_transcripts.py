#!/usr/bin/env python3
import os
import sys
from pathlib import Path

def remove_duplicate_transcripts(directory="youtube_downloads", dry_run=False):
    """
    Removes duplicate transcript files by deleting the *_transcript.vtt files
    when a corresponding *.en-orig.vtt file exists with identical content.
    
    Args:
        directory: Directory containing the transcript files
        dry_run: If True, show what would be done without actually removing files
    """
    directory_path = Path(directory)
    if not directory_path.exists() or not directory_path.is_dir():
        print(f"Error: Directory '{directory}' does not exist")
        return False
    
    # Find all transcript files with _transcript.vtt suffix
    transcript_files = list(directory_path.glob("*_transcript.vtt"))
    
    # No _transcript.vtt files found
    if not transcript_files:
        print(f"No *_transcript.vtt files found in {directory}")
        return True
    
    print(f"Found {len(transcript_files)} *_transcript.vtt files")
    
    # Process all *_transcript.vtt files
    removed_count = 0
    for file_path in transcript_files:
        try:
            # Get video ID from filename (before _transcript)
            video_id = file_path.stem.split('_')[0]
            
            # Check if there's a corresponding .en-orig.vtt file
            en_orig_file = directory_path / f"{video_id}.en-orig.vtt"
            
            if en_orig_file.exists():
                # Compare file sizes to verify they're likely duplicates
                # For more thorough check, could compare file content
                if file_path.stat().st_size == en_orig_file.stat().st_size:
                    if dry_run:
                        print(f"Would remove: {file_path.name} (duplicate of {en_orig_file.name})")
                        removed_count += 1
                    else:
                        file_path.unlink()
                        print(f"Removed duplicate: {file_path.name} (keeping {en_orig_file.name})")
                        removed_count += 1
                else:
                    print(f"Skipping {file_path.name}: Size differs from {en_orig_file.name}")
            else:
                print(f"No matching .en-orig.vtt file found for {file_path.name}")
        except Exception as e:
            print(f"Error processing {file_path.name}: {str(e)}")
    
    print(f"\nSummary: {'Would remove' if dry_run else 'Removed'} {removed_count} of {len(transcript_files)} duplicate transcript files")
    return True

def main():
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Remove duplicate transcript files")
    parser.add_argument("--directory", default="youtube_downloads", 
                      help="Directory containing transcript files (default: youtube_downloads)")
    parser.add_argument("--dry-run", action="store_true",
                      help="Show what would be done without actually removing files")
    
    args = parser.parse_args()
    
    # Run the cleanup
    success = remove_duplicate_transcripts(args.directory, args.dry_run)
    
    # Return status code
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())