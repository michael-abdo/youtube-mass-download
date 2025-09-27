#!/usr/bin/env python3
import os
import sys
from pathlib import Path

def clean_transcript_files(directory="youtube_downloads", dry_run=False):
    """
    Clean up transcript files, keeping only one transcript per video.
    Priority: .en-orig.vtt > .en.vtt > other language-specific files
    
    Args:
        directory: Directory containing the transcript files
        dry_run: If True, show what would be done without actually removing files
    """
    directory_path = Path(directory)
    if not directory_path.exists() or not directory_path.is_dir():
        print(f"Error: Directory '{directory}' does not exist")
        return False
    
    # Group files by video ID
    video_transcripts = {}
    total_files = 0
    
    # Find all VTT files
    vtt_files = list(directory_path.glob("*.vtt"))
    
    for file_path in vtt_files:
        # Extract video ID (everything before the first dot)
        video_id = file_path.stem.split('.')[0]
        
        # Group files by video ID
        if video_id not in video_transcripts:
            video_transcripts[video_id] = []
        
        video_transcripts[video_id].append(file_path)
        total_files += 1
    
    print(f"Found {total_files} transcript files for {len(video_transcripts)} videos")
    
    # Process each video's transcripts
    removed_count = 0
    for video_id, files in video_transcripts.items():
        if len(files) <= 1:
            print(f"Skipping {video_id}: Only one transcript file")
            continue
        
        # Find the best transcript to keep based on priority
        keep_file = None
        
        # First priority: .en-orig.vtt
        for file in files:
            if ".en-orig.vtt" in str(file):
                keep_file = file
                break
        
        # Second priority: .en.vtt
        if not keep_file:
            for file in files:
                if str(file).endswith(".en.vtt"):
                    keep_file = file
                    break
        
        # Last resort: keep the first file
        if not keep_file and files:
            keep_file = files[0]
        
        # Remove all other files
        for file in files:
            if file != keep_file:
                if dry_run:
                    print(f"Would remove: {file.name} (keeping {keep_file.name})")
                    removed_count += 1
                else:
                    try:
                        file.unlink()
                        print(f"Removed: {file.name} (keeping {keep_file.name})")
                        removed_count += 1
                    except Exception as e:
                        print(f"Error removing {file.name}: {str(e)}")
    
    print(f"\nSummary: {'Would remove' if dry_run else 'Removed'} {removed_count} of {total_files} transcript files")
    return True

def main():
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Clean up duplicate transcript files")
    parser.add_argument("--directory", default="youtube_downloads", 
                      help="Directory containing transcript files (default: youtube_downloads)")
    parser.add_argument("--dry-run", action="store_true",
                      help="Show what would be done without actually removing files")
    
    args = parser.parse_args()
    
    # Run the cleanup
    success = clean_transcript_files(args.directory, args.dry_run)
    
    # Return status code
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())