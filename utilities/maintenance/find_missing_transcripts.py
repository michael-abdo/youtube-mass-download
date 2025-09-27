#!/usr/bin/env python3
import os
import sys
from pathlib import Path

def find_missing_transcripts(directory="youtube_downloads"):
    """
    Find videos that are missing transcript files.
    
    Args:
        directory: Directory containing the video and transcript files
    """
    directory_path = Path(directory)
    if not directory_path.exists() or not directory_path.is_dir():
        print(f"Error: Directory '{directory}' does not exist")
        return False
    
    # Find all MP4 files (videos)
    video_files = list(directory_path.glob("*.mp4"))
    video_ids = {f.stem for f in video_files}
    
    # Find all VTT files (transcripts)
    transcript_files = list(directory_path.glob("*.vtt"))
    
    # Extract video IDs from transcript filenames
    transcript_ids = set()
    for f in transcript_files:
        # Handle different transcript file naming patterns
        video_id = f.stem.split('.')[0]
        transcript_ids.add(video_id)
    
    # Find videos without transcripts
    missing_transcripts = video_ids - transcript_ids
    
    # Find transcripts without videos (unusual but possible)
    extra_transcripts = transcript_ids - video_ids
    
    print(f"Found {len(video_files)} video files")
    print(f"Found {len(transcript_ids)} videos with transcripts")
    
    if missing_transcripts:
        print("\nVideos missing transcripts:")
        for video_id in sorted(missing_transcripts):
            print(f"  {video_id}.mp4")
    else:
        print("\nAll videos have transcript files")
    
    if extra_transcripts:
        print("\nTranscripts without corresponding video files:")
        for video_id in sorted(extra_transcripts):
            transcript_file = next((f for f in transcript_files if f.stem.split('.')[0] == video_id), None)
            if transcript_file:
                print(f"  {transcript_file.name}")
    
    return True

def main():
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Find videos missing transcript files")
    parser.add_argument("--directory", default="youtube_downloads", 
                      help="Directory containing video and transcript files (default: youtube_downloads)")
    
    args = parser.parse_args()
    
    # Run the search
    success = find_missing_transcripts(args.directory)
    
    # Return status code
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())