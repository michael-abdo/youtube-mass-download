#!/usr/bin/env python3
"""
Download all Google Drive files and YouTube videos from outputs/output.csv.
"""
import csv
import os
import sys
import time
import argparse
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import download modules
from download_drive import process_drive_url
from download_youtube import download_video

def download_all_media(csv_path, start_row=0, max_rows=None, delay=2, 
                      video_resolution="720", skip_existing=True):
    """
    Download all Google Drive files and YouTube videos from the CSV file.
    
    Args:
        csv_path: Path to the CSV file
        start_row: Row number to start processing from (0-based, after header)
        max_rows: Maximum number of rows to process (None for all)
        delay: Delay between downloads to avoid rate limiting
        video_resolution: Resolution for YouTube videos
        skip_existing: Skip already downloaded files
    """
    # Stats tracking
    processed = 0
    drive_downloaded = 0
    youtube_downloaded = 0
    errors = 0
    
    with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for i, row in enumerate(reader):
            # Skip rows before start_row
            if i < start_row:
                continue
            
            # Stop if we've processed max_rows
            if max_rows is not None and processed >= max_rows:
                break
                
            # Process the row
            name = row.get('name', f'Row {i+1}')
            print(f"\n{'='*80}\nProcessing row {i+1}: {name}")
            
            # Download Google Drive files
            drive_links = row.get('google_drive', '').split('|') if row.get('google_drive') else []
            drive_links = [link for link in drive_links if link and link not in ['-', "'-", 'None', 'nan', '']]
            
            if drive_links:
                print(f"Found {len(drive_links)} Google Drive links")
                for j, drive_link in enumerate(drive_links):
                    print(f"\nDownloading Drive file {j+1}/{len(drive_links)}: {drive_link}")
                    try:
                        file_path, _ = process_drive_url(drive_link, save_metadata_flag=True)
                        if file_path:
                            drive_downloaded += 1
                            print(f"Successfully downloaded: {file_path}")
                        else:
                            print(f"Failed to download: {drive_link}")
                            errors += 1
                            
                        # Add delay to avoid rate limiting
                        if delay > 0 and j < len(drive_links) - 1:
                            time.sleep(delay)
                    except Exception as e:
                        print(f"Error downloading Drive file: {str(e)}")
                        errors += 1
            else:
                print("No Google Drive links found")
                
            # Download YouTube videos
            youtube_links = row.get('youtube_playlist', '').split('|') if row.get('youtube_playlist') else []
            youtube_links = [link for link in youtube_links if link and link not in ['-', "'-", 'None', 'nan', '']]
            
            if youtube_links:
                print(f"\nFound {len(youtube_links)} YouTube links")
                for j, yt_link in enumerate(youtube_links):
                    print(f"\nDownloading YouTube video {j+1}/{len(youtube_links)}: {yt_link}")
                    try:
                        video_file, transcript_file = download_video(
                            yt_link, 
                            transcript_only=False,
                            resolution=video_resolution
                        )
                        if video_file:
                            youtube_downloaded += 1
                            print(f"Successfully downloaded: {video_file}")
                        else:
                            print(f"Failed to download video: {yt_link}")
                            errors += 1
                            
                        # Add delay to avoid rate limiting
                        if delay > 0 and j < len(youtube_links) - 1:
                            time.sleep(delay)
                    except Exception as e:
                        print(f"Error downloading YouTube video: {str(e)}")
                        errors += 1
            else:
                print("No YouTube links found")
                
            processed += 1
            print(f"Completed row {i+1}")
            
            # Add delay between rows
            if delay > 0 and processed < (max_rows if max_rows else float('inf')):
                time.sleep(delay)
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"Download Summary:")
    print(f"  Processed rows: {processed}")
    print(f"  Drive files downloaded: {drive_downloaded}")
    print(f"  YouTube videos downloaded: {youtube_downloaded}")
    print(f"  Errors: {errors}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download all Google Drive files and YouTube videos from CSV')
    parser.add_argument('--csv', default='/Users/Mike/ops_typing_log/ongoing_clients/outputs/output.csv',
                      help='Path to CSV file (default: outputs/output.csv)')
    parser.add_argument('--start', type=int, default=0,
                      help='Row number to start processing from (0-based, after header)')
    parser.add_argument('--max', type=int, default=None,
                      help='Maximum number of rows to process')
    parser.add_argument('--delay', type=float, default=2.0,
                      help='Delay in seconds between downloads')
    parser.add_argument('--resolution', default='720',
                      help='Resolution for YouTube videos (default: 720)')
    parser.add_argument('--skip-existing', action='store_true',
                      help='Skip already downloaded files')
    
    args = parser.parse_args()
    
    print(f"Starting download of all media from: {args.csv}")
    print(f"Starting from row: {args.start}")
    print(f"Max rows to process: {args.max if args.max else 'all'}")
    print(f"Delay between downloads: {args.delay} seconds")
    print(f"YouTube resolution: {args.resolution}p")
    
    # Check if yt-dlp is installed
    try:
        import subprocess
        subprocess.run(["yt-dlp", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: yt-dlp is not installed. Install it with: pip install yt-dlp")
        sys.exit(1)
    
    download_all_media(
        args.csv, 
        args.start, 
        args.max, 
        args.delay, 
        args.resolution,
        args.skip_existing
    )