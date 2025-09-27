#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import csv
from pathlib import Path

# Add parent directory to path to access utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import pipeline_run, get_pipeline_logger
from utils.logging_config import get_logger
from utils.parallel_processor import parallel_download_youtube_videos
from utils.config import get_config
from utils.csv_tracker import ensure_tracking_columns, get_pending_downloads, update_csv_download_status
from utils.row_context import create_row_context_from_csv_row
from utils.download_youtube import download_youtube_with_context
from utils.download_drive import download_drive_with_context

# Get configuration and set CSV field size limit
config = get_config()
csv.field_size_limit(config.get('file_processing.max_csv_field_size', sys.maxsize))

def run_process(command, description=None, component='main', logger=None):
    """Run a process with the given command and print its output in real-time"""
    # If logger is provided, use it; otherwise fall back to print
    if logger:
        return logger.log_subprocess(command, description, component)
    
    # Original implementation for when logger is not available
    if description:
        print(f"\n{'=' * 80}\n{description}\n{'=' * 80}")
    
    # Get the virtual environment's Python interpreter path
    venv_python = sys.executable
    
    # If command is a list starting with sys.executable or 'python', replace it with venv_python
    if isinstance(command, list) and (command[0] == sys.executable or command[0].endswith('python') or command[0].endswith('python3')):
        command[0] = venv_python
    
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    
    # Print output in real-time
    for line in process.stdout:
        print(line, end='')
    
    process.wait()
    return process.returncode

def get_unprocessed_links(csv_file, link_type="google_drive"):
    """
    Extract links that haven't been processed yet from the CSV file
    
    Args:
        csv_file: Path to the CSV file
        link_type: Type of links to extract ("google_drive" or "youtube_playlist")
    
    Returns:
        List of links that haven't been processed yet
    """
    logger = get_logger(__name__)
    if not os.path.exists(csv_file):
        logger.warning(f"CSV file not found: {csv_file}")
        return []
    
    links = []
    
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if link_type == "google_drive" and row.get("google_drive") and row.get("google_drive") != "-":
                # Split multiple drive links
                drive_links = row.get("google_drive").split("|")
                links.extend(drive_links)
            
            elif link_type == "youtube_playlist" and row.get("youtube_playlist") and row.get("youtube_playlist") != "-":
                links.append(row.get("youtube_playlist"))
    
    # Filter out links that have already been processed
    if link_type == "google_drive":
        # Check for downloaded files in drive_downloads directory
        download_dir = Path("drive_downloads")
        if download_dir.exists():
            downloaded_ids = set()
            for item in download_dir.glob("*_metadata.json"):
                file_id = item.name.split("_metadata.json")[0]
                downloaded_ids.add(file_id)
            
            # Filter links that haven't been downloaded yet
            filtered_links = []
            for link in links:
                # Extract file ID from the URL
                if "/file/d/" in link:
                    file_id = link.split("/file/d/")[1].split("/")[0]
                elif "id=" in link:
                    file_id = link.split("id=")[1].split("&")[0]
                else:
                    file_id = None
                
                if file_id and file_id not in downloaded_ids:
                    filtered_links.append(link)
            
            links = filtered_links
    
    elif link_type == "youtube_playlist":
        # Check for downloaded videos in youtube_downloads directory
        download_dir = Path("youtube_downloads")
        if download_dir.exists():
            downloaded_ids = set()
            for item in download_dir.glob("*.mp4"):
                video_id = item.name.split(".mp4")[0]
                downloaded_ids.add(video_id)
            
            # Filter links that haven't been downloaded yet
            filtered_links = []
            for link in links:
                # For simplicity, just add all YouTube playlist links
                # Individual video downloading will be handled by the download_youtube.py script
                filtered_links.append(link)
            
            links = filtered_links
    
    return links

def main():
    parser = argparse.ArgumentParser(description="Run the complete workflow: scrape Google Sheet, download Google Drive files and YouTube videos")
    parser.add_argument("--max-rows", type=int, default=None, help="Maximum number of rows to process for Google Sheet scraping")
    parser.add_argument("--reset", action="store_true", help="Reset processed status and reprocess all rows")
    parser.add_argument("--skip-sheet", action="store_true", help="Skip Google Sheet scraping")
    parser.add_argument("--skip-drive", action="store_true", help="Skip Google Drive downloads")
    parser.add_argument("--skip-youtube", action="store_true", help="Skip YouTube downloads")
    parser.add_argument("--max-youtube", type=int, default=None, help="Maximum number of YouTube videos to download")
    parser.add_argument("--max-drive", type=int, default=None, help="Maximum number of Google Drive files to download")
    parser.add_argument("--no-logging", action="store_true", help="Disable logging to files")
    
    args = parser.parse_args()
    
    # Use logging unless disabled
    if args.no_logging:
        return main_workflow(args)
    else:
        with pipeline_run() as pipeline_logger:
            return main_workflow(args, pipeline_logger)

def main_workflow(args, logger=None):
    """Main workflow logic with row-centric tracking"""
    
    # Step 0: Ensure CSV has tracking columns
    output_csv_path = config.get('paths.output_csv', 'output.csv')
    if os.path.exists(output_csv_path):
        tracking_added = ensure_tracking_columns(output_csv_path)
        if tracking_added and logger:
            logger.get_logger('main').info("Enhanced CSV with tracking columns for row-centric downloads")
    
    # Step 1: Scrape Google Sheet and extract links
    if not args.skip_sheet:
        command = [sys.executable, "utils/master_scraper.py", "--force-download"]
        if args.max_rows is not None:
            command.extend(["--max-rows", str(args.max_rows)])
        if args.reset:
            command.append("--reset")
        
        exit_code = run_process(command, "Step 1: Scraping Google Sheet and extracting links", 'scraper', logger)
        if exit_code != 0:
            if logger:
                logger.log_error(f"Error in Step 1: Google Sheet scraping failed with exit code {exit_code}")
            else:
                print(f"Error in Step 1: Google Sheet scraping failed with exit code {exit_code}")
            return exit_code
    
    # Step 2: Download Google Drive files using row-centric tracking
    if not args.skip_drive:
        # Get pending Drive downloads from CSV tracking system
        pending_drive = get_pending_downloads(output_csv_path, 'drive')
        
        if pending_drive:
            # Apply max-drive limit if specified
            if args.max_drive is not None and args.max_drive > 0:
                pending_drive = pending_drive[:args.max_drive]
                if logger:
                    logger.get_logger('main').info(f"Step 2: Downloading {len(pending_drive)} Google Drive files (limited by --max-drive)")
                else:
                    print(f"\n{'=' * 80}\nStep 2: Downloading {len(pending_drive)} Google Drive files (limited by --max-drive)\n{'=' * 80}")
            else:
                if logger:
                    logger.get_logger('main').info(f"Step 2: Downloading {len(pending_drive)} Google Drive files")
                else:
                    print(f"\n{'=' * 80}\nStep 2: Downloading {len(pending_drive)} Google Drive files\n{'=' * 80}")
            
            # Create the drive_downloads directory if it doesn't exist
            os.makedirs("drive_downloads", exist_ok=True)
            
            # Process downloads with row context tracking
            import pandas as pd
            df = pd.read_csv(output_csv_path)
            
            drive_count = 0
            for row_context in pending_drive:
                if drive_count >= len(pending_drive):
                    break
                    
                # Get the actual row data
                row = df.iloc[row_context.row_index]
                drive_url = row.get('google_drive')
                
                if drive_url and str(drive_url).strip() not in ['', '-', 'nan']:
                    # Handle multiple drive links separated by |
                    drive_urls = str(drive_url).split('|') if '|' in str(drive_url) else [str(drive_url)]
                    
                    for drive_link in drive_urls:
                        drive_link = drive_link.strip()
                        if drive_link and "drive.google.com" in drive_link:
                            if logger:
                                logger.get_logger('drive').info(f"Downloading Google Drive file for {row_context.name} (Type: {row_context.type}): {drive_link}")
                            else:
                                print(f"\nDownloading Google Drive file for {row_context.name} (Type: {row_context.type}): {drive_link}")
                            
                            # Use new row-centric download function
                            result = download_drive_with_context(drive_link, row_context)
                            
                            # Update CSV with result
                            update_csv_download_status(row_context.row_index, 'drive', result)
                            
                            if result.success:
                                if logger:
                                    logger.update_stats(drive_downloads=logger.run_stats['drive_downloads'] + 1)
                                drive_count += 1
                            else:
                                if logger:
                                    logger.log_error(f"Failed to download Google Drive file for {row_context.name}: {result.error_message}")
                                else:
                                    print(f"Warning: Failed to download Google Drive file for {row_context.name}: {result.error_message}")
            
            if logger:
                logger.get_logger('main').info(f"Completed {drive_count} Google Drive downloads with row tracking")
    
    # Step 3: Download YouTube videos using row-centric tracking
    if not args.skip_youtube:
        # Get pending YouTube downloads from CSV tracking system
        pending_youtube = get_pending_downloads(output_csv_path, 'youtube')
        
        if pending_youtube:
            # Apply max-youtube limit if specified
            if args.max_youtube is not None and args.max_youtube > 0:
                pending_youtube = pending_youtube[:args.max_youtube]
                if logger:
                    logger.get_logger('main').info(f"Step 3: Downloading {len(pending_youtube)} YouTube videos (limited by --max-youtube)")
                else:
                    print(f"\n{'=' * 80}\nStep 3: Downloading {len(pending_youtube)} YouTube videos (limited by --max-youtube)\n{'=' * 80}")
            else:
                if logger:
                    logger.get_logger('main').info(f"Step 3: Downloading {len(pending_youtube)} YouTube videos")
                else:
                    print(f"\n{'=' * 80}\nStep 3: Downloading {len(pending_youtube)} YouTube videos\n{'=' * 80}")
            
            # Create the youtube_downloads directory if it doesn't exist
            os.makedirs("youtube_downloads", exist_ok=True)
            
            # Process downloads with row context tracking
            import pandas as pd
            df = pd.read_csv(output_csv_path)
            
            youtube_count = 0
            for row_context in pending_youtube:
                if youtube_count >= len(pending_youtube):
                    break
                    
                # Get the actual row data
                row = df.iloc[row_context.row_index]
                youtube_url = row.get('youtube_playlist')
                
                if youtube_url and str(youtube_url).strip() not in ['', '-', 'nan']:
                    youtube_link = str(youtube_url).strip()
                    
                    if logger:
                        logger.get_logger('youtube').info(f"Downloading YouTube content for {row_context.name} (Type: {row_context.type}): {youtube_link}")
                    else:
                        print(f"\nDownloading YouTube content for {row_context.name} (Type: {row_context.type}): {youtube_link}")
                    
                    # Use new row-centric download function
                    result = download_youtube_with_context(youtube_link, row_context)
                    
                    # Update CSV with result
                    update_csv_download_status(row_context.row_index, 'youtube', result)
                    
                    if result.success:
                        if logger:
                            logger.update_stats(youtube_downloads=logger.run_stats['youtube_downloads'] + 1)
                        youtube_count += 1
                    else:
                        if logger:
                            logger.log_error(f"Failed to download YouTube content for {row_context.name}: {result.error_message}")
                        else:
                            print(f"Warning: Failed to download YouTube content for {row_context.name}: {result.error_message}")
            
            if logger:
                logger.get_logger('main').info(f"Completed {youtube_count} YouTube downloads with row tracking")
    
    # Update total rows processed if logger is available
                    continue
                    
                # Skip non-YouTube URLs (CSS files, JavaScript, etc.)
                if any(link.endswith(ext) for ext in ['.css', '.js', '.png', '.jpg', '.gif']):
                    continue
                
                # Skip URLs that don't contain youtube.com or youtu.be
                if "youtube.com" not in link and "youtu.be" not in link:
                    continue
                
                # First clean the URL to remove control characters
                try:
                    cleaned_link = clean_url(link)
                except Exception as e:
                    if logger:
                        logger.log_error(f"Failed to clean YouTube URL: {link} - {str(e)}")
                    else:
                        print(f"Warning: Failed to clean YouTube URL: {link} - {str(e)}")
                    continue
                
                # Handle playlist URLs
                if "watch_videos?video_ids=" in cleaned_link:
                    try:
                        # Validate and clean the playlist URL
                        validated_url, video_ids = validate_youtube_playlist_url(cleaned_link)
                        if video_ids:  # Only add if there are valid video IDs
                            cleaned_youtube_links.append(validated_url)
                        else:
                            if logger:
                                logger.log_error(f"No valid video IDs found in playlist URL: {link}")
                            else:
                                print(f"Warning: No valid video IDs found in playlist URL: {link}")
                    except ValidationError as e:
                        if logger:
                            logger.log_error(f"Invalid YouTube playlist URL: {link} - {str(e)}")
                        else:
                            print(f"Warning: Invalid YouTube playlist URL: {link} - {str(e)}")
                    except Exception as e:
                        if logger:
                            logger.log_error(f"Failed to validate YouTube playlist URL: {link} - {str(e)}")
                        else:
                            print(f"Warning: Failed to validate YouTube playlist URL: {link} - {str(e)}")
                
                # Handle regular YouTube URLs
                elif "/watch?v=" in cleaned_link or "youtu.be/" in cleaned_link:
                    try:
                        # Validate single video URL
                        validated_url, video_id = validate_youtube_url(cleaned_link)
                        cleaned_youtube_links.append(validated_url)
                    except ValidationError as e:
                        if logger:
                            logger.log_error(f"Invalid YouTube URL: {link} - {str(e)}")
                        else:
                            print(f"Warning: Invalid YouTube URL: {link} - {str(e)}")
                    except Exception as e:
                        if logger:
                            logger.log_error(f"Failed to validate YouTube URL: {link} - {str(e)}")
                        else:
                            print(f"Warning: Failed to validate YouTube URL: {link} - {str(e)}")
                
                # Handle regular playlist URLs
                elif "playlist?list=" in cleaned_link:
                    try:
                        # Validate playlist URL
                        validated_url, _ = validate_youtube_playlist_url(cleaned_link)
                        cleaned_youtube_links.append(validated_url)
                    except ValidationError as e:
                        if logger:
                            logger.log_error(f"Invalid YouTube playlist URL: {link} - {str(e)}")
                        else:
                            print(f"Warning: Invalid YouTube playlist URL: {link} - {str(e)}")
                    except Exception as e:
                        if logger:
                            logger.log_error(f"Failed to validate YouTube playlist URL: {link} - {str(e)}")
                        else:
                            print(f"Warning: Failed to validate YouTube playlist URL: {link} - {str(e)}")
            
            # Replace the original links with the cleaned and validated ones
            youtube_links = cleaned_youtube_links
            
            # Apply max-youtube limit if specified
            if args.max_youtube is not None and args.max_youtube > 0:
                youtube_links = youtube_links[:args.max_youtube]
                if logger:
                    logger.get_logger('main').info(f"Step 3: Downloading YouTube videos from {len(youtube_links)} playlists (limited by --max-youtube)")
                else:
                    print(f"\n{'=' * 80}\nStep 3: Downloading YouTube videos from {len(youtube_links)} playlists (limited by --max-youtube)\n{'=' * 80}")
            else:
                if logger:
                    logger.get_logger('main').info(f"Step 3: Downloading YouTube videos from {len(youtube_links)} playlists")
                else:
                    print(f"\n{'=' * 80}\nStep 3: Downloading YouTube videos from {len(youtube_links)} playlists\n{'=' * 80}")
            
            # Create the youtube_downloads directory if it doesn't exist
            os.makedirs("youtube_downloads", exist_ok=True)
            
            # Use parallel processing for YouTube downloads
            if logger:
                logger.get_logger('youtube').info(f"Starting parallel download of {len(youtube_links)} YouTube videos/playlists")
            else:
                print(f"\nStarting parallel download of {len(youtube_links)} YouTube videos/playlists")
            
            # Filter out empty links
            valid_links = [link for link in youtube_links if link]
            
            if valid_links:
                # Download videos in parallel with max 4 concurrent downloads
                results = parallel_download_youtube_videos(
                    valid_links,
                    max_workers=4,  # Limit concurrent downloads
                    transcript_only=False,
                    resolution="720",
                    logger=logger.get_logger('youtube') if logger else None
                )
                
                # Count successes and failures
                successful = 0
                failed = 0
                
                for url, result in results:
                    if isinstance(result, Exception):
                        failed += 1
                        if logger:
                            logger.log_error(f"Failed to download YouTube video: {url} - {result}")
                        else:
                            print(f"Warning: Failed to download YouTube video: {url} - {result}")
                    else:
                        video_file, transcript_file = result
                        if video_file or transcript_file:
                            successful += 1
                            if logger:
                                logger.update_stats(youtube_downloads=logger.run_stats['youtube_downloads'] + 1)
                        else:
                            failed += 1
                
                if logger:
                    logger.get_logger('youtube').info(f"YouTube downloads complete: {successful} successful, {failed} failed")
                else:
                    print(f"\nYouTube downloads complete: {successful} successful, {failed} failed")
    
    # Update total rows processed if logger is available
    if logger and not args.skip_sheet:
        csv_rows = 0
        output_csv_path = config.get('paths.output_csv', 'output.csv')
        if os.path.exists(output_csv_path):
            with open(output_csv_path, 'r', newline='', encoding='utf-8') as f:
                csv_rows = sum(1 for _ in csv.DictReader(f))
        logger.update_stats(rows_processed=csv_rows)
    
    if logger:
        logger.get_logger('main').success("All steps completed successfully!")
    else:
        print(f"\n{'=' * 80}\nAll steps completed successfully!\n{'=' * 80}")
    return 0

if __name__ == "__main__":
    sys.exit(main())