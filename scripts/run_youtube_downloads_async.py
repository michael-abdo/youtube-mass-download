#!/usr/bin/env python3
"""
Run YouTube downloads in background from CSV file
"""
import os
import sys
import csv
import subprocess
import time
from datetime import datetime

# Add parent directory to path to access utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import get_config
from utils.validation import validate_youtube_url

def get_youtube_urls_from_csv():
    """Extract all YouTube URLs from the CSV file"""
    config = get_config()
    csv_path = config.get('csv', {}).get('output_file', 'data/output.csv')
    
    youtube_urls = []
    
    # Read the CSV and extract YouTube playlist URLs
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Check if youtube_playlist column exists and has a value
            if 'youtube_playlist' in row and row['youtube_playlist']:
                playlist_url = row['youtube_playlist'].strip()
                if playlist_url:
                    youtube_urls.append({
                        'name': row.get('name', 'Unknown'),
                        'url': playlist_url
                    })
    
    return youtube_urls

def download_youtube_async(urls, max_downloads=None):
    """Download YouTube videos asynchronously"""
    venv_python = os.path.join(os.path.dirname(__file__), 'venv', 'bin', 'python')
    download_script = os.path.join(os.path.dirname(__file__), 'utils', 'download_youtube.py')
    
    print(f"Found {len(urls)} YouTube playlists to process")
    
    if max_downloads:
        urls = urls[:max_downloads]
        print(f"Limiting to {max_downloads} downloads")
    
    # Create log file for this run
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'youtube_downloads_{timestamp}.log'
    
    with open(log_file, 'w') as log:
        log.write(f"YouTube download started at {datetime.now()}\n")
        log.write(f"Processing {len(urls)} playlists\n\n")
        
        for i, item in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] Processing {item['name']}: {item['url']}")
            log.write(f"\n[{i}/{len(urls)}] Processing {item['name']}: {item['url']}\n")
            
            try:
                # Run download command
                cmd = [venv_python, download_script, item['url']]
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    print(f"✓ Successfully processed {item['name']}")
                    log.write(f"✓ Success\n")
                else:
                    print(f"✗ Failed to process {item['name']}: {result.stderr}")
                    log.write(f"✗ Failed: {result.stderr}\n")
                    
            except Exception as e:
                print(f"✗ Error processing {item['name']}: {str(e)}")
                log.write(f"✗ Error: {str(e)}\n")
            
            # Small delay between downloads to be respectful
            time.sleep(2)
    
    print(f"\nDownload log saved to: {log_file}")
    return log_file

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Download YouTube videos from CSV in background')
    parser.add_argument('--max-downloads', type=int, help='Maximum number of playlists to download')
    
    args = parser.parse_args()
    
    # Get YouTube URLs from CSV
    urls = get_youtube_urls_from_csv()
    
    if not urls:
        print("No YouTube playlists found in CSV")
        sys.exit(0)
    
    # Start downloads
    log_file = download_youtube_async(urls, args.max_downloads)
    print(f"\nDownloads complete. Check {log_file} for details.")