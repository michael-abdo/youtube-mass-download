#!/usr/bin/env python3
"""
Run Google Drive downloads in background from CSV file
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
from utils.validation import validate_google_drive_url

def get_drive_urls_from_csv():
    """Extract all Google Drive URLs from the CSV file"""
    config = get_config()
    csv_path = config.get('csv', {}).get('output_file', 'data/output.csv')
    
    drive_urls = []
    
    # Read the CSV and extract Drive links
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Check if google_drive column exists and has a value
            if 'google_drive' in row and row['google_drive']:
                drive_links_str = row['google_drive'].strip()
                if drive_links_str and drive_links_str != '[]':
                    # Parse the list of drive links
                    try:
                        # Remove brackets and split by comma
                        links = drive_links_str.strip('[]').split(',')
                        for link in links:
                            link = link.strip().strip("'\"")
                            if link and link.startswith('http'):
                                drive_urls.append({
                                    'name': row.get('name', 'Unknown'),
                                    'url': link
                                })
                    except:
                        pass
    
    return drive_urls

def download_drive_async(urls, max_downloads=None):
    """Download Google Drive files asynchronously"""
    venv_python = os.path.join(os.path.dirname(__file__), 'venv', 'bin', 'python')
    download_script = os.path.join(os.path.dirname(__file__), 'utils', 'download_drive.py')
    
    print(f"Found {len(urls)} Google Drive files to process")
    
    if max_downloads:
        urls = urls[:max_downloads]
        print(f"Limiting to {max_downloads} downloads")
    
    # Create log file for this run
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'drive_downloads_{timestamp}.log'
    
    with open(log_file, 'w') as log:
        log.write(f"Google Drive download started at {datetime.now()}\n")
        log.write(f"Processing {len(urls)} files\n\n")
        
        for i, item in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] Processing {item['name']}: {item['url']}")
            log.write(f"\n[{i}/{len(urls)}] Processing {item['name']}: {item['url']}\n")
            
            try:
                # Run download command with metadata flag
                cmd = [venv_python, download_script, item['url'], '--metadata']
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    print(f"✓ Successfully downloaded from {item['name']}")
                    log.write(f"✓ Success\n")
                else:
                    print(f"✗ Failed to download from {item['name']}: {result.stderr}")
                    log.write(f"✗ Failed: {result.stderr}\n")
                    
            except Exception as e:
                print(f"✗ Error processing {item['name']}: {str(e)}")
                log.write(f"✗ Error: {str(e)}\n")
            
            # Small delay between downloads to be respectful
            time.sleep(1)
    
    print(f"\nDownload log saved to: {log_file}")
    return log_file

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Download Google Drive files from CSV in background')
    parser.add_argument('--max-downloads', type=int, help='Maximum number of files to download')
    
    args = parser.parse_args()
    
    # Get Drive URLs from CSV
    urls = get_drive_urls_from_csv()
    
    if not urls:
        print("No Google Drive files found in CSV")
        sys.exit(0)
    
    # Start downloads
    log_file = download_drive_async(urls, args.max_downloads)
    print(f"\nDownloads complete. Check {log_file} for details.")