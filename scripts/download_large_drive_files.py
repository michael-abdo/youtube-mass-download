#!/usr/bin/env python3
"""
Download large Drive files with extended timeouts and better progress tracking
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.download_drive_files_from_html import DriveFileDownloader
from pathlib import Path
import time

class LargeFileDownloader(DriveFileDownloader):
    def __init__(self):
        super().__init__()
        self.min_size_gb = 1.0  # Only process files >= 1GB
        self.processed_large = []
    
    def wait_for_download(self, timeout=7200, check_interval=10):
        """Extended timeout for large files (2 hours)"""
        return super().wait_for_download(timeout=timeout, check_interval=check_interval)
    
    def process_html_file(self, html_file):
        """Override to handle large files specially"""
        file_id = html_file['file_id']
        
        # Check if already downloaded or skipped
        status = self.mapping.get(file_id, {}).get('status', '')
        if status in ['success', 'skipped_too_large']:
            logger.info(f"File {file_id} already processed ({status}), skipping...")
            return True
            
        # Read HTML to check file size
        with open(html_file['path'], 'r', encoding='utf-8') as f:
            html_content = f.read()
            
        # Look for file size in HTML
        import re
        size_match = re.search(r'\(([0-9.]+)([GMK])\)', html_content)
        
        if size_match:
            size_value = float(size_match.group(1))
            size_unit = size_match.group(2)
            
            # Convert to GB
            if size_unit == 'G':
                size_gb = size_value
            elif size_unit == 'M':
                size_gb = size_value / 1024
            elif size_unit == 'K':
                size_gb = size_value / (1024 * 1024)
            else:
                size_gb = 0
            
            if size_gb < self.min_size_gb:
                print(f"Skipping {file_id} - file too small: {size_value}{size_unit} ({size_gb:.2f} GB)")
                return False
                
            # Log large file processing
            rows = self.mapping.get(file_id, {}).get('rows', [])
            names = [r['name'] for r in rows]
            print(f"\n{'='*60}")
            print(f"Processing LARGE file: {file_id}")
            print(f"Size: {size_value}{size_unit}")
            print(f"Person: {', '.join(names)}")
            print(f"{'='*60}")
            
            self.processed_large.append({
                'file_id': file_id,
                'size': f"{size_value}{size_unit}",
                'size_gb': size_gb,
                'names': names
            })
        
        # Process with extended timeout
        start_time = time.time()
        result = super().process_html_file(html_file)
        elapsed = time.time() - start_time
        
        if result:
            print(f"✅ Download completed in {elapsed/60:.1f} minutes")
        else:
            print(f"❌ Download failed after {elapsed/60:.1f} minutes")
            
        return result
    
    def generate_report(self):
        """Enhanced report for large files"""
        super().generate_report()
        
        if self.processed_large:
            print(f"\n📦 Processed {len(self.processed_large)} large files (>= {self.min_size_gb} GB):")
            for file_info in self.processed_large:
                status = self.mapping.get(file_info['file_id'], {}).get('status', 'unknown')
                print(f"  - {file_info['file_id']}: {file_info['size']} - {', '.join(file_info['names'])} [{status}]")

if __name__ == "__main__":
    import argparse
    from utils.logging_config import get_logger
    
    logger = get_logger(__name__)
    
    parser = argparse.ArgumentParser(description='Download large Drive files')
    parser.add_argument('--min-gb', type=float, default=1.0, 
                        help='Minimum file size in GB (default: 1.0)')
    parser.add_argument('--test-one', action='store_true',
                        help='Only process one file for testing')
    
    args = parser.parse_args()
    
    print(f"\nStarting download of files >= {args.min_gb} GB...")
    print("Note: Large files may take 10-30 minutes each to download")
    print("The script will wait up to 2 hours per file\n")
    
    downloader = LargeFileDownloader()
    downloader.min_size_gb = args.min_gb
    
    if args.test_one:
        # Override to only process one file
        original_run = downloader.run
        def test_run():
            downloader.build_file_mapping()
            html_files = downloader.scan_html_files()
            
            # Find first large file
            for html_file in html_files:
                with open(html_file['path'], 'r') as f:
                    if 'G)' in f.read():
                        print(f"Testing with single file: {html_file['file_id']}")
                        downloader.setup_chrome_driver()
                        downloader.process_html_file(html_file)
                        downloader.save_mapping()
                        break
            
            if downloader.driver:
                downloader.driver.quit()
            downloader.generate_report()
        
        downloader.run = test_run
    
    downloader.run()