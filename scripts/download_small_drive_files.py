#!/usr/bin/env python3
"""
Download only small Drive files (< 100MB) for testing
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.download_drive_files_from_html import DriveFileDownloader
from pathlib import Path

class SmallFileDownloader(DriveFileDownloader):
    def __init__(self, max_size_mb=100):
        super().__init__()
        self.max_size_mb = max_size_mb
        self.skipped_large = []
    
    def wait_for_download(self, timeout=600, check_interval=5):
        """Use shorter timeout for small files (10 minutes)"""
        return super().wait_for_download(timeout=timeout, check_interval=check_interval)
    
    def process_html_file(self, html_file):
        """Override to check file size first"""
        file_id = html_file['file_id']
        
        # Check if there's already a partial download
        partial_files = list(self.files_dir.glob('*.crdownload'))
        for pf in partial_files:
            if file_id in str(pf):
                print(f"Skipping {file_id} - partial download already exists: {pf.name}")
                return False
        
        # Read HTML to check file size
        with open(html_file['path'], 'r', encoding='utf-8') as f:
            html_content = f.read()
            
        # Look for file size in HTML (e.g., "4.8G" or "100M")
        import re
        size_match = re.search(r'\(([0-9.]+)([GMK])\)', html_content)
        
        if size_match:
            size_value = float(size_match.group(1))
            size_unit = size_match.group(2)
            
            # Convert to MB
            if size_unit == 'G':
                size_mb = size_value * 1024
            elif size_unit == 'M':
                size_mb = size_value
            elif size_unit == 'K':
                size_mb = size_value / 1024
            else:
                size_mb = 0
            
            if size_mb > self.max_size_mb:
                print(f"Skipping {file_id} - file too large: {size_value}{size_unit} ({size_mb:.1f} MB)")
                self.skipped_large.append({
                    'file_id': file_id,
                    'size': f"{size_value}{size_unit}",
                    'size_mb': size_mb
                })
                
                # Update mapping
                if file_id in self.mapping:
                    self.mapping[file_id]['status'] = 'skipped_too_large'
                    self.mapping[file_id]['file_size'] = f"{size_value}{size_unit}"
                
                return False
        
        # Process normally if size is OK
        return super().process_html_file(html_file)
    
    def generate_report(self):
        """Enhanced report with skipped files"""
        super().generate_report()
        
        if self.skipped_large:
            print(f"\n📦 Skipped {len(self.skipped_large)} large files (> {self.max_size_mb} MB):")
            for file_info in self.skipped_large:
                rows = self.mapping.get(file_info['file_id'], {}).get('rows', [])
                names = [r['name'] for r in rows]
                print(f"  - {file_info['file_id']}: {file_info['size']} - {', '.join(names)}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Download small Drive files only')
    parser.add_argument('--max-size', type=int, default=100, 
                        help='Maximum file size in MB (default: 100)')
    
    args = parser.parse_args()
    
    # Clean up partial downloads first
    files_dir = Path('drive_downloads/files')
    if files_dir.exists():
        partial_files = list(files_dir.glob('*.crdownload'))
        if partial_files:
            print(f"Found {len(partial_files)} partial downloads:")
            for pf in partial_files:
                print(f"  - {pf.name} ({pf.stat().st_size / (1024*1024):.1f} MB)")
            
            response = input("\nDelete partial downloads? (y/N): ")
            if response.lower() == 'y':
                for pf in partial_files:
                    pf.unlink()
                print("Deleted partial downloads")
    
    print(f"\nStarting download of files < {args.max_size} MB...")
    downloader = SmallFileDownloader(max_size_mb=args.max_size)
    downloader.run()