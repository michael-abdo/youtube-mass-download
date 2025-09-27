#!/usr/bin/env python3
"""
Test downloading a single specific Drive file
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.download_drive_files_from_html import DriveFileDownloader
from pathlib import Path

class SingleFileDownloader(DriveFileDownloader):
    def __init__(self, target_file_id):
        super().__init__()
        self.target_file_id = target_file_id
    
    def scan_html_files(self):
        """Override to only process target file"""
        target_path = self.html_dir / f"{self.target_file_id}.html"
        if target_path.exists():
            return [{
                'file_id': self.target_file_id,
                'path': target_path
            }]
        else:
            print(f"File not found: {target_path}")
            return []

if __name__ == "__main__":
    # Target the 181MB file
    target_id = "19v8EN0EfTFPw-vvXNq_6fNBN4oUcyQw1"
    
    print(f"Testing download of single file: {target_id}")
    print("This is a 181MB file that should download quickly")
    
    downloader = SingleFileDownloader(target_id)
    downloader.run()