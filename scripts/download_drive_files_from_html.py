#!/usr/bin/env python3
"""
Download actual Google Drive files by processing HTML preview pages and clicking "Download anyway" button
"""
import os
import sys
import csv
import json
import time
import re
from pathlib import Path
from datetime import datetime

# Add parent directory to path to access utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from utils.config import get_config
from utils.logging_config import get_logger
from utils.download_drive import extract_file_id

logger = get_logger(__name__)
config = get_config()

# Increase CSV field size limit
csv.field_size_limit(config.get('file_processing.max_csv_field_size', sys.maxsize))

class DriveFileDownloader:
    def __init__(self):
        self.output_csv = config.get('paths.output_csv', 'outputs/output.csv')
        self.drive_downloads_dir = Path(config.get('paths.drive_downloads', 'drive_downloads'))
        self.html_dir = self.drive_downloads_dir
        self.files_dir = self.drive_downloads_dir / 'files'
        self.mapping_file = self.drive_downloads_dir / 'download_mapping.json'
        self.mapping = {}
        self.driver = None
        
        # Create directories if they don't exist
        self.files_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing mapping if it exists
        if self.mapping_file.exists():
            with open(self.mapping_file, 'r') as f:
                self.mapping = json.load(f)
    
    # File ID extraction moved to utils.download_drive.extract_file_id for consistency
    
    def build_file_mapping(self):
        """Build mapping of file_id to row information from CSV"""
        logger.info("Building file ID to row mapping from CSV...")
        
        file_to_rows = {}  # file_id -> list of row info
        
        with open(self.output_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 to account for header
                google_drive_links = row.get('google_drive', '')
                
                if google_drive_links and google_drive_links != '-':
                    # Split multiple links by pipe
                    links = google_drive_links.split('|')
                    
                    for link in links:
                        link = link.strip()
                        if link and link.startswith('http'):
                            file_id = extract_file_id(link)
                            
                            if file_id:
                                if file_id not in file_to_rows:
                                    file_to_rows[file_id] = []
                                
                                file_to_rows[file_id].append({
                                    'row_id': row.get('row_id', str(row_num)),
                                    'row_num': row_num,
                                    'name': row.get('name', 'Unknown'),
                                    'email': row.get('email', ''),
                                    'type': row.get('type', ''),
                                    'original_url': link
                                })
        
        logger.info(f"Found {len(file_to_rows)} unique Drive files referenced in CSV")
        
        # Update mapping with CSV data
        for file_id, rows in file_to_rows.items():
            if file_id not in self.mapping:
                self.mapping[file_id] = {
                    'rows': rows,
                    'status': 'pending',
                    'attempts': 0
                }
            else:
                # Update row information but preserve download status
                self.mapping[file_id]['rows'] = rows
        
        self.save_mapping()
        return file_to_rows
    
    def scan_html_files(self):
        """Scan drive_downloads directory for HTML files"""
        logger.info("Scanning for HTML files...")
        
        html_files = []
        for file_path in self.html_dir.glob('*.html'):
            # Extract file ID from filename (format: {file_id}.html or {file_id}_metadata.json)
            filename = file_path.stem
            if '_metadata' not in filename:
                html_files.append({
                    'file_id': filename,
                    'path': file_path
                })
        
        logger.info(f"Found {len(html_files)} HTML files to process")
        return html_files
    
    def setup_chrome_driver(self):
        """Configure Chrome for automatic downloads"""
        logger.info("Setting up Chrome driver with download preferences...")
        
        chrome_options = webdriver.ChromeOptions()
        
        # Set download directory
        prefs = {
            "download.default_directory": str(self.files_dir.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,  # This allows "Download anyway" functionality
            "safebrowsing.disable_download_protection": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Add other useful options
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Run headless for automated execution (comment out for debugging)
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920,1080")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(10)
        
        # Enable automatic download of multiple files
        self.driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": str(self.files_dir.absolute())
        })
    
    def wait_for_download(self, timeout=3600, check_interval=5):
        """Wait for download to complete by monitoring the downloads directory"""
        logger.info("Waiting for download to complete...")
        
        start_time = time.time()
        last_size = 0
        no_progress_count = 0
        
        while time.time() - start_time < timeout:
            # Check for .crdownload files (Chrome temporary download files)
            temp_files = list(self.files_dir.glob('*.crdownload'))
            
            if not temp_files:
                # No temporary files, check if we have any new files
                time.sleep(2)
                return True
            
            # Check download progress
            if temp_files:
                current_file = temp_files[0]
                current_size = current_file.stat().st_size
                
                # Show progress
                size_mb = current_size / (1024 * 1024)
                elapsed = time.time() - start_time
                speed_mb = (current_size - last_size) / (1024 * 1024) / check_interval if last_size > 0 else 0
                
                logger.info(f"Download progress: {size_mb:.1f} MB ({speed_mb:.1f} MB/s)")
                
                # Check if download is stalled
                if current_size == last_size:
                    no_progress_count += 1
                    if no_progress_count > 12:  # No progress for 60 seconds
                        logger.warning("Download appears to be stalled")
                        return False
                else:
                    no_progress_count = 0
                
                last_size = current_size
            
            time.sleep(check_interval)
        
        logger.warning(f"Download timeout after {timeout} seconds")
        return False
    
    def get_latest_download(self):
        """Get the most recently downloaded file"""
        files = [f for f in self.files_dir.iterdir() if f.is_file() and not f.name.endswith('.crdownload')]
        if not files:
            return None
        
        # Get the most recent file
        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        return latest_file
    
    def process_html_file(self, html_file):
        """Process a single HTML file and download the actual file"""
        file_id = html_file['file_id']
        file_path = html_file['path']
        
        logger.info(f"Processing {file_id}.html...")
        
        # Check if already downloaded
        if self.mapping.get(file_id, {}).get('status') == 'success':
            logger.info(f"File {file_id} already downloaded, skipping...")
            return True
        
        # Get files before download
        before_files = set(f.name for f in self.files_dir.iterdir() if f.is_file())
        
        try:
            # Load the HTML file
            file_url = f"file://{file_path.absolute()}"
            self.driver.get(file_url)
            
            # Wait for page to load
            time.sleep(3)
            
            # Try multiple strategies to find download button
            download_clicked = False
            
            # Strategy 1: Look for "Download anyway" button (could be input or button)
            try:
                # Try input submit button first (most common for Drive virus scan pages)
                download_element = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//input[@type='submit'][@value='Download anyway']"))
                )
                download_element.click()
                download_clicked = True
                logger.info("Clicked 'Download anyway' submit button")
            except TimeoutException:
                # Try regular button
                try:
                    download_button = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Download anyway')]")
                    download_button.click()
                    download_clicked = True
                    logger.info("Clicked 'Download anyway' button")
                except NoSuchElementException:
                    logger.debug("'Download anyway' button not found, trying other strategies...")
            
            # Strategy 2: Look for any download button
            if not download_clicked:
                try:
                    # Try various download button selectors
                    selectors = [
                        "//button[contains(@aria-label, 'Download')]",
                        "//div[@role='button'][contains(@aria-label, 'Download')]",
                        "//button[contains(text(), 'Download')]",
                        "//a[contains(@aria-label, 'Download')]",
                        "//div[contains(@class, 'download')]//button",
                    ]
                    
                    for selector in selectors:
                        try:
                            download_element = self.driver.find_element(By.XPATH, selector)
                            download_element.click()
                            download_clicked = True
                            logger.info(f"Clicked download element: {selector}")
                            break
                        except NoSuchElementException:
                            continue
                            
                except Exception as e:
                    logger.debug(f"Could not find standard download button: {e}")
            
            if not download_clicked:
                logger.warning(f"Could not find any download button for {file_id}")
                if file_id not in self.mapping:
                    self.mapping[file_id] = {'rows': [], 'status': 'no_csv_entry'}
                self.mapping[file_id]['status'] = 'no_download_button'
                self.mapping[file_id]['attempts'] = self.mapping[file_id].get('attempts', 0) + 1
                return False
            
            # Wait for download to complete
            if self.wait_for_download():
                # Get new files
                after_files = set(f.name for f in self.files_dir.iterdir() if f.is_file())
                new_files = after_files - before_files
                
                if new_files:
                    # Get the downloaded file
                    downloaded_file = self.files_dir / list(new_files)[0]
                    
                    # Rename file to include row reference
                    rows = self.mapping[file_id].get('rows', [])
                    if rows:
                        # Use first row's ID for naming
                        first_row = rows[0]
                        row_id = first_row['row_id']
                        
                        # Handle .crdownload extension
                        if downloaded_file.suffix == '.crdownload':
                            # Remove .crdownload extension
                            actual_name = downloaded_file.stem
                            new_name = f"row_{row_id}_{actual_name}"
                        else:
                            actual_name = downloaded_file.name
                            new_name = f"row_{row_id}_{actual_name}"
                        
                        new_path = self.files_dir / new_name
                        
                        # Rename file
                        downloaded_file.rename(new_path)
                        
                        # Update mapping
                        self.mapping[file_id]['status'] = 'success'
                        self.mapping[file_id]['downloaded_file'] = new_name
                        self.mapping[file_id]['original_name'] = actual_name
                        self.mapping[file_id]['download_date'] = datetime.now().isoformat()
                        self.mapping[file_id]['file_size_mb'] = round(new_path.stat().st_size / (1024 * 1024), 1)
                        
                        logger.info(f"Successfully downloaded: {new_name}")
                        return True
                    else:
                        logger.warning(f"No row information for {file_id}")
                        self.mapping[file_id]['status'] = 'success_no_row'
                        return True
                else:
                    logger.warning(f"Download completed but no new file found for {file_id}")
                    self.mapping[file_id]['status'] = 'download_failed'
                    return False
            else:
                logger.error(f"Download timeout for {file_id}")
                self.mapping[file_id]['status'] = 'timeout'
                return False
                
        except Exception as e:
            logger.error(f"Error processing {file_id}: {str(e)}")
            if file_id not in self.mapping:
                self.mapping[file_id] = {'rows': [], 'status': 'no_csv_entry'}
            self.mapping[file_id]['status'] = 'error'
            self.mapping[file_id]['error'] = str(e)
            self.mapping[file_id]['attempts'] = self.mapping[file_id].get('attempts', 0) + 1
            return False
    
    def save_mapping(self):
        """Save mapping to JSON file"""
        with open(self.mapping_file, 'w') as f:
            json.dump(self.mapping, f, indent=2)
    
    def generate_report(self):
        """Generate summary report of downloads"""
        logger.info("\n" + "="*60)
        logger.info("DOWNLOAD SUMMARY")
        logger.info("="*60)
        
        stats = {
            'total': len(self.mapping),
            'success': 0,
            'failed': 0,
            'pending': 0,
            'no_button': 0,
            'timeout': 0,
            'error': 0
        }
        
        for file_id, info in self.mapping.items():
            status = info.get('status', 'pending')
            if status == 'success':
                stats['success'] += 1
            elif status == 'pending':
                stats['pending'] += 1
            elif status == 'no_download_button':
                stats['no_button'] += 1
            elif status == 'timeout':
                stats['timeout'] += 1
            elif status in ['error', 'download_failed']:
                stats['failed'] += 1
        
        logger.info(f"Total files: {stats['total']}")
        logger.info(f"✅ Successfully downloaded: {stats['success']}")
        logger.info(f"⏳ Pending: {stats['pending']}")
        logger.info(f"❌ Failed: {stats['failed']}")
        logger.info(f"🚫 No download button: {stats['no_button']}")
        logger.info(f"⏱️ Timeout: {stats['timeout']}")
        
        # List failed files
        if stats['failed'] > 0 or stats['no_button'] > 0:
            logger.info("\nFailed downloads:")
            for file_id, info in self.mapping.items():
                if info.get('status') in ['error', 'download_failed', 'no_download_button', 'timeout']:
                    rows = info.get('rows', [])
                    names = [r['name'] for r in rows]
                    logger.info(f"  - {file_id}: {', '.join(names)} ({info.get('status')})")
    
    def run(self):
        """Main execution method"""
        try:
            # Phase 1: Build mapping
            self.build_file_mapping()
            
            # Phase 2: Scan HTML files
            html_files = self.scan_html_files()
            
            if not html_files:
                logger.warning("No HTML files found to process")
                return
            
            # Phase 3: Setup Chrome
            self.setup_chrome_driver()
            
            # Phase 4: Process each HTML file
            success_count = 0
            for i, html_file in enumerate(html_files, 1):
                logger.info(f"\nProcessing file {i}/{len(html_files)}")
                
                if self.process_html_file(html_file):
                    success_count += 1
                
                # Save mapping after each download
                self.save_mapping()
                
                # Small delay between downloads
                time.sleep(2)
            
            logger.info(f"\nProcessed {len(html_files)} files, {success_count} successful downloads")
            
        finally:
            # Cleanup
            if self.driver:
                self.driver.quit()
            
            # Generate final report
            self.generate_report()
            
            # Save final mapping
            self.save_mapping()

if __name__ == "__main__":
    downloader = DriveFileDownloader()
    downloader.run()