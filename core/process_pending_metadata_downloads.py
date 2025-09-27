#!/usr/bin/env python3
"""
Process pending metadata downloads from S3 clients/ directory.
Downloads missing media files for people who have metadata but no actual media files.

Target people with missing media:
- 476 - Patryk Makara (Drive folder + file)
- 477 - Shelly Chen (Drive file)
- 483 - Taro (2 Drive files + folder)
- 484 - Emilie (Drive folder)
- 496 - James Kirton (2 YouTube playlists)
"""

import argparse
import json
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import traceback

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

from utils.s3_manager import UnifiedS3Manager
from utils.downloader import UnifiedDownloader, DownloadStrategy, DownloadConfig
from utils.csv_manager import CSVManager
from utils.row_context import RowContext
from utils.logging_config import get_logger, print_section_header
# Setup logging
logger = get_logger(__name__)

# Progress tracking file
PROGRESS_FILE = "metadata_download_progress.json"

class MetadataDownloadProcessor:
    """Process metadata files from S3 and download missing media."""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        # DRY CONSOLIDATION: Use centralized S3 client
        from utils.s3_manager import get_s3_client
        self.s3_client = get_s3_client()
        self.bucket_name = 'typing-clients-uuid-system'
        self.s3_manager = UnifiedS3Manager()
        self.csv_manager = CSVManager()
        self.progress = self._load_progress()
        self.stats = {
            'metadata_found': 0,
            'downloads_attempted': 0,
            'downloads_succeeded': 0,
            'downloads_failed': 0,
            'csv_updated': 0
        }
        
    def _load_progress(self) -> Dict:
        """Load progress tracking from file."""
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load progress file: {e}")
        return {'processed': [], 'failed': {}}
    
    def _save_progress(self):
        """Save progress tracking to file."""
        try:
            with open(PROGRESS_FILE, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")
    
    def load_metadata_from_s3(self) -> List[Dict]:
        """Load all metadata files from S3 clients/ directory."""
        metadata_list = []
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix='clients/'):
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('.json'):
                        try:
                            # Skip if already processed
                            if obj['Key'] in self.progress['processed']:
                                logger.info(f"Skipping already processed: {obj['Key']}")
                                continue
                                
                            response = self.s3_client.get_object(
                                Bucket=self.bucket_name, 
                                Key=obj['Key']
                            )
                            metadata = json.loads(response['Body'].read())
                            metadata['_s3_key'] = obj['Key']
                            metadata_list.append(metadata)
                            self.stats['metadata_found'] += 1
                            
                        except Exception as e:
                            logger.error(f"Error reading {obj['Key']}: {e}")
                            
        except Exception as e:
            logger.error(f"Error listing metadata files: {e}")
            
        return metadata_list
    
    def verify_csv_rows(self, target_rows: List[int]) -> Dict[int, Dict]:
        """Verify that target CSV rows exist and get their data."""
        csv_data = {}
        
        try:
            df = self.csv_manager.read('outputs/output.csv')
            for _, row in df.iterrows():
                row_id = str(row.get('row_id', '')).strip()
                if row_id and row_id.isdigit() and int(row_id) in target_rows:
                    csv_data[int(row_id)] = row.to_dict()
                    
            # Check which rows are missing
            missing = set(target_rows) - set(csv_data.keys())
            if missing:
                logger.warning(f"Missing CSV rows: {missing}")
                
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            
        return csv_data
    
    def _download_youtube_playlist_direct(self, url: str, row_context: RowContext) -> List[str]:
        """Download YouTube playlist directly using yt-dlp, bypassing error handler issues."""
        try:
            import subprocess
            import re
            import os
            import uuid
            
            # DRY: Use standardized download path creation
            from utils.path_utils import create_download_path
            output_dir = create_download_path(row_context.row_id, row_context.name, 'youtube')
            
            # Extract playlist ID
            playlist_match = re.search(r'list=([a-zA-Z0-9_-]+)', url)
            if not playlist_match:
                logger.error(f"Could not extract playlist ID from URL: {url}")
                return []
            
            playlist_id = playlist_match.group(1)
            logger.info(f"Downloading playlist {playlist_id}")
            
            # Use yt-dlp to download playlist
            cmd = [
                'yt-dlp',
                '--format', 'best[height<=720]',  # 720p max quality
                '--output', f'{output_dir}/%(title)s.%(ext)s',
                '--write-info-json',  # Save metadata
                '--no-overwrites',
                url
            ]
            
            logger.info(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                # List downloaded files
                downloaded_files = []
                for file in os.listdir(output_dir):
                    if not file.endswith('.json'):  # Skip metadata files
                        file_path = os.path.join(output_dir, file)
                        downloaded_files.append(file_path)
                        logger.info(f"Downloaded: {file}")
                
                # Upload to S3 and get UUID paths
                s3_files = self._upload_files_to_s3(downloaded_files, row_context)
                
                # Clean up local files
                import shutil
                shutil.rmtree(output_dir)
                
                return s3_files
            else:
                logger.error(f"yt-dlp failed: {result.stderr}")
                return []
                
        except Exception as e:
            logger.error(f"Error in direct YouTube download: {e}")
            return []
    
    def _upload_files_to_s3(self, local_files: List[str], row_context: RowContext) -> List[str]:
        """Upload local files to S3 files/ directory with UUID names."""
        s3_files = []
        
        try:
            for local_file in local_files:
                # Generate UUID for the file
                file_uuid = str(uuid.uuid4())
                
                # Get file extension
                # DRY CONSOLIDATION - Step 2: Use centralized extension handling
                from utils.path_utils import extract_extension
                ext = extract_extension(local_file)
                # DRY CONSOLIDATION - Step 1: Use centralized S3 key generation
                from utils.s3_manager import UnifiedS3Manager
                s3_key = UnifiedS3Manager.generate_uuid_s3_key(file_uuid, ext)
                
                # Upload to S3
                self.s3_client.upload_file(local_file, self.bucket_name, s3_key)
                s3_files.append(s3_key)
                
                logger.info(f"Uploaded {local_file} -> s3://{self.bucket_name}/{s3_key}")
                
        except Exception as e:
            logger.error(f"Error uploading files to S3: {e}")
            
        return s3_files

    def check_existing_media(self, row_id: int) -> bool:
        """Check if person already has media files in S3."""
        try:
            # Check for files in S3 files/ directory
            prefix = f"files/"
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            # Get CSV data to check s3_paths
            df = self.csv_manager.read('outputs/output.csv')
            for _, row in df.iterrows():
                if str(row.get('row_id', '')).strip() == str(row_id):
                    # DRY: Use CSVManager for S3 path loading
                    paths = CSVManager.load_s3_paths(row)
                    if paths:
                            logger.info(f"Row {row_id} already has {len(paths)} files in S3")
                            return True
                            
        except Exception as e:
            logger.error(f"Error checking existing media: {e}")
            
        return False
    
    def process_metadata(self, metadata: Dict, csv_row: Dict) -> Tuple[bool, List[str]]:
        """Process a single metadata file and download content."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would process: {metadata}")
            return True, []
            
        try:
            # Create row context
            row_context = RowContext(
                row_id=str(metadata.get('row_id')),
                row_index=int(metadata.get('row_id', 0)),
                type=csv_row.get('type', ''),
                name=metadata.get('person', ''),
                email=csv_row.get('email', '')
            )
            
            # Determine download type
            metadata_type = metadata.get('type', '')
            url = metadata.get('url', '')
            
            downloaded_files = []
            
            if metadata_type == 'youtube_playlist':
                logger.info(f"Processing YouTube playlist: {url}")
                downloaded_files = self._download_youtube_playlist_direct(url, row_context)
                    
            elif metadata_type == 'drive_file':
                logger.info(f"Processing Drive file: {url}")
                config = DownloadConfig()
                downloader = UnifiedDownloader(config=config)
                success, message = downloader.save_drive_info(url, row_context.name, int(row_context.row_id))
                if success:
                    downloaded_files = [message]  # message contains downloaded filename
                    
            elif metadata_type == 'drive_folder':
                logger.info(f"Processing Drive folder: {url}")
                config = DownloadConfig()
                downloader = UnifiedDownloader(config=config)
                success, message = downloader.save_drive_info(url, row_context.name, int(row_context.row_id))
                if success:
                    downloaded_files = [message]  # message contains downloaded filename
                    
            else:
                logger.warning(f"Unknown metadata type: {metadata_type}")
                return False, []
                
            if downloaded_files:
                self.stats['downloads_succeeded'] += 1
                return True, downloaded_files
            else:
                self.stats['downloads_failed'] += 1
                return False, []
                
        except Exception as e:
            logger.error(f"Error processing metadata: {e}")
            logger.error(traceback.format_exc())
            self.stats['downloads_failed'] += 1
            return False, []
    
    def update_csv_with_results(self, row_id: int, downloaded_files: List[str]) -> bool:
        """Update CSV with downloaded file information."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would update CSV row {row_id} with {len(downloaded_files)} files")
            return True
            
        try:
            if not downloaded_files:
                logger.warning(f"No files to update for row {row_id}")
                return False
                
            # DRY: Use CSVManager to find row
            row = self.csv_manager.find_row_by_id(row_id)
            if row is None:
                logger.error(f"Row {row_id} not found in CSV")
                return False
            
            # Prepare s3_paths and file_uuids mappings
            s3_paths = {}
            file_uuids = {}
            
            for s3_file in downloaded_files:
                # Extract UUID from s3 path (files/uuid.ext)
                filename = os.path.basename(s3_file)
                file_uuid = filename.split('.')[0] if '.' in filename else filename
                
                s3_paths[file_uuid] = s3_file
                file_uuids[filename] = file_uuid
            
            # Create backup before update
            backup_file = self.csv_manager.create_backup(f'metadata_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
            logger.info(f"Created CSV backup: {backup_file}")
            
            # DRY: Use CSVManager to update row
            updates = {
                's3_paths': CSVManager.save_s3_paths(s3_paths),
                'file_uuids': CSVManager.save_file_uuids(file_uuids)
            }
            
            if not self.csv_manager.update_row_by_id(row_id, updates):
                logger.error(f"Failed to update row {row_id}")
                return False
            
            logger.info(f"Updated CSV row {row_id} with {len(downloaded_files)} files")
            logger.info(f"  s3_paths: {CSVManager.save_s3_paths(s3_paths)}")
            logger.info(f"  file_uuids: {CSVManager.save_file_uuids(file_uuids)}")
            
            self.stats['csv_updated'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Error updating CSV: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def run(self, target_rows: Optional[List[int]] = None):
        """Run the metadata download process."""
        print_section_header("METADATA DOWNLOAD PROCESSOR")
        
        # Default target rows if not specified
        if target_rows is None:
            target_rows = [476, 477, 483, 484, 496]
            
        logger.info(f"Target rows: {target_rows}")
        logger.info(f"Dry run: {self.dry_run}")
        
        # Step 1: Verify CSV rows exist
        logger.info("Verifying CSV rows...")
        csv_data = self.verify_csv_rows(target_rows)
        logger.info(f"Found {len(csv_data)} of {len(target_rows)} target rows in CSV")
        
        # Step 2: Load metadata from S3
        logger.info("Loading metadata from S3...")
        metadata_list = self.load_metadata_from_s3()
        logger.info(f"Found {len(metadata_list)} metadata files")
        
        # Step 3: Filter metadata for target rows
        filtered_metadata = [
            m for m in metadata_list 
            if m.get('row_id') in target_rows
        ]
        logger.info(f"Found {len(filtered_metadata)} metadata files for target rows")
        
        # Step 4: Process each metadata file
        for metadata in filtered_metadata:
            row_id = metadata.get('row_id')
            person_name = metadata.get('person', 'Unknown')
            
            logger.info(f"\nProcessing {row_id} - {person_name}")
            
            # Check if already has media
            if self.check_existing_media(row_id):
                logger.info(f"Row {row_id} already has media files, skipping")
                continue
                
            # Get CSV row data
            if row_id not in csv_data:
                logger.warning(f"No CSV row found for {row_id}, skipping")
                continue
                
            # Process the metadata
            self.stats['downloads_attempted'] += 1
            success, files = self.process_metadata(metadata, csv_data[row_id])
            
            if success:
                # Update CSV with results
                self.update_csv_with_results(row_id, files)
                
                # Mark as processed
                self.progress['processed'].append(metadata['_s3_key'])
                self._save_progress()
            else:
                # Track failure
                self.progress['failed'][metadata['_s3_key']] = datetime.now().isoformat()
                self._save_progress()
        
        # Report statistics
        print_section_header("PROCESSING COMPLETE")
        logger.info(f"Metadata files found: {self.stats['metadata_found']}")
        logger.info(f"Downloads attempted: {self.stats['downloads_attempted']}")
        logger.info(f"Downloads succeeded: {self.stats['downloads_succeeded']}")
        logger.info(f"Downloads failed: {self.stats['downloads_failed']}")
        logger.info(f"CSV rows updated: {self.stats['csv_updated']}")
        

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Process pending metadata downloads from S3"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run in dry-run mode (no downloads, no updates)'
    )
    parser.add_argument(
        '--row-id',
        type=int,
        help='Process only a specific row ID'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Process all pending metadata (default is target rows only)'
    )
    
    args = parser.parse_args()
    
    # Determine target rows
    if args.row_id:
        target_rows = [args.row_id]
    elif args.all:
        target_rows = None  # Process all
    else:
        target_rows = [476, 477, 483, 484, 496]  # Default targets
    
    # Create processor and run
    processor = MetadataDownloadProcessor(dry_run=args.dry_run)
    processor.run(target_rows=target_rows)
    

if __name__ == "__main__":
    main()