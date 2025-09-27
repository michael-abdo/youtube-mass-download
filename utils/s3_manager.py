#!/usr/bin/env python3
"""
Unified S3 Manager (DRY Consolidation)

Consolidates S3 upload functionality from:
- upload_to_s3.py
- upload_direct_to_s3.py

Provides both local-then-upload and direct-streaming capabilities.
"""

import os
import subprocess
import requests
import boto3
import pandas as pd
from pathlib import Path
from io import BytesIO
import tempfile
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union, Callable
from dataclasses import dataclass
from enum import Enum
import mimetypes

# DRY CONSOLIDATION: Simplified import pattern
try:
    from .config import get_config, get_s3_bucket
    from .logging_config import get_logger
    from .sanitization import sanitize_error_message
    from .database_manager import get_database_manager
    from .yt_dlp_updater import ensure_yt_dlp_updated, get_yt_dlp_command
except ImportError:
    from config import get_config, get_s3_bucket
    from logging_config import get_logger
    from sanitization import sanitize_error_message
    from database_manager import get_database_manager
    from yt_dlp_updater import ensure_yt_dlp_updated, get_yt_dlp_command


def get_s3_client(region_name: str = 'us-east-1') -> boto3.client:
    """
    Get standardized S3 client with centralized configuration (DRY consolidation).
    
    Consolidates the repeated pattern found in 91+ files:
        s3_client = boto3.client('s3')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3_client = boto3.client('s3', region_name=region)
    
    Args:
        region_name: AWS region name
        
    Returns:
        Configured boto3 S3 client
        
    Example:
        s3_client = get_s3_client()
        s3_client = get_s3_client('us-west-2')
    """
    try:
        # Get region and profile from config if available
        config = get_config()
        aws_region = config.get('aws_region', region_name)
        aws_profile = config.get('aws_profile', None)
    except Exception:
        aws_region = region_name
        aws_profile = None
    
    # Check environment variable for AWS profile
    import os
    aws_profile = os.environ.get('AWS_PROFILE', aws_profile)
    
    # Create session with profile if specified
    if aws_profile:
        session = boto3.Session(profile_name=aws_profile)
        return session.client('s3', region_name=aws_region)
    else:
        return boto3.client('s3', region_name=aws_region)


class UploadMode(Enum):
    """S3 upload mode options"""
    LOCAL_THEN_UPLOAD = "local_then_upload"
    DIRECT_STREAMING = "direct_streaming"
    HYBRID = "hybrid"


@dataclass
class S3Config:
    """Configuration for S3 operations"""
    bucket_name: str = None  # Will use get_s3_bucket() if None
    region: str = 'us-east-1'
    upload_mode: UploadMode = UploadMode.LOCAL_THEN_UPLOAD
    organize_by_person: bool = True
    add_metadata: bool = True
    update_csv: bool = True
    csv_file: str = 'outputs/output.csv'
    downloads_dir: str = 'downloads'
    create_public_urls: bool = True


@dataclass
class UploadResult:
    """Result of an S3 upload operation"""
    success: bool
    s3_key: str
    s3_url: Optional[str] = None
    error: Optional[str] = None
    file_size: Optional[int] = None
    upload_time: Optional[float] = None


class UnifiedS3Manager:
    """
    Unified S3 manager that handles both local-then-upload and direct streaming
    """
    
    def __init__(self, config: Optional[S3Config] = None):
        self.config = config or S3Config()
        self.logger = get_logger(__name__)
        
        # DRY: Use centralized S3 bucket configuration
        if self.config.bucket_name is None:
            self.config.bucket_name = get_s3_bucket()
        
        # DRY CONSOLIDATION: Use centralized S3 client initialization
        self.s3_client = get_s3_client(region_name=self.config.region)
        
        # Initialize paths
        self.downloads_dir = Path(self.config.downloads_dir)
        self.csv_file = self.config.csv_file
        
        # Upload tracking
        self.upload_report = {
            "started_at": datetime.now().isoformat(),
            "bucket": self.config.bucket_name,
            "mode": self.config.upload_mode.value,
            "uploads": [],
            "errors": []
        }
    
    def get_content_type(self, file_path: str) -> str:
        """Get MIME type for file"""
        content_type, _ = mimetypes.guess_type(file_path)
        if content_type:
            return content_type
        
        # Default types for common files
        file_ext = Path(file_path).suffix.lower()
        ext_mapping = {
            '.mp3': 'audio/mpeg',
            '.mp4': 'video/mp4',
            '.webm': 'video/webm',
            '.m4a': 'audio/mp4',
            '.json': 'application/json',
            '.txt': 'text/plain',
            '.csv': 'text/csv',
            '.pdf': 'application/pdf',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        
        return ext_mapping.get(file_ext, 'application/octet-stream')
    
    def generate_s3_key(self, row_id: int, person_name: str, filename: str) -> str:
        """Generate S3 key for organized storage"""
        if self.config.organize_by_person:
            # Clean person name for S3 key
            clean_name = re.sub(r'[^\w\s-]', '', person_name)
            clean_name = re.sub(r'[-\s]+', '_', clean_name)
            return f"{row_id}/{clean_name}/{filename}"
        else:
            return f"{row_id}/{filename}"
    
    @staticmethod
    def generate_uuid_s3_key(file_uuid: str, extension: str) -> str:
        """
        Generate S3 key for UUID-based storage (DRY CONSOLIDATION - Step 1).
        
        Centralizes the pattern `f"files/{file_uuid}{ext}"` found in:
        - core/process_pending_metadata_downloads.py:200
        - core/stream_folder_contents_direct.py:100
        - core/fix_s3_extensions.py:72-73
        - utils/streaming_integration.py:133
        
        Args:
            file_uuid: UUID for the file
            extension: File extension (with or without leading dot)
            
        Returns:
            S3 key in format "files/{uuid}{ext}"
            
        Example:
            s3_key = S3Manager.generate_uuid_s3_key("abc123", ".mp3")
            # Returns: "files/abc123.mp3"
        """
        from .constants import S3Constants
        
        # Ensure extension starts with dot
        if extension and not extension.startswith('.'):
            extension = f'.{extension}'
        
        return S3Constants.KEY_PATTERN_UUID_FILE.format(uuid=file_uuid, ext=extension)
    
    @staticmethod
    def parse_uuid_from_s3_key(s3_key: str) -> Tuple[str, str]:
        """
        Parse UUID and extension from S3 key (DRY CONSOLIDATION - Step 1).
        
        Centralizes UUID extraction logic scattered across files.
        
        Args:
            s3_key: S3 key like "files/abc123.mp3"
            
        Returns:
            Tuple of (uuid, extension) or (None, None) if invalid
            
        Example:
            uuid, ext = S3Manager.parse_uuid_from_s3_key("files/abc123.mp3")
            # Returns: ("abc123", ".mp3")
        """
        from .constants import S3Constants
        
        if not s3_key or not s3_key.startswith(S3Constants.UUID_FILES_PREFIX):
            return None, None
        
        # Remove prefix
        filename = s3_key[len(S3Constants.UUID_FILES_PREFIX):]
        
        # Split UUID and extension
        if '.' in filename:
            uuid_part, ext = filename.rsplit('.', 1)
            return uuid_part, f'.{ext}'
        else:
            return filename, ''
    
    def upload_file_to_s3(self, local_path: Union[str, Path], s3_key: str, 
                         content_type: Optional[str] = None) -> UploadResult:
        """Upload a local file to S3"""
        local_path = Path(local_path)
        
        if not local_path.exists():
            return UploadResult(success=False, s3_key=s3_key, error="Local file not found")
        
        if not content_type:
            content_type = self.get_content_type(str(local_path))
        
        try:
            start_time = datetime.now()
            
            # Upload with metadata
            extra_args = {'ContentType': content_type}
            if self.config.add_metadata:
                extra_args['Metadata'] = {
                    'uploaded_at': start_time.isoformat(),
                    'source': 'typing-clients-ingestion',
                    'original_filename': local_path.name
                }
            
            self.s3_client.upload_file(
                str(local_path),
                self.config.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            upload_time = (datetime.now() - start_time).total_seconds()
            file_size = local_path.stat().st_size
            
            # Generate public URL if requested
            s3_url = None
            if self.config.create_public_urls:
                s3_url = f"https://{self.config.bucket_name}.s3.amazonaws.com/{s3_key}"
            
            return UploadResult(
                success=True,
                s3_key=s3_key,
                s3_url=s3_url,
                file_size=file_size,
                upload_time=upload_time
            )
            
        except Exception as e:
            return UploadResult(
                success=False,
                s3_key=s3_key,
                error=sanitize_error_message(str(e))
            )
    
    def stream_youtube_to_s3(self, url: str, s3_key: str, person_name: str) -> UploadResult:
        """Stream YouTube directly to S3 using named pipe with thread-safe timeout"""
        import select
        import time
        import threading
        
        sanitized_name = "".join(c for c in person_name if c.isalnum() or c in '-_')[:20]
        pipe_path = f"/tmp/youtube_{sanitized_name}_{os.getpid()}_{threading.get_ident()}"
        process = None
        
        try:
            # Ensure yt-dlp is updated to latest version (if enabled in config)
            config = get_config()
            if config.get("downloads.youtube.auto_update_yt_dlp", True):
                self.logger.info("🔄 Ensuring yt-dlp is up to date...")
                ensure_yt_dlp_updated()
            else:
                self.logger.info("⏭️ Skipping yt-dlp update (disabled in config)")
            
            # Create named pipe
            if os.path.exists(pipe_path):
                os.remove(pipe_path)
            os.mkfifo(pipe_path)
            self.logger.info(f"🔧 PIPE_CREATE: {pipe_path}")
            
            self.logger.info(f"  📥 Streaming YouTube to S3: {s3_key}")
            
            # Start yt-dlp process - use best video format for mp4 with updated command
            cmd = get_yt_dlp_command(["-f", "best[ext=mp4]/best", "-o", pipe_path, url])
            self.logger.info(f"🚀 PROCESS_START: {' '.join(cmd)}")
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Wait for process to actually start and begin writing
            self.logger.info("⏳ Waiting for yt-dlp to start writing to pipe...")
            wait_attempts = 0
            max_wait_attempts = 30  # 15 seconds max
            
            while wait_attempts < max_wait_attempts:
                # Check if process died
                if process.poll() is not None:
                    stderr_output = process.stderr.read().decode()
                    raise RuntimeError(f"yt-dlp process died before writing to pipe. Error: {stderr_output[:200]}")
                
                # Check if pipe has data ready (non-blocking)
                try:
                    # Open pipe in non-blocking mode to test
                    pipe_fd = os.open(pipe_path, os.O_RDONLY | os.O_NONBLOCK)
                    ready, _, _ = select.select([pipe_fd], [], [], 0.1)  # 100ms timeout
                    os.close(pipe_fd)
                    
                    if ready:
                        self.logger.info("✅ PIPE_READY: yt-dlp started writing to pipe")
                        break
                except (OSError, BlockingIOError):
                    # Pipe not ready yet, continue waiting
                    pass
                
                time.sleep(0.5)
                wait_attempts += 1
                
                if wait_attempts % 10 == 0:  # Log every 5 seconds
                    self.logger.info(f"⏳ Still waiting for yt-dlp to start... ({wait_attempts/2:.1f}s)")
            
            if wait_attempts >= max_wait_attempts:
                raise TimeoutError(f"yt-dlp did not start writing to pipe within 15 seconds. URL may be invalid: {url}")
            
            # Upload from pipe to S3 with thread-safe timeout
            start_time = datetime.now()
            upload_completed = threading.Event()
            upload_exception = None
            
            def upload_with_timeout():
                nonlocal upload_exception
                try:
                    self.logger.info(f"📖 PIPE_OPEN_ATTEMPT: {pipe_path}")
                    
                    with open(pipe_path, 'rb') as pipe_file:
                        self.logger.info("✅ PIPE_OPEN_SUCCESS: Starting S3 upload")
                        extra_args = {'ContentType': 'video/mp4'}
                        if self.config.add_metadata:
                            extra_args['Metadata'] = {
                                'uploaded_at': start_time.isoformat(),
                                'source': 'typing-clients-ingestion-youtube-stream',
                                'original_url': url
                            }
                        
                        self.s3_client.upload_fileobj(
                            pipe_file,
                            self.config.bucket_name,
                            s3_key,
                            ExtraArgs=extra_args
                        )
                except Exception as e:
                    upload_exception = e
                finally:
                    upload_completed.set()
            
            # Start upload in a thread
            upload_thread = threading.Thread(target=upload_with_timeout)
            upload_thread.start()
            
            # Wait for upload with timeout
            if not upload_completed.wait(timeout=600):  # 10 minute timeout
                raise TimeoutError("S3 upload timed out after 10 minutes")
            
            # Check if upload had an exception
            if upload_exception:
                raise upload_exception
            
            # Wait for yt-dlp process to complete
            process.wait()
            upload_time = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(f"✅ PIPE_UPLOAD_COMPLETE: {upload_time:.1f}s")
            
            if process.returncode == 0:
                s3_url = f"https://{self.config.bucket_name}.s3.amazonaws.com/{s3_key}"
                return UploadResult(
                    success=True,
                    s3_key=s3_key,
                    s3_url=s3_url,
                    upload_time=upload_time
                )
            else:
                stdout_output = process.stdout.read().decode() if process.stdout else ""
                stderr_output = process.stderr.read().decode() if process.stderr else ""
                full_error = f"stdout: {stdout_output[:100]} stderr: {stderr_output[:100]}"
                self.logger.error(f"❌ yt-dlp failed with return code {process.returncode}: {full_error}")
                return UploadResult(
                    success=False,
                    s3_key=s3_key,
                    error=f"yt-dlp failed (code {process.returncode}): {stderr_output[:100]}"
                )
                
        except Exception as e:
            # Cleanup process
            try:
                if process and process.poll() is None:
                    self.logger.info("🛑 Terminating yt-dlp process due to error")
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
            except:
                pass  # Ignore cleanup errors
            
            self.logger.error(f"❌ PIPE_ERROR: {str(e)}")
            return UploadResult(
                success=False,
                s3_key=s3_key,
                error=sanitize_error_message(str(e))
            )
        finally:
            # Always cleanup pipe
            try:
                if os.path.exists(pipe_path):
                    self.logger.info(f"🧹 PIPE_CLEANUP: {pipe_path}")
                    os.remove(pipe_path)
            except Exception as cleanup_error:
                self.logger.warning(f"⚠️ Pipe cleanup failed: {cleanup_error}")
    
    def stream_drive_to_s3(self, drive_id: str, s3_key: str) -> UploadResult:
        """Stream Drive file directly to S3"""
        download_url = f"https://drive.google.com/uc?id={drive_id}&export=download"
        
        try:
            self.logger.info(f"  📁 Streaming Drive to S3: {s3_key}")
            
            start_time = datetime.now()
            session = requests.Session()
            
            # First request to get cookies and check for virus scan warning
            response = session.get(download_url, stream=True)
            response.raise_for_status()
            
            # Check if we got a virus scan warning page
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' in content_type:
                # Read the first chunk to check for virus scan warning
                first_chunk = next(response.iter_content(chunk_size=8192), b'')
                response_text = first_chunk.decode('utf-8', errors='ignore')
                
                if 'virus scan warning' in response_text.lower():
                    self.logger.info("Large file detected with virus scan warning, bypassing confirmation...")
                    
                    # Read the rest of the response to get the full HTML
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            response_text += chunk.decode('utf-8', errors='ignore')
                    
                    # Parse the confirmation parameters
                    confirm_match = re.search(r'name="confirm" value="([^"]*)"', response_text)
                    uuid_match = re.search(r'name="uuid" value="([^"]*)"', response_text)
                    
                    if confirm_match:
                        confirm_code = confirm_match.group(1)
                        
                        # Build the proper download URL with all parameters
                        download_params = {
                            'id': drive_id,
                            'export': 'download',
                            'confirm': confirm_code
                        }
                        
                        if uuid_match:
                            download_params['uuid'] = uuid_match.group(1)
                        
                        # Use drive.usercontent.google.com for direct downloads
                        direct_download_url = "https://drive.usercontent.google.com/download"
                        
                        # Make the download request with all parameters
                        response = session.get(direct_download_url, params=download_params, stream=True)
                        response.raise_for_status()
                    else:
                        return UploadResult(
                            success=False,
                            s3_key=s3_key,
                            error="Could not parse virus scan confirmation code"
                        )
                else:
                    # Not a virus scan warning, but still HTML - might be an error page
                    return UploadResult(
                        success=False,
                        s3_key=s3_key,
                        error="Received HTML response instead of file content"
                    )
            else:
                # Reset stream position if we didn't consume the response
                response = session.get(download_url, stream=True)
                response.raise_for_status()
            
            # Stream the file content to BytesIO
            file_obj = BytesIO()
            file_size = 0
            
            for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                if chunk:
                    file_obj.write(chunk)
                    file_size += len(chunk)
                    
                    # Log progress for large files
                    if file_size > 0 and file_size % (100 * 1024 * 1024) == 0:  # Every 100MB
                        self.logger.info(f"    Progress: {file_size / (1024 * 1024):.1f} MB downloaded...")
            
            file_obj.seek(0)
            
            # Determine content type from response headers
            response_content_type = response.headers.get('Content-Type', 'application/octet-stream')
            if response_content_type == 'application/octet-stream':
                # Try to guess from s3_key extension
                content_type = self.get_content_type(s3_key)
            else:
                content_type = response_content_type
            
            extra_args = {'ContentType': content_type}
            if self.config.add_metadata:
                extra_args['Metadata'] = {
                    'uploaded_at': start_time.isoformat(),
                    'source': 'typing-clients-ingestion-drive-stream',
                    'drive_id': drive_id,
                    'original_size': str(file_size)
                }
            
            self.s3_client.upload_fileobj(
                file_obj,
                self.config.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            upload_time = (datetime.now() - start_time).total_seconds()
            s3_url = f"https://{self.config.bucket_name}.s3.amazonaws.com/{s3_key}"
            
            self.logger.info(f"    ✅ Successfully uploaded {file_size / (1024 * 1024):.1f} MB to S3")
            
            return UploadResult(
                success=True,
                s3_key=s3_key,
                s3_url=s3_url,
                file_size=file_size,
                upload_time=upload_time
            )
            
        except Exception as e:
            return UploadResult(
                success=False,
                s3_key=s3_key,
                error=sanitize_error_message(str(e))
            )
    
    def upload_local_downloads(self) -> Dict[int, Dict]:
        """Upload all local downloads to S3"""
        self.logger.info("🚀 Starting Local Downloads Upload")
        
        person_s3_data = {}
        
        # Process each person's directory
        for person_dir in self.downloads_dir.iterdir():
            if not person_dir.is_dir():
                continue
            
            # Extract row_id and name
            dir_parts = person_dir.name.split('_', 1)
            if len(dir_parts) != 2:
                continue
            
            row_id = int(dir_parts[0])
            person_name = dir_parts[1]
            
            self.logger.info(f"\n📤 Uploading files for {person_name} (Row {row_id})")
            
            person_s3_data[row_id] = {
                'youtube_urls': [],
                'drive_urls': [],
                'all_files': []
            }
            
            # Upload each file
            for file_path in person_dir.iterdir():
                if file_path.is_file():
                    s3_key = self.generate_s3_key(row_id, person_name, file_path.name)
                    
                    self.logger.info(f"  📎 Uploading {file_path.name}...")
                    result = self.upload_file_to_s3(file_path, s3_key)
                    
                    if result.success:
                        self.logger.info(f"     ✅ Uploaded to S3")
                        
                        # Categorize by type
                        if 'youtube' in file_path.name:
                            person_s3_data[row_id]['youtube_urls'].append(result.s3_url)
                        elif 'drive' in file_path.name:
                            person_s3_data[row_id]['drive_urls'].append(result.s3_url)
                        
                        person_s3_data[row_id]['all_files'].append(result.s3_url)
                        
                        # Track in report
                        self.upload_report['uploads'].append({
                            'row_id': row_id,
                            'person': person_name,
                            'file': file_path.name,
                            's3_key': s3_key,
                            's3_url': result.s3_url,
                            'size': result.file_size,
                            'upload_time': result.upload_time
                        })
                    else:
                        self.logger.info(f"     ❌ Failed: {result.error}")
                        self.upload_report['errors'].append({
                            'row_id': row_id,
                            'person': person_name,
                            'file': file_path.name,
                            'error': result.error
                        })
        
        return person_s3_data
    
    def process_direct_streaming(self, csv_file: Optional[str] = None) -> Dict[int, List[str]]:
        """Process direct streaming uploads for all people"""
        if not csv_file:
            csv_file = self.config.csv_file
        
        self.logger.info("🚀 Starting Direct Streaming Upload")
        
        # DRY CONSOLIDATION: Use existing CSVManager instead of direct pandas
        from .csv_manager import CSVManager
        csv_mgr = CSVManager(csv_file)
        df = csv_mgr.read_csv_safe()
        person_s3_data = {}
        
        for _, row in df.iterrows():
            row_id = row['row_id']
            person_name = row['name'].replace(' ', '_')
            
            self.logger.info(f"\n📤 Direct streaming for {row['name']} (Row {row_id})")
            
            s3_urls = []
            
            # Process YouTube links
            youtube_links = self._extract_links(row, 'youtube_playlist')
            for i, url in enumerate(youtube_links):
                s3_key = self.generate_s3_key(row_id, person_name, f"youtube_direct_{i}.webm")
                result = self.stream_youtube_to_s3(url, s3_key, person_name)
                
                if result.success:
                    s3_urls.append(result.s3_url)
                    self.upload_report['uploads'].append({
                        'row_id': row_id,
                        'person': person_name,
                        'type': 'youtube_stream',
                        's3_key': s3_key,
                        's3_url': result.s3_url,
                        'upload_time': result.upload_time
                    })
                else:
                    self.upload_report['errors'].append({
                        'row_id': row_id,
                        'person': person_name,
                        'type': 'youtube_stream',
                        'error': result.error
                    })
            
            # Process Drive links
            drive_links = self._extract_links(row, 'google_drive')
            for i, url in enumerate(drive_links):
                # Extract Drive ID
                drive_id = self._extract_drive_id(url)
                if drive_id:
                    s3_key = self.generate_s3_key(row_id, person_name, f"drive_direct_{i}")
                    result = self.stream_drive_to_s3(drive_id, s3_key)
                    
                    if result.success:
                        s3_urls.append(result.s3_url)
                        self.upload_report['uploads'].append({
                            'row_id': row_id,
                            'person': person_name,
                            'type': 'drive_stream',
                            's3_key': s3_key,
                            's3_url': result.s3_url,
                            'file_size': result.file_size,
                            'upload_time': result.upload_time
                        })
                    else:
                        self.upload_report['errors'].append({
                            'row_id': row_id,
                            'person': person_name,
                            'type': 'drive_stream',
                            'error': result.error
                        })
            
            person_s3_data[row_id] = s3_urls
        
        return person_s3_data
    
    def sync_database_files(self, target_rows: Optional[List[int]] = None) -> Dict[int, List[str]]:
        """Sync files from database to ensure they exist in S3"""
        self.logger.info("🚀 Starting Database File Sync")
        
        db = get_database_manager()
        person_s3_data = {}
        
        if target_rows:
            # Process specific rows
            for row_id in target_rows:
                person = db.get_person_by_row_id(row_id)
                if person:
                    files = db.get_person_files(row_id)
                    s3_urls = []
                    
                    self.logger.info(f"\n📤 Checking files for {person['name']} (Row {row_id})")
                    self.logger.info(f"  📁 Found {len(files)} files in database")
                    
                    for file in files:
                        s3_key = file['storage_path']
                        
                        # Check if file exists in S3
                        if self.check_s3_exists(s3_key):
                            s3_url = f"https://{self.config.bucket_name}.s3.amazonaws.com/{s3_key}"
                            s3_urls.append(s3_url)
                            self.logger.info(f"  ✅ Verified: {file['original_filename']}")
                        else:
                            self.logger.warning(f"  ⚠️  Missing in S3: {s3_key}")
                            self.upload_report['errors'].append({
                                'row_id': row_id,
                                'person': person['name'],
                                'file_id': str(file['file_id']),
                                'error': 'File not found in S3'
                            })
                    
                    person_s3_data[row_id] = s3_urls
        else:
            # Process all people with files
            summary = db.get_person_file_summary()
            
            for entry in summary:
                if entry['total_files'] > 0:
                    row_id = entry['row_id']
                    files = db.get_person_files(row_id)
                    s3_urls = []
                    
                    self.logger.info(f"\n📤 Checking files for {entry['name']} (Row {row_id})")
                    self.logger.info(f"  📁 Found {len(files)} files in database")
                    
                    for file in files:
                        s3_key = file['storage_path']
                        
                        # Check if file exists in S3
                        if self.check_s3_exists(s3_key):
                            s3_url = f"https://{self.config.bucket_name}.s3.amazonaws.com/{s3_key}"
                            s3_urls.append(s3_url)
                            self.logger.info(f"  ✅ Verified: {file['original_filename']}")
                        else:
                            self.logger.warning(f"  ⚠️  Missing in S3: {s3_key}")
                            self.upload_report['errors'].append({
                                'row_id': row_id,
                                'person': entry['name'],
                                'file_id': str(file['file_id']),
                                'error': 'File not found in S3'
                            })
                    
                    person_s3_data[row_id] = s3_urls
        
        return person_s3_data
    
    def check_s3_exists(self, s3_key: str) -> bool:
        """Check if an object exists in S3"""
        try:
            self.s3_client.head_object(Bucket=self.config.bucket_name, Key=s3_key)
            return True
        except:
            return False
    
    def _extract_links(self, row: pd.Series, column: str) -> List[str]:
        """Extract links from CSV row"""
        # DRY CONSOLIDATION: Use url_utils for link parsing
        from .url_utils import parse_url_links
        return parse_url_links(str(row.get(column, '')))
    
    def _extract_drive_id(self, url: str) -> Optional[str]:
        """Extract Drive ID from URL"""
        # DRY CONSOLIDATION: Use url_utils for Drive ID extraction
        from .url_utils import extract_drive_id
        return extract_drive_id(url)
    
    def update_csv_with_s3_urls(self, person_s3_data: Dict, csv_file: Optional[str] = None):
        """Update CSV with S3 URLs"""
        if not csv_file:
            csv_file = self.config.csv_file
        
        self.logger.info("\n📊 Updating CSV with S3 URLs...")
        
        # Read existing CSV
        # DRY CONSOLIDATION: Use existing CSVManager instead of direct pandas
        from .csv_manager import CSVManager
        csv_mgr = CSVManager(csv_file)
        df = csv_mgr.read_csv_safe()
        
        # Add new columns if they don't exist
        for col in ['s3_youtube_urls', 's3_drive_urls', 's3_all_files']:
            if col not in df.columns:
                df[col] = ''
        
        # Update each row with S3 URLs
        for row_id, urls_data in person_s3_data.items():
            mask = df['row_id'] == row_id
            if mask.any():
                if isinstance(urls_data, dict):
                    # Local upload format
                    df.loc[mask, 's3_youtube_urls'] = '|'.join(urls_data.get('youtube_urls', []))
                    df.loc[mask, 's3_drive_urls'] = '|'.join(urls_data.get('drive_urls', []))
                    df.loc[mask, 's3_all_files'] = '|'.join(urls_data.get('all_files', []))
                else:
                    # Direct streaming format (list of URLs)
                    df.loc[mask, 's3_all_files'] = '|'.join(urls_data)
        
        # Save updated CSV
        # DRY CONSOLIDATION: Use CSVManager for consistent CSV writing
        csv_mgr.write_csv(df)
        self.logger.info("✅ CSV updated with S3 URLs")
        
        return df
    
    def save_report(self, report_file: Optional[str] = None):
        """Save upload report"""
        if not report_file:
            report_file = f's3_upload_report_{self.config.upload_mode.value}.json'
        
        self.upload_report['completed_at'] = datetime.now().isoformat()
        
        # DRY CONSOLIDATION: Use json_utils for consistent JSON writing
        from .json_utils import write_json_safe
        write_json_safe(report_file, self.upload_report)
        
        self.logger.info(f"\n📄 Upload report saved to {report_file}")
    
    def run_upload_process(self, mode: Optional[UploadMode] = None) -> Dict:
        """Run the complete upload process"""
        if mode:
            self.config.upload_mode = mode
        
        self.logger.info(f"🚀 Starting S3 Upload Process")
        self.logger.info(f"🪣 Bucket: {self.config.bucket_name}")
        self.logger.info(f"🔄 Mode: {self.config.upload_mode.value}")
        self.logger.info("=" * 70)
        
        # Execute based on mode
        if self.config.upload_mode == UploadMode.LOCAL_THEN_UPLOAD:
            person_s3_data = self.upload_local_downloads()
        elif self.config.upload_mode == UploadMode.DIRECT_STREAMING:
            person_s3_data = self.process_direct_streaming()
        else:
            raise ValueError(f"Unsupported upload mode: {self.config.upload_mode}")
        
        # Update CSV if requested
        if self.config.update_csv:
            self.update_csv_with_s3_urls(person_s3_data)
        
        # Save report
        self.save_report()
        
        # Print summary
        self._print_summary()
        
        return person_s3_data
    
    def _print_summary(self):
        """Print upload summary"""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("📊 UPLOAD SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"✅ Files uploaded: {len(self.upload_report['uploads'])}")
        self.logger.info(f"❌ Errors: {len(self.upload_report['errors'])}")
        
        # Calculate total size
        total_size = sum(
            u.get('size', 0) for u in self.upload_report['uploads'] 
            if u.get('size')
        )
        if total_size > 0:
            self.logger.info(f"💾 Total size uploaded: {total_size / (1024**3):.2f} GB")
        
        # Calculate average upload time
        upload_times = [
            u.get('upload_time', 0) for u in self.upload_report['uploads'] 
            if u.get('upload_time')
        ]
        if upload_times:
            avg_time = sum(upload_times) / len(upload_times)
            self.logger.info(f"⏱️ Average upload time: {avg_time:.2f} seconds")


def create_local_uploader(bucket_name: str = None) -> UnifiedS3Manager:
    """Create S3 manager for local-then-upload mode"""
    config = S3Config(
        bucket_name=bucket_name,
        upload_mode=UploadMode.LOCAL_THEN_UPLOAD,
        organize_by_person=True,
        update_csv=True
    )
    return UnifiedS3Manager(config)


def create_streaming_uploader(bucket_name: str = None) -> UnifiedS3Manager:
    """Create S3 manager for direct streaming mode"""
    config = S3Config(
        bucket_name=bucket_name,
        upload_mode=UploadMode.DIRECT_STREAMING,
        organize_by_person=True,
        update_csv=True
    )
    return UnifiedS3Manager(config)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Unified S3 Upload Manager')
    parser.add_argument('--bucket', default=None, help='S3 bucket name (uses config default if not specified)')
    parser.add_argument('--mode', choices=['local', 'streaming', 'database'], default='local', help='Upload mode')
    parser.add_argument('--csv', default='outputs/output.csv', help='CSV file to update')
    parser.add_argument('--downloads-dir', default='downloads', help='Downloads directory')
    parser.add_argument('--no-csv-update', action='store_true', help='Skip CSV update')
    parser.add_argument('--rows', type=str, help='Specific row IDs to process (comma-separated)')
    parser.add_argument('--use-database', action='store_true', help='Use database mode for file sync')
    
    args = parser.parse_args()
    
    # Create manager based on mode
    if args.mode == 'database' or args.use_database:
        # Database mode - sync files from database
        config = S3Config(
            bucket_name=args.bucket,
            upload_mode=UploadMode.LOCAL_THEN_UPLOAD,  # Not used for database sync
            organize_by_person=True,
            update_csv=False  # Database mode doesn't update CSV
        )
        manager = UnifiedS3Manager(config)
        
        # Parse target rows if provided
        target_rows = None
        if args.rows:
            target_rows = [int(row.strip()) for row in args.rows.split(',')]
        
        # Run database sync
        results = manager.sync_database_files(target_rows=target_rows)
        
        # Save report
        manager.save_report()
        manager._print_summary()
    else:
        # Traditional modes
        if args.mode == 'local':
            manager = create_local_uploader(args.bucket)
        else:
            manager = create_streaming_uploader(args.bucket)
        
        # Override config if needed
        manager.config.csv_file = args.csv
        manager.config.downloads_dir = args.downloads_dir
        manager.config.update_csv = not args.no_csv_update
        
        # Run upload process
        results = manager.run_upload_process()


# ============================================================================
# UNIFIED S3 OPERATIONS (DRY ITERATION 3 - Step 4)
# ============================================================================

class UnifiedS3Operations:
    """
    Unified S3 operations to eliminate duplication across codebase.
    
    CONSOLIDATES PATTERNS FROM:
    - 91+ files with s3_client = boto3.client('s3') variations
    - 30+ locations with different S3 upload implementations
    - Multiple S3 key generation patterns
    - Inconsistent error handling for S3 operations
    
    BUSINESS IMPACT: Prevents S3 operation failures and ensures consistent behavior
    """
    
    # Singleton S3 client instance
    _s3_client = None
    _bucket_name = None
    
    @classmethod
    def get_client(cls) -> boto3.client:
        """
        Get singleton S3 client instance.
        
        ELIMINATES 91+ boto3.client('s3') initializations.
        
        Returns:
            Shared S3 client instance
            
        Example:
            s3 = UnifiedS3Operations.get_client()
            s3.list_objects_v2(Bucket='my-bucket')
        """
        if cls._s3_client is None:
            cls._s3_client = get_s3_client()
        return cls._s3_client
    
    @classmethod
    def get_bucket_name(cls) -> str:
        """Get configured S3 bucket name."""
        if cls._bucket_name is None:
            cls._bucket_name = get_s3_bucket()
        return cls._bucket_name
    
    @classmethod
    def upload_file(cls, local_path: Union[str, Path], s3_key: str,
                   bucket_name: Optional[str] = None,
                   metadata: Optional[Dict[str, str]] = None,
                   public_read: bool = False) -> UploadResult:
        """
        Unified file upload with consistent error handling.
        
        CONSOLIDATES 30+ S3 upload implementations.
        
        Args:
            local_path: Path to local file
            s3_key: S3 object key
            bucket_name: S3 bucket (uses default if None)
            metadata: Optional object metadata
            public_read: Whether to make object publicly readable
            
        Returns:
            UploadResult with status and details
            
        Example:
            result = UnifiedS3Operations.upload_file('video.mp4', 'files/uuid.mp4')
        """
        from utils.error_handling import s3_retry
        
        bucket_name = bucket_name or cls.get_bucket_name()
        local_path = Path(local_path)
        
        if not local_path.exists():
            return UploadResult(
                success=False,
                s3_key=s3_key,
                error=f"File not found: {local_path}"
            )
        
        start_time = datetime.now()
        
        try:
            # Get file size
            file_size = local_path.stat().st_size
            
            # Prepare upload args
            upload_args = {
                'Filename': str(local_path),
                'Bucket': bucket_name,
                'Key': s3_key
            }
            
            # Add metadata if provided
            if metadata:
                upload_args['ExtraArgs'] = {'Metadata': metadata}
            
            # Add public read ACL if requested
            if public_read:
                if 'ExtraArgs' not in upload_args:
                    upload_args['ExtraArgs'] = {}
                upload_args['ExtraArgs']['ACL'] = 'public-read'
            
            # Upload with retry
            s3_client = cls.get_client()
            s3_retry.retry_operation(
                s3_client.upload_file,
                **upload_args,
                operation_name=f"Upload {s3_key}"
            )
            
            # Generate URL
            s3_url = None
            if public_read:
                s3_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
            
            upload_time = (datetime.now() - start_time).total_seconds()
            
            return UploadResult(
                success=True,
                s3_key=s3_key,
                s3_url=s3_url,
                file_size=file_size,
                upload_time=upload_time
            )
            
        except Exception as e:
            return UploadResult(
                success=False,
                s3_key=s3_key,
                error=f"Upload failed: {str(e)}"
            )
    
    @classmethod
    def upload_fileobj(cls, file_obj: BytesIO, s3_key: str,
                      bucket_name: Optional[str] = None,
                      content_type: Optional[str] = None) -> UploadResult:
        """
        Upload file object (for streaming).
        
        Args:
            file_obj: File-like object
            s3_key: S3 object key
            bucket_name: S3 bucket
            content_type: MIME content type
            
        Returns:
            UploadResult
        """
        from utils.error_handling import s3_retry
        
        bucket_name = bucket_name or cls.get_bucket_name()
        start_time = datetime.now()
        
        try:
            # Prepare upload args
            upload_args = {
                'Fileobj': file_obj,
                'Bucket': bucket_name,
                'Key': s3_key
            }
            
            if content_type:
                upload_args['ExtraArgs'] = {'ContentType': content_type}
            
            # Upload with retry
            s3_client = cls.get_client()
            s3_retry.retry_operation(
                s3_client.upload_fileobj,
                **upload_args,
                operation_name=f"Stream upload {s3_key}"
            )
            
            upload_time = (datetime.now() - start_time).total_seconds()
            
            return UploadResult(
                success=True,
                s3_key=s3_key,
                upload_time=upload_time
            )
            
        except Exception as e:
            return UploadResult(
                success=False,
                s3_key=s3_key,
                error=f"Stream upload failed: {str(e)}"
            )
    
    @classmethod
    def generate_uuid_s3_key(cls, extension: str, folder: str = "files") -> str:
        """
        Generate standardized UUID-based S3 key.
        
        CONSOLIDATES PATTERN: f"files/{uuid}{ext}" repeated in 10+ files
        
        Args:
            extension: File extension (with or without dot)
            folder: S3 folder (default: "files")
            
        Returns:
            S3 key like "files/123e4567-e89b-12d3-a456-426614174000.mp4"
            
        Example:
            s3_key = UnifiedS3Operations.generate_uuid_s3_key('.mp4')
        """
        import uuid
        
        # Normalize extension
        if not extension.startswith('.'):
            extension = f'.{extension}'
        extension = extension.lower()
        
        # Generate UUID
        file_uuid = str(uuid.uuid4())
        
        # Construct key
        return f"{folder}/{file_uuid}{extension}"
    
    @classmethod
    def check_exists(cls, s3_key: str, bucket_name: Optional[str] = None) -> bool:
        """
        Check if S3 object exists.
        
        Args:
            s3_key: S3 object key
            bucket_name: S3 bucket
            
        Returns:
            True if object exists
        """
        bucket_name = bucket_name or cls.get_bucket_name()
        s3_client = cls.get_client()
        
        try:
            s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            return True
        except:
            return False
    
    @classmethod
    def batch_upload(cls, file_mappings: List[Tuple[str, str]],
                    bucket_name: Optional[str] = None,
                    progress_callback: Optional[Callable] = None) -> List[UploadResult]:
        """
        Batch upload multiple files with progress tracking.
        
        Args:
            file_mappings: List of (local_path, s3_key) tuples
            bucket_name: S3 bucket
            progress_callback: Optional callback(current, total, filename)
            
        Returns:
            List of UploadResult objects
            
        Example:
            files = [('video1.mp4', 'files/uuid1.mp4'), ('video2.mp4', 'files/uuid2.mp4')]
            results = UnifiedS3Operations.batch_upload(files)
        """
        results = []
        total = len(file_mappings)
        
        for i, (local_path, s3_key) in enumerate(file_mappings):
            if progress_callback:
                progress_callback(i + 1, total, Path(local_path).name)
            
            result = cls.upload_file(local_path, s3_key, bucket_name)
            results.append(result)
        
        return results


# ============================================================================
# S3 KEY GENERATION HELPERS
# ============================================================================

def generate_s3_key_for_file(file_path: Union[str, Path], 
                           person_name: Optional[str] = None,
                           use_uuid: bool = True) -> str:
    """
    Generate S3 key for a file with consistent format.
    
    CONSOLIDATES multiple S3 key generation patterns.
    
    Args:
        file_path: Path to file
        person_name: Optional person name for organization
        use_uuid: Whether to use UUID (True) or preserve filename
        
    Returns:
        S3 key
        
    Example:
        key = generate_s3_key_for_file('video.mp4', 'John Doe')
        # Returns: "files/123e4567-e89b.mp4" or "John Doe/video.mp4"
    """
    file_path = Path(file_path)
    
    if use_uuid:
        # UUID-based key
        return UnifiedS3Operations.generate_uuid_s3_key(file_path.suffix)
    else:
        # Name-based key
        if person_name:
            # Sanitize person name for S3
            safe_name = re.sub(r'[^a-zA-Z0-9\s\-_]', '', person_name).strip()
            safe_name = re.sub(r'\s+', '_', safe_name)
            return f"{safe_name}/{file_path.name}"
        else:
            return f"files/{file_path.name}"


# ============================================================================
# MIGRATION HELPERS
# ============================================================================

# Global S3 client for backward compatibility
_global_s3_client = None

def get_global_s3_client():
    """
    DEPRECATED: Use UnifiedS3Operations.get_client() instead.
    
    Backward compatibility function.
    """
    global _global_s3_client
    if _global_s3_client is None:
        _global_s3_client = UnifiedS3Operations.get_client()
    return _global_s3_client