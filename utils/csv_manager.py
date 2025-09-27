#!/usr/bin/env python3
"""
CSV Manager - Unified CSV operations with atomic, streaming, tracking, and integrity capabilities
Consolidates functionality from csv_tracker.py, atomic_csv.py, streaming_csv.py, 
csv_file_integrity_mapper.py, and fix_csv_file_mappings.py
"""

import os
import csv
import pandas as pd
import json
import shutil
import tempfile
import gzip
import glob
import re
import warnings
from pathlib import Path
from typing import List, Optional, Dict, Any, Callable, Union, Tuple
from datetime import datetime
from contextlib import contextmanager
from dataclasses import dataclass

try:
    from .file_lock import file_lock
    from .sanitization import sanitize_error_message, sanitize_csv_field
    from .config import get_config
    from .row_context import RowContext, DownloadResult
    # Consolidated error handling imports
    from .error_handling import (
        handle_file_operations, 
        handle_validation_errors,
        csv_error, 
        file_error, 
        validation_error
    )
    from .logging_config import get_logger
    # Import CSV S3 versioning
    from .csv_s3_versioning import get_csv_versioning
except ImportError:
    from .file_lock import file_lock
    from .sanitization import sanitize_error_message, sanitize_csv_field
    from .config import get_config
    from .row_context import RowContext, DownloadResult
    # Consolidated error handling imports
    from .error_handling import (
        handle_file_operations,
        handle_validation_errors,
        csv_error,
        file_error,
        validation_error
    )
    from .logging_config import get_logger
    # Import CSV S3 versioning
    from .csv_s3_versioning import get_csv_versioning

# Setup module logger
logger = get_logger(__name__)

# DRY CONSOLIDATION: CSV Column Schema Definitions
CSV_SCHEMA = {
    'required_columns': ['row_id', 'name', 'processed'],
    'tracking_columns': ['s3_paths', 'file_uuids', 'download_errors', 'last_download_attempt'],
    'youtube_columns': ['youtube_playlist', 'youtube_status', 'youtube_files', 'youtube_media_id'],
    'drive_columns': ['google_drive', 'drive_status', 'drive_files', 'drive_media_id'], 
    'optional_columns': ['document_text', 'extracted_links', 'permanent_failure']
}

# Get configuration
config = get_config()


def safe_get_na_value(column_name: str = None, dtype: str = 'string') -> Any:
    """
    Get the appropriate null/NA value for a given column and data type.
    This ensures consistent null representation across the entire CSV.
    
    Args:
        column_name: Name of the column (for column-specific rules if needed)
        dtype: The pandas dtype of the column ('string', 'float', 'int', etc.)
        
    Returns:
        The appropriate null value (None for most cases, which becomes NaN in pandas)
    """
    # For all string columns, use None (which becomes NaN in pandas)
    if dtype in ['string', 'object', str]:
        return None
    # For numeric types, also use None (becomes NaN)
    elif dtype in ['float', 'int', 'float64', 'int64']:
        return None
    # For any other type, default to None
    else:
        return None


class CSVManager:
    """Unified CSV operations manager with atomic, streaming, tracking, and integrity capabilities"""
    
    def __init__(self, 
                 csv_path: str = None,
                 chunk_size: int = 1000, 
                 use_file_lock: bool = True, 
                 auto_backup: bool = True,
                 timeout: float = 30.0,
                 encoding: str = 'utf-8'):
        """Initialize CSV manager with configurable defaults"""
        if csv_path is None:
            csv_path = get_config().get('paths.output_csv', 'outputs/output.csv')
        self.csv_path = Path(csv_path)
        self.chunk_size = chunk_size
        self.use_file_lock = use_file_lock
        self.auto_backup = auto_backup
        self.timeout = timeout
        self.encoding = encoding
        
        # Initialize tracking state
        self._df_cache = None
        self._last_modified = None
    
    # === CORE OPERATIONS ===
    
    @staticmethod
    def safe_csv_read(csv_path: str, dtype_spec: str = 'tracking') -> pd.DataFrame:
        """
        Standardized CSV reading with consistent dtype specifications.
        
        Args:
            csv_path: Path to CSV file
            dtype_spec: Predefined dtype specification ('tracking', 'basic', 'all_string')
            
        Returns:
            DataFrame with appropriate dtypes applied
        """
        dtype_specs = {
            'tracking': {
                'row_id': 'string',
                'name': 'string', 
                'email': 'string',
                'type': 'string',
                'link': 'string',
                'youtube_status': 'string',
                'drive_status': 'string',
                'youtube_error': 'string',
                'drive_error': 'string'
            },
            'basic': {
                'row_id': 'string',
                'name': 'string',
                'email': 'string', 
                'type': 'string'
            },
            'all_string': 'string'
        }
        
        dtype = dtype_specs.get(dtype_spec, 'string')
        
        try:
            return pd.read_csv(csv_path, dtype=dtype, na_values=[''], keep_default_na=False)
        except Exception as e:
            logger.error(csv_error('CSV_READ_ERROR', path=csv_path, error=str(e)))
            raise
    
    def read(self, dtype_spec: str = 'tracking') -> pd.DataFrame:
        """
        Instance method to read the CSV using the manager's csv_path.
        
        Args:
            dtype_spec: dtype specification ('tracking', 'all_string', 'infer')
            
        Returns:
            DataFrame with loaded CSV data
        """
        return self.safe_csv_read(str(self.csv_path), dtype_spec)
    
    @handle_file_operations("CSV write operation")
    def safe_csv_write(self, df: pd.DataFrame, operation_name: str = "write", 
                      expected_columns: Optional[List[str]] = None) -> bool:
        """
        Standardized CSV writing with validation and backup.
        
        Args:
            df: DataFrame to write
            operation_name: Name of the operation for backup naming
            expected_columns: Expected columns for validation
            
        Returns:
            True if successful, False otherwise
        """
        if self.auto_backup and self.csv_path.exists():
            backup_path = self.create_backup(operation_name)
            logger.debug(f"Created backup: {backup_path}")
        
        try:
            # Validate expected columns if provided
            if expected_columns:
                missing_cols = set(expected_columns) - set(df.columns)
                if missing_cols:
                    raise ValueError(csv_error('COLUMN_MISSING', column=', '.join(missing_cols)))
            
            # Write with file locking
            if self.use_file_lock:
                with file_lock(str(self.csv_path), timeout=self.timeout):
                    df.to_csv(self.csv_path, index=False, encoding=self.encoding)
            else:
                df.to_csv(self.csv_path, index=False, encoding=self.encoding)
            
            # Upload CSV version to S3 automatically
            try:
                # Check if S3 storage is enabled
                if config.get('downloads.storage_mode', 'local') == 's3':
                    versioning = get_csv_versioning()
                    metadata = {
                        'operation': operation_name,
                        'row_count': str(len(df)),
                        'column_count': str(len(df.columns))
                    }
                    upload_result = versioning.upload_csv_version(str(self.csv_path), metadata)
                    if upload_result['success']:
                        logger.info(f"CSV version uploaded to S3: {upload_result['versioned_name']}")
                    else:
                        logger.warning(f"Failed to upload CSV version to S3: {upload_result.get('error')}")
            except Exception as s3_error:
                # Don't fail the operation if S3 upload fails
                logger.warning(f"CSV S3 versioning error (non-fatal): {str(s3_error)}")
            
            # Clear cache
            self._df_cache = None
            self._last_modified = None
            
            return True
            
        except Exception as e:
            logger.error(csv_error('CSV_WRITE_ERROR', path=str(self.csv_path), error=str(e)))
            return False
    
    # === ATOMIC OPERATIONS (from atomic_csv.py) ===
    
    @contextmanager
    def atomic_context(self, mode: str = 'w', fieldnames: Optional[List[str]] = None):
        """Context manager for atomic CSV operations"""
        temp_file = None
        lock_context = None
        
        try:
            # Create backup if requested
            if self.auto_backup and self.csv_path.exists() and mode in ['w', 'r+']:
                backup_path = self.create_backup("atomic_operation")
                logger.debug(f"Created atomic backup: {backup_path}")
            
            # Create temporary file
            temp_fd, temp_path = tempfile.mkstemp(suffix='.csv', dir=self.csv_path.parent)
            temp_file = Path(temp_path)
            
            # Set up file locking if enabled
            if self.use_file_lock:
                lock_context = file_lock(str(self.csv_path), timeout=self.timeout)
                lock_context.__enter__()
            
            # Create CSV writer
            with open(temp_file, mode, encoding=self.encoding, newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames) if fieldnames else csv.writer(f)
                
                # Write header if fieldnames provided
                if fieldnames and hasattr(writer, 'writeheader'):
                    writer.writeheader()
                
                yield writer
            
            # Atomically move temp file to final location
            shutil.move(str(temp_file), str(self.csv_path))
            
        except Exception as e:
            # Clean up temp file on error
            if temp_file and temp_file.exists():
                temp_file.unlink()
            logger.error(csv_error('CSV_WRITE_ERROR', path=str(self.csv_path), error=str(e)))
            raise
        finally:
            # Release lock
            if lock_context:
                lock_context.__exit__(None, None, None)
            
            # Clear cache
            self._df_cache = None
    
    @handle_file_operations("Atomic CSV write")
    def atomic_write(self, rows: List[Dict], fieldnames: Optional[List[str]] = None) -> bool:
        """Write rows atomically to CSV"""
        try:
            if not fieldnames and rows:
                fieldnames = list(rows[0].keys())
            
            with self.atomic_context(mode='w', fieldnames=fieldnames) as writer:
                for row in rows:
                    # Sanitize all fields
                    sanitized_row = {k: sanitize_csv_field(v) for k, v in row.items()}
                    writer.writerow(sanitized_row)
            
            return True
        except Exception as e:
            logger.error(csv_error('CSV_WRITE_ERROR', path=str(self.csv_path), error=str(e)))
            return False
    
    @handle_file_operations("Atomic CSV append")
    def atomic_append(self, rows: List[Dict], fieldnames: Optional[List[str]] = None) -> bool:
        """Append rows atomically to CSV"""
        try:
            # Read existing data if file exists
            existing_rows = []
            if self.csv_path.exists():
                df = self.safe_csv_read(str(self.csv_path))
                existing_rows = df.to_dict('records')
                if not fieldnames:
                    fieldnames = list(df.columns)
            elif not fieldnames and rows:
                fieldnames = list(rows[0].keys())
            
            # Combine existing and new rows
            all_rows = existing_rows + rows
            
            return self.atomic_write(all_rows, fieldnames)
            
        except Exception as e:
            logger.error(csv_error('CSV_WRITE_ERROR', path=str(self.csv_path), error=str(e)))
            return False
    
    # === STREAMING OPERATIONS (from streaming_csv.py) ===
    
    @handle_file_operations("Streaming CSV processing")
    def stream_process(self, process_func: Callable, output_path: Optional[str] = None, 
                      fieldnames: Optional[List[str]] = None, has_header: bool = True) -> int:
        """
        Process CSV file in chunks using streaming approach.
        
        Args:
            process_func: Function that takes a list of rows and returns processed rows
            output_path: Output file path (defaults to input path)
            fieldnames: CSV field names
            has_header: Whether CSV has header row
            
        Returns:
            Number of rows processed
        """
        if output_path is None:
            output_path = str(self.csv_path)
        
        output_path = Path(output_path)
        rows_processed = 0
        
        # Create backup if processing in place
        if output_path == self.csv_path and self.auto_backup:
            backup_path = self.create_backup("stream_process")
            logger.debug(f"Created streaming backup: {backup_path}")
        
        # Create temporary output file
        temp_fd, temp_path = tempfile.mkstemp(suffix='.csv', dir=output_path.parent)
        temp_file = Path(temp_path)
        
        try:
            with open(self.csv_path, 'r', encoding=self.encoding) as infile:
                reader = csv.DictReader(infile) if has_header else csv.reader(infile)
                
                # Get fieldnames
                if has_header and not fieldnames:
                    fieldnames = reader.fieldnames
                
                with open(temp_file, 'w', encoding=self.encoding, newline='') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames) if fieldnames else csv.writer(outfile)
                    
                    # Write header
                    if fieldnames and hasattr(writer, 'writeheader'):
                        writer.writeheader()
                    
                    # Process in chunks
                    chunk = []
                    for row in reader:
                        chunk.append(row)
                        
                        if len(chunk) >= self.chunk_size:
                            processed_chunk = process_func(chunk)
                            for processed_row in processed_chunk:
                                writer.writerow(processed_row)
                            rows_processed += len(processed_chunk)
                            chunk = []
                    
                    # Process final chunk
                    if chunk:
                        processed_chunk = process_func(chunk)
                        for processed_row in processed_chunk:
                            writer.writerow(processed_row)
                        rows_processed += len(processed_chunk)
            
            # Atomically move temp file to final location
            shutil.move(str(temp_file), str(output_path))
            
            # Clear cache if processing in place
            if output_path == self.csv_path:
                self._df_cache = None
            
            return rows_processed
            
        except Exception as e:
            # Clean up temp file on error
            if temp_file.exists():
                temp_file.unlink()
            logger.error(csv_error('CSV_WRITE_ERROR', path=str(output_path), error=str(e)))
            raise
    
    @handle_file_operations("Streaming CSV filter")
    def stream_filter(self, filter_func: Callable, output_path: str) -> int:
        """Filter CSV using streaming approach"""
        def process_chunk(chunk):
            return [row for row in chunk if filter_func(row)]
        
        return self.stream_process(process_chunk, output_path)
    
    # === TRACKING OPERATIONS (from csv_tracker.py) ===
    
    def ensure_tracking_columns(self) -> bool:
        """Ensure CSV has all required tracking columns"""
        required_columns = [
            'row_id', 'name', 'email', 'type', 'link',
            'youtube_status', 'drive_status', 'youtube_error', 'drive_error'
        ]
        
        try:
            if not self.csv_path.exists():
                # Create new CSV with required columns
                df = pd.DataFrame(columns=required_columns)
                return self.safe_csv_write(df, "ensure_tracking_columns")
            
            df = self.safe_csv_read(str(self.csv_path))
            missing_columns = set(required_columns) - set(df.columns)
            
            if missing_columns:
                for col in missing_columns:
                    df[col] = safe_get_na_value(col, 'string')
                return self.safe_csv_write(df, "add_tracking_columns")
            
            return True
            
        except Exception as e:
            logger.error(csv_error('CSV_VALIDATION_FAILED', path=str(self.csv_path), details=str(e)))
            return False
    
    def get_pending_downloads(self, download_type: str = 'both', 
                            include_failed: bool = True, retry_attempts: int = 3) -> List[RowContext]:
        """Get list of pending downloads"""
        try:
            df = self.safe_csv_read(str(self.csv_path))
            pending_rows = []
            
            for _, row in df.iterrows():
                row_context = RowContext(
                    row_id=str(row.get('row_id', '')),
                    name=str(row.get('name', '')),
                    email=str(row.get('email', '')),
                    type=str(row.get('type', '')),
                    link=str(row.get('link', ''))
                )
                
                # Check YouTube status
                if download_type in ['both', 'youtube']:
                    youtube_status = str(row.get('youtube_status', ''))
                    if (youtube_status in ['pending', ''] or 
                        (include_failed and youtube_status == 'failed')):
                        pending_rows.append(row_context)
                        continue
                
                # Check Drive status
                if download_type in ['both', 'drive']:
                    drive_status = str(row.get('drive_status', ''))
                    if (drive_status in ['pending', ''] or 
                        (include_failed and drive_status == 'failed')):
                        pending_rows.append(row_context)
            
            return pending_rows
            
        except Exception as e:
            logger.error(csv_error('CSV_READ_ERROR', path=str(self.csv_path), error=str(e)))
            return []
    
    @handle_file_operations("Update download status")
    def update_download_status(self, row_index: int, download_type: str, 
                             result: DownloadResult) -> bool:
        """Update download status for a specific row"""
        try:
            df = self.safe_csv_read(str(self.csv_path))
            
            if row_index >= len(df):
                logger.error(f"Row index {row_index} out of bounds")
                return False
            
            # Update status columns
            status_col = f'{download_type}_status'
            error_col = f'{download_type}_error'
            
            df.loc[row_index, status_col] = result.status
            if result.error_message:
                df.loc[row_index, error_col] = sanitize_csv_field(result.error_message)
            
            return self.safe_csv_write(df, f"update_{download_type}_status")
            
        except Exception as e:
            logger.error(csv_error('CSV_WRITE_ERROR', path=str(self.csv_path), error=str(e)))
            return False
    
    def get_download_status_summary(self) -> Dict[str, Any]:
        """Get summary of download statuses"""
        try:
            df = self.safe_csv_read(str(self.csv_path))
            
            summary = {
                'total_rows': len(df),
                'youtube': {
                    'pending': len(df[df['youtube_status'].isin(['pending', ''])]),
                    'completed': len(df[df['youtube_status'] == 'completed']),
                    'failed': len(df[df['youtube_status'] == 'failed'])
                },
                'drive': {
                    'pending': len(df[df['drive_status'].isin(['pending', ''])]),
                    'completed': len(df[df['drive_status'] == 'completed']),
                    'failed': len(df[df['drive_status'] == 'failed'])
                }
            }
            
            return summary
            
        except Exception as e:
            logger.error(csv_error('CSV_READ_ERROR', path=str(self.csv_path), error=str(e)))
            return {}
    
    # Note: File integrity and mapping operations are handled by FileMapper
    # Use utils.comprehensive_file_mapper.FileMapper for:
    # - analyze_file_integrity() -> mapper.validate_mappings()
    # - identify_unmapped_files() -> mapper.get_unmapped_files()
    # - find_mismatched_mappings() -> mapper.find_mapping_conflicts()
    # - find_orphaned_files() -> mapper.fix_orphaned_files()
    
    # === RECORD FACTORY OPERATIONS (DRY) ===
    
    @staticmethod
    def create_record(person: Dict[str, Any], mode: str = 'basic', 
                     doc_text: str = '', links: Optional[Dict[str, List[str]]] = None,
                     s3_uuids: Optional[Dict[str, Dict]] = None) -> Dict[str, str]:
        """
        Factory function to create CSV records in different modes (DRY).
        
        Args:
            person: Dictionary with basic person info (row_id, name, email, type, link)
            mode: Record mode - 'basic', 'text', or 'full'
            doc_text: Document text content (for text/full modes)
            links: Dictionary of extracted links (for full mode)
                  Should contain: 'youtube', 'drive_files', 'drive_folders', 'all_links'
            s3_uuids: Dictionary with S3 UUID mappings (for full mode with streaming)
                  Should contain: 'file_uuids' and 's3_paths' dictionaries
                  
        Returns:
            Dictionary with appropriate fields for the specified mode
        """
        # Basic record (5 columns)
        record = {
            'row_id': person.get('row_id', ''),
            'name': person.get('name', ''),
            'email': person.get('email', ''),
            'type': person.get('type', ''),
            'link': person.get('doc_link', person.get('link', ''))
        }
        
        if mode == 'basic':
            return record
        
        # Text mode: add document text and processing info
        if mode == 'text':
            record.update({
                'document_text': doc_text,
                'processed': 'yes',
                'extraction_date': datetime.now().isoformat()
            })
            return record
        
        # Full mode: add all columns
        if mode == 'full':
            # Handle links data
            if links:
                youtube_links = links.get('youtube', [])
                drive_files = links.get('drive_files', [])
                drive_folders = links.get('drive_folders', [])
                all_links = links.get('all_links', [])
                
                # Combine drive files and folders
                google_drive_links = drive_files + drive_folders
            else:
                youtube_links = []
                google_drive_links = []
                all_links = []
            
            # Handle S3 UUIDs if provided (from streaming integration)
            if s3_uuids:
                file_uuids = s3_uuids.get('file_uuids', {})
                s3_paths = s3_uuids.get('s3_paths', {})
                
                record.update({
                    'extracted_links': '|'.join(all_links) if all_links else '',
                    'youtube_playlist': '|'.join(youtube_links) if youtube_links else '',
                    'google_drive': '|'.join(google_drive_links) if google_drive_links else '',
                    'processed': 'yes',
                    'document_text': doc_text,
                    'youtube_status': 'streamed' if any('YouTube' in desc for desc in file_uuids) else '',
                    'youtube_files': '',
                    'youtube_media_id': '',
                    'drive_status': 'streamed' if any('Drive' in desc for desc in file_uuids) else '',
                    'drive_files': '',
                    'drive_media_id': '',
                    'last_download_attempt': '',
                    'download_errors': '',
                    'permanent_failure': '',
                    # S3 UUID mappings
                    'file_uuids': json.dumps(file_uuids) if file_uuids else '{}',
                    's3_paths': json.dumps(s3_paths) if s3_paths else '{}'
                })
            else:
                # Original behavior without S3 streaming
                record.update({
                    'extracted_links': '|'.join(all_links) if all_links else '',
                    'youtube_playlist': '|'.join(youtube_links) if youtube_links else '',
                    'google_drive': '|'.join(google_drive_links) if google_drive_links else '',
                    'processed': 'yes',
                    'document_text': doc_text,
                    'youtube_status': '',
                    'youtube_files': '',
                    'youtube_media_id': '',
                    'drive_status': '',
                    'drive_files': '',
                    'drive_media_id': '',
                    'last_download_attempt': '',
                    'download_errors': '',
                    'permanent_failure': '',
                    # Empty S3 mappings
                    'file_uuids': '{}',
                    's3_paths': '{}'
                })
            return record
        
        # Default to basic if mode not recognized
        logger.warning(f"Unknown record mode: {mode}. Defaulting to basic.")
        return record
    
    @staticmethod
    def create_error_record(person: Dict[str, Any], mode: str = 'text', 
                           error_message: str = '') -> Dict[str, str]:
        """
        Create a record for failed extraction cases (DRY).
        
        Args:
            person: Dictionary with basic person info
            mode: Record mode - 'text' or 'full'
            error_message: Error message to include
            
        Returns:
            Dictionary with error information
        """
        if mode == 'text':
            return CSVManager.create_record(
                person, 
                mode='text', 
                doc_text=f"EXTRACTION_FAILED: {error_message}"
            )
        else:
            # For full mode, still include error in document_text
            return CSVManager.create_record(
                person,
                mode='full',
                doc_text=f"EXTRACTION_FAILED: {error_message}",
                links=None
            )
    
    # === S3 PATH/UUID JSON OPERATIONS (DRY) ===
    
    @staticmethod
    def load_s3_paths(row: pd.Series) -> Dict[str, str]:
        """
        Load s3_paths from CSV row with proper JSON parsing.
        
        Args:
            row: DataFrame row
            
        Returns:
            Dictionary of UUID to S3 path mappings
        """
        s3_paths = row.get('s3_paths', '{}')
        if not s3_paths or s3_paths in ['{}', '[]', None, 'nan']:
            return {}
        try:
            return json.loads(s3_paths)
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Invalid s3_paths JSON: {s3_paths}")
            return {}
    
    @staticmethod
    def load_file_uuids(row: pd.Series) -> Dict[str, str]:
        """
        Load file_uuids from CSV row with proper JSON parsing.
        
        Args:
            row: DataFrame row
            
        Returns:
            Dictionary of UUID to file description mappings
        """
        file_uuids = row.get('file_uuids', '{}')
        if not file_uuids or file_uuids in ['{}', '[]', None, 'nan']:
            return {}
        try:
            return json.loads(file_uuids)
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Invalid file_uuids JSON: {file_uuids}")
            return {}
    
    @staticmethod
    def save_s3_paths(s3_paths: Dict[str, str]) -> str:
        """
        Convert s3_paths dictionary to JSON string for CSV storage.
        
        Args:
            s3_paths: Dictionary of UUID to S3 path mappings
            
        Returns:
            JSON string representation
        """
        return json.dumps(s3_paths) if s3_paths else '{}'
    
    @staticmethod
    def save_file_uuids(file_uuids: Dict[str, str]) -> str:
        """
        Convert file_uuids dictionary to JSON string for CSV storage.
        
        Args:
            file_uuids: Dictionary of UUID to file description mappings
            
        Returns:
            JSON string representation
        """
        return json.dumps(file_uuids) if file_uuids else '{}'
    
    def update_s3_mappings(self, row_id: int, s3_paths: Dict[str, str], 
                          file_uuids: Dict[str, str]) -> bool:
        """
        Update s3_paths and file_uuids for a specific row.
        
        Args:
            row_id: Row ID to update
            s3_paths: New s3_paths dictionary
            file_uuids: New file_uuids dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            df = self.read_csv_safe()
            mask = df['row_id'].astype(str) == str(row_id)
            
            if not df[mask].empty:
                df.loc[mask, 's3_paths'] = self.save_s3_paths(s3_paths)
                df.loc[mask, 'file_uuids'] = self.save_file_uuids(file_uuids)
                self.write_csv(df)
                return True
            else:
                logger.warning(f"Row ID {row_id} not found in CSV")
                return False
        except Exception as e:
            logger.error(f"Error updating S3 mappings: {e}")
            return False
    
    # === CSV ROW OPERATIONS (DRY) ===
    
    def find_row_by_id(self, row_id: Union[int, str]) -> Optional[pd.Series]:
        """
        Find a row by row_id.
        
        Args:
            row_id: Row ID to find
            
        Returns:
            Row as pandas Series or None if not found
        """
        # DRY: Use centralized row ID validation
        from .validation import validate_row_id
        is_valid, validated_id = validate_row_id(row_id)
        
        if not is_valid:
            logger.warning(f"Invalid row ID provided: {row_id}")
            return None
        
        df = self.read_csv_safe()
        mask = df['row_id'].astype(str) == str(validated_id)
        
        if mask.any():
            return df[mask].iloc[0]
        return None
    
    def find_rows_by_criteria(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """
        Find rows matching multiple criteria.
        
        Args:
            criteria: Dictionary of column:value pairs to match
            
        Returns:
            DataFrame with matching rows
        """
        df = self.read_csv_safe()
        mask = pd.Series([True] * len(df))
        
        for column, value in criteria.items():
            if column in df.columns:
                mask &= (df[column] == value)
        
        return df[mask]
    
    def update_row_by_id(self, row_id: Union[int, str], updates: Dict[str, Any]) -> bool:
        """
        Update a row by row_id with given values.
        
        Args:
            row_id: Row ID to update
            updates: Dictionary of column:value pairs to update
            
        Returns:
            True if successful, False otherwise
        """
        try:
            df = self.read_csv_safe()
            mask = df['row_id'].astype(str) == str(row_id)
            
            if not mask.any():
                logger.warning(f"Row ID {row_id} not found in CSV")
                return False
            
            # Get the index of the row
            row_index = df.index[mask].tolist()[0]
            
            # Update each field
            for column, value in updates.items():
                if column in df.columns:
                    df.at[row_index, column] = value
                else:
                    logger.warning(f"Column '{column}' not found in CSV")
            
            # Save the updated dataframe
            self.write_csv(df)
            return True
            
        except Exception as e:
            logger.error(f"Error updating row {row_id}: {e}")
            return False
    
    def iterate_rows(self, filter_func=None):
        """
        Iterator for rows with optional filtering.
        
        Args:
            filter_func: Optional function to filter rows
            
        Yields:
            Tuple of (index, row) for each matching row
        """
        df = self.read_csv_safe()
        
        for idx, row in df.iterrows():
            if filter_func is None or filter_func(row):
                yield idx, row
    
    # === CSV SCHEMA OPERATIONS (DRY) ===
    
    @staticmethod
    def get_standard_columns() -> List[str]:
        """Get standard CSV columns for consistent schema"""
        return (CSV_SCHEMA['required_columns'] + 
                CSV_SCHEMA['tracking_columns'] + 
                CSV_SCHEMA['youtube_columns'] + 
                CSV_SCHEMA['drive_columns'] + 
                CSV_SCHEMA['optional_columns'])
    
    @staticmethod
    def get_required_columns() -> List[str]:
        """Get required columns that must be present"""
        return CSV_SCHEMA['required_columns']
    
    @staticmethod 
    def get_columns_by_type(column_type: str) -> List[str]:
        """Get columns by type (required, tracking, youtube, drive, optional)"""
        return CSV_SCHEMA.get(f'{column_type}_columns', [])
    
    def validate_schema(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate DataFrame has required columns"""
        missing_cols = []
        required = self.get_required_columns()
        
        for col in required:
            if col not in df.columns:
                missing_cols.append(col)
        
        return len(missing_cols) == 0, missing_cols
    
    # === BACKUP & UTILITY OPERATIONS ===
    
    def create_backup(self, operation_name: str = "backup") -> str:
        """Create a compressed backup of the CSV file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = self.csv_path.parent / 'backups' / 'output'
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        backup_filename = f"output_{timestamp}_{operation_name}.csv.gz"
        backup_path = backup_dir / backup_filename
        
        # Compress and save backup
        with open(self.csv_path, 'rb') as f_in:
            with gzip.open(backup_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        return str(backup_path)


# === STANDALONE FUNCTIONS FOR BACKWARD COMPATIBILITY ===

def create_csv_backup(csv_path: str, operation_name: str = "backup") -> str:
    """Create a backup of the CSV file (backward compatibility function)"""
    manager = CSVManager(csv_path=csv_path)
    return manager.create_backup(operation_name)


def safe_csv_read(csv_path: str, dtype_spec: str = 'tracking') -> pd.DataFrame:
    """Standardized CSV reading (backward compatibility function)"""
    return CSVManager.safe_csv_read(csv_path, dtype_spec)


def safe_csv_write(df: pd.DataFrame, csv_path: str, operation_name: str = "write", 
                  expected_columns: Optional[List[str]] = None) -> bool:
    """Standardized CSV writing (backward compatibility function)"""
    manager = CSVManager(csv_path=csv_path)
    return manager.safe_csv_write(df, operation_name, expected_columns)


# === CONTEXT MANAGERS FOR BACKWARD COMPATIBILITY ===

@contextmanager
def atomic_csv_update(filename, fieldnames=None, encoding='utf-8', use_lock=True, timeout=30.0):
    """Backward compatibility context manager for atomic CSV updates"""
    manager = CSVManager(csv_path=filename, use_file_lock=use_lock, timeout=timeout, encoding=encoding)
    with manager.atomic_context(mode='w', fieldnames=fieldnames) as writer:
        yield writer


@contextmanager
def streaming_csv_update(filename, process_func, chunk_size=1000, fieldnames=None, 
                        encoding='utf-8', use_lock=True, timeout=30.0):
    """Backward compatibility context manager for streaming CSV updates"""
    manager = CSVManager(csv_path=filename, chunk_size=chunk_size, use_file_lock=use_lock, 
                        timeout=timeout, encoding=encoding)
    rows_processed = manager.stream_process(process_func, fieldnames=fieldnames)
    yield rows_processed


# === DRY PHASE 2: CONSOLIDATED CSV PATTERNS ===

@staticmethod
def load_output_csv(csv_path: str = 'outputs/output.csv') -> pd.DataFrame:
    """
    Standard way to load the main output CSV with consistent error handling.
    
    Consolidates the repeated pattern found in 15+ files:
        import pandas as pd
        df = pd.read_csv('outputs/output.csv')
    
    Args:
        csv_path: Path to CSV file (default: outputs/output.csv)
        
    Returns:
        DataFrame with loaded CSV data
        
    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ValueError: If CSV file is empty or malformed
    """
    try:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Output CSV not found: {csv_path}")
        
        df = pd.read_csv(csv_path)
        
        if df.empty:
            raise ValueError(f"CSV file is empty: {csv_path}")
        
        logger.info(f"Loaded {len(df)} rows from {csv_path}")
        return df
        
    except pd.errors.EmptyDataError:
        raise ValueError(f"CSV file is empty or malformed: {csv_path}")
    except pd.errors.ParserError as e:
        raise ValueError(f"CSV parsing error in {csv_path}: {e}")
    except Exception as e:
        raise ValueError(f"Error loading CSV {csv_path}: {e}")


@staticmethod
def extract_links_from_row(row: pd.Series, column: str) -> List[str]:
    """
    Extract and clean links from CSV row - eliminates 3-line pattern.
    
    Consolidates the repeated pattern found in 15+ files:
        youtube_links = str(row.get('youtube_playlist', '')).split('|')
        youtube_links = [l.strip() for l in youtube_links if l and l != 'nan']
    
    Args:
        row: Pandas Series (CSV row)
        column: Column name to extract links from
        
    Returns:
        List of cleaned links
        
    Example:
        youtube_links = CSVManager.extract_links_from_row(row, 'youtube_playlist')
        drive_links = CSVManager.extract_links_from_row(row, 'google_drive')
    """
    if pd.isna(row.get(column)):
        return []
    
    # Get column value and split by pipe delimiter
    links = str(row.get(column, '')).split('|')
    
    # Clean and filter links
    cleaned_links = []
    for link in links:
        link = link.strip()
        # Filter out empty, 'nan', and 'None' values
        if link and link not in ['nan', 'None', '']:
            cleaned_links.append(link)
    
    return cleaned_links


@staticmethod
def extract_all_links_from_row(row: pd.Series) -> Dict[str, List[str]]:
    """
    Extract all types of links from a CSV row.
    
    Args:
        row: Pandas Series (CSV row)
        
    Returns:
        Dictionary with link types as keys and lists of links as values
        
    Example:
        links = CSVManager.extract_all_links_from_row(row)
        youtube_links = links['youtube']
        drive_links = links['drive']
    """
    return {
        'youtube': CSVManager.extract_links_from_row(row, 'youtube_playlist'),
        'drive': CSVManager.extract_links_from_row(row, 'google_drive'),
        's3_youtube': CSVManager.extract_links_from_row(row, 's3_youtube_urls'),
        's3_drive': CSVManager.extract_links_from_row(row, 's3_drive_urls'),
        's3_all': CSVManager.extract_links_from_row(row, 's3_all_files')
    }


def get_standard_csv_path() -> str:
    """Get the standard output CSV path from configuration."""
    try:
        from config import get_default_csv_file
        return get_default_csv_file()
    except ImportError:
        return 'outputs/output.csv'


# === CONVENIENCE FUNCTIONS FOR COMMON PATTERNS ===

def load_and_filter_csv(csv_path: str = None, 
                       has_youtube: bool = False,
                       has_drive: bool = False,
                       row_ids: Optional[List[int]] = None) -> pd.DataFrame:
    """
    Load CSV and filter for common patterns.
    
    Args:
        csv_path: CSV file path (default: from config)
        has_youtube: Filter to rows with YouTube links
        has_drive: Filter to rows with Drive links  
        row_ids: Filter to specific row IDs
        
    Returns:
        Filtered DataFrame
    """
    if csv_path is None:
        csv_path = get_standard_csv_path()
    
    df = CSVManager.load_output_csv(csv_path)
    
    # Apply filters
    if row_ids is not None:
        df = df[df['row_id'].isin(row_ids)]
    
    if has_youtube:
        df = df[df.apply(lambda row: len(CSVManager.extract_links_from_row(row, 'youtube_playlist')) > 0, axis=1)]
    
    if has_drive:
        df = df[df.apply(lambda row: len(CSVManager.extract_links_from_row(row, 'google_drive')) > 0, axis=1)]
    
    return df


def count_links_by_type(csv_path: str = None) -> Dict[str, int]:
    """
    Count total links by type across entire CSV.
    
    Args:
        csv_path: CSV file path (default: from config)
        
    Returns:
        Dictionary with link counts by type
    """
    if csv_path is None:
        csv_path = get_standard_csv_path()
    
    df = CSVManager.load_output_csv(csv_path)
    
    counts = {
        'youtube': 0,
        'drive': 0,
        'total_people': len(df),
        'people_with_youtube': 0,
        'people_with_drive': 0
    }
    
    for _, row in df.iterrows():
        youtube_links = CSVManager.extract_links_from_row(row, 'youtube_playlist')
        drive_links = CSVManager.extract_links_from_row(row, 'google_drive')
        
        counts['youtube'] += len(youtube_links)
        counts['drive'] += len(drive_links)
        
        if youtube_links:
            counts['people_with_youtube'] += 1
        if drive_links:
            counts['people_with_drive'] += 1
    
    return counts


if __name__ == "__main__":
    # Example usage and testing
    print("=== CSV Manager Test ===")
    
    # Test CSV manager initialization
    manager = CSVManager('test_output.csv')
    print(f"Initialized CSV manager for: {manager.csv_path}")
    
    # Test ensuring tracking columns
    if manager.ensure_tracking_columns():
        print("✅ Tracking columns ensured")
    
    # Test status summary
    summary = manager.get_download_status_summary()
    print(f"📊 Status summary: {summary}")
    
    # Test new DRY Phase 2 functions
    print("\n=== Testing DRY Phase 2 Functions ===")
    
    try:
        # Test load_output_csv
        df = CSVManager.load_output_csv('outputs/output.csv')
        print(f"✅ Loaded {len(df)} rows from output CSV")
        
        # Test extract_links_from_row with first row
        if not df.empty:
            first_row = df.iloc[0]
            youtube_links = CSVManager.extract_links_from_row(first_row, 'youtube_playlist')
            drive_links = CSVManager.extract_links_from_row(first_row, 'google_drive')
            print(f"✅ Extracted {len(youtube_links)} YouTube and {len(drive_links)} Drive links from first row")
            
            # Test extract_all_links_from_row
            all_links = CSVManager.extract_all_links_from_row(first_row)
            print(f"✅ Extracted all links: {sum(len(links) for links in all_links.values())} total")
        
        # Test count_links_by_type
        link_counts = count_links_by_type()
        print(f"✅ Link counts: {link_counts}")
        
    except Exception as e:
        print(f"⚠️ Could not test with actual CSV (expected in test environment): {e}")
    
    print("📋 CSV Manager ready with DRY consolidation!")

# ============================================================================
# SIMPLIFIED CSV OPERATIONS API (DRY ITERATION 1 - Step 2)
# ============================================================================

def process_csv_safely(csv_path: str, process_func: Callable[[Dict], Dict],
                      filter_func: Optional[Callable[[Dict], bool]] = None,
                      backup: bool = True) -> Tuple[bool, int, Optional[str]]:
    """
    Safely process a CSV file with automatic locking, backup, and error handling.
    
    CONSOLIDATES THE PATTERN found in 20+ utility scripts that do:
        with open(csv, 'r') as f:
            reader = csv.DictReader(f)
            with open(temp, 'w') as out:
                writer = csv.DictWriter(out)
                for row in reader:
                    # process row
                    writer.writerow(row)
    
    Args:
        csv_path: Path to CSV file
        process_func: Function that takes a row dict and returns modified row dict
        filter_func: Optional function to filter rows (return True to keep row)
        backup: Whether to create backup before processing
        
    Returns:
        Tuple of (success, rows_processed, error_message)
        
    Example:
        # Replace this pattern:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            # ... process rows ...
            
        # With this:
        def update_links(row):
            if 'drive.google.com' in row.get('link', ''):
                row['google_drive'] = row['link']
            return row
            
        success, count, error = process_csv_safely(
            'output.csv',
            update_links,
            filter_func=lambda row: row.get('status') == 'pending'
        )
    """
    manager = CSVManager(csv_path, auto_backup=backup)
    rows_processed = 0
    
    try:
        # Read all rows
        df = manager.read()
        if df.empty:
            return True, 0, None
        
        # Convert to list of dicts for processing
        rows = df.to_dict('records')
        processed_rows = []
        
        for row in rows:
            # Apply filter if provided
            if filter_func and not filter_func(row):
                processed_rows.append(row)
                continue
            
            # Process row
            try:
                processed_row = process_func(row)
                processed_rows.append(processed_row)
                rows_processed += 1
            except Exception as e:
                logger.error(f"Error processing row {row.get('row_id', 'unknown')}: {e}")
                processed_rows.append(row)  # Keep original on error
        
        # Write back atomically
        success = manager.atomic_write(processed_rows)
        return success, rows_processed, None
        
    except Exception as e:
        error_msg = f"CSV processing failed: {str(e)}"
        logger.error(error_msg)
        return False, rows_processed, error_msg


def update_csv_column(csv_path: str, column_name: str, 
                     value_func: Callable[[Dict], Any],
                     condition_func: Optional[Callable[[Dict], bool]] = None) -> Tuple[bool, int]:
    """
    Update a specific column in CSV based on a function.
    
    CONSOLIDATES PATTERN of updating single columns found in multiple scripts.
    
    Args:
        csv_path: Path to CSV file
        column_name: Column to update
        value_func: Function that takes row and returns new column value
        condition_func: Optional condition for which rows to update
        
    Returns:
        Tuple of (success, rows_updated)
        
    Example:
        # Update google_drive column for rows with drive links
        success, count = update_csv_column(
            'output.csv',
            'google_drive',
            lambda row: extract_drive_links(row['extracted_links']),
            lambda row: 'drive.google.com' in row.get('link', '')
        )
    """
    def process_row(row):
        if condition_func is None or condition_func(row):
            row[column_name] = value_func(row)
        return row
    
    success, count, _ = process_csv_safely(csv_path, process_row)
    return success, count
