#!/usr/bin/env python3
"""
CSV S3 Versioning - Automatically upload CSV files to S3 with timestamps
"""

import boto3
from datetime import datetime
from pathlib import Path
import json
from typing import Optional, Dict, Any

try:
    from .logging_config import get_logger
    from .config import get_config
except ImportError:
    from logging_config import get_logger
    from config import get_config

logger = get_logger(__name__)
config = get_config()


class CSVS3Versioning:
    """Handles CSV versioning in S3 with timestamp-based naming"""
    
    def __init__(self, bucket_name: Optional[str] = None, folder_prefix: str = "csv-versions"):
        """Initialize S3 versioning for CSV files
        
        Args:
            bucket_name: S3 bucket name (defaults to config)
            folder_prefix: Folder prefix in S3 for CSV versions
        """
        self.bucket_name = bucket_name or config.get('downloads.s3.default_bucket', 'typing-clients-uuid-system')
        self.folder_prefix = folder_prefix
        
        # Use profile-aware S3 client
        try:
            from .s3_manager import get_s3_client
            self.s3_client = get_s3_client()
        except ImportError:
            from s3_manager import get_s3_client
            self.s3_client = get_s3_client()
        
    def generate_versioned_name(self, original_path: Path) -> str:
        """Generate timestamped version name for CSV
        
        Args:
            original_path: Original CSV file path
            
        Returns:
            Timestamped filename like output_2025-07-21_143052.csv
        """
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        stem = original_path.stem
        suffix = original_path.suffix
        return f"{stem}_{timestamp}{suffix}"
    
    def generate_s3_key(self, versioned_name: str) -> str:
        """Generate S3 key for versioned CSV
        
        Args:
            versioned_name: Timestamped filename
            
        Returns:
            S3 key like csv-versions/2025/07/output_2025-07-21_143052.csv
        """
        # Organize by year/month for easier browsing
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        return f"{self.folder_prefix}/{year}/{month}/{versioned_name}"
    
    def upload_csv_version(self, local_csv_path: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Upload CSV to S3 with versioning
        
        Args:
            local_csv_path: Path to local CSV file
            metadata: Optional metadata to include
            
        Returns:
            Dict with upload details including S3 key and URL
        """
        local_path = Path(local_csv_path)
        
        if not local_path.exists():
            raise FileNotFoundError(f"CSV file not found: {local_csv_path}")
        
        # Generate versioned name and S3 key
        versioned_name = self.generate_versioned_name(local_path)
        s3_key = self.generate_s3_key(versioned_name)
        
        # Prepare metadata
        upload_metadata = {
            'original_filename': local_path.name,
            'upload_timestamp': datetime.now().isoformat(),
            'file_size': str(local_path.stat().st_size)
        }
        
        if metadata:
            upload_metadata.update(metadata)
        
        try:
            # Upload to S3
            logger.info(f"Uploading CSV version to S3: {s3_key}")
            
            self.s3_client.upload_file(
                str(local_path),
                self.bucket_name,
                s3_key,
                ExtraArgs={
                    'ContentType': 'text/csv',
                    'Metadata': upload_metadata
                }
            )
            
            # Generate S3 URL
            s3_url = f"https://{self.bucket_name}.s3.amazonaws.com/{s3_key}"
            
            result = {
                'success': True,
                'local_path': str(local_path),
                'versioned_name': versioned_name,
                's3_key': s3_key,
                's3_url': s3_url,
                'bucket': self.bucket_name,
                'timestamp': upload_metadata['upload_timestamp'],
                'file_size': int(upload_metadata['file_size'])
            }
            
            logger.info(f"✅ CSV version uploaded: {versioned_name}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to upload CSV version: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'local_path': str(local_path)
            }
    
    def list_csv_versions(self, prefix_filter: Optional[str] = None, limit: int = 100) -> list:
        """List CSV versions in S3
        
        Args:
            prefix_filter: Optional prefix to filter results (e.g., "output_")
            limit: Maximum number of versions to return
            
        Returns:
            List of CSV version details
        """
        prefix = self.folder_prefix
        if prefix_filter:
            prefix = f"{self.folder_prefix}/"
            
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=limit
            )
            
            versions = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    # Extract filename from key
                    filename = key.split('/')[-1]
                    
                    # Filter if needed
                    if prefix_filter and not filename.startswith(prefix_filter):
                        continue
                    
                    versions.append({
                        's3_key': key,
                        'filename': filename,
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat(),
                        'url': f"https://{self.bucket_name}.s3.amazonaws.com/{key}"
                    })
            
            # Sort by last modified (newest first)
            versions.sort(key=lambda x: x['last_modified'], reverse=True)
            return versions
            
        except Exception as e:
            logger.error(f"Failed to list CSV versions: {str(e)}")
            return []
    
    def get_latest_version(self, prefix_filter: str = "output_") -> Optional[Dict[str, Any]]:
        """Get the latest version of a CSV file
        
        Args:
            prefix_filter: Prefix to filter (e.g., "output_")
            
        Returns:
            Latest version details or None
        """
        versions = self.list_csv_versions(prefix_filter=prefix_filter, limit=1)
        return versions[0] if versions else None


# Singleton instance for easy access
_csv_versioning = None

def get_csv_versioning() -> CSVS3Versioning:
    """Get or create singleton CSV versioning instance"""
    global _csv_versioning
    if _csv_versioning is None:
        _csv_versioning = CSVS3Versioning()
    return _csv_versioning


def upload_csv_to_s3(csv_path: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Convenience function to upload CSV with versioning
    
    Args:
        csv_path: Path to CSV file
        metadata: Optional metadata
        
    Returns:
        Upload result dict
    """
    versioning = get_csv_versioning()
    return versioning.upload_csv_version(csv_path, metadata)