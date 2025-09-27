#!/usr/bin/env python3
"""
Row Context - Core data structures for row-centric download tracking
Maintains perfect traceability between downloaded files and CSV rows
"""

import os
import json
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from datetime import datetime

# Import centralized state management (DRY)
try:
    from config import save_json_state, load_json_state
except ImportError:
    from .config import save_json_state, load_json_state


@dataclass
class RowContext:
    """Context object that travels with every download to maintain CSV row relationship"""
    row_id: str          # Primary key from CSV
    row_index: int       # Position in CSV for atomic updates  
    type: str           # Personality type - CRITICAL to preserve
    name: str           # Person name for human-readable tracking
    email: str          # Additional identifier
    
    def to_metadata_dict(self) -> Dict[str, Any]:
        """Embed row context in download metadata files"""
        return {
            'source_csv_row_id': self.row_id,
            'source_csv_index': self.row_index, 
            'personality_type': self.type,
            'person_name': self.name,
            'person_email': self.email,
            'download_timestamp': datetime.now().isoformat(),
            'tracking_version': '1.0'
        }
    
    def to_filename_suffix(self) -> str:
        """Create unique filename suffix for organization"""
        # Clean type for filename safety
        clean_type = self.type.replace('/', '-').replace(' ', '_').replace('#', 'num').replace('(', '').replace(')', '')
        return f"_row{self.row_id}_{clean_type}"
    
    def to_safe_name_prefix(self) -> str:
        """Create safe filename prefix from person name"""
        # Clean name for filename safety
        safe_name = ''.join(c for c in self.name if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_name = safe_name.replace(' ', '_')[:20]  # Limit length
        return safe_name


@dataclass 
class DownloadResult:
    """Standardized result object maintaining full traceability"""
    success: bool
    files_downloaded: List[str]     # Actual filenames created
    media_id: Optional[str]         # YouTube video_id or Drive file_id
    error_message: Optional[str]
    metadata_file: Optional[str]    # Path to metadata file
    row_context: RowContext         # Preserve complete source data
    download_type: str              # 'youtube' or 'drive'
    permanent_failure: bool = False # Mark as permanent failure to skip retries
    
    def get_summary(self) -> Dict[str, Any]:
        """Generate summary for CSV update"""
        return {
            'status': 'completed' if self.success else 'failed',
            'files': ','.join(self.files_downloaded),
            'media_id': self.media_id or '',
            'error': self.error_message or '',
            'last_attempt': datetime.now().isoformat()
        }
    
    def save_metadata(self, downloads_dir: str) -> str:
        """Save metadata file with complete context"""
        if not self.metadata_file:
            return ""
            
        metadata_path = os.path.join(downloads_dir, self.metadata_file)
        
        # DRY CONSOLIDATION: Use path_utils for directory creation
        from .path_utils import ensure_directory
        ensure_directory(downloads_dir)
        
        metadata = {
            'download_result': {
                'success': self.success,
                'files_downloaded': self.files_downloaded,
                'media_id': self.media_id,
                'error_message': self.error_message,
                'download_type': self.download_type
            },
            **self.row_context.to_metadata_dict()
        }
        
        # Use centralized state management (DRY)
        save_json_state(metadata_path, metadata)
        if os.path.exists(metadata_path):
            return metadata_path
        else:
            print(f"Warning: Could not save metadata file {metadata_path}")
            return ""


def create_row_context_from_csv_row(row, row_index: int) -> RowContext:
    """Create RowContext from pandas DataFrame row"""
    return RowContext(
        row_id=str(row['row_id']),
        row_index=row_index,
        type=str(row['type']),
        name=str(row['name']),
        email=str(row['email'])
    )


def load_row_context_from_metadata(metadata_file_path: str) -> Optional[RowContext]:
    """Load RowContext from metadata file"""
    # Use centralized state management (DRY)
    metadata = load_json_state(metadata_file_path, default=None)
    if metadata:
        try:
            return RowContext(
                row_id=metadata['source_csv_row_id'],
                row_index=metadata['source_csv_index'],
                type=metadata['personality_type'],
                name=metadata['person_name'],
                email=metadata['person_email']
            )
        except KeyError as e:
            print(f"Error loading row context from {metadata_file_path}: missing key {e}")
            return None
    return None


def find_metadata_files(downloads_dir: str, pattern: str = "*_metadata.json") -> List[str]:
    """Find all metadata files in downloads directory"""
    import glob
    return glob.glob(os.path.join(downloads_dir, pattern))


def verify_type_preservation(original_type: str, metadata_file_path: str) -> bool:
    """Verify that type data was preserved in metadata"""
    # Use centralized state management (DRY)
    metadata = load_json_state(metadata_file_path, default={})
    return metadata.get('personality_type') == original_type


if __name__ == "__main__":
    """CLI interface for row context operations"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Row Context Utility")
    parser.add_argument('--test-context', action='store_true',
                       help='Test RowContext creation and methods')
    parser.add_argument('--find-metadata', type=str,
                       help='Find metadata files in directory')
    parser.add_argument('--verify-preservation', nargs=2, metavar=('TYPE', 'METADATA_FILE'),
                       help='Verify type preservation in metadata file')
    
    args = parser.parse_args()
    
    if args.test_context:
        # Test RowContext functionality
        test_context = RowContext(
            row_id="487",
            row_index=2,
            type="FF-Fi/Se-CP/B(S) #4",
            name="Olivia Tomlinson",
            email="oliviatomlinson8@gmail.com"
        )
        
        print(f"Test RowContext:")
        print(f"  Name: {test_context.name}")
        print(f"  Type: {test_context.type}")
        print(f"  Filename suffix: {test_context.to_filename_suffix()}")
        print(f"  Safe name prefix: {test_context.to_safe_name_prefix()}")
        
        metadata = test_context.to_metadata_dict()
        print(f"  Metadata keys: {list(metadata.keys())}")
        
    elif args.find_metadata:
        files = find_metadata_files(args.find_metadata)
        print(f"Found {len(files)} metadata files in {args.find_metadata}:")
        for f in files[:10]:  # Show first 10
            print(f"  {os.path.basename(f)}")
        if len(files) > 10:
            print(f"  ... and {len(files) - 10} more")
            
    elif args.verify_preservation:
        original_type, metadata_file = args.verify_preservation
        preserved = verify_type_preservation(original_type, metadata_file)
        print(f"Type preservation check: {'✓' if preserved else '✗'}")
        if preserved:
            print(f"  Original type '{original_type}' correctly preserved")
        else:
            print(f"  ERROR: Type '{original_type}' not preserved correctly")
    else:
        print("Use --help to see available options")