#!/usr/bin/env python3
"""
Comprehensive File Mapper - Maps ALL files to personality types and identifies issues.
Uses multiple strategies to achieve 100% file identification.
"""

import json
import os
import re
import hashlib
import glob
import pandas as pd
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional
import argparse


class ComprehensiveFileMapper:
    """Complete file analysis and mapping system"""
    
    def __init__(self, csv_path: str = 'outputs/output.csv'):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path)
        
        # File categories
        self.mapped_files = {}          # file_path -> mapping_info
        self.unmapped_files = []        # files with no mapping
        self.duplicate_files = defaultdict(list)  # hash -> [file_paths]
        self.temporary_files = []       # .part, .ytdl, etc.
        self.orphaned_csv_entries = []  # in CSV but not on disk
        self.missing_metadata = []      # files without metadata
        
        # Mapping sources tracking
        self.mapping_sources = defaultdict(int)
        
        # File hashes for duplicate detection
        self.file_hashes = {}
        
    def scan_all_files(self) -> None:
        """Scan all files in download directories"""
        print("=== PHASE 1: COMPREHENSIVE FILE SCAN ===")
        
        all_files = []
        for pattern in ['youtube_downloads/**/*', 'drive_downloads/**/*']:
            all_files.extend(glob.glob(pattern, recursive=True))
        
        total_files = len([f for f in all_files if os.path.isfile(f)])
        print(f"Found {total_files} total files to analyze")
        
        # Categorize files
        for file_path in all_files:
            if not os.path.isfile(file_path):
                continue
                
            basename = os.path.basename(file_path)
            
            # Check for temporary files
            if any(basename.endswith(ext) for ext in ['.part', '.ytdl', '.tmp', '.temp']):
                self.temporary_files.append(file_path)
                continue
            
            # Skip metadata files for now (will process separately)
            if basename.endswith('metadata.json'):
                continue
                
            # Calculate file hash for duplicate detection (first 1MB only for speed)
            file_hash = self._calculate_file_hash(file_path)
            if file_hash:
                self.file_hashes[file_path] = file_hash
                self.duplicate_files[file_hash].append(file_path)
    
    def map_from_metadata(self) -> None:
        """Strategy 1: Map files using metadata JSON files (most reliable)"""
        print("\n=== STRATEGY 1: METADATA MAPPING ===")
        
        metadata_files = glob.glob('*_downloads/**/*metadata.json', recursive=True)
        print(f"Processing {len(metadata_files)} metadata files...")
        
        mapped_count = 0
        for metadata_path in metadata_files:
            try:
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                # Extract mapping info
                row_id = metadata.get('source_csv_row_id', 'unknown')
                personality_type = metadata.get('personality_type', 'unknown')
                person_name = metadata.get('person_name', 'unknown')
                person_email = metadata.get('person_email', 'unknown')
                
                # Find associated content files
                metadata_dir = os.path.dirname(metadata_path)
                
                # Map files from download result
                if 'download_result' in metadata:
                    files = metadata['download_result'].get('files_downloaded', [])
                    for file in files:
                        file_path = os.path.join(metadata_dir, file)
                        if os.path.exists(file_path):
                            self._add_mapping(file_path, {
                                'row_id': row_id,
                                'type': personality_type,
                                'name': person_name,
                                'email': person_email,
                                'source': 'metadata',
                                'metadata_path': metadata_path
                            })
                            mapped_count += 1
                
                # Also check for files in same directory as metadata
                dir_files = glob.glob(os.path.join(metadata_dir, '*'))
                for file_path in dir_files:
                    if os.path.isfile(file_path) and not file_path.endswith('.json'):
                        if file_path not in self.mapped_files:
                            # Infer association by proximity
                            self._add_mapping(file_path, {
                                'row_id': row_id,
                                'type': personality_type,
                                'name': person_name,
                                'email': person_email,
                                'source': 'metadata_proximity',
                                'metadata_path': metadata_path
                            })
                            mapped_count += 1
                            
            except Exception as e:
                print(f"  Error processing {metadata_path}: {e}")
                
        print(f"  Mapped {mapped_count} files from metadata")
    
    def map_from_csv(self) -> None:
        """Strategy 2: Map files using CSV file listings"""
        print("\n=== STRATEGY 2: CSV MAPPING ===")
        
        mapped_count = 0
        
        for idx, row in self.df.iterrows():
            row_id = row['row_id']
            personality_type = row['type']
            person_name = row['name']
            person_email = row['email']
            
            # Check YouTube files
            if pd.notna(row.get('youtube_files')):
                files = str(row['youtube_files']).split(';')
                for file in files:
                    file = file.strip()
                    if file:
                        # Try multiple possible locations
                        possible_paths = [
                            os.path.join('youtube_downloads', file),
                            os.path.join('youtube_downloads', f"*{file}*")
                        ]
                        
                        for path_pattern in possible_paths:
                            matches = glob.glob(path_pattern)
                            for file_path in matches:
                                if os.path.isfile(file_path) and file_path not in self.mapped_files:
                                    self._add_mapping(file_path, {
                                        'row_id': row_id,
                                        'type': personality_type,
                                        'name': person_name,
                                        'email': person_email,
                                        'source': 'csv_youtube',
                                        'csv_field': 'youtube_files'
                                    })
                                    mapped_count += 1
            
            # Check Drive files
            if pd.notna(row.get('drive_files')):
                files = str(row['drive_files']).split(',')
                for file in files:
                    file = file.strip()
                    if file:
                        possible_paths = [
                            os.path.join('drive_downloads', file),
                            os.path.join('drive_downloads', f"*{file}*")
                        ]
                        
                        for path_pattern in possible_paths:
                            matches = glob.glob(path_pattern)
                            for file_path in matches:
                                if os.path.isfile(file_path) and file_path not in self.mapped_files:
                                    self._add_mapping(file_path, {
                                        'row_id': row_id,
                                        'type': personality_type,
                                        'name': person_name,
                                        'email': person_email,
                                        'source': 'csv_drive',
                                        'csv_field': 'drive_files'
                                    })
                                    mapped_count += 1
                                    
        print(f"  Mapped {mapped_count} files from CSV")
    
    def map_from_filename_patterns(self) -> None:
        """Strategy 3: Map files using filename patterns"""
        print("\n=== STRATEGY 3: FILENAME PATTERN MAPPING ===")
        
        mapped_count = 0
        
        # Pattern 1: Files with row ID in name
        row_pattern = re.compile(r'_row(\d+)_')
        
        # Pattern 2: Files with type in name
        type_patterns = [
            re.compile(r'(FF-[^_/]+)'),
            re.compile(r'(FM-[^_/]+)'),
            re.compile(r'(MF-[^_/]+)'),
            re.compile(r'(MM-[^_/]+)')
        ]
        
        for file_path in self.file_hashes.keys():
            if file_path in self.mapped_files:
                continue
                
            basename = os.path.basename(file_path)
            
            # Try to extract row ID
            row_match = row_pattern.search(basename)
            if row_match:
                row_id = row_match.group(1)
                
                # Find corresponding CSV row
                csv_row = self.df[self.df['row_id'] == row_id]
                if not csv_row.empty:
                    row_data = csv_row.iloc[0]
                    self._add_mapping(file_path, {
                        'row_id': row_id,
                        'type': row_data['type'],
                        'name': row_data['name'],
                        'email': row_data['email'],
                        'source': 'filename_rowid',
                        'pattern': f'row{row_id}'
                    })
                    mapped_count += 1
                    continue
            
            # Try to extract type
            for type_pattern in type_patterns:
                type_match = type_pattern.search(basename)
                if type_match:
                    personality_type = type_match.group(1)
                    
                    # Find matching rows by type
                    matching_rows = self.df[self.df['type'].str.contains(personality_type, na=False)]
                    if len(matching_rows) == 1:
                        # Unique match
                        row_data = matching_rows.iloc[0]
                        self._add_mapping(file_path, {
                            'row_id': row_data['row_id'],
                            'type': row_data['type'],
                            'name': row_data['name'],
                            'email': row_data['email'],
                            'source': 'filename_type',
                            'pattern': personality_type
                        })
                        mapped_count += 1
                        
        print(f"  Mapped {mapped_count} files from filename patterns")
    
    def map_by_content_matching(self) -> None:
        """Strategy 4: Map files by matching content IDs"""
        print("\n=== STRATEGY 4: CONTENT ID MATCHING ===")
        
        mapped_count = 0
        
        # YouTube video ID pattern
        youtube_pattern = re.compile(r'([a-zA-Z0-9_-]{11})(?:\.|_)')
        
        # Drive file ID pattern  
        drive_pattern = re.compile(r'([a-zA-Z0-9_-]{28,33})(?:\.|_)')
        
        for file_path in self.file_hashes.keys():
            if file_path in self.mapped_files:
                continue
                
            basename = os.path.basename(file_path)
            
            # Check YouTube pattern
            if 'youtube' in file_path:
                match = youtube_pattern.search(basename)
                if match:
                    video_id = match.group(1)
                    
                    # Search in youtube_media_id column
                    matching_rows = self.df[self.df['youtube_media_id'] == video_id]
                    if not matching_rows.empty:
                        row_data = matching_rows.iloc[0]
                        self._add_mapping(file_path, {
                            'row_id': row_data['row_id'],
                            'type': row_data['type'],
                            'name': row_data['name'],
                            'email': row_data['email'],
                            'source': 'youtube_id',
                            'media_id': video_id
                        })
                        mapped_count += 1
                        continue
                    
                    # Search in youtube_files text
                    for idx, row in self.df.iterrows():
                        if pd.notna(row.get('youtube_files')) and video_id in str(row['youtube_files']):
                            self._add_mapping(file_path, {
                                'row_id': row['row_id'],
                                'type': row['type'],
                                'name': row['name'],
                                'email': row['email'],
                                'source': 'youtube_id_in_files',
                                'media_id': video_id
                            })
                            mapped_count += 1
                            break
            
            # Check Drive pattern
            elif 'drive' in file_path:
                match = drive_pattern.search(basename)
                if match:
                    drive_id = match.group(1)
                    
                    # Search in drive_media_id column
                    matching_rows = self.df[self.df['drive_media_id'] == drive_id]
                    if not matching_rows.empty:
                        row_data = matching_rows.iloc[0]
                        self._add_mapping(file_path, {
                            'row_id': row_data['row_id'],
                            'type': row_data['type'],
                            'name': row_data['name'],
                            'email': row_data['email'],
                            'source': 'drive_id',
                            'media_id': drive_id
                        })
                        mapped_count += 1
                        
        print(f"  Mapped {mapped_count} files by content ID")
    
    def identify_unmapped_and_issues(self) -> None:
        """Identify remaining unmapped files and data issues"""
        print("\n=== PHASE 2: ISSUE IDENTIFICATION ===")
        
        # Identify unmapped files
        for file_path in self.file_hashes.keys():
            if file_path not in self.mapped_files:
                self.unmapped_files.append(file_path)
        
        # Check for orphaned CSV entries
        print("Checking for orphaned CSV entries...")
        
        for idx, row in self.df.iterrows():
            orphaned = False
            
            # Check YouTube files
            if pd.notna(row.get('youtube_files')) and row.get('youtube_status') == 'completed':
                files = str(row['youtube_files']).split(';')
                files_found = 0
                for file in files:
                    file = file.strip()
                    if file:
                        # Check if file exists anywhere
                        found = any(file in fp for fp in self.file_hashes.keys())
                        if found:
                            files_found += 1
                
                if files_found == 0:
                    self.orphaned_csv_entries.append({
                        'row_id': row['row_id'],
                        'name': row['name'],
                        'type': row['type'],
                        'field': 'youtube_files',
                        'files': row['youtube_files']
                    })
            
            # Check Drive files
            if pd.notna(row.get('drive_files')) and row.get('drive_status') == 'completed':
                files = str(row['drive_files']).split(',')
                files_found = 0
                for file in files:
                    file = file.strip()
                    if file:
                        found = any(file in fp for fp in self.file_hashes.keys())
                        if found:
                            files_found += 1
                
                if files_found == 0:
                    self.orphaned_csv_entries.append({
                        'row_id': row['row_id'],
                        'name': row['name'],
                        'type': row['type'],
                        'field': 'drive_files',
                        'files': row['drive_files']
                    })
        
        # Identify files without metadata
        for file_path in self.mapped_files:
            if self.mapped_files[file_path].get('source') != 'metadata':
                metadata_exists = any(
                    os.path.exists(os.path.join(os.path.dirname(file_path), f))
                    for f in ['metadata.json', '*_metadata.json']
                )
                if not metadata_exists:
                    self.missing_metadata.append(file_path)
    
    def generate_comprehensive_report(self) -> None:
        """Generate detailed analysis report"""
        print("\n=== COMPREHENSIVE MAPPING REPORT ===")
        
        total_files = len(self.file_hashes)
        
        print(f"\nFILE STATISTICS:")
        print(f"  Total files scanned: {total_files}")
        print(f"  Successfully mapped: {len(self.mapped_files)} ({len(self.mapped_files)/total_files*100:.1f}%)")
        print(f"  Unmapped files: {len(self.unmapped_files)} ({len(self.unmapped_files)/total_files*100:.1f}%)")
        print(f"  Temporary files: {len(self.temporary_files)}")
        
        # Mapping source breakdown
        print(f"\nMAPPING SOURCES:")
        source_counts = defaultdict(int)
        for info in self.mapped_files.values():
            source_counts[info['source']] += 1
        for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {source}: {count} files")
        
        # Duplicate analysis
        print(f"\nDUPLICATE ANALYSIS:")
        duplicate_sets = [files for files in self.duplicate_files.values() if len(files) > 1]
        print(f"  Duplicate sets found: {len(duplicate_sets)}")
        total_duplicates = sum(len(files) - 1 for files in duplicate_sets)
        print(f"  Total duplicate files: {total_duplicates}")
        
        if duplicate_sets:
            print("  Sample duplicates:")
            for files in duplicate_sets[:3]:
                print(f"    - {os.path.basename(files[0])}")
                for dup in files[1:]:
                    print(f"      = {dup}")
        
        # Data integrity issues
        print(f"\nDATA INTEGRITY ISSUES:")
        print(f"  Orphaned CSV entries: {len(self.orphaned_csv_entries)}")
        print(f"  Files without metadata: {len(self.missing_metadata)}")
        
        # Unmapped file analysis
        if self.unmapped_files:
            print(f"\nUNMAPPED FILE ANALYSIS:")
            
            # Categorize unmapped files
            unmapped_categories = defaultdict(list)
            for file_path in self.unmapped_files:
                basename = os.path.basename(file_path)
                if re.match(r'^[a-zA-Z0-9_-]{11}\.(mp4|webm|mkv)', basename):
                    unmapped_categories['youtube_videos'].append(file_path)
                elif re.match(r'^[a-zA-Z0-9_-]{28,33}\.', basename):
                    unmapped_categories['drive_files'].append(file_path)
                elif basename.endswith('.vtt') or basename.endswith('.srt'):
                    unmapped_categories['transcripts'].append(file_path)
                else:
                    unmapped_categories['other'].append(file_path)
            
            for category, files in unmapped_categories.items():
                print(f"  {category}: {len(files)} files")
                for file in files[:2]:
                    print(f"    - {os.path.basename(file)}")
        
        # Save detailed reports
        self._save_detailed_reports()
    
    def _add_mapping(self, file_path: str, mapping_info: Dict) -> None:
        """Add a file mapping"""
        if file_path not in self.mapped_files:
            self.mapped_files[file_path] = mapping_info
            self.mapping_sources[mapping_info['source']] += 1
    
    def _calculate_file_hash(self, file_path: str, chunk_size: int = 1024*1024) -> Optional[str]:
        """Calculate file hash (first 1MB for speed)"""
        try:
            hasher = hashlib.md5()
            with open(file_path, 'rb') as f:
                chunk = f.read(chunk_size)
                hasher.update(chunk)
            return hasher.hexdigest()
        except:
            return None
    
    def _save_detailed_reports(self) -> None:
        """Save all findings to CSV files"""
        
        # Main mapping report
        mapping_data = []
        for file_path, info in self.mapped_files.items():
            mapping_data.append({
                'file_path': file_path,
                'filename': os.path.basename(file_path),
                'file_size': os.path.getsize(file_path),
                'row_id': info['row_id'],
                'personality_type': info['type'],
                'person_name': info['name'],
                'person_email': info['email'],
                'mapping_source': info['source'],
                'has_metadata': 'metadata_path' in info
            })
        
        if mapping_data:
            df_mapped = pd.DataFrame(mapping_data)
            df_mapped.to_csv('comprehensive_file_mapping.csv', index=False)
            print(f"\nMapped files saved to: comprehensive_file_mapping.csv")
        
        # Unmapped files report
        if self.unmapped_files:
            unmapped_data = []
            for file_path in self.unmapped_files:
                unmapped_data.append({
                    'file_path': file_path,
                    'filename': os.path.basename(file_path),
                    'file_size': os.path.getsize(file_path),
                    'directory': os.path.dirname(file_path),
                    'file_type': os.path.splitext(file_path)[1]
                })
            
            df_unmapped = pd.DataFrame(unmapped_data)
            df_unmapped.to_csv('unmapped_files.csv', index=False)
            print(f"Unmapped files saved to: unmapped_files.csv")
        
        # Duplicates report
        duplicate_data = []
        for file_hash, files in self.duplicate_files.items():
            if len(files) > 1:
                for i, file_path in enumerate(files):
                    duplicate_data.append({
                        'duplicate_set': file_hash[:8],
                        'file_path': file_path,
                        'filename': os.path.basename(file_path),
                        'file_size': os.path.getsize(file_path),
                        'is_primary': i == 0
                    })
        
        if duplicate_data:
            df_duplicates = pd.DataFrame(duplicate_data)
            df_duplicates.to_csv('duplicate_files.csv', index=False)
            print(f"Duplicate files saved to: duplicate_files.csv")
        
        # Orphaned entries report
        if self.orphaned_csv_entries:
            df_orphaned = pd.DataFrame(self.orphaned_csv_entries)
            df_orphaned.to_csv('orphaned_csv_entries.csv', index=False)
            print(f"Orphaned CSV entries saved to: orphaned_csv_entries.csv")
        
        # Summary statistics
        summary = {
            'total_files': len(self.file_hashes),
            'mapped_files': len(self.mapped_files),
            'unmapped_files': len(self.unmapped_files),
            'temporary_files': len(self.temporary_files),
            'duplicate_sets': len([f for f in self.duplicate_files.values() if len(f) > 1]),
            'orphaned_csv_entries': len(self.orphaned_csv_entries),
            'files_without_metadata': len(self.missing_metadata),
            'mapping_success_rate': f"{len(self.mapped_files)/len(self.file_hashes)*100:.1f}%"
        }
        
        with open('mapping_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"Summary statistics saved to: mapping_summary.json")


def main():
    parser = argparse.ArgumentParser(description='Comprehensive file mapping and analysis')
    parser.add_argument('--csv-path', default='outputs/output.csv',
                       help='Path to CSV file')
    parser.add_argument('--fix-unmapped', action='store_true',
                       help='Attempt to fix unmapped files')
    parser.add_argument('--clean-duplicates', action='store_true',
                       help='Remove duplicate files (keep primary)')
    
    args = parser.parse_args()
    
    # Run comprehensive mapping
    mapper = ComprehensiveFileMapper(args.csv_path)
    
    # Phase 1: Scan all files
    mapper.scan_all_files()
    
    # Apply all mapping strategies
    mapper.map_from_metadata()
    mapper.map_from_csv()
    mapper.map_from_filename_patterns()
    mapper.map_by_content_matching()
    
    # Phase 2: Identify issues
    mapper.identify_unmapped_and_issues()
    
    # Generate comprehensive report
    mapper.generate_comprehensive_report()
    
    # Optional: Fix issues
    if args.fix_unmapped:
        print("\n=== ATTEMPTING TO FIX UNMAPPED FILES ===")
        # Implementation for fixing unmapped files
        
    if args.clean_duplicates:
        print("\n=== CLEANING DUPLICATE FILES ===")
        # Implementation for cleaning duplicates


if __name__ == "__main__":
    main()