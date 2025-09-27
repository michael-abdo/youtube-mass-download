#!/usr/bin/env python3
"""
CSV File Integrity Mapper
Maps each downloaded file to its correct CSV row and ensures each row with links has files.
"""

import os
import glob
import json
import pandas as pd
import re
from collections import defaultdict
from typing import Dict, List, Set, Tuple
import argparse


class CSVFileIntegrityMapper:
    """Ensures complete integrity between CSV rows and downloaded files"""
    
    def __init__(self, csv_path: str = 'outputs/output.csv'):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path)
        
        # Core mappings
        self.row_to_files = defaultdict(list)  # row_id -> [file_paths]
        self.file_to_row = {}                  # file_path -> row_id
        self.unmapped_files = []               # files with no CSV row
        self.rows_without_files = []           # CSV rows that should have files but don't
        
    def analyze_csv_expectations(self) -> Dict:
        """Analyze what files we expect based on CSV data"""
        print("=== ANALYZING CSV EXPECTATIONS ===")
        
        expectations = {
            'rows_with_youtube': 0,
            'rows_with_drive': 0,
            'rows_expecting_files': [],
            'total_expected_files': 0
        }
        
        for idx, row in self.df.iterrows():
            row_id = str(row['row_id'])
            expects_files = False
            
            # Check YouTube expectations
            if (pd.notna(row.get('youtube_playlist')) and 
                str(row['youtube_playlist']).strip() not in ['', 'nan'] and
                'youtube.com' in str(row['youtube_playlist'])):
                
                if row.get('youtube_status') == 'completed':
                    expects_files = True
                    expectations['rows_with_youtube'] += 1
                    
                    # Count expected files from youtube_files column
                    if pd.notna(row.get('youtube_files')):
                        files = str(row['youtube_files']).split(';')
                        expectations['total_expected_files'] += len([f for f in files if f.strip()])
            
            # Check Drive expectations
            if (pd.notna(row.get('google_drive')) and 
                str(row['google_drive']).strip() not in ['', 'nan'] and
                'drive.google.com' in str(row['google_drive'])):
                
                if row.get('drive_status') == 'completed':
                    expects_files = True
                    expectations['rows_with_drive'] += 1
                    
                    # Count expected files from drive_files column
                    if pd.notna(row.get('drive_files')):
                        files = str(row['drive_files']).split(',')
                        expectations['total_expected_files'] += len([f for f in files if f.strip()])
            
            if expects_files:
                expectations['rows_expecting_files'].append({
                    'row_id': row_id,
                    'name': row['name'],
                    'type': row['type'],
                    'youtube_status': row.get('youtube_status'),
                    'drive_status': row.get('drive_status')
                })
        
        print(f"  Rows expecting YouTube files: {expectations['rows_with_youtube']}")
        print(f"  Rows expecting Drive files: {expectations['rows_with_drive']}")
        print(f"  Total rows expecting files: {len(expectations['rows_expecting_files'])}")
        print(f"  Total expected files: {expectations['total_expected_files']}")
        
        return expectations
    
    def map_files_from_metadata(self) -> None:
        """Map files using metadata JSON files (most accurate)"""
        print("\n=== MAPPING FILES FROM METADATA ===")
        
        metadata_files = glob.glob('*_downloads/**/*metadata.json', recursive=True)
        mapped_count = 0
        
        for metadata_path in metadata_files:
            try:
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                row_id = str(metadata.get('source_csv_row_id', ''))
                if not row_id or row_id == 'unknown':
                    continue
                
                # Get the directory containing metadata
                metadata_dir = os.path.dirname(metadata_path)
                
                # Map files from download result
                if 'download_result' in metadata:
                    files = metadata['download_result'].get('files_downloaded', [])
                    for file in files:
                        file_path = os.path.join(metadata_dir, file)
                        if os.path.exists(file_path):
                            self.row_to_files[row_id].append(file_path)
                            self.file_to_row[file_path] = row_id
                            mapped_count += 1
                
                # Also map the metadata file itself
                self.row_to_files[row_id].append(metadata_path)
                self.file_to_row[metadata_path] = row_id
                
            except Exception as e:
                print(f"  Error reading {metadata_path}: {e}")
        
        print(f"  Mapped {mapped_count} content files from metadata")
    
    def map_files_from_csv_listings(self) -> None:
        """Map files using CSV file listings"""
        print("\n=== MAPPING FILES FROM CSV LISTINGS ===")
        
        mapped_count = 0
        
        for idx, row in self.df.iterrows():
            row_id = str(row['row_id'])
            
            # Map YouTube files
            if pd.notna(row.get('youtube_files')):
                files = str(row['youtube_files']).split(';')
                for file in files:
                    file = file.strip()
                    if file:
                        # Search for file
                        search_patterns = [
                            f"youtube_downloads/{file}",
                            f"youtube_downloads/**/{file}",
                            f"youtube_downloads/**/*{file}"
                        ]
                        
                        for pattern in search_patterns:
                            matches = glob.glob(pattern, recursive=True)
                            for match in matches:
                                if match not in self.file_to_row:
                                    self.row_to_files[row_id].append(match)
                                    self.file_to_row[match] = row_id
                                    mapped_count += 1
                                    break
                            if matches:
                                break
            
            # Map Drive files
            if pd.notna(row.get('drive_files')):
                files = str(row['drive_files']).split(',')
                for file in files:
                    file = file.strip()
                    if file:
                        search_patterns = [
                            f"drive_downloads/{file}",
                            f"drive_downloads/**/{file}",
                            f"drive_downloads/**/*{file}"
                        ]
                        
                        for pattern in search_patterns:
                            matches = glob.glob(pattern, recursive=True)
                            for match in matches:
                                if match not in self.file_to_row:
                                    self.row_to_files[row_id].append(match)
                                    self.file_to_row[match] = row_id
                                    mapped_count += 1
                                    break
                            if matches:
                                break
        
        print(f"  Mapped {mapped_count} additional files from CSV listings")
    
    def map_files_by_row_id_in_name(self) -> None:
        """Map files that have row ID in their filename"""
        print("\n=== MAPPING FILES BY ROW ID IN FILENAME ===")
        
        all_files = glob.glob('*_downloads/**/*', recursive=True)
        row_pattern = re.compile(r'_row(\d+)_|row_(\d+)_|_row(\d+)\.')
        mapped_count = 0
        
        for file_path in all_files:
            if not os.path.isfile(file_path) or file_path in self.file_to_row:
                continue
            
            basename = os.path.basename(file_path)
            match = row_pattern.search(basename)
            
            if match:
                row_id = match.group(1) or match.group(2) or match.group(3)
                
                # Verify row exists in CSV
                if any(self.df['row_id'] == int(row_id)):
                    self.row_to_files[row_id].append(file_path)
                    self.file_to_row[file_path] = row_id
                    mapped_count += 1
        
        print(f"  Mapped {mapped_count} files by row ID in filename")
    
    def identify_unmapped_files(self) -> None:
        """Identify files that couldn't be mapped to any CSV row"""
        print("\n=== IDENTIFYING UNMAPPED FILES ===")
        
        all_files = glob.glob('*_downloads/**/*', recursive=True)
        
        for file_path in all_files:
            if not os.path.isfile(file_path):
                continue
            
            if file_path not in self.file_to_row:
                # Skip certain file types
                basename = os.path.basename(file_path)
                if any(basename.endswith(ext) for ext in ['.part', '.ytdl', '.tmp']):
                    continue
                    
                self.unmapped_files.append(file_path)
        
        print(f"  Found {len(self.unmapped_files)} unmapped files")
        
        if self.unmapped_files:
            print("  Sample unmapped files:")
            for file in self.unmapped_files[:5]:
                print(f"    - {file}")
    
    def identify_rows_without_files(self, expectations: Dict) -> None:
        """Identify CSV rows that should have files but don't"""
        print("\n=== IDENTIFYING ROWS WITHOUT FILES ===")
        
        for row_info in expectations['rows_expecting_files']:
            row_id = row_info['row_id']
            
            if row_id not in self.row_to_files or not self.row_to_files[row_id]:
                self.rows_without_files.append(row_info)
        
        print(f"  Found {len(self.rows_without_files)} rows expecting files but have none")
        
        if self.rows_without_files:
            print("  Sample rows without files:")
            for row in self.rows_without_files[:5]:
                print(f"    - Row {row['row_id']}: {row['name']} ({row['type']})")
                print(f"      YouTube: {row['youtube_status']}, Drive: {row['drive_status']}")
    
    def generate_integrity_report(self) -> None:
        """Generate comprehensive integrity report"""
        print("\n=== CSV-FILE INTEGRITY REPORT ===")
        
        # Calculate statistics
        total_csv_rows = len(self.df)
        rows_with_files = len(self.row_to_files)
        total_mapped_files = sum(len(files) for files in self.row_to_files.values())
        
        print(f"\nOverview:")
        print(f"  Total CSV rows: {total_csv_rows}")
        print(f"  Rows with mapped files: {rows_with_files}")
        print(f"  Total mapped files: {total_mapped_files}")
        print(f"  Unmapped files: {len(self.unmapped_files)}")
        print(f"  Rows missing expected files: {len(self.rows_without_files)}")
        
        # Save detailed mappings
        self._save_detailed_reports()
    
    def _save_detailed_reports(self) -> None:
        """Save all mappings and issues to files"""
        
        # Save row-to-file mappings
        mapping_data = []
        for row_id, files in self.row_to_files.items():
            # Get row info from CSV
            row_data = self.df[self.df['row_id'] == int(row_id)]
            if not row_data.empty:
                row = row_data.iloc[0]
                for file_path in files:
                    mapping_data.append({
                        'row_id': row_id,
                        'name': row['name'],
                        'type': row['type'],
                        'file_path': file_path,
                        'filename': os.path.basename(file_path),
                        'file_type': 'youtube' if 'youtube' in file_path else 'drive'
                    })
        
        if mapping_data:
            df_mapping = pd.DataFrame(mapping_data)
            df_mapping.to_csv('csv_file_integrity_mapping.csv', index=False)
            print(f"\nMappings saved to: csv_file_integrity_mapping.csv")
        
        # Save unmapped files
        if self.unmapped_files:
            unmapped_data = []
            for file_path in self.unmapped_files:
                unmapped_data.append({
                    'file_path': file_path,
                    'filename': os.path.basename(file_path),
                    'directory': os.path.dirname(file_path),
                    'size': os.path.getsize(file_path)
                })
            
            df_unmapped = pd.DataFrame(unmapped_data)
            df_unmapped.to_csv('unmapped_files_integrity.csv', index=False)
            print(f"Unmapped files saved to: unmapped_files_integrity.csv")
        
        # Save rows without files
        if self.rows_without_files:
            df_missing = pd.DataFrame(self.rows_without_files)
            df_missing.to_csv('rows_missing_files.csv', index=False)
            print(f"Rows missing files saved to: rows_missing_files.csv")
        
        # Save summary
        summary = {
            'total_csv_rows': len(self.df),
            'rows_with_files': len(self.row_to_files),
            'total_mapped_files': sum(len(files) for files in self.row_to_files.values()),
            'unmapped_files': len(self.unmapped_files),
            'rows_missing_files': len(self.rows_without_files),
            'integrity_score': f"{len(self.row_to_files) / len(self.rows_without_files + list(self.row_to_files.keys())) * 100:.1f}%" if (self.rows_without_files or self.row_to_files) else "100%"
        }
        
        with open('csv_file_integrity_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"Summary saved to: csv_file_integrity_summary.json")


def main():
    parser = argparse.ArgumentParser(description='Map files to CSV rows and ensure integrity')
    parser.add_argument('--csv', default='outputs/output.csv', help='Path to CSV file')
    parser.add_argument('--fix', action='store_true', help='Attempt to fix integrity issues')
    
    args = parser.parse_args()
    
    # Run integrity check
    mapper = CSVFileIntegrityMapper(args.csv)
    
    # Analyze expectations
    expectations = mapper.analyze_csv_expectations()
    
    # Map files using multiple strategies
    mapper.map_files_from_metadata()
    mapper.map_files_from_csv_listings()
    mapper.map_files_by_row_id_in_name()
    
    # Identify issues
    mapper.identify_unmapped_files()
    mapper.identify_rows_without_files(expectations)
    
    # Generate report
    mapper.generate_integrity_report()
    
    print("\n✅ CSV-File integrity check complete!")


if __name__ == "__main__":
    main()