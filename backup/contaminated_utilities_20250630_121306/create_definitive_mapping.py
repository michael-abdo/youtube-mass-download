#!/usr/bin/env python3
"""
Create Definitive CSV-to-File Mapping
Creates the authoritative mapping of files to CSV rows based on CSV file listings.
"""

import os
import glob
import json
import pandas as pd
from collections import defaultdict
from typing import Dict, List, Set, Tuple
import shutil


class DefinitiveMapper:
    """Creates definitive CSV row to file mappings"""
    
    def __init__(self, csv_path: str = 'outputs/output.csv'):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path)
        
        # The definitive mapping we'll build
        self.definitive_mapping = {}  # row_id -> {files: [], metadata: {}}
        
        # Track all files we've mapped
        self.mapped_files = set()
        
        # Track issues
        self.conflicts = []
        self.missing_files = []
        self.unmapped_files = []
        
    def build_definitive_mapping(self) -> None:
        """Build the definitive mapping based on CSV listings"""
        print("=== BUILDING DEFINITIVE CSV-TO-FILE MAPPING ===")
        
        for idx, row in self.df.iterrows():
            row_id = str(row['row_id'])
            
            self.definitive_mapping[row_id] = {
                'name': row['name'],
                'type': row['type'],
                'email': row['email'],
                'youtube_files': [],
                'drive_files': [],
                'all_files': [],
                'missing_files': []
            }
            
            # Process YouTube files
            if pd.notna(row.get('youtube_files')) and row.get('youtube_status') == 'completed':
                files = str(row['youtube_files']).split(';')
                for file in files:
                    file = file.strip()
                    if not file:
                        continue
                    
                    # Find this file
                    found_paths = glob.glob(f'*_downloads/**/{file}', recursive=True)
                    
                    if found_paths:
                        # Use the first match (prefer youtube_downloads)
                        best_path = None
                        for path in found_paths:
                            if 'youtube_downloads' in path and not '/organized_by_type/' in path:
                                best_path = path
                                break
                        if not best_path:
                            best_path = found_paths[0]
                        
                        self.definitive_mapping[row_id]['youtube_files'].append(best_path)
                        self.definitive_mapping[row_id]['all_files'].append(best_path)
                        self.mapped_files.add(best_path)
                        
                        # Check if file is currently mapped to wrong row
                        current_mapping = self._check_current_mapping(best_path)
                        if current_mapping and current_mapping != row_id:
                            self.conflicts.append({
                                'file': best_path,
                                'correct_row': row_id,
                                'current_row': current_mapping,
                                'correct_name': row['name']
                            })
                    else:
                        self.definitive_mapping[row_id]['missing_files'].append(('youtube', file))
                        self.missing_files.append({
                            'row_id': row_id,
                            'name': row['name'],
                            'file': file,
                            'type': 'youtube'
                        })
            
            # Process Drive files
            if pd.notna(row.get('drive_files')) and row.get('drive_status') == 'completed':
                # Handle both comma and semicolon separators
                files = str(row['drive_files']).replace(';', ',').split(',')
                for file in files:
                    file = file.strip()
                    if not file:
                        continue
                    
                    found_paths = glob.glob(f'*_downloads/**/{file}', recursive=True)
                    
                    if found_paths:
                        best_path = None
                        for path in found_paths:
                            if 'drive_downloads' in path and not '/organized_by_type/' in path:
                                best_path = path
                                break
                        if not best_path:
                            best_path = found_paths[0]
                        
                        self.definitive_mapping[row_id]['drive_files'].append(best_path)
                        self.definitive_mapping[row_id]['all_files'].append(best_path)
                        self.mapped_files.add(best_path)
                        
                        current_mapping = self._check_current_mapping(best_path)
                        if current_mapping and current_mapping != row_id:
                            self.conflicts.append({
                                'file': best_path,
                                'correct_row': row_id,
                                'current_row': current_mapping,
                                'correct_name': row['name']
                            })
                    else:
                        self.definitive_mapping[row_id]['missing_files'].append(('drive', file))
                        self.missing_files.append({
                            'row_id': row_id,
                            'name': row['name'],
                            'file': file,
                            'type': 'drive'
                        })
        
        # Find unmapped files
        all_content_files = glob.glob('*_downloads/**/*', recursive=True)
        for file_path in all_content_files:
            if (os.path.isfile(file_path) and 
                file_path not in self.mapped_files and
                not any(file_path.endswith(ext) for ext in ['.json', '.part', '.ytdl', '.tmp']) and
                '/organized_by_type/' not in file_path):
                
                self.unmapped_files.append(file_path)
        
        # Print summary
        total_mapped = sum(len(m['all_files']) for m in self.definitive_mapping.values())
        print(f"\nMapping Summary:")
        print(f"  Total rows with files: {sum(1 for m in self.definitive_mapping.values() if m['all_files'])}")
        print(f"  Total files mapped: {total_mapped}")
        print(f"  Conflicts found: {len(self.conflicts)}")
        print(f"  Missing files: {len(self.missing_files)}")
        print(f"  Unmapped files: {len(self.unmapped_files)}")
    
    def _check_current_mapping(self, file_path: str) -> str:
        """Check what row a file is currently mapped to via metadata"""
        dir_path = os.path.dirname(file_path)
        metadata_files = glob.glob(os.path.join(dir_path, '*metadata.json'))
        
        for metadata_file in metadata_files:
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    return str(metadata.get('source_csv_row_id', ''))
            except:
                pass
        
        return None
    
    def create_corrected_metadata(self) -> None:
        """Create corrected metadata for all mapped files"""
        print("\n=== CREATING CORRECTED METADATA ===")
        
        metadata_created = 0
        
        for row_id, mapping in self.definitive_mapping.items():
            if not mapping['all_files']:
                continue
            
            # Create metadata for this row's files
            metadata = {
                'source_csv_row_id': int(row_id),
                'personality_type': mapping['type'],
                'person_name': mapping['name'],
                'person_email': mapping['email'],
                'mapping_source': 'definitive_csv_listing',
                'files_in_row': len(mapping['all_files'])
            }
            
            for file_path in mapping['all_files']:
                # Create definitive metadata next to each file
                metadata_path = file_path.replace(
                    os.path.splitext(file_path)[1], 
                    f'_row{row_id}_definitive.json'
                )
                
                file_metadata = metadata.copy()
                file_metadata['file_path'] = file_path
                file_metadata['file_type'] = 'youtube' if file_path in mapping['youtube_files'] else 'drive'
                
                with open(metadata_path, 'w') as f:
                    json.dump(file_metadata, f, indent=2)
                
                metadata_created += 1
        
        print(f"  Created {metadata_created} definitive metadata files")
    
    def save_definitive_mapping(self) -> None:
        """Save the complete definitive mapping"""
        print("\n=== SAVING DEFINITIVE MAPPING ===")
        
        # Create flat format for CSV
        flat_mappings = []
        for row_id, mapping in self.definitive_mapping.items():
            for file_path in mapping['all_files']:
                flat_mappings.append({
                    'row_id': row_id,
                    'name': mapping['name'],
                    'type': mapping['type'],
                    'email': mapping['email'],
                    'file_path': file_path,
                    'filename': os.path.basename(file_path),
                    'file_type': 'youtube' if file_path in mapping['youtube_files'] else 'drive'
                })
        
        if flat_mappings:
            df_mapping = pd.DataFrame(flat_mappings)
            df_mapping.to_csv('definitive_csv_file_mapping.csv', index=False)
            print(f"  Saved {len(flat_mappings)} mappings to: definitive_csv_file_mapping.csv")
        
        # Save conflicts
        if self.conflicts:
            df_conflicts = pd.DataFrame(self.conflicts)
            df_conflicts.to_csv('mapping_conflicts.csv', index=False)
            print(f"  Saved {len(self.conflicts)} conflicts to: mapping_conflicts.csv")
        
        # Save missing files
        if self.missing_files:
            df_missing = pd.DataFrame(self.missing_files)
            df_missing.to_csv('definitive_missing_files.csv', index=False)
            print(f"  Saved {len(self.missing_files)} missing files to: definitive_missing_files.csv")
        
        # Save unmapped files
        if self.unmapped_files:
            df_unmapped = pd.DataFrame({'file_path': self.unmapped_files})
            df_unmapped.to_csv('definitive_unmapped_files.csv', index=False)
            print(f"  Saved {len(self.unmapped_files)} unmapped files to: definitive_unmapped_files.csv")
        
        # Save complete mapping as JSON
        with open('definitive_mapping_complete.json', 'w') as f:
            json.dump(self.definitive_mapping, f, indent=2)
        print(f"  Saved complete mapping to: definitive_mapping_complete.json")
        
        # Summary statistics
        summary = {
            'total_csv_rows': len(self.df),
            'rows_with_files': sum(1 for m in self.definitive_mapping.values() if m['all_files']),
            'total_files_mapped': len(self.mapped_files),
            'conflicts': len(self.conflicts),
            'missing_files': len(self.missing_files),
            'unmapped_files': len(self.unmapped_files),
            'success_rate': f"{len(self.mapped_files) / (len(self.mapped_files) + len(self.missing_files)) * 100:.1f}%"
        }
        
        with open('definitive_mapping_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\n  Summary saved to: definitive_mapping_summary.json")


def main():
    mapper = DefinitiveMapper()
    
    # Build the definitive mapping from CSV
    mapper.build_definitive_mapping()
    
    # Create corrected metadata
    mapper.create_corrected_metadata()
    
    # Save all results
    mapper.save_definitive_mapping()
    
    print("\n✅ Definitive CSV-to-file mapping complete!")
    print("\nNext steps:")
    print("1. Review mapping_conflicts.csv to see files mapped to wrong rows")
    print("2. Review definitive_missing_files.csv to see what's missing")
    print("3. Use definitive_csv_file_mapping.csv as the authoritative mapping")


if __name__ == "__main__":
    main()