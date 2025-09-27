#!/usr/bin/env python3
"""
Fix CSV-File Mapping Issues
Corrects mismatched file-to-row mappings based on CSV data.
"""

import os
import glob
import pandas as pd
import json
import shutil
from typing import Dict, List, Set, Tuple
import argparse


class CSVFileMappingFixer:
    """Fixes incorrect file-to-row mappings"""
    
    def __init__(self, csv_path: str = 'outputs/output.csv'):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path)
        self.fixes_applied = []
        self.conflicts_found = []
        
    def find_mismatched_mappings(self) -> List[Dict]:
        """Find files that are mapped to wrong rows based on CSV listings"""
        print("=== FINDING MISMATCHED MAPPINGS ===")
        
        mismatches = []
        
        # Check each row's listed files
        for idx, row in self.df.iterrows():
            row_id = str(row['row_id'])
            
            # Check YouTube files
            if pd.notna(row.get('youtube_files')) and row.get('youtube_status') == 'completed':
                files = str(row['youtube_files']).split(';')
                for file in files:
                    file = file.strip()
                    if not file:
                        continue
                    
                    # Find where this file actually is
                    found_files = glob.glob(f'*_downloads/**/{file}', recursive=True)
                    
                    for found_file in found_files:
                        # Check if it has a different row ID in its path or metadata
                        actual_row = self._get_file_row_mapping(found_file)
                        
                        if actual_row and actual_row != row_id:
                            mismatches.append({
                                'file': found_file,
                                'filename': file,
                                'correct_row_id': row_id,
                                'correct_name': row['name'],
                                'current_row_id': actual_row,
                                'type': 'youtube'
                            })
            
            # Check Drive files
            if pd.notna(row.get('drive_files')) and row.get('drive_status') == 'completed':
                files = str(row['drive_files']).split(',')
                for file in files:
                    file = file.strip()
                    if not file:
                        continue
                    
                    found_files = glob.glob(f'*_downloads/**/{file}', recursive=True)
                    
                    for found_file in found_files:
                        actual_row = self._get_file_row_mapping(found_file)
                        
                        if actual_row and actual_row != row_id:
                            mismatches.append({
                                'file': found_file,
                                'filename': file,
                                'correct_row_id': row_id,
                                'correct_name': row['name'],
                                'current_row_id': actual_row,
                                'type': 'drive'
                            })
        
        print(f"  Found {len(mismatches)} mismatched file mappings")
        
        # Group by conflict
        if mismatches:
            print("\n  Sample mismatches:")
            for m in mismatches[:5]:
                print(f"    {m['filename']} is with row {m['current_row_id']} but belongs to row {m['correct_row_id']} ({m['correct_name']})")
        
        return mismatches
    
    def _get_file_row_mapping(self, file_path: str) -> str:
        """Get the row ID a file is currently associated with"""
        # Check for metadata file
        dir_path = os.path.dirname(file_path)
        metadata_files = glob.glob(os.path.join(dir_path, '*metadata.json'))
        
        for metadata_file in metadata_files:
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    return str(metadata.get('source_csv_row_id', ''))
            except:
                pass
        
        # Check organized directory structure
        if 'organized_by_type' in file_path:
            import re
            match = re.search(r'/(\d+)_', file_path)
            if match:
                return match.group(1)
        
        return None
    
    def create_correct_mappings(self, mismatches: List[Dict]) -> None:
        """Create correct metadata files for mismatched files"""
        print("\n=== CREATING CORRECT MAPPINGS ===")
        
        fixed_count = 0
        
        for mismatch in mismatches:
            file_path = mismatch['file']
            correct_row_id = mismatch['correct_row_id']
            
            # Get correct row data
            row_data = self.df[self.df['row_id'] == int(correct_row_id)]
            if row_data.empty:
                continue
            
            row = row_data.iloc[0]
            
            # Create correct metadata
            metadata = {
                'source_csv_row_id': int(correct_row_id),
                'source_csv_index': int(row.name),
                'personality_type': row['type'],
                'person_name': row['name'],
                'person_email': row['email'],
                'download_timestamp': 'fixed_mapping',
                'original_mapping': mismatch['current_row_id'],
                'fix_reason': 'csv_file_listing_mismatch'
            }
            
            # Save metadata next to file
            metadata_path = file_path.replace(os.path.splitext(file_path)[1], '_corrected_metadata.json')
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            self.fixes_applied.append({
                'file': file_path,
                'correct_row_id': correct_row_id,
                'metadata_created': metadata_path
            })
            
            fixed_count += 1
            
        print(f"  Created {fixed_count} corrected metadata files")
    
    def find_orphaned_files(self) -> List[str]:
        """Find files that exist but aren't listed in any CSV row"""
        print("\n=== FINDING ORPHANED FILES ===")
        
        orphaned = []
        
        # Get all files in CSV
        all_csv_files = set()
        
        for idx, row in self.df.iterrows():
            if pd.notna(row.get('youtube_files')):
                files = str(row['youtube_files']).split(';')
                all_csv_files.update(f.strip() for f in files if f.strip())
            
            if pd.notna(row.get('drive_files')):
                files = str(row['drive_files']).split(',')
                all_csv_files.update(f.strip() for f in files if f.strip())
        
        # Check all downloaded files
        all_files = glob.glob('*_downloads/**/*', recursive=True)
        
        for file_path in all_files:
            if not os.path.isfile(file_path):
                continue
            
            basename = os.path.basename(file_path)
            
            # Skip metadata and temporary files
            if any(basename.endswith(ext) for ext in ['.json', '.part', '.ytdl', '.tmp']):
                continue
            
            # Check if file is in CSV
            if basename not in all_csv_files:
                # Also check without extension variations
                name_without_ext = os.path.splitext(basename)[0]
                found = False
                for csv_file in all_csv_files:
                    if name_without_ext in csv_file or csv_file in basename:
                        found = True
                        break
                
                if not found:
                    orphaned.append(file_path)
        
        print(f"  Found {len(orphaned)} orphaned files not listed in CSV")
        
        if orphaned:
            print("  Sample orphaned files:")
            for f in orphaned[:5]:
                print(f"    - {f}")
        
        return orphaned
    
    def verify_all_csv_files_exist(self) -> List[Dict]:
        """Verify all files listed in CSV actually exist on disk"""
        print("\n=== VERIFYING CSV LISTED FILES EXIST ===")
        
        missing = []
        
        for idx, row in self.df.iterrows():
            row_id = str(row['row_id'])
            
            # Check YouTube files
            if pd.notna(row.get('youtube_files')) and row.get('youtube_status') == 'completed':
                files = str(row['youtube_files']).split(';')
                for file in files:
                    file = file.strip()
                    if not file:
                        continue
                    
                    # Search for file
                    found = glob.glob(f'*_downloads/**/{file}', recursive=True)
                    
                    if not found:
                        missing.append({
                            'row_id': row_id,
                            'name': row['name'],
                            'file': file,
                            'type': 'youtube',
                            'status': row['youtube_status']
                        })
            
            # Check Drive files
            if pd.notna(row.get('drive_files')) and row.get('drive_status') == 'completed':
                files = str(row['drive_files']).split(',')
                for file in files:
                    file = file.strip()
                    if not file:
                        continue
                    
                    found = glob.glob(f'*_downloads/**/{file}', recursive=True)
                    
                    if not found:
                        missing.append({
                            'row_id': row_id,
                            'name': row['name'],
                            'file': file,
                            'type': 'drive',
                            'status': row['drive_status']
                        })
        
        print(f"  Found {len(missing)} files listed in CSV but missing from disk")
        
        if missing:
            print("  Sample missing files:")
            for m in missing[:5]:
                print(f"    Row {m['row_id']} ({m['name']}): {m['file']}")
        
        return missing
    
    def generate_fix_report(self, mismatches: List[Dict], orphaned: List[str], missing: List[Dict]) -> None:
        """Generate comprehensive fix report"""
        print("\n=== FIX REPORT ===")
        
        print(f"\nIssues Found:")
        print(f"  Mismatched mappings: {len(mismatches)}")
        print(f"  Orphaned files: {len(orphaned)}")
        print(f"  Missing files: {len(missing)}")
        print(f"  Fixes applied: {len(self.fixes_applied)}")
        
        # Save detailed reports
        if mismatches:
            pd.DataFrame(mismatches).to_csv('mismatched_mappings.csv', index=False)
            print(f"\nMismatched mappings saved to: mismatched_mappings.csv")
        
        if orphaned:
            pd.DataFrame({'file_path': orphaned}).to_csv('orphaned_files.csv', index=False)
            print(f"Orphaned files saved to: orphaned_files.csv")
        
        if missing:
            pd.DataFrame(missing).to_csv('missing_csv_files.csv', index=False)
            print(f"Missing files saved to: missing_csv_files.csv")
        
        if self.fixes_applied:
            pd.DataFrame(self.fixes_applied).to_csv('mapping_fixes_applied.csv', index=False)
            print(f"Fixes applied saved to: mapping_fixes_applied.csv")
        
        # Summary
        summary = {
            'total_issues': len(mismatches) + len(orphaned) + len(missing),
            'mismatched_mappings': len(mismatches),
            'orphaned_files': len(orphaned),
            'missing_files': len(missing),
            'fixes_applied': len(self.fixes_applied),
            'unfixed_issues': len(mismatches) - len(self.fixes_applied) + len(orphaned) + len(missing)
        }
        
        with open('csv_file_fix_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\nSummary saved to: csv_file_fix_summary.json")


def main():
    parser = argparse.ArgumentParser(description='Fix CSV-file mapping issues')
    parser.add_argument('--csv', default='outputs/output.csv', help='Path to CSV file')
    parser.add_argument('--apply-fixes', action='store_true', help='Apply fixes (create corrected metadata)')
    
    args = parser.parse_args()
    
    fixer = CSVFileMappingFixer(args.csv)
    
    # Find issues
    mismatches = fixer.find_mismatched_mappings()
    orphaned = fixer.find_orphaned_files()
    missing = fixer.verify_all_csv_files_exist()
    
    # Apply fixes if requested
    if args.apply_fixes and mismatches:
        fixer.create_correct_mappings(mismatches)
    
    # Generate report
    fixer.generate_fix_report(mismatches, orphaned, missing)
    
    print("\n✅ CSV-file mapping analysis complete!")


if __name__ == "__main__":
    main()