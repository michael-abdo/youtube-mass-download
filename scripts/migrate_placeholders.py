#!/usr/bin/env python3
"""
Migration script to convert dash placeholders to None/NaN in CSV files.
This fixes the issue where CSV injection protection transformed '-' to "'-".
"""

import pandas as pd
import numpy as np
import shutil
from datetime import datetime
import os
import sys

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.csv_tracker import safe_csv_write
from utils.csv_backup import backup_csv
from utils.file_lock import file_lock

def analyze_placeholders(csv_path: str) -> dict:
    """Analyze current placeholder usage in CSV"""
    df = pd.read_csv(csv_path, dtype=str)
    
    analysis = {
        'total_rows': len(df),
        'youtube_placeholder_counts': {},
        'drive_placeholder_counts': {},
        'affected_rows': set()
    }
    
    # Values to convert to NaN
    placeholder_values = ['-', "'-", "'", '"-"', "'-'"]
    
    # Check youtube_playlist column
    if 'youtube_playlist' in df.columns:
        for placeholder in placeholder_values:
            count = (df['youtube_playlist'] == placeholder).sum()
            if count > 0:
                analysis['youtube_placeholder_counts'][placeholder] = count
                rows = df[df['youtube_playlist'] == placeholder].index.tolist()
                analysis['affected_rows'].update(rows)
    
    # Check google_drive column
    if 'google_drive' in df.columns:
        for placeholder in placeholder_values:
            count = (df['google_drive'] == placeholder).sum()
            if count > 0:
                analysis['drive_placeholder_counts'][placeholder] = count
                rows = df[df['google_drive'] == placeholder].index.tolist()
                analysis['affected_rows'].update(rows)
    
    analysis['total_affected_rows'] = len(analysis['affected_rows'])
    
    return analysis

def migrate_placeholders_to_nan(csv_path: str, dry_run: bool = True) -> bool:
    """
    Convert all placeholder values to NaN in the CSV file.
    
    Args:
        csv_path: Path to CSV file
        dry_run: If True, only analyze without making changes
        
    Returns:
        True if migration successful, False otherwise
    """
    print(f"{'[DRY RUN] ' if dry_run else ''}Migrating placeholders to NaN in: {csv_path}")
    
    # Analyze current state
    print("\n=== Pre-migration Analysis ===")
    pre_analysis = analyze_placeholders(csv_path)
    
    if not pre_analysis['youtube_placeholder_counts'] and not pre_analysis['drive_placeholder_counts']:
        print("No placeholder values found in CSV. No migration needed.")
        return True
    
    print(f"Total rows: {pre_analysis['total_rows']}")
    print(f"Rows with placeholders: {pre_analysis['total_affected_rows']}")
    
    if pre_analysis['youtube_placeholder_counts']:
        print("\nYouTube Playlist placeholders:")
        for placeholder, count in pre_analysis['youtube_placeholder_counts'].items():
            print(f"  '{placeholder}': {count} occurrences")
    
    if pre_analysis['drive_placeholder_counts']:
        print("\nGoogle Drive placeholders:")
        for placeholder, count in pre_analysis['drive_placeholder_counts'].items():
            print(f"  '{placeholder}': {count} occurrences")
    
    # Check current validation status
    is_valid, message = validate_csv_integrity(csv_path)
    print(f"\nCurrent validation status: {'Valid' if is_valid else f'Invalid - {message}'}")
    
    if dry_run:
        # Simulate the migration
        df = pd.read_csv(csv_path, dtype=str)
        df_simulated = df.copy()
        
        # Values to convert to NaN
        placeholder_values = ['-', "'-", "'", '"-"', "'-'"]
        
        # Replace placeholders with NaN
        if 'youtube_playlist' in df_simulated.columns:
            df_simulated['youtube_playlist'] = df_simulated['youtube_playlist'].replace(placeholder_values, np.nan)
            
        if 'google_drive' in df_simulated.columns:
            df_simulated['google_drive'] = df_simulated['google_drive'].replace(placeholder_values, np.nan)
        
        # Check what validation would be after migration
        temp_path = csv_path + '.temp_simulation'
        df_simulated.to_csv(temp_path, index=False)
        
        is_valid_after, message_after = validate_csv_integrity(temp_path)
        print(f"\nPost-migration validation (simulated): {'Valid' if is_valid_after else f'Invalid - {message_after}'}")
        
        # Show sample of changes
        changed_rows = []
        for idx in list(pre_analysis['affected_rows'])[:5]:  # Show first 5 changes
            if idx < len(df):
                before_yt = df.iloc[idx]['youtube_playlist'] if 'youtube_playlist' in df.columns else None
                after_yt = df_simulated.iloc[idx]['youtube_playlist'] if 'youtube_playlist' in df_simulated.columns else None
                before_drive = df.iloc[idx]['google_drive'] if 'google_drive' in df.columns else None
                after_drive = df_simulated.iloc[idx]['google_drive'] if 'google_drive' in df_simulated.columns else None
                
                if before_yt != after_yt or before_drive != after_drive:
                    changed_rows.append({
                        'row_id': df.iloc[idx]['row_id'] if 'row_id' in df.columns else idx,
                        'name': df.iloc[idx]['name'] if 'name' in df.columns else 'Unknown',
                        'youtube_before': before_yt,
                        'youtube_after': 'NaN' if pd.isna(after_yt) else after_yt,
                        'drive_before': before_drive,
                        'drive_after': 'NaN' if pd.isna(after_drive) else after_drive
                    })
        
        if changed_rows:
            print("\n=== Sample of Changes (first 5) ===")
            for row in changed_rows:
                print(f"Row {row['row_id']} ({row['name']}):")
                if row['youtube_before'] != row['youtube_after']:
                    print(f"  YouTube: '{row['youtube_before']}' → {row['youtube_after']}")
                if row['drive_before'] != row['drive_after']:
                    print(f"  Drive: '{row['drive_before']}' → {row['drive_after']}")
        
        # Clean up temp file
        os.remove(temp_path)
        
        print("\n[DRY RUN] No changes made. Run with dry_run=False to apply migration.")
        return True
    
    # Actual migration
    with file_lock(f'{csv_path}.lock'):
        # Create backup
        backup_path = backup_csv(csv_path, 'migrate_placeholders')
        print(f"\nCreated backup: {backup_path}")
        
        # Read CSV with string dtypes to preserve data
        df = pd.read_csv(csv_path, dtype=str)
        
        # Values to convert to NaN
        placeholder_values = ['-', "'-", "'", '"-"', "'-'"]
        
        # Replace placeholders with NaN
        changes_made = 0
        
        if 'youtube_playlist' in df.columns:
            before_count = df['youtube_playlist'].isin(placeholder_values).sum()
            df['youtube_playlist'] = df['youtube_playlist'].replace(placeholder_values, np.nan)
            changes_made += before_count
            
        if 'google_drive' in df.columns:
            before_count = df['google_drive'].isin(placeholder_values).sum()
            df['google_drive'] = df['google_drive'].replace(placeholder_values, np.nan)
            changes_made += before_count
        
        print(f"\nReplacing {changes_made} placeholder values with NaN...")
        
        # Save migrated CSV
        expected_cols = df.columns.tolist()
        success = safe_csv_write(df, csv_path, 'migrate_placeholders', expected_cols)
        
        if success:
            print("✅ Migration completed successfully")
            
            # Verify post-migration state
            is_valid_after, message_after = validate_csv_integrity(csv_path)
            print(f"\nPost-migration validation: {'Valid' if is_valid_after else f'Invalid - {message_after}'}")
            
            # Final analysis
            post_analysis = analyze_placeholders(csv_path)
            remaining_count = (len(post_analysis['youtube_placeholder_counts']) + 
                             len(post_analysis['drive_placeholder_counts']))
            
            if remaining_count > 0:
                print("\n⚠️ Warning: Some placeholder values remain:")
                for placeholder, count in post_analysis['youtube_placeholder_counts'].items():
                    print(f"  YouTube '{placeholder}': {count}")
                for placeholder, count in post_analysis['drive_placeholder_counts'].items():
                    print(f"  Drive '{placeholder}': {count}")
            else:
                print("\n✅ All placeholder values successfully converted to NaN")
            
            return True
        else:
            print("❌ Migration failed - CSV write validation failed")
            print(f"Backup preserved at: {backup_path}")
            return False

def main():
    """Run the migration with safety checks"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate CSV placeholders to NaN')
    from utils.config import get_config
    config = get_config()
    default_csv = config.get('paths.output_csv', 'outputs/output.csv')
    parser.add_argument('--csv', default=default_csv, help='Path to CSV file')
    parser.add_argument('--apply', action='store_true', help='Apply migration (default is dry run)')
    
    args = parser.parse_args()
    
    # Always do dry run first
    if args.apply:
        # First show what will change
        print("=== Preview of changes ===")
        migrate_placeholders_to_nan(args.csv, dry_run=True)
        
        # Ask for confirmation
        print("\n" + "="*50)
        response = input("Do you want to proceed with the migration? (yes/no): ")
        if response.lower() == 'yes':
            print("\n=== Running migration ===")
            success = migrate_placeholders_to_nan(args.csv, dry_run=False)
        else:
            print("Migration cancelled.")
            success = False
    else:
        print("=== Dry run mode (use --apply to make changes) ===")
        success = migrate_placeholders_to_nan(args.csv, dry_run=True)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())