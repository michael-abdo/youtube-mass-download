#!/usr/bin/env python3

import pandas as pd
import json
from datetime import datetime
from utils.csv_manager import CSVManager
from utils.config import get_s3_bucket
from utils.s3_manager import get_s3_client
from utils.constants import CSVConstants

def fix_s3_file_extensions():
    """Fix the .bin extensions on S3 files to proper media extensions."""
    
    # DRY CONSOLIDATION: Use centralized S3 client
    s3_client = get_s3_client()
    # DRY: Use centralized S3 bucket configuration
    bucket_name = get_s3_bucket()
    
    # Mapping from UUID to correct filename and extension
    file_mappings = {
        # Patryk Makara files
        '47386648-f609-408b-ad23-c2da35936f06': {
            'filename': 'Lifting weight.mp4',
            'extension': '.mp4',
            'content_type': 'video/mp4'
        },
        'a3ac649b-7770-4abc-a2b3-95490ff569e8': {
            'filename': 'Optional - Old Typing Video worse quality.mp4', 
            'extension': '.mp4',
            'content_type': 'video/mp4'
        },
        '26f7198a-d924-4290-a2f2-df04f1d66415': {
            'filename': 'Random guitar faces.mp4',
            'extension': '.mp4', 
            'content_type': 'video/mp4'
        },
        'fb2cde1f-2b5a-4511-98e1-04c6c6be3015': {
            'filename': 'Blooper 2.mp4',
            'extension': '.mp4',
            'content_type': 'video/mp4'
        },
        '1b6b8cc0-5c62-472c-94ac-ddcaad1d3605': {
            'filename': 'Blooper 1.mp4',
            'extension': '.mp4',
            'content_type': 'video/mp4'
        },
        # Emilie files
        '072d3622-45b2-4b32-9399-ce52ecb9a87e': {
            'filename': '2.Questions I answered.docx',
            'extension': '.docx',
            'content_type': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        },
        '8506e708-7309-4c59-88ab-f877dd09a22f': {
            'filename': '1.Please read me.docx',
            'extension': '.docx',
            'content_type': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        }
    }
    
    # Create backup of CSV
    backup_path = f"outputs/output.csv.backup_extension_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    import os
    os.system(f"cp outputs/output.csv {backup_path}")
    print(f"✅ Created CSV backup: {backup_path}")
    
    print(f"\\n{'='*70}")
    print(f"🔧 FIXING S3 FILE EXTENSIONS")
    print(f"{'='*70}")
    
    fixed_mappings = {}
    
    # DRY CONSOLIDATION - Step 1: Import S3Manager for key generation
    from utils.s3_manager import UnifiedS3Manager
    
    for uuid, info in file_mappings.items():
        old_key = UnifiedS3Manager.generate_uuid_s3_key(uuid, '.bin')
        new_key = UnifiedS3Manager.generate_uuid_s3_key(uuid, info['extension'])
        filename = info['filename']
        content_type = info['content_type']
        
        print(f"\\n📄 {filename}")
        print(f"   UUID: {uuid}")
        print(f"   From: {old_key}")
        print(f"   To: {new_key}")
        
        try:
            # Check if old file exists
            try:
                s3_client.head_object(Bucket=bucket_name, Key=old_key)
            except s3_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print(f"   ⚠️  Source file not found: {old_key}")
                    continue
                else:
                    raise
            
            # Copy with new extension and content type
            copy_source = {'Bucket': bucket_name, 'Key': old_key}
            
            # Get original metadata
            original = s3_client.head_object(Bucket=bucket_name, Key=old_key)
            original_metadata = original.get('Metadata', {})
            
            # Add filename to metadata
            new_metadata = original_metadata.copy()
            new_metadata['original_filename'] = filename
            
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource=copy_source,
                Key=new_key,
                MetadataDirective='REPLACE',
                Metadata=new_metadata,
                ContentType=content_type
            )
            
            # Delete old file
            s3_client.delete_object(Bucket=bucket_name, Key=old_key)
            
            fixed_mappings[uuid] = new_key
            print(f"   ✅ SUCCESS: Fixed extension and content type")
            
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
            continue
    
    # Update CSV with corrected S3 paths
    print(f"\\n📝 Updating CSV with corrected file paths...")
    
    # DRY CONSOLIDATION - Step 1: Use atomic CSV operations to prevent corruption
    from utils.csv_manager import CSVManager
    csv_manager = CSVManager('outputs/output.csv')
    df = csv_manager.safe_csv_read('outputs/output.csv')
    
    for row_id in [476, 484]:
        mask = df[CSVConstants.Columns.ROW_ID] == row_id
        if mask.any():
            s3_paths = df.loc[mask, CSVConstants.Columns.S3_PATHS].iloc[0]
            
            if s3_paths and s3_paths not in ['[]', '{}', None]:
                try:
                    # DRY: Use CSVManager for loading S3 paths
                    row = df.loc[mask].iloc[0]
                    paths_dict = CSVManager.load_s3_paths(row)
                    updated_paths = {}
                    
                    for uuid, old_path in paths_dict.items():
                        if uuid in fixed_mappings:
                            updated_paths[uuid] = fixed_mappings[uuid]
                            print(f"   ✅ Updated path for {uuid[:8]}...")
                        else:
                            updated_paths[uuid] = old_path
                    
                    # DRY: Use CSVManager for saving S3 paths
                    df.loc[mask, CSVConstants.Columns.S3_PATHS] = CSVManager.save_s3_paths(updated_paths)
                    
                except Exception as e:
                    print(f"   ❌ Error updating CSV for row {row_id}: {e}")
    
    # Save updated CSV atomically to prevent corruption
    csv_manager.safe_csv_write(df, "fix_s3_extensions")
    print(f"\\n✅ CSV updated with corrected file extensions")
    
    # Summary
    print(f"\\n{'='*70}")
    print(f"📊 EXTENSION FIX SUMMARY")
    print(f"{'='*70}")
    
    for uuid, new_key in fixed_mappings.items():
        filename = file_mappings[uuid]['filename']
        extension = file_mappings[uuid]['extension']
        print(f"✅ s3://{bucket_name}/{new_key}")
        print(f"   File: {filename}")
        print(f"   Fixed: .bin → {extension}")
        print()
    
    print(f"🎯 FIXED {len(fixed_mappings)} FILES WITH PROPER EXTENSIONS!")
    
    return fixed_mappings

if __name__ == "__main__":
    fix_s3_file_extensions()