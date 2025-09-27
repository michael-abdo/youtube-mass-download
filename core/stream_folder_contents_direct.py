#!/usr/bin/env python3

import os
import json
import uuid
import pandas as pd
from datetime import datetime

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

from utils.s3_manager import UnifiedS3Manager, S3Config, UploadMode
from utils.download_drive import list_folder_files, extract_file_id
from utils.logging_config import get_logger
from utils.csv_manager import CSVManager
from utils.constants import CSVConstants

logger = get_logger(__name__)

def stream_drive_folders_direct():
    """Stream Google Drive folder contents directly to S3 without local storage."""
    
    # Folder information from metadata
    folders = [
        {
            'row_id': 476,
            'person_name': 'Patryk Makara',
            'folder_url': 'https://drive.google.com/drive/folders/1lk1xVjPKQvPsGMcBPUvA0KpvHhYJaI71',
            'folder_id': '1lk1xVjPKQvPsGMcBPUvA0KpvHhYJaI71'
        },
        {
            'row_id': 484,
            'person_name': 'Emilie',
            'folder_url': 'https://drive.google.com/drive/folders/1nrNku9G5dnWxGmfawSi6gLNb9Jaij_2r',
            'folder_id': '1nrNku9G5dnWxGmfawSi6gLNb9Jaij_2r'
        }
    ]
    
    # Initialize S3 manager for direct streaming
    s3_config = S3Config(
        bucket_name='typing-clients-uuid-system',
        upload_mode=UploadMode.DIRECT_STREAMING,
        organize_by_person=False,  # We handle our own UUID structure
        add_metadata=True,
        update_csv=True,
        csv_file='outputs/output.csv'
    )
    s3_manager = UnifiedS3Manager(s3_config)
    
    # Create backup of CSV
    backup_path = f"outputs/output.csv.backup_direct_stream_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.system(f"cp outputs/output.csv {backup_path}")
    logger.info(f"✅ Created CSV backup: {backup_path}")
    
    # DRY CONSOLIDATION: Use CSVManager instead of direct pandas
    csv_manager = CSVManager('outputs/output.csv')
    df = csv_manager.read_csv_safe()
    
    all_uploaded_files = {}
    
    for folder_info in folders:
        row_id = folder_info['row_id']
        person_name = folder_info['person_name']
        folder_url = folder_info['folder_url']
        folder_id = folder_info['folder_id']
        
        logger.info(f"\\n{'='*70}")
        logger.info(f"🚀 DIRECT STREAMING: {person_name} (Row {row_id})")
        logger.info(f"📁 Folder: {folder_url}")
        logger.info(f"📡 Mode: Google Drive → S3 (no local storage)")
        logger.info(f"{'='*70}")
        
        try:
            # List files in the folder
            logger.info(f"📋 Listing files in folder {folder_id}...")
            folder_files = list_folder_files(folder_url, logger)
            
            if not folder_files:
                logger.warning(f"⚠️  No files found in {person_name}'s folder")
                continue
                
            logger.info(f"✅ Found {len(folder_files)} files to stream")
            
            uploaded_files = []
            
            # Stream each file directly to S3
            for file_info in folder_files:
                file_id = file_info['id']
                file_name = file_info['name']
                
                # Generate UUID for S3 file
                file_uuid = str(uuid.uuid4())
                
                # Determine file extension from name
                # DRY CONSOLIDATION - Step 2: Use centralized extension handling
                from utils.path_utils import get_extension_or_default
                file_ext = get_extension_or_default(file_name, '.bin')
                
                # DRY CONSOLIDATION - Step 1: Use centralized S3 key generation
                from utils.s3_manager import UnifiedS3Manager
                s3_key = UnifiedS3Manager.generate_uuid_s3_key(file_uuid, file_ext)
                
                logger.info(f"\\n📤 Streaming: {file_name}")
                logger.info(f"   Drive ID: {file_id}")
                logger.info(f"   UUID: {file_uuid}")
                logger.info(f"   S3 Key: {s3_key}")
                
                try:
                    # Use the existing direct streaming method
                    result = s3_manager.stream_drive_to_s3(file_id, s3_key)
                    
                    if result.success:
                        file_size_mb = (result.file_size or 0) / (1024 * 1024)
                        
                        uploaded_files.append({
                            'uuid': file_uuid,
                            's3_key': s3_key,
                            'description': f"From folder: {file_name} ({file_size_mb:.1f} MB)",
                            'original_filename': file_name,
                            'file_size_bytes': result.file_size or 0,
                            'upload_time': result.upload_time or 0
                        })
                        
                        logger.info(f"   ✅ SUCCESS: {file_size_mb:.1f} MB in {result.upload_time:.1f}s")
                        
                    else:
                        logger.error(f"   ❌ FAILED: {result.error}")
                        continue
                        
                except Exception as e:
                    logger.error(f"   ❌ EXCEPTION: {e}")
                    continue
            
            if uploaded_files:
                all_uploaded_files[row_id] = uploaded_files
                
                # Update CSV with streamed file UUIDs
                logger.info(f"\\n📝 Updating CSV for {person_name}...")
                
                # Create JSON mappings
                file_uuids = {}
                s3_paths = {}
                
                for file_info in uploaded_files:
                    file_uuids[file_info['description']] = file_info['uuid']
                    s3_paths[file_info['uuid']] = file_info['s3_key']
                
                # Find row in DataFrame and update
                mask = df[CSVConstants.Columns.ROW_ID] == row_id
                if mask.any():
                    # Get existing mappings and merge
                    existing_uuids = df.loc[mask, CSVConstants.Columns.FILE_UUIDS].iloc[0]
                    existing_paths = df.loc[mask, CSVConstants.Columns.S3_PATHS].iloc[0]
                    
                    # DRY: Use CSVManager for loading existing UUIDs
                    existing_uuids_dict = CSVManager.load_file_uuids(df.loc[mask].iloc[0])
                    if existing_uuids_dict:
                        file_uuids.update(existing_uuids_dict)
                    
                    # DRY: Use CSVManager for loading existing paths
                    existing_paths_dict = CSVManager.load_s3_paths(df.loc[mask].iloc[0])
                    if existing_paths_dict:
                        s3_paths.update(existing_paths_dict)
                    
                    # DRY: Use CSVManager for S3 mapping updates
                    df.loc[mask, CSVConstants.Columns.FILE_UUIDS] = CSVManager.save_file_uuids(file_uuids)
                    df.loc[mask, CSVConstants.Columns.S3_PATHS] = CSVManager.save_s3_paths(s3_paths)
                    
                    logger.info(f"✅ Updated CSV with {len(uploaded_files)} files for {person_name}")
                else:
                    logger.error(f"❌ Row {row_id} not found in CSV")
            else:
                logger.warning(f"⚠️  No files streamed for {person_name}")
                
        except Exception as e:
            logger.error(f"❌ Error processing {person_name}'s folder: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Save updated CSV
    df.to_csv('outputs/output.csv', index=False)
    logger.info(f"\\n✅ CSV updated with direct-streamed file mappings")
    
    # Summary
    logger.info(f"\\n{'='*70}")
    logger.info(f"🎯 DIRECT STREAMING SUMMARY")
    logger.info(f"{'='*70}")
    
    total_files = 0
    total_size = 0
    total_time = 0
    
    for row_id, files in all_uploaded_files.items():
        folder_info = next(f for f in folders if f['row_id'] == row_id)
        logger.info(f"\\n{folder_info['person_name']} (Row {row_id}): {len(files)} files")
        
        for file_info in files:
            size_mb = file_info['file_size_bytes'] / (1024 * 1024)
            time_s = file_info['upload_time']
            speed_mbps = size_mb / time_s if time_s > 0 else 0
            
            logger.info(f"  ✅ s3://typing-clients-uuid-system/{file_info['s3_key']}")
            logger.info(f"     UUID: {file_info['uuid']}")
            logger.info(f"     File: {file_info['original_filename']} ({size_mb:.1f} MB)")
            logger.info(f"     Time: {time_s:.1f}s ({speed_mbps:.1f} MB/s)")
            
            total_size += file_info['file_size_bytes']
            total_time += time_s
        
        total_files += len(files)
    
    total_size_mb = total_size / (1024 * 1024)
    total_size_gb = total_size / (1024 * 1024 * 1024)
    avg_speed = total_size_mb / total_time if total_time > 0 else 0
    
    logger.info(f"\\n📊 TOTALS:")
    logger.info(f"   Files streamed: {total_files}")
    logger.info(f"   Total size: {total_size_mb:.1f} MB ({total_size_gb:.2f} GB)")
    logger.info(f"   Total time: {total_time:.1f}s")
    logger.info(f"   Average speed: {avg_speed:.1f} MB/s")
    logger.info(f"\\n🚀 ALL FOLDER CONTENTS STREAMED DIRECTLY TO S3!")
    logger.info(f"💾 No local storage used - efficient and reliable!")
    
    return all_uploaded_files

if __name__ == "__main__":
    stream_drive_folders_direct()