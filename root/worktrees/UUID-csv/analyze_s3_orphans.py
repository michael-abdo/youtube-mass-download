#!/usr/bin/env python3
"""
Analyze orphaned S3 files - files in S3 bucket without CSV mappings.
"""

import boto3
import pandas as pd
import json
from collections import defaultdict
from pathlib import Path

def get_s3_client():
    """Get S3 client."""
    # Use the zenex AWS profile
    session = boto3.Session(profile_name='zenex')
    return session.client('s3')

def list_all_s3_files(bucket_name='typing-clients-uuid-system'):
    """List all files in S3 bucket."""
    s3_client = get_s3_client()
    all_files = []
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    all_files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat(),
                        'etag': obj['ETag'].strip('"')
                    })
    except Exception as e:
        print(f"Error listing S3 objects: {e}")
        return []
    
    return all_files

def extract_uuid_from_key(key):
    """Extract UUID from S3 key like 'files/uuid.ext'."""
    if key.startswith('files/'):
        filename = key[6:]  # Remove 'files/' prefix
        if '.' in filename:
            uuid_part = filename.split('.')[0]
            return uuid_part
    return None

def load_csv_mappings(csv_path='outputs/output.csv'):
    """Load UUID mappings from CSV."""
    try:
        df = pd.read_csv(csv_path)
        
        # Extract all UUIDs from file_uuids column
        all_uuids = set()
        
        for idx, row in df.iterrows():
            if pd.notna(row.get('file_uuids')) and row['file_uuids'] != '{}':
                try:
                    # Parse the JSON-like string
                    uuid_dict = json.loads(row['file_uuids'].replace("'", '"'))
                    for uuid_val in uuid_dict.values():
                        all_uuids.add(uuid_val)
                except:
                    pass
        
        return all_uuids, df
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return set(), None

def analyze_orphans():
    """Main analysis function."""
    print("Analyzing S3 orphaned files...")
    print("-" * 80)
    
    # List all S3 files
    print("1. Listing all S3 files...")
    s3_files = list_all_s3_files()
    print(f"   Found {len(s3_files)} total files in S3")
    
    # Filter for UUID files
    uuid_files = {}
    for file_info in s3_files:
        uuid = extract_uuid_from_key(file_info['key'])
        if uuid:
            uuid_files[uuid] = file_info
    
    print(f"   Found {len(uuid_files)} UUID files in S3")
    
    # Load CSV mappings
    print("\n2. Loading CSV mappings...")
    csv_uuids, df = load_csv_mappings()
    print(f"   Found {len(csv_uuids)} UUIDs mapped in CSV")
    
    # Find orphans
    print("\n3. Analyzing orphaned files...")
    orphaned_uuids = set(uuid_files.keys()) - csv_uuids
    print(f"   Found {len(orphaned_uuids)} orphaned UUID files!")
    
    # Analyze file types
    print("\n4. Analyzing orphaned file types...")
    type_counts = defaultdict(int)
    total_size = 0
    
    for uuid in orphaned_uuids:
        file_info = uuid_files[uuid]
        key = file_info['key']
        ext = Path(key).suffix.lower()
        type_counts[ext] += 1
        total_size += file_info['size']
    
    print("   File type breakdown:")
    for ext, count in sorted(type_counts.items()):
        print(f"     {ext}: {count} files")
    
    print(f"\n   Total size of orphaned files: {total_size / (1024*1024):.2f} MB")
    
    # Check if CSV has the expected columns
    if df is not None:
        print("\n5. Analyzing CSV structure...")
        print(f"   Total rows in CSV: {len(df)}")
        
        # Count rows with non-empty UUID mappings
        rows_with_uuids = 0
        for idx, row in df.iterrows():
            if pd.notna(row.get('file_uuids')) and row['file_uuids'] != '{}':
                rows_with_uuids += 1
        
        print(f"   Rows with UUID mappings: {rows_with_uuids}")
        
        # Show sample of rows with mappings
        print("\n   Sample rows with UUID mappings:")
        count = 0
        for idx, row in df.iterrows():
            if pd.notna(row.get('file_uuids')) and row['file_uuids'] != '{}':
                print(f"     Row {row['row_id']}: {row['name']} - {row['file_uuids']}")
                count += 1
                if count >= 5:
                    break
    
    # Save orphaned UUIDs to file
    print("\n6. Saving orphaned UUIDs to file...")
    with open('orphaned_uuids.json', 'w') as f:
        orphan_data = {
            'total_s3_files': len(s3_files),
            'total_uuid_files': len(uuid_files),
            'total_csv_mappings': len(csv_uuids),
            'total_orphaned': len(orphaned_uuids),
            'orphaned_uuids': sorted(list(orphaned_uuids)),
            'file_type_counts': dict(type_counts),
            'total_orphaned_size_mb': total_size / (1024*1024),
            'sample_orphaned_files': [uuid_files[uuid] for uuid in list(orphaned_uuids)[:10]]
        }
        json.dump(orphan_data, f, indent=2)
    
    print("   Saved to orphaned_uuids.json")
    
    # List non-UUID files for additional context
    print("\n7. Analyzing non-UUID files in S3...")
    non_uuid_files = []
    for file_info in s3_files:
        if not file_info['key'].startswith('files/'):
            non_uuid_files.append(file_info)
    
    print(f"   Found {len(non_uuid_files)} non-UUID files:")
    for file_info in non_uuid_files[:20]:  # Show first 20
        print(f"     {file_info['key']} ({file_info['size']} bytes)")
    
    return orphan_data

if __name__ == "__main__":
    analyze_orphans()