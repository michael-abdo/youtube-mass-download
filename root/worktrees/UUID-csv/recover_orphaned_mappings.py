#!/usr/bin/env python3
"""
Recovery script to reconstruct UUID mappings for orphaned S3 files.

Based on analysis:
- 148 UUID files in S3
- Only 6 mapped in CSV
- 142 orphaned files (35.4 GB)
- Metadata JSON files exist for some clients in S3

Recovery strategy:
1. Use metadata timestamps to correlate with file upload times
2. Match file types with client link types (YouTube vs Drive)
3. Use file sizes and types to make educated guesses
4. Create mapping proposals for review
"""

import boto3
import pandas as pd
import json
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
import re

def get_s3_client():
    """Get S3 client with zenex profile."""
    session = boto3.Session(profile_name='zenex')
    return session.client('s3')

def load_orphaned_data():
    """Load the orphaned UUID analysis."""
    with open('orphaned_uuids.json', 'r') as f:
        return json.load(f)

def load_client_metadata():
    """Load client metadata."""
    with open('all_client_metadata.json', 'r') as f:
        return json.load(f)

def get_all_s3_file_details(bucket_name='typing-clients-uuid-system'):
    """Get detailed info for all S3 files including upload timestamps."""
    s3_client = get_s3_client()
    
    all_files = {}
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix='files/')
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.startswith('files/') and '.' in key:
                    uuid = key[6:].split('.')[0]
                    all_files[uuid] = {
                        'key': key,
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj['ETag'].strip('"'),
                        'extension': Path(key).suffix
                    }
    
    return all_files

def analyze_csv_for_patterns():
    """Analyze existing CSV mappings to understand patterns."""
    df = pd.read_csv('outputs/output.csv')
    
    patterns = {
        'youtube_videos': [],
        'drive_files': [],
        'drive_folders': []
    }
    
    for idx, row in df.iterrows():
        if pd.notna(row.get('file_uuids')) and row['file_uuids'] != '{}':
            try:
                uuid_dict = json.loads(row['file_uuids'].replace("'", '"'))
                for source, uuid in uuid_dict.items():
                    if 'YouTube' in source:
                        patterns['youtube_videos'].append({
                            'row_id': row['row_id'],
                            'name': row['name'],
                            'uuid': uuid,
                            'source': source
                        })
                    elif 'Drive file' in source:
                        patterns['drive_files'].append({
                            'row_id': row['row_id'],
                            'name': row['name'],
                            'uuid': uuid,
                            'source': source
                        })
            except:
                pass
    
    return patterns, df

def correlate_by_timing(client_metadata, s3_files):
    """Correlate files by upload timing."""
    correlations = defaultdict(list)
    
    # For each client with metadata
    for client_id, metadata_list in client_metadata.items():
        for meta_info in metadata_list:
            metadata = meta_info['metadata']
            
            # Get metadata save time
            if 'saved_at' in metadata:
                # Parse metadata time
                meta_time_str = metadata['saved_at'].replace('Z', '+00:00')
                if '.' in meta_time_str and '+' in meta_time_str:
                    # Handle microseconds
                    meta_time = datetime.fromisoformat(meta_time_str)
                else:
                    meta_time = datetime.fromisoformat(meta_time_str)
                
                # Look for files uploaded around the same time (within 24 hours)
                for uuid, file_info in s3_files.items():
                    file_time = file_info['last_modified']
                    
                    # Ensure both are timezone-aware
                    if file_time.tzinfo is None:
                        from datetime import timezone
                        file_time = file_time.replace(tzinfo=timezone.utc)
                    if meta_time.tzinfo is None:
                        from datetime import timezone
                        meta_time = meta_time.replace(tzinfo=timezone.utc)
                    
                    time_diff = abs((file_time - meta_time).total_seconds())
                    
                    if time_diff < 86400:  # 24 hours
                        correlations[client_id].append({
                            'uuid': uuid,
                            'file_info': file_info,
                            'metadata': metadata,
                            'time_diff_hours': time_diff / 3600,
                            'confidence': 'high' if time_diff < 3600 else 'medium'
                        })
    
    return correlations

def generate_recovery_proposals(correlations, csv_df, orphaned_uuids):
    """Generate recovery proposals for orphaned files."""
    proposals = []
    
    for client_id, matches in correlations.items():
        # Get client info from CSV
        client_row = csv_df[csv_df['row_id'] == int(client_id)]
        if client_row.empty:
            continue
        
        client_info = client_row.iloc[0]
        
        # Sort matches by confidence and time difference
        sorted_matches = sorted(matches, 
                              key=lambda x: (0 if x['confidence'] == 'high' else 1, x['time_diff_hours']))
        
        for match in sorted_matches:
            if match['uuid'] in orphaned_uuids:
                proposal = {
                    'row_id': int(client_id),
                    'name': client_info['name'],
                    'email': client_info['email'],
                    'uuid': match['uuid'],
                    'file_key': match['file_info']['key'],
                    'file_size_mb': match['file_info']['size'] / (1024*1024),
                    'file_type': match['file_info']['extension'],
                    'metadata_type': match['metadata']['type'],
                    'metadata_url': match['metadata'].get('url', ''),
                    'confidence': match['confidence'],
                    'time_diff_hours': round(match['time_diff_hours'], 2)
                }
                proposals.append(proposal)
    
    return proposals

def create_update_script(proposals):
    """Create a script to update the CSV with recovered mappings."""
    script_content = '''#!/usr/bin/env python3
"""
Auto-generated script to update CSV with recovered UUID mappings.
Review proposals before running!
"""

import pandas as pd
import json

def update_csv_with_mappings():
    df = pd.read_csv('outputs/output.csv')
    
    # Mapping proposals (review these before running!)
    updates = [
'''
    
    for prop in proposals[:20]:  # Limit to first 20 for review
        source_type = 'YouTube' if 'youtube' in prop['metadata_type'] else 'Drive file'
        update = f'''        {{
            'row_id': {prop['row_id']},
            'uuid': '{prop['uuid']}',
            'source': '{source_type}: {prop['metadata_url']}',
            'confidence': '{prop['confidence']}'
        }},
'''
        script_content += update
    
    script_content += '''    ]
    
    # Apply updates
    for update in updates:
        row_idx = df[df['row_id'] == update['row_id']].index
        if len(row_idx) > 0:
            idx = row_idx[0]
            
            # Get existing UUIDs
            existing = df.at[idx, 'file_uuids']
            if pd.isna(existing) or existing == '{}':
                uuid_dict = {}
            else:
                uuid_dict = json.loads(existing.replace("'", '"'))
            
            # Add new mapping
            uuid_dict[update['source']] = update['uuid']
            
            # Update DataFrame
            df.at[idx, 'file_uuids'] = str(uuid_dict)
            
            # Also update s3_paths
            s3_dict = {update['source']: f"files/{update['uuid']}.mp4"}  # Adjust extension
            df.at[idx, 's3_paths'] = str(s3_dict)
            
            print(f"Updated row {update['row_id']} with UUID {update['uuid']}")
    
    # Save updated CSV
    df.to_csv('outputs/output_recovered.csv', index=False)
    print("Saved to outputs/output_recovered.csv")

if __name__ == "__main__":
    update_csv_with_mappings()
'''
    
    with open('apply_recovered_mappings.py', 'w') as f:
        f.write(script_content)
    
    print("Created apply_recovered_mappings.py")

def main():
    print("UUID Mapping Recovery Process")
    print("=" * 80)
    
    # Load data
    print("\n1. Loading data...")
    orphaned_data = load_orphaned_data()
    client_metadata = load_client_metadata()
    patterns, csv_df = analyze_csv_for_patterns()
    
    print(f"   - Found {orphaned_data['total_orphaned']} orphaned UUIDs")
    print(f"   - Found metadata for {len(client_metadata)} clients")
    
    # Get detailed S3 file info
    print("\n2. Getting S3 file details...")
    s3_files = get_all_s3_file_details()
    print(f"   - Retrieved details for {len(s3_files)} UUID files")
    
    # Correlate by timing
    print("\n3. Correlating files by upload timing...")
    correlations = correlate_by_timing(client_metadata, s3_files)
    
    total_correlations = sum(len(matches) for matches in correlations.values())
    print(f"   - Found {total_correlations} potential correlations")
    
    # Generate proposals
    print("\n4. Generating recovery proposals...")
    proposals = generate_recovery_proposals(correlations, csv_df, orphaned_data['orphaned_uuids'])
    
    # Save proposals
    with open('recovery_proposals.json', 'w') as f:
        json.dump(proposals, f, indent=2, default=str)
    
    print(f"   - Generated {len(proposals)} recovery proposals")
    print("   - Saved to recovery_proposals.json")
    
    # Show sample proposals
    print("\n5. Sample recovery proposals:")
    for prop in proposals[:10]:
        print(f"\n   Client: {prop['name']} (row {prop['row_id']})")
        print(f"   UUID: {prop['uuid']}")
        print(f"   File: {prop['file_type']} ({prop['file_size_mb']:.2f} MB)")
        print(f"   Metadata: {prop['metadata_type']} - {prop['metadata_url'][:50]}...")
        print(f"   Confidence: {prop['confidence']} (time diff: {prop['time_diff_hours']} hours)")
    
    # Create update script
    print("\n6. Creating update script...")
    create_update_script(proposals)
    
    # Summary
    print("\n" + "=" * 80)
    print("RECOVERY SUMMARY:")
    print(f"- Total orphaned files: {orphaned_data['total_orphaned']}")
    print(f"- Proposals generated: {len(proposals)}")
    print(f"- High confidence: {len([p for p in proposals if p['confidence'] == 'high'])}")
    print(f"- Medium confidence: {len([p for p in proposals if p['confidence'] == 'medium'])}")
    print("\nNext steps:")
    print("1. Review recovery_proposals.json")
    print("2. Run apply_recovered_mappings.py to update CSV")
    print("3. Manually investigate remaining orphaned files")

if __name__ == "__main__":
    main()