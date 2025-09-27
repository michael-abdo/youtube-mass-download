#!/usr/bin/env python3
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
    ]
    
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
