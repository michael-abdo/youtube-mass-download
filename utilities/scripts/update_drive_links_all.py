#!/usr/bin/env python3
"""
Update all rows in the CSV with Google Drive links extracted from HTML.
"""
import csv
import os
import sys
from extract_links import process_url
from utils.constants import CSVConstants

def update_drive_links(csv_path, rows_to_process=None):
    """Update Google Drive links for all rows in the CSV file"""
    temp_file = csv_path + '.temp'
    
    processed = 0
    updated_count = 0
    
    with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile, \
         open(temp_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        
        for i, row in enumerate(reader):
            # Check if we should stop processing
            if rows_to_process is not None and processed >= rows_to_process:
                writer.writerow(row)
                continue
            
            link = row.get(CSVConstants.Columns.LINK, '')
            if link and 'docs.google.com/document' in link:
                # Only process Google Docs links
                print(f"Processing {i+1}: {row[CSVConstants.Columns.NAME]} - {link}")
                
                try:
                    # Get links, YouTube playlist, and Drive links
                    _, _, drive_links = process_url(link, limit=0, debug=False)
                    
                    # Only update if we found Drive links
                    if drive_links:
                        old_drive_links = row.get(CSVConstants.Columns.GOOGLE_DRIVE, '').split('|') if row.get(CSVConstants.Columns.GOOGLE_DRIVE) else []
                        old_drive_links = [l for l in old_drive_links if l]  # Remove empty strings
                        
                        # Combine existing and new drive links
                        combined_links = list(old_drive_links)
                        for link in drive_links:
                            if link not in combined_links:
                                combined_links.append(link)
                        
                        # Update row
                        row[CSVConstants.Columns.GOOGLE_DRIVE] = '|'.join(combined_links)
                        updated_count += 1
                        
                        print(f"  Found {len(drive_links)} Drive links, updated to {len(combined_links)} total")
                    else:
                        print(f"  No Drive links found")
                    
                    processed += 1
                    
                except Exception as e:
                    print(f"  Error processing {link}: {str(e)}")
            
            writer.writerow(row)
    
    # Replace original file with temp file
    os.replace(temp_file, csv_path)
    print(f"\nProcessed {processed} Google Docs, updated {updated_count} rows in {csv_path}")

if __name__ == "__main__":
    # Get parameters from command line
    csv_path = '/Users/Mike/ops_typing_log/ongoing_clients/outputs/output.csv'
    
    # Get number of rows to process
    rows_to_process = None
    if len(sys.argv) > 1:
        try:
            rows_to_process = int(sys.argv[1])
            print(f"Processing first {rows_to_process} Google Docs in {csv_path}")
        except ValueError:
            pass
    else:
        print(f"Processing all Google Docs in {csv_path}")
    
    # Run the update
    update_drive_links(csv_path, rows_to_process)