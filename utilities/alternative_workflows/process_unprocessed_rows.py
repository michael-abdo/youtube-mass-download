#!/usr/bin/env python3
"""
Process rows in the CSV that have empty YouTube and Google Drive columns.
"""
import csv
import os
import sys
import time

# Add parent directory to path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.extract_links import process_url

def process_unprocessed_rows(csv_path, start_row=0, max_rows=None, delay_seconds=2):
    """
    Process rows in the CSV that have empty YouTube and Google Drive columns.
    
    Args:
        csv_path: Path to the CSV file
        start_row: Row number to start processing from (0-based, after header)
        max_rows: Maximum number of rows to process (None for all)
        delay_seconds: Delay between processing rows to avoid rate limiting
    """
    temp_file = csv_path + '.temp'
    
    processed_count = 0
    updated_count = 0
    skipped_count = 0
    
    with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile, \
         open(temp_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        
        for i, row in enumerate(reader):
            # Skip rows before start_row
            if i < start_row:
                writer.writerow(row)
                continue
            
            # Stop if we've processed max_rows
            if max_rows is not None and processed_count >= max_rows:
                writer.writerow(row)
                continue
            
            # Only process rows where both YouTube and Google Drive columns are empty
            youtube_empty = not row.get('youtube_playlist') or row.get('youtube_playlist') in ['-', "'-", '', 'None', 'nan']
            drive_empty = not row.get('google_drive') or row.get('google_drive') in ['-', "'-", '', 'None', 'nan']
            
            # Skip if row has been processed
            if not youtube_empty or not drive_empty:
                writer.writerow(row)
                skipped_count += 1
                continue
            
            # Process the row if it has a link
            link = row.get('link', '')
            if link:
                print(f"Processing row {i+1}: {row['name']} - {link}")
                
                try:
                    # Process URL to get links and extract YouTube/Drive info
                    links, yt_playlist, drive_links = process_url(link, limit=10)
                    
                    # Update YouTube column
                    if yt_playlist:
                        row['youtube_playlist'] = yt_playlist
                        print(f"  Found YouTube playlist: {yt_playlist}")
                    else:
                        row['youtube_playlist'] = ''
                        print("  No YouTube content found")
                    
                    # Update Google Drive column
                    if drive_links:
                        row['google_drive'] = '|'.join(drive_links)
                        print(f"  Found {len(drive_links)} Google Drive links")
                    else:
                        row['google_drive'] = ''
                        print("  No Google Drive links found")
                    
                    # Update the all links column if it doesn't already have content
                    if not row.get('extracted_links') and links:
                        row['extracted_links'] = '|'.join(links)
                    
                    updated_count += 1
                    
                except Exception as e:
                    print(f"  Error processing {link}: {str(e)}")
                    # Mark as processed with error
                    row['youtube_playlist'] = ''
                    row['google_drive'] = ''
                
                processed_count += 1
                
                # Add delay to avoid rate limiting
                if delay_seconds > 0 and processed_count < max_rows:
                    time.sleep(delay_seconds)
            else:
                # No link to process
                row['youtube_playlist'] = ''
                row['google_drive'] = ''
                processed_count += 1
            
            writer.writerow(row)
    
    # Replace original file with temp file
    os.replace(temp_file, csv_path)
    print(f"\nResults:")
    print(f"  Processed: {processed_count} rows")
    print(f"  Updated: {updated_count} rows with content")
    print(f"  Skipped: {skipped_count} rows (already processed)")
    print(f"CSV file updated: {csv_path}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process unprocessed rows in CSV file')
    parser.add_argument('--csv', default='/Users/Mike/ops_typing_log/ongoing_clients/outputs/output.csv',
                      help='Path to CSV file (default: outputs/output.csv)')
    parser.add_argument('--start', type=int, default=0,
                      help='Row number to start processing from (0-based, after header)')
    parser.add_argument('--max', type=int, default=None,
                      help='Maximum number of rows to process')
    parser.add_argument('--delay', type=float, default=2.0,
                      help='Delay in seconds between processing rows')
    
    args = parser.parse_args()
    
    print(f"Processing CSV file: {args.csv}")
    print(f"Starting from row: {args.start}")
    print(f"Max rows to process: {args.max if args.max else 'all'}")
    print(f"Delay between rows: {args.delay} seconds")
    
    process_unprocessed_rows(args.csv, args.start, args.max, args.delay)