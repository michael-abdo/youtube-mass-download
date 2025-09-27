#!/usr/bin/env python3
import csv
import os
import sys
import time
from utils.extract_links import process_url

def update_empty_links_in_csv(max_rows=None):
    """
    Process only rows that have both youtube_playlist and google_drive empty.
    
    Args:
        max_rows (int, optional): Maximum number of rows to process. Defaults to None (process all).
    """
    import sys, os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'utils'))
    from config import get_config
    config = get_config()
    input_filename = config.get('paths.output_csv', 'outputs/output.csv')
    temp_filename = "output_with_links.csv"
    
    if not os.path.exists(input_filename):
        print(f"Error: {input_filename} not found.")
        return
    
    rows = []
    processed_count = 0
    print("Processing links from CSV...")
    with open(input_filename, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        fieldnames = list(reader.fieldnames)
        
        # Make sure the required columns exist
        if "youtube_playlist" not in fieldnames:
            fieldnames.append("youtube_playlist")
        if "google_drive" not in fieldnames:
            fieldnames.append("google_drive")
        if "extracted_links" not in fieldnames:
            fieldnames.append("extracted_links")
            
        for row in reader:
            link = row.get("link", "")
            
            # Handle None values properly
            youtube_playlist = row.get("youtube_playlist")
            google_drive = row.get("google_drive")
            
            # Convert None to empty string for strip() operation
            youtube_playlist = "" if youtube_playlist is None else youtube_playlist.strip()
            google_drive = "" if google_drive is None else google_drive.strip()
            
            # Check if both youtube_playlist and google_drive are empty
            # and only process if both are empty
            if not youtube_playlist and not google_drive and link:
                # Check if we've reached the maximum rows to process
                if max_rows is not None and processed_count >= max_rows:
                    print(f"Reached maximum rows limit ({max_rows}), skipping remaining rows")
                    rows.append(row)
                    continue
                
                print(f"Processing {link} for {row.get('name', 'unknown')}...")
                try:
                    # Process the URL and get links, YouTube playlist, and Google Drive links
                    # With use_dash_for_empty=True, "-" will be used for empty values
                    links, youtube_playlist, drive_links = process_url(link, limit=10, use_dash_for_empty=True)
                    row["extracted_links"] = "|".join(links) if links else ""
                    row["youtube_playlist"] = youtube_playlist  # Already "-" if empty
                    
                    # Check if drive_links contains only a dash
                    if drive_links == ["-"]:
                        row["google_drive"] = "-"
                    else:
                        row["google_drive"] = "|".join(drive_links)
                    
                    print(f"  Found {len(links)} links, {'a' if youtube_playlist else 'no'} YouTube playlist, " +
                          f"and {len(drive_links) if drive_links else 0} Google Drive links")
                    
                    processed_count += 1
                except Exception as e:
                    print(f"  Error processing {link}: {str(e)}")
                    row["extracted_links"] = row.get("extracted_links", "")
                    row["youtube_playlist"] = "-"
                    row["google_drive"] = "-"
                    processed_count += 1
            else:
                print(f"Skipping row for {row.get('name', 'unknown')} - already has YouTube or Drive links")
            rows.append(row)
    
    # Write results to new CSV
    print(f"Writing updated data to {temp_filename}...")
    with open(temp_filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    # Replace original file with updated one
    os.replace(temp_filename, input_filename)
    print(f"Successfully updated {input_filename}")
    print(f"Processed {processed_count} rows")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Update empty YouTube playlist and Google Drive links with "-"')
    parser.add_argument('--max-rows', type=int, default=None, 
                        help='Maximum number of rows to process')
    
    args = parser.parse_args()
    
    update_empty_links_in_csv(max_rows=args.max_rows)