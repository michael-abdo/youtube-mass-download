#!/usr/bin/env python3
import csv
import os
import sys

# Add utils directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'utils'))

from extract_links import process_url

# Get the first N rows from CSV file
def process_first_n_rows(csv_path, n=5):
    results = []
    
    with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for i, row in enumerate(reader):
            if i >= n:
                break
                
            link = row.get('link', '')
            if link:
                print(f"\nProcessing {i+1}: {row['name']} - {link}")
                try:
                    # Process URL with limit=10 to get more links
                    links, yt_playlist, drive_links = process_url(link, limit=10, debug=False)
                    
                    results.append({
                        'name': row['name'],
                        'link': link,
                        'links_found': len(links),
                        'links': links,
                        'youtube_playlist': yt_playlist,
                        'drive_links': drive_links
                    })
                    
                    print(f"  Found {len(links)} links")
                    if yt_playlist:
                        print(f"  YouTube playlist: {yt_playlist}")
                    if drive_links:
                        print(f"  Drive links: {len(drive_links)}")
                        for dl in drive_links:
                            print(f"    - {dl}")
                    
                except Exception as e:
                    print(f"  Error processing {link}: {str(e)}")
    
    return results

if __name__ == "__main__":
    csv_path = '/Users/Mike/ops_typing_log/ongoing_clients/outputs/output.csv'
    
    # Get number of rows to process
    n = 5
    if len(sys.argv) > 1:
        try:
            n = int(sys.argv[1])
        except ValueError:
            pass
    
    print(f"Processing first {n} rows from {csv_path}")
    results = process_first_n_rows(csv_path, n)
    
    print("\nSummary:")
    for result in results:
        print(f"{result['name']}: {result['links_found']} links" + 
              (f", YouTube playlist" if result['youtube_playlist'] else "") + 
              (f", {len(result['drive_links'])} Drive links" if result['drive_links'] else ""))