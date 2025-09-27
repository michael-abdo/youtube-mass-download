#!/usr/bin/env python3
"""
Cleanup utility to fix malformed URLs in the CSV file.
This script:
1. Fixes YouTube playlist URLs with trailing text/newlines
2. Removes duplicate video IDs from playlists
3. Filters out non-YouTube URLs (CSS files, etc.)
"""

import csv
import re
import sys
import os
from pathlib import Path

# Add parent directory to path to import utilities
sys.path.append(str(Path(__file__).parent.parent.parent))
from utils.extract_links import clean_url

def clean_youtube_playlist_url(url):
    """Clean a YouTube playlist URL by extracting valid video IDs and removing duplicates"""
    if not url or url == '-':
        return url
    
    # Skip non-YouTube URLs
    if 'youtube.com' not in url:
        return '-'
    
    # Skip CSS/JS files
    if any(url.endswith(ext) for ext in ['.css', '.js']):
        return '-'
    
    # Handle playlist URLs
    if 'watch_videos?video_ids=' in url:
        try:
            # Extract video IDs part
            video_ids_part = url.split('watch_videos?video_ids=')[1]
            
            # Extract valid video IDs (11 characters, alphanumeric with - and _)
            video_ids = re.findall(r'[a-zA-Z0-9_-]{11}', video_ids_part)
            
            if video_ids:
                # Remove duplicates while preserving order
                seen = set()
                unique_ids = []
                for vid in video_ids:
                    if vid not in seen:
                        seen.add(vid)
                        unique_ids.append(vid)
                
                # Return cleaned playlist URL
                return f'https://www.youtube.com/watch_videos?video_ids={",".join(unique_ids)}'
            else:
                return '-'
        except Exception as e:
            print(f"Error cleaning playlist URL {url}: {e}")
            return '-'
    
    # For regular YouTube URLs, just clean them
    return clean_url(url)

def cleanup_csv(input_file=None, output_file=None):
    if input_file is None:
        import sys, os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'utils'))
        from config import get_config
        config = get_config()
        input_file = config.get('paths.output_csv', 'outputs/output.csv')
    if output_file is None:
        output_file = input_file.replace('.csv', '_cleaned.csv')
    """Clean up malformed URLs in the CSV file"""
    
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found")
        return
    
    rows_processed = 0
    playlists_cleaned = 0
    duplicates_removed = 0
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in reader:
            rows_processed += 1
            
            # Clean YouTube playlist URLs
            if 'youtube_playlist' in row and row['youtube_playlist'] and row['youtube_playlist'] != '-':
                original = row['youtube_playlist']
                cleaned = clean_youtube_playlist_url(original)
                
                if cleaned != original:
                    playlists_cleaned += 1
                    
                    # Count removed duplicates
                    if 'watch_videos?video_ids=' in original and 'watch_videos?video_ids=' in cleaned:
                        try:
                            orig_ids = original.split('watch_videos?video_ids=')[1].split(',')
                            clean_ids = cleaned.split('watch_videos?video_ids=')[1].split(',')
                            duplicates_removed += len(orig_ids) - len(clean_ids)
                        except:
                            pass
                
                row['youtube_playlist'] = cleaned
            
            writer.writerow(row)
    
    print(f"\nCleanup Summary:")
    print(f"- Rows processed: {rows_processed}")
    print(f"- Playlists cleaned: {playlists_cleaned}")
    print(f"- Duplicate video IDs removed: {duplicates_removed}")
    print(f"\nCleaned data saved to: {output_file}")
    
    # Ask if user wants to replace original file
    response = input(f"\nReplace original {input_file} with cleaned version? (y/n): ")
    if response.lower() == 'y':
        import shutil
        shutil.move(output_file, input_file)
        print(f"Original file replaced with cleaned version")
    else:
        print(f"Cleaned file kept as: {output_file}")

if __name__ == "__main__":
    # Change to project root directory
    os.chdir(Path(__file__).parent.parent.parent)
    
    print("YouTube Playlist URL Cleanup Utility")
    print("====================================")
    import sys, os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'utils'))
    from config import get_config
    config = get_config()
    csv_path = config.get('paths.output_csv', 'outputs/output.csv')
    print(f"This will clean malformed YouTube playlist URLs in {csv_path}")
    print()
    
    cleanup_csv()