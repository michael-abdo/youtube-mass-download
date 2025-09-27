#!/usr/bin/env python3
"""
Normalize all transcript file names to use consistent naming convention.
Converts all transcript files to the format: {VIDEO_ID}_transcript.vtt
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pathlib import Path
import re
import shutil
from datetime import datetime

# Base directory for YouTube downloads
YOUTUBE_DIR = Path(__file__).parent.parent.parent / "youtube_downloads"

def normalize_transcript_names():
    """Normalize all transcript file names to {VIDEO_ID}_transcript.vtt format"""
    
    # Patterns to match different transcript naming conventions
    patterns = [
        # Pattern 1: {VIDEO_ID}.en.vtt or {VIDEO_ID}.en-orig.vtt
        (r'^([a-zA-Z0-9_-]{11})\.(en|en-[A-Z]{2}|en-orig|en-[A-Z]{2}-orig)\.vtt$', r'\1_transcript.vtt'),
        # Pattern 2: {VIDEO_ID}.{lang}.vtt where lang is not 'en'
        (r'^([a-zA-Z0-9_-]{11})\.([a-z]{2}|[a-z]{2}-[A-Z]{2})\.vtt$', r'\1_transcript.vtt'),
        # Pattern 3: Already correct format {VIDEO_ID}_transcript.vtt
        (r'^([a-zA-Z0-9_-]{11})_transcript\.vtt$', None),
    ]
    
    # Track changes
    renamed_count = 0
    already_correct = 0
    errors = []
    
    print(f"Scanning {YOUTUBE_DIR} for transcript files...")
    
    # Get all VTT files
    vtt_files = list(YOUTUBE_DIR.glob("*.vtt"))
    print(f"Found {len(vtt_files)} VTT files")
    
    for vtt_file in vtt_files:
        filename = vtt_file.name
        matched = False
        
        for pattern, replacement in patterns:
            match = re.match(pattern, filename)
            if match:
                matched = True
                if replacement is None:
                    # Already in correct format
                    already_correct += 1
                    print(f"✓ Already correct: {filename}")
                else:
                    # Need to rename
                    new_name = re.sub(pattern, replacement, filename)
                    new_path = vtt_file.parent / new_name
                    
                    # Check if target already exists
                    if new_path.exists() and new_path != vtt_file:
                        # Compare file sizes to decide which to keep
                        existing_size = new_path.stat().st_size
                        current_size = vtt_file.stat().st_size
                        
                        if current_size > existing_size:
                            # Current file is larger, replace existing
                            print(f"⚠️  Replacing smaller file: {new_name} (existing: {existing_size} bytes, new: {current_size} bytes)")
                            new_path.unlink()
                            vtt_file.rename(new_path)
                            renamed_count += 1
                        else:
                            # Existing file is larger or same size, remove current
                            print(f"⚠️  Removing duplicate: {filename} (keeping larger {new_name})")
                            vtt_file.unlink()
                    else:
                        # No conflict, rename
                        try:
                            vtt_file.rename(new_path)
                            renamed_count += 1
                            print(f"✅ Renamed: {filename} → {new_name}")
                        except Exception as e:
                            errors.append(f"Failed to rename {filename}: {e}")
                            print(f"❌ Error renaming {filename}: {e}")
                break
        
        if not matched:
            print(f"❓ Unknown format: {filename}")
    
    # Summary
    print("\n" + "="*50)
    print(f"Summary:")
    print(f"  Already correct: {already_correct}")
    print(f"  Renamed: {renamed_count}")
    print(f"  Errors: {len(errors)}")
    print(f"  Total VTT files: {len(vtt_files)}")
    
    if errors:
        print("\nErrors:")
        for error in errors:
            print(f"  - {error}")
    
    return renamed_count, already_correct, errors

def main():
    print("Normalizing transcript file names...")
    print(f"Working directory: {YOUTUBE_DIR}")
    print()
    
    renamed, correct, errors = normalize_transcript_names()
    
    if renamed > 0:
        print(f"\n✅ Successfully normalized {renamed} transcript files")
    else:
        print(f"\n✅ All transcript files already have correct naming convention")

if __name__ == "__main__":
    main()