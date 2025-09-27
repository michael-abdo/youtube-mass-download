#!/usr/bin/env python3
"""
Monitor download progress for YouTube and Drive downloads
"""
import os
import time
import subprocess
from datetime import datetime
import glob

def count_files(directory, pattern):
    """Count files matching pattern in directory"""
    files = glob.glob(os.path.join(directory, pattern))
    return len(files)

def get_latest_log_content(log_pattern, lines=5):
    """Get last N lines from the most recent log file"""
    logs = glob.glob(log_pattern)
    if not logs:
        return "No log files found"
    
    latest_log = max(logs, key=os.path.getctime)
    try:
        with open(latest_log, 'r') as f:
            content = f.readlines()
            return ''.join(content[-lines:])
    except:
        return "Could not read log file"

def check_process(process_name):
    """Check if a process is running"""
    try:
        result = subprocess.run(['pgrep', '-f', process_name], 
                              capture_output=True, text=True)
        return len(result.stdout.strip()) > 0
    except:
        return False

def monitor_downloads():
    """Monitor download progress"""
    print("=" * 80)
    print(f"Download Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Check YouTube downloads
    yt_videos = count_files('youtube_downloads', '*.mp4')
    yt_transcripts = count_files('youtube_downloads', '*.vtt')
    yt_running = check_process('scripts/run_youtube_downloads_async.py')
    
    print(f"\n📺 YouTube Downloads:")
    print(f"   Videos: {yt_videos}")
    print(f"   Transcripts: {yt_transcripts}")
    print(f"   Status: {'🟢 Running' if yt_running else '🔴 Not running'}")
    
    if yt_running:
        print("\n   Latest activity:")
        log_content = get_latest_log_content('youtube_downloads_*.log')
        for line in log_content.strip().split('\n'):
            print(f"   {line}")
    
    # Check Drive downloads
    drive_files = len([f for f in glob.glob('drive_downloads/*') 
                      if not f.endswith('_metadata.json')])
    drive_metadata = count_files('drive_downloads', '*_metadata.json')
    drive_running = check_process('scripts/run_drive_downloads_async.py')
    
    print(f"\n📁 Google Drive Downloads:")
    print(f"   Files: {drive_files}")
    print(f"   Metadata files: {drive_metadata}")
    print(f"   Status: {'🟢 Running' if drive_running else '🔴 Not running'}")
    
    if drive_running:
        print("\n   Latest activity:")
        log_content = get_latest_log_content('drive_downloads_*.log')
        for line in log_content.strip().split('\n'):
            print(f"   {line}")
    
    # Check disk space
    stat = os.statvfs('.')
    free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
    print(f"\n💾 Disk Space: {free_gb:.1f} GB free")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor download progress')
    parser.add_argument('--watch', action='store_true', 
                       help='Continuously monitor (updates every 30 seconds)')
    parser.add_argument('--interval', type=int, default=30,
                       help='Update interval in seconds (default: 30)')
    
    args = parser.parse_args()
    
    if args.watch:
        try:
            while True:
                os.system('clear' if os.name == 'posix' else 'cls')
                monitor_downloads()
                print(f"\nRefreshing in {args.interval} seconds... (Ctrl+C to stop)")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")
    else:
        monitor_downloads()