# CLAUDE_INSTRUCTIONS.md - Instructions for Claude

## Development Setup

1. Always use the virtual environment when running Python scripts:
   ```bash
   source venv/bin/activate
   ```

2. Required dependencies:
   - Always make sure the following are installed in the virtual environment:
     ```bash
     pip install -r requirements.txt
     ```
   - Specifically ensure yt-dlp is installed for YouTube video downloading
   
3. Known issues and fixes:
   - URLs with newlines or special characters: The run_complete_workflow.py script cleans any malformed YouTube playlist URLs
   - Missing transcripts: The download_youtube.py script tries to download both regular and auto-generated subtitles
   - YouTube playlists: The download_youtube.py script now processes each video in a playlist individually, downloading videos and transcripts for each one

3. Always use the absolute path to yt-dlp in the virtual environment:
   ```python
   # Get the path to yt-dlp in the virtual environment
   import os
   import sys
   
   # First check if we're in a virtual environment
   if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
       # We're in a virtual environment
       venv_path = sys.prefix
       yt_dlp_path = os.path.join(venv_path, "bin", "yt-dlp")
   else:
       # Not in a virtual environment, use command as-is
       yt_dlp_path = "yt-dlp"
   ```

## Workflow Commands

1. Complete workflow (fetch sheet, extract links, download media):
   ```bash
   python run_complete_workflow.py

   # Limit number of rows to process in the Google Sheet
   python run_complete_workflow.py --max-rows 10

   # Limit number of YouTube videos to download
   python run_complete_workflow.py --max-youtube 5

   # Limit number of Google Drive files to download
   python run_complete_workflow.py --max-drive 5

   # Skip specific steps
   python run_complete_workflow.py --skip-sheet  # Skip Google Sheet scraping
   python run_complete_workflow.py --skip-drive  # Skip Drive downloads
   python run_complete_workflow.py --skip-youtube  # Skip YouTube downloads
   ```

2. Force download new Google Sheet:
   ```bash
   python master_scraper.py --force-download
   ```

3. Download YouTube videos:
   ```bash
   python download_youtube.py [URL]
   python download_youtube.py [URL] --transcript-only  # Only download transcript
   python download_youtube.py [URL] --resolution 1080  # Specify resolution
   ```

4. Download Google Drive files:
   ```bash
   python download_drive.py [URL] --metadata
   ```