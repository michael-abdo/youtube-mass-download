# Large Google Drive File Download Implementation

## Overview
This document describes the implementation of robust Google Drive file downloading, specifically handling large files that trigger Google's virus scan warnings.

## Problem Statement
When downloading large files from Google Drive (typically > 100MB), Google displays a virus scan warning page instead of directly downloading the file. The warning page requires user confirmation to proceed with the download.

## Solution

### 1. Enhanced URL Validation
Updated `utils/validation.py` to accept `drive.usercontent.google.com` URLs:
```python
allowed_domains = ['drive.google.com', 'docs.google.com', 'drive.usercontent.google.com']
```

### 2. Direct Download URL Handling
Added new functionality in `utils/download_drive.py`:

#### New Function: `process_direct_download_url()`
- Handles direct `drive.usercontent.google.com/download` URLs
- Automatically retries when encountering virus scan warning pages
- Extracts filename from HTML response when available
- Implements proper progress tracking with speed and ETA

#### Updated Function: `download_drive_file()`
- Enhanced virus scan warning detection
- Improved handling of confirmation parameters (confirm, uuid)
- Support for both regular Drive URLs and direct download URLs

### 3. Key Features

#### Virus Scan Warning Handling
```python
# Detect virus scan warning page
if 'text/html' in content_type and 'virus scan warning' in response.text:
    # Parse confirmation parameters
    confirm_match = re.search(r'confirm=([0-9a-zA-Z_-]+)', response.text)
    uuid_match = re.search(r'uuid=([0-9a-zA-Z_-]+)', response.text)
    
    # Build proper download URL with all parameters
    download_params = {
        'id': file_id,
        'export': 'download',
        'confirm': confirm_code,
        'uuid': uuid_value  # if present
    }
```

#### Adaptive Chunk Size
- Files > 100MB: 8MB chunks
- Files > 10MB: 2MB chunks  
- Files < 10MB: 1MB chunks

#### Progress Tracking
- Real-time download progress percentage
- Current/total MB downloaded
- Download speed (MB/s)
- Estimated time remaining (ETA)

### 4. Usage Examples

#### Command Line
```bash
# Download with standard Drive URL
python utils/download_drive.py "https://drive.google.com/file/d/FILE_ID/view"

# Download with direct download URL (includes virus scan bypass)
python utils/download_drive.py "https://drive.usercontent.google.com/download?id=FILE_ID&export=download&confirm=t&uuid=UUID"

# Download with metadata
python utils/download_drive.py URL --metadata
```

#### Python Script
```python
from utils.download_drive import process_drive_url

# Download any Drive URL type
file_path, metadata_path = process_drive_url(
    url="https://drive.usercontent.google.com/download?id=...",
    output_filename="custom_name.mp4",  # optional
    save_metadata_flag=True  # optional
)
```

### 5. Error Handling

- **Retry Logic**: Automatic retry with exponential backoff for network failures
- **File Locking**: Prevents concurrent downloads of the same file
- **Atomic Operations**: Downloads to temp file, then atomic rename
- **Validation**: Comprehensive URL and file ID validation

### 6. Performance

- Successfully tested with 944MB file
- Average download speed: ~30 MB/s
- Memory efficient streaming with adaptive chunks
- Progress updates without overwhelming terminal

## Testing

### Test Script
Created `scripts/download_large_drive_file_direct.py` for testing large file downloads:
- Handles virus scan confirmation
- Detailed progress reporting
- ETA calculation
- Speed monitoring

### Test Results
- File: ShelseaEvans.mp4 (944MB)
- Download time: 33.2 seconds
- Average speed: 28.4 MB/s
- Status: ✅ Success

## Integration

The enhancement is fully integrated into the existing workflow:
- `run_complete_workflow.py` uses the updated download functionality
- Backward compatible with existing Drive URLs
- Transparent handling of virus scan warnings

## Security Considerations

1. **URL Validation**: All URLs are validated against allowed domains
2. **File ID Validation**: Alphanumeric characters only (prevents injection)
3. **Path Traversal Prevention**: Output paths are sanitized
4. **No Shell Execution**: Direct HTTP requests without shell commands