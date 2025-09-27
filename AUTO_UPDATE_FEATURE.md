# yt-dlp Auto-Update Feature

This system now includes automatic yt-dlp updates to ensure compatibility with the latest YouTube changes.

## How it Works

1. **Before each YouTube download**, the system automatically checks and updates yt-dlp to the latest version
2. **Version comparison**: Compares current version with latest available on PyPI
3. **Smart updating**: Only updates if a newer version is available
4. **Fallback handling**: If update fails, continues with existing version

## Configuration Options

### Config File (config.yaml)
```yaml
downloads:
  youtube:
    auto_update_yt_dlp: true  # Set to false to disable auto-updates
```

### Command Line Options
```bash
# Normal operation (auto-update enabled)
python3 simple_workflow.py --test-limit 10

# Skip auto-update for this run
python3 simple_workflow.py --test-limit 10 --no-yt-dlp-update
```

## Benefits

1. **Automatic compatibility**: Always uses the latest yt-dlp version
2. **Reduced failures**: Fewer "Some web client https formats have been skipped" errors
3. **Zero maintenance**: No manual intervention required
4. **Configurable**: Can be disabled if needed

## Implementation Details

### Files Modified
- `utils/yt_dlp_updater.py` - New auto-updater utility
- `utils/s3_manager.py` - Modified to use auto-updater
- `simple_workflow.py` - Added CLI option
- `config/config.yaml` - Added configuration option

### Key Functions
- `ensure_yt_dlp_updated()` - Main update function
- `get_yt_dlp_command()` - Returns proper yt-dlp command to use
- `get_current_yt_dlp_version()` - Checks installed version
- `get_latest_yt_dlp_version()` - Fetches latest from PyPI

## Error Handling

- **Update failures**: Logs warning but continues with existing version
- **Network issues**: Gracefully handles offline scenarios
- **Permission errors**: Uses --user flag for pip installs
- **Timeouts**: 2-minute timeout for update operations

## Performance Impact

- **First run**: ~10-30 seconds for update check/installation
- **Subsequent runs**: ~2-5 seconds for version comparison
- **No update needed**: Minimal overhead (~1 second)

## Example Output

```
🔄 Ensuring yt-dlp is up to date...
INFO: Updating yt-dlp from v2025.6.30 to v2025.08.11
INFO: ✅ yt-dlp successfully updated to v2025.08.11
📥 Streaming YouTube to S3: files/video-uuid.mp4
🚀 PROCESS_START: /home/user/.local/bin/yt-dlp -f best[ext=mp4]/best -o /tmp/pipe https://youtube.com/watch?v=...
```

## Troubleshooting

### If auto-update fails:
1. Check internet connection
2. Verify pip is working: `pip install --upgrade yt-dlp --user`
3. Temporarily disable: `auto_update_yt_dlp: false`

### If downloads still fail after update:
1. The video might be region-locked or private
2. Check video URL is accessible in browser
3. Try manual download: `yt-dlp "URL"`

## Manual Testing

```bash
# Test the updater directly
python3 utils/yt_dlp_updater.py

# Test with workflow (dry run)
python3 simple_workflow.py --test-limit 1

# Force update even if latest version
python3 -c "from utils.yt_dlp_updater import update_yt_dlp; update_yt_dlp(force=True)"
```