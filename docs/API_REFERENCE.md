# API Reference - Quick Reference Guide

**Note**: Complete API documentation has been consolidated into `COMPLETE_SYSTEM_ARCHITECTURE.md`

## Quick Function Reference

### Core Classes
- **RowContext**: `utils/row_context.py` - CSV row relationship management
- **DownloadResult**: `utils/row_context.py` - Standardized result tracking

### CSV Operations
- **ensure_tracking_columns()**: Add tracking columns to CSV
- **get_pending_downloads()**: Get rows needing downloads
- **update_csv_download_status()**: Atomic CSV updates

### S3 Streaming
- **stream_youtube_to_s3()**: Direct YouTube→S3 streaming
- **stream_drive_to_s3()**: Direct Drive→S3 with virus scan bypass
- **upload_file_to_s3()**: Traditional file upload

## 📖 Complete Documentation

For comprehensive API documentation, architecture details, and usage examples, see:

**[COMPLETE_SYSTEM_ARCHITECTURE.md](./COMPLETE_SYSTEM_ARCHITECTURE.md)**

This consolidated document includes:
- Complete API reference with examples
- System architecture and data flow
- UUID implementation details
- Performance optimization guide
- Security and reliability features
- Troubleshooting and maintenance guide