# YouTube Mass Download

A standalone, high-performance tool for downloading YouTube channels at scale with direct S3 streaming, comprehensive error recovery, and intelligent resource management.

## Overview

YouTube Mass Download enables bulk processing of YouTube channels with:
- **Concurrent Processing**: Download multiple channels and videos simultaneously
- **Direct S3 Streaming**: No local storage required - streams directly to S3
- **Automatic Resource Management**: Intelligent throttling and resource limits
- **Comprehensive Error Recovery**: Circuit breakers, retry logic, and failure handling
- **Progress Tracking**: Real-time progress with ETA calculations
- **Resume Capability**: Continue interrupted downloads seamlessly
- **Multiple Input Formats**: Support for CSV, JSON, and TXT files

## Installation

```bash
# Clone the repository
git clone https://github.com/michael-abdo/youtube-mass-download.git
cd youtube-mass-download

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your S3 credentials and configuration
```

## Quick Start

### Basic Usage

```bash
# Download channels from CSV file
python run_mass_download.py examples/channels.csv

# Dry run to preview what would be processed
python run_mass_download.py examples/channels.csv --dry-run

# Limit videos per channel
python run_mass_download.py examples/channels.json --max-videos 50
```

### Input File Formats

#### CSV Format
```csv
name,channel_url
"NASA Official","https://youtube.com/@NASA"
"SpaceX","https://youtube.com/@SpaceX"
```

#### JSON Format
```json
[
  {
    "name": "NASA Official",
    "channel_url": "https://youtube.com/@NASA"
  },
  {
    "name": "SpaceX",
    "channel_url": "https://youtube.com/@SpaceX"
  }
]
```

#### TXT Format (URLs only)
```
https://youtube.com/@NASA
https://youtube.com/@SpaceX
```

## Configuration

### Environment Variables

Create a `.env` file with:

```bash
# S3 Configuration
S3_BUCKET_NAME=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-east-1

# Database Configuration (Optional)
DATABASE_URL=postgresql://user:pass@localhost/dbname

# Processing Configuration
MAX_CONCURRENT_CHANNELS=3
MAX_CONCURRENT_VIDEOS=5
MAX_RETRIES=3
```

### Advanced Options

```bash
# Set concurrent processing limits
python run_mass_download.py channels.csv \
  --max-concurrent-channels 5 \
  --max-concurrent-videos 10

# Enable debug logging
python run_mass_download.py channels.csv --debug

# Resume from previous state
python run_mass_download.py --resume
```

## Features

### Error Recovery
- **Circuit Breakers**: Prevent cascade failures
- **Exponential Backoff**: Smart retry logic
- **Partial Recovery**: Continue processing even if some videos fail
- **Detailed Error Logs**: Comprehensive error tracking

### Resource Management
- **Dynamic Throttling**: Adjusts based on system resources
- **Memory Monitoring**: Prevents out-of-memory errors
- **Bandwidth Management**: Respects rate limits

### Progress Tracking
- **Real-time Updates**: See progress as downloads complete
- **ETA Calculations**: Know when processing will finish
- **Statistics**: Success/failure rates and performance metrics

## Output Structure

Downloads are organized in S3:
```
s3://your-bucket/
├── channel-name/
│   ├── video1_[id].mp4
│   ├── video2_[id].mp4
│   └── metadata.json
└── logs/
    └── mass_download_20250921_123456.log
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**: Check your YouTube cookies are valid
2. **S3 Upload Failures**: Verify AWS credentials and bucket permissions
3. **Memory Issues**: Reduce concurrent processing limits
4. **Rate Limiting**: Enable throttling or reduce concurrent downloads

### Debug Mode

Run with `--debug` for detailed logging:
```bash
python run_mass_download.py channels.csv --debug
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions welcome! Please read CONTRIBUTING.md for guidelines.

## Credits

Originally developed as part of the typing-clients-ingestion project.