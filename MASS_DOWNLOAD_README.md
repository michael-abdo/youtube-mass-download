# Mass YouTube Channel Download Feature

This feature enables bulk downloading and processing of multiple YouTube channels with fail-fast/fail-loud/fail-safely principles.

## Features

- **Multi-format input support**: CSV, JSON, and TXT files
- **Concurrent processing**: Configurable concurrent channel and video processing
- **Progress tracking**: Real-time progress monitoring with ETA calculations
- **Error recovery**: Circuit breakers, retry logic, and comprehensive error handling
- **Resource management**: Dynamic resource monitoring and limits
- **Database integration**: Full integration with existing database schema
- **S3 streaming**: Direct streaming to S3 storage
- **Resume capability**: Job resumption after interruptions

## Quick Start

### 1. Prepare Input File

Create an input file in one of the supported formats:

#### CSV Format (`examples/channels.csv`)
```csv
channel_url,channel_name,person_name
https://youtube.com/@NASA,NASA Official,NASA
https://youtube.com/@SpaceX,SpaceX,Elon Musk's SpaceX
https://youtube.com/@TED,TED,TED
```

#### JSON Format (`examples/channels.json`)
```json
{
  "channels": [
    {
      "channel_url": "https://youtube.com/@NASA",
      "name": "NASA Official", 
      "person_name": "NASA"
    }
  ]
}
```

#### Text Format (`examples/channels.txt`)
```
https://youtube.com/@NASA
https://youtube.com/@SpaceX
https://youtube.com/@TED
```

### 2. Run Mass Download

```bash
# Using the wrapper script
python run_mass_download.py examples/channels.csv --job-id my_download_job

# Or using the CLI directly  
python mass_download_cli.py examples/channels.csv --job-id my_download_job --max-channels 5
```

## Configuration

The mass download feature is configured in `config/config.yaml`:

```yaml
mass_download:
  max_concurrent_channels: 3
  max_concurrent_downloads: 5
  max_videos_per_channel: 100
  skip_existing_videos: true
  continue_on_error: true
  download_videos: true
  download_mode: "stream_to_s3"
  
  resource_limits:
    max_cpu_percent: 80.0
    max_memory_percent: 80.0
    
  error_recovery:
    circuit_breaker:
      failure_threshold: 5
      recovery_timeout: 60
      half_open_requests: 1
    retry:
      max_retries: 3
      initial_delay: 1.0
      max_delay: 30.0
      exponential_base: 2.0
```

## Performance Characteristics

Based on performance testing:

- **Memory usage**: ~38MB peak memory for 1000 operations
- **Throughput**: 100,000+ ops/sec for duplicate detection
- **Concurrent operations**: 137+ ops/sec with 5 concurrent workers
- **No memory leaks**: Stable memory usage across multiple cycles
- **Resource monitoring**: Dynamic CPU/memory monitoring and limits

## Input File Formats

### CSV Format
- **Required columns**: `channel_url`
- **Optional columns**: `channel_name`, `person_name`
- **Delimiter**: Comma (`,`)
- **Encoding**: UTF-8

### JSON Format  
- **Structure**: Object with `channels` array
- **Required fields**: `channel_url` 
- **Optional fields**: `name`, `person_name`

### Text Format
- **Structure**: One URL per line
- **Format**: Plain text with YouTube channel URLs
- **Comments**: Lines starting with `#` are ignored

## Channel URL Formats

Supported YouTube channel URL formats:
- `https://youtube.com/@channelname`
- `https://www.youtube.com/@channelname` 
- `https://youtube.com/channel/UCxxxxxxxxxxxxxxxxxx`
- `https://www.youtube.com/channel/UCxxxxxxxxxxxxxxxxxx`

## Error Handling

The system implements comprehensive error handling:

### Circuit Breaker
- Prevents cascading failures
- Configurable failure thresholds
- Automatic recovery attempts

### Retry Logic
- Exponential backoff with jitter
- Configurable max retries and delays
- Different strategies for different error types

### Progress Recovery
- Database-persisted progress tracking
- Job resumption after interruptions
- Checkpoint-based recovery for long operations

## Monitoring and Logging

### Real-time Progress
- Live progress bars with ETA calculations
- Channel and video processing status
- Download speed and resource usage metrics

### Logging
- Structured logging with rotation
- Separate error logs
- Integration with existing logging infrastructure
- Log levels: DEBUG, INFO, WARNING, ERROR

### Database Tracking
- Progress persistence in database
- Job status and metadata
- Detailed processing statistics

## Testing

The feature includes comprehensive test suites:

```bash
# Run all tests
python -m pytest mass_download/

# Run specific test categories
python mass_download/test_performance.py           # Performance tests
python mass_download/test_real_integration.py      # Integration tests  
python mass_download/test_channel_discovery.py     # Unit tests
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Use the wrapper scripts in the project root
2. **Memory Issues**: Adjust `resource_limits` in configuration
3. **Rate Limiting**: Built-in rate limiting handles YouTube API limits
4. **Database Schema**: Ensure database schema is up to date

### Performance Tuning

1. **Concurrent Processing**: Adjust `max_concurrent_channels` and `max_concurrent_downloads`
2. **Resource Limits**: Tune `max_cpu_percent` and `max_memory_percent`
3. **Error Recovery**: Modify retry and circuit breaker settings
4. **Video Limits**: Set `max_videos_per_channel` to control scope

## Architecture

The mass download feature consists of several key components:

- **Input Handler**: Parses CSV, JSON, and TXT input files
- **Channel Discovery**: Discovers and validates YouTube channels
- **Mass Coordinator**: Orchestrates the entire download process
- **Concurrent Processor**: Manages concurrent operations with resource limits
- **Progress Monitor**: Provides real-time progress tracking and reporting
- **Error Recovery**: Implements circuit breakers and retry logic
- **Database Operations**: Extends existing database schema and operations

## API Compatibility

The feature integrates with the existing codebase:
- Uses existing database schema and operations
- Integrates with existing S3 streaming infrastructure  
- Compatible with existing configuration and logging systems
- Extends existing error handling and rate limiting

## Security Considerations

- Input validation for all file formats
- URL validation and sanitization
- Resource limits to prevent system overload
- Error message sanitization to prevent information leakage
- Secure database operations with transaction management

## Future Enhancements

- Support for additional input formats (YAML, XML)
- Advanced filtering and selection criteria  
- Batch processing with job queuing
- Advanced analytics and reporting
- Multi-region S3 support
- API endpoints for programmatic access