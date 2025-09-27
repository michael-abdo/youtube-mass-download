# Mass Download Usage Examples

This file contains practical examples of how to use the mass download feature.

## Basic Usage

### 1. Simple CSV Download
```bash
# Download channels from CSV file
python run_mass_download.py examples/channels.csv --job-id space_channels

# With custom concurrency settings
python run_mass_download.py examples/channels.csv --job-id space_channels --max-channels 2 --max-downloads 3
```

### 2. JSON Format Download
```bash
# Download from JSON file
python run_mass_download.py examples/channels.json --job-id json_channels

# Resume interrupted job
python run_mass_download.py examples/channels.json --job-id json_channels --resume
```

### 3. Text File Download
```bash
# Simple text file with URLs
python run_mass_download.py examples/channels.txt --job-id text_channels
```

## Advanced Usage

### 4. Performance Testing
```bash
# Run performance tests
python mass_download/test_performance.py

# Run integration tests  
python mass_download/test_real_integration.py
```

### 5. Configuration Examples

#### High-throughput Configuration
```yaml
mass_download:
  max_concurrent_channels: 5
  max_concurrent_downloads: 10
  max_videos_per_channel: 50
  resource_limits:
    max_cpu_percent: 90.0
    max_memory_percent: 85.0
```

#### Conservative Configuration
```yaml
mass_download:
  max_concurrent_channels: 1
  max_concurrent_downloads: 2
  max_videos_per_channel: 20
  resource_limits:
    max_cpu_percent: 60.0
    max_memory_percent: 70.0
```

### 6. Error Recovery Examples

#### Circuit Breaker Configuration
```yaml
error_recovery:
  circuit_breaker:
    failure_threshold: 3    # Lower threshold for sensitive operations
    recovery_timeout: 120   # Longer recovery time
    half_open_requests: 2   # More test requests
```

#### Retry Configuration
```yaml  
error_recovery:
  retry:
    max_retries: 5          # More retry attempts
    initial_delay: 2.0      # Longer initial delay
    max_delay: 60.0         # Higher max delay
    exponential_base: 1.5   # Gentler exponential growth
```

## Input File Examples

### Complex CSV Example
```csv
channel_url,channel_name,person_name,max_videos,priority
https://youtube.com/@NASA,NASA Official,NASA,100,high
https://youtube.com/@SpaceX,SpaceX,Elon Musk's SpaceX,50,high  
https://youtube.com/@TED,TED,TED,25,medium
https://youtube.com/@BBC,BBC,BBC,75,medium
```

### Complex JSON Example
```json
{
  "metadata": {
    "batch_name": "Educational Channels",
    "created_date": "2025-08-25",
    "description": "High-quality educational content channels"
  },
  "channels": [
    {
      "channel_url": "https://youtube.com/@MITOpenCourseWare",
      "name": "MIT OpenCourseWare",
      "person_name": "MIT",
      "category": "education",
      "priority": "high",
      "max_videos": 200
    },
    {
      "channel_url": "https://youtube.com/@StanfordOnline", 
      "name": "Stanford Online",
      "person_name": "Stanford University",
      "category": "education",
      "priority": "high",
      "max_videos": 150
    }
  ]
}
```

### Commented Text File Example
```
# Space and Science Channels
https://youtube.com/@NASA
https://youtube.com/@SpaceX

# Educational Content
https://youtube.com/@TED
https://youtube.com/@MITOpenCourseWare

# News and Media
https://youtube.com/@BBC
https://youtube.com/@NationalGeographic
```

## Monitoring Examples

### 1. Check Job Progress
```bash
# View progress in real-time (logs will show progress)
tail -f logs/mass_download/mass_download.log

# Check error logs
tail -f logs/mass_download/mass_download_errors.log
```

### 2. Database Queries
```sql
-- Check job progress
SELECT * FROM progress WHERE job_id = 'space_channels';

-- View channel processing statistics
SELECT 
    job_id,
    total_channels,
    channels_processed,
    (channels_processed * 100.0 / total_channels) as progress_percent
FROM progress 
WHERE job_id = 'space_channels';

-- View video statistics
SELECT 
    p.name as person_name,
    COUNT(v.id) as video_count,
    SUM(CASE WHEN v.s3_path IS NOT NULL THEN 1 ELSE 0 END) as downloaded_count
FROM persons p
LEFT JOIN videos v ON p.id = v.person_id  
GROUP BY p.id, p.name;
```

## Performance Optimization

### 1. Resource Monitoring
```bash
# Monitor system resources during download
htop              # Monitor CPU and memory
iostat -x 1       # Monitor disk I/O
nethogs           # Monitor network usage
```

### 2. Database Optimization
```sql
-- Add indexes for better performance
CREATE INDEX IF NOT EXISTS idx_videos_person_id ON videos(person_id);
CREATE INDEX IF NOT EXISTS idx_videos_video_id ON videos(video_id);
CREATE INDEX IF NOT EXISTS idx_persons_channel_url ON persons(channel_url);
```

### 3. S3 Optimization
- Use appropriate S3 storage classes
- Configure multipart uploads for large files
- Use transfer acceleration for global access
- Monitor S3 request rates and costs

## Troubleshooting Examples

### 1. Debug Mode
```bash
# Run with debug logging
python run_mass_download.py examples/channels.csv --job-id debug_job --log-level DEBUG
```

### 2. Dry Run Mode
```bash
# Test without actually downloading (if supported)
python run_mass_download.py examples/channels.csv --job-id test_job --dry-run
```

### 3. Single Channel Testing
```bash
# Create test file with single channel
echo "https://youtube.com/@NASA" > test_single.txt
python run_mass_download.py test_single.txt --job-id single_test
```

## Best Practices

1. **Start Small**: Begin with 1-2 channels to test configuration
2. **Monitor Resources**: Watch CPU, memory, and network usage
3. **Use Appropriate Limits**: Set realistic `max_videos_per_channel` values
4. **Enable Continue on Error**: Use `continue_on_error: true` for large batches
5. **Regular Progress Checks**: Monitor logs and database progress
6. **Backup Configuration**: Save working configurations for reuse
7. **Test Error Scenarios**: Validate error handling with problematic channels
8. **Resource Planning**: Estimate storage and bandwidth requirements

## Common Patterns

### Batch Processing Pattern
1. Prepare input file with channels
2. Set appropriate concurrency limits  
3. Enable error recovery and continue on error
4. Monitor progress and resources
5. Handle failed channels separately if needed

### Resume Pattern
1. Start initial job with unique job ID
2. Monitor for interruptions or failures
3. Resume using same job ID
4. System automatically continues from last checkpoint

### Validation Pattern
1. Start with small test batch
2. Validate configuration and performance
3. Adjust settings based on results
4. Scale up to full batch processing