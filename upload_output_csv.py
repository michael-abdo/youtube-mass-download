#!/usr/bin/env python3
"""
Upload the current outputs/output.csv file to S3 using the existing CSV versioning system
"""

import sys
from pathlib import Path

# Add the utils directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from utils.csv_s3_versioning import upload_csv_to_s3
from utils.logging_config import get_logger

logger = get_logger(__name__)

def main():
    """Upload outputs/output.csv to S3"""
    csv_path = Path(__file__).parent / "outputs" / "output.csv"
    
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return 1
    
    logger.info(f"Uploading CSV file: {csv_path}")
    
    # Add metadata about the upload
    metadata = {
        'upload_source': 'manual_upload',
        'uploaded_by': 'upload_output_csv.py'
    }
    
    # Upload the CSV
    result = upload_csv_to_s3(str(csv_path), metadata=metadata)
    
    if result['success']:
        logger.info("✅ CSV upload successful!")
        logger.info(f"   Versioned name: {result['versioned_name']}")
        logger.info(f"   S3 key: {result['s3_key']}")
        logger.info(f"   S3 URL: {result['s3_url']}")
        logger.info(f"   File size: {result['file_size']:,} bytes")
        return 0
    else:
        logger.error(f"❌ CSV upload failed: {result.get('error', 'Unknown error')}")
        return 1

if __name__ == "__main__":
    sys.exit(main())