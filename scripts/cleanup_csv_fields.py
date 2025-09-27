#!/usr/bin/env python3
"""
Cleanup script to fix oversized CSV fields
This script reads the existing CSV with large fields and truncates them to safe sizes
"""
import csv
import sys
import os
from pathlib import Path

# Add parent directory to path to access utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import get_config
from utils.logging_config import get_logger

def cleanup_csv():
    """Clean up oversized fields in the CSV file"""
    logger = get_logger(__name__)
    config = get_config()
    
    # Set maximum CSV field size to handle existing large fields
    csv.field_size_limit(sys.maxsize)
    
    # Get file paths
    from utils.config import get_config
    config = get_config()
    input_file = config.get('paths.output_csv', 'outputs/output.csv')  # Current location
    output_file = "output_cleaned.csv"
    max_field_size = 100000  # 100KB limit to be safe
    
    if not os.path.exists(input_file):
        logger.error(f"Input file {input_file} not found")
        return False
    
    logger.info(f"Reading {input_file} and cleaning oversized fields...")
    
    rows_processed = 0
    fields_truncated = 0
    
    # Read and clean the CSV
    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            reader = csv.DictReader(infile)
            
            # Get fieldnames
            fieldnames = reader.fieldnames
            if not fieldnames:
                logger.error("No fieldnames found in CSV")
                return False
                
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 to account for header
                rows_processed += 1
                
                # Check and truncate oversized fields
                for field, value in row.items():
                    if value and len(str(value)) > max_field_size:
                        original_size = len(str(value))
                        row[field] = str(value)[:max_field_size] + "... [TRUNCATED]"
                        fields_truncated += 1
                        logger.warning(f"Row {row_num} ({row.get('name', 'unknown')}): "
                                     f"Truncated field '{field}' from {original_size:,} to {max_field_size:,} bytes")
                
                writer.writerow(row)
                
                # Progress update every 100 rows
                if rows_processed % 100 == 0:
                    logger.info(f"Processed {rows_processed} rows...")
    
    logger.info(f"Cleanup complete: {rows_processed} rows processed, {fields_truncated} fields truncated")
    logger.info(f"Cleaned CSV saved to: {output_file}")
    
    # Verify the cleaned file
    logger.info("Verifying cleaned file...")
    try:
        csv.field_size_limit(131072)  # Reset to default limit
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            test_reader = csv.DictReader(f)
            test_rows = sum(1 for _ in test_reader)
        logger.success(f"Verification successful: {test_rows} rows can be read with default field limit")
        return True
    except csv.Error as e:
        logger.error(f"Verification failed: {e}")
        return False

if __name__ == "__main__":
    success = cleanup_csv()
    if success:
        print("\nCleanup successful! To replace the original file:")
        print(f"  mv {input_file} {input_file}.backup_large_fields")
        print(f"  mv output_cleaned.csv {input_file}")
    else:
        print("\nCleanup failed. Check the logs for details.")
        sys.exit(1)