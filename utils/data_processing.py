#!/usr/bin/env python3
"""
Centralized Data Processing Utilities (DRY Phase 5)

Consolidates common data processing patterns found across the codebase:
- CSV operations and transformations
- JSON processing and validation
- Data cleaning and normalization
- Batch processing utilities
- Data conversion helpers
"""

import os
import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from datetime import datetime
import re
from functools import wraps

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

from utils.logging_config import get_logger
from utils.error_handling import handle_file_operations, handle_csv_operations, ErrorMessages
from utils.config import get_csv_config, ensure_parent_dir, get_csv_delimiter

logger = get_logger(__name__)


# ============================================================================
# CSV PROCESSING UTILITIES
# ============================================================================

@handle_csv_operations("read_csv_safe", return_on_error=pd.DataFrame())
def read_csv_safe(file_path: Union[str, Path], 
                  required_columns: Optional[List[str]] = None,
                  dtype_map: Optional[Dict[str, type]] = None,
                  encoding: str = 'utf-8',
                  **kwargs) -> pd.DataFrame:
    """
    Read CSV file with consistent error handling and validation.
    
    Consolidates the repeated pattern found in 50+ files:
        try:
            df = pd.read_csv('file.csv')
        except Exception as e:
            print(f"Error reading CSV: {e}")
            return pd.DataFrame()
    
    Args:
        file_path: Path to CSV file
        required_columns: List of columns that must exist
        dtype_map: Dictionary mapping column names to data types
        encoding: File encoding (default: utf-8)
        **kwargs: Additional arguments passed to pd.read_csv
        
    Returns:
        DataFrame or empty DataFrame on error
        
    Example:
        df = read_csv_safe('output.csv', required_columns=['row_id', 'person_name'])
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        logger.warning(f"CSV file not found: {file_path}")
        return pd.DataFrame()
    
    # Set default dtypes if not provided
    if dtype_map is None:
        dtype_map = {
            'row_id': 'object',
            'person_name': 'object',
            'email': 'object',
            'youtube_playlist': 'object',
            'google_drive': 'object',
            's3_youtube_urls': 'object',
            's3_drive_urls': 'object',
            's3_all_files': 'object'
        }
    
    # Read CSV with error handling
    df = pd.read_csv(
        file_path, 
        dtype=dtype_map,
        encoding=encoding,
        **kwargs
    )
    
    # Validate required columns
    if required_columns:
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return pd.DataFrame()
    
    # Clean column names (remove spaces, lowercase)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    return df


@handle_csv_operations("write_csv_safe", return_on_error=False)
def write_csv_safe(df: pd.DataFrame, 
                   file_path: Union[str, Path],
                   backup: bool = True,
                   validate: bool = True,
                   encoding: str = 'utf-8',
                   **kwargs) -> bool:
    """
    Write DataFrame to CSV with consistent error handling and backup.
    
    Consolidates the repeated pattern found in 30+ files:
        try:
            df.to_csv('output.csv', index=False)
        except Exception as e:
            print(f"Error writing CSV: {e}")
    
    Args:
        df: DataFrame to write
        file_path: Output file path
        backup: Whether to create backup before writing
        validate: Whether to validate written file
        encoding: File encoding (default: utf-8)
        **kwargs: Additional arguments passed to df.to_csv
        
    Returns:
        True if successful, False otherwise
        
    Example:
        success = write_csv_safe(df, 'output.csv', backup=True)
    """
    file_path = Path(file_path)
    
    # Ensure parent directory exists
    ensure_parent_dir(file_path, logger=logger)
    
    # Create backup if requested and file exists
    if backup and file_path.exists():
        backup_path = create_timestamped_backup(file_path)
        logger.info(f"Created backup: {backup_path}")
    
    # Set default parameters
    kwargs.setdefault('index', False)
    
    # Write CSV
    df.to_csv(file_path, encoding=encoding, **kwargs)
    
    # Validate if requested
    if validate:
        test_df = pd.read_csv(file_path, nrows=5)
        if len(test_df.columns) == 0:
            logger.error("Written CSV has no columns")
            return False
    
    logger.info(f"Successfully wrote CSV: {file_path}")
    return True


def create_timestamped_backup(file_path: Union[str, Path]) -> Path:
    """
    Create timestamped backup of file.
    
    Consolidates backup creation pattern found in 20+ files.
    
    Args:
        file_path: File to backup
        
    Returns:
        Path to backup file
        
    Example:
        backup_path = create_timestamped_backup('output.csv')
        # Creates: output.csv.backup_20231225_120000
    """
    file_path = Path(file_path)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = file_path.parent / f"{file_path.name}.backup_{timestamp}"
    
    if file_path.exists():
        import shutil
        shutil.copy2(file_path, backup_path)
    
    return backup_path


def merge_csv_updates(original_df: pd.DataFrame, 
                      updates_df: pd.DataFrame,
                      key_column: str = 'row_id',
                      update_columns: Optional[List[str]] = None,
                      how: str = 'left') -> pd.DataFrame:
    """
    Merge updates into original DataFrame.
    
    Consolidates CSV merging patterns found in multiple update scripts.
    
    Args:
        original_df: Original DataFrame
        updates_df: DataFrame with updates
        key_column: Column to merge on
        update_columns: Columns to update (all if None)
        how: Merge type ('left', 'right', 'outer', 'inner')
        
    Returns:
        Merged DataFrame
        
    Example:
        updated_df = merge_csv_updates(original_df, new_data, key_column='row_id')
    """
    # If no specific columns, update all overlapping columns
    if update_columns is None:
        update_columns = [col for col in updates_df.columns if col != key_column]
    
    # Ensure key column exists in both DataFrames
    if key_column not in original_df.columns or key_column not in updates_df.columns:
        logger.error(f"Key column '{key_column}' not found in both DataFrames")
        return original_df
    
    # Merge DataFrames
    merged = original_df.merge(
        updates_df[[key_column] + update_columns],
        on=key_column,
        how=how,
        suffixes=('', '_new')
    )
    
    # Update columns with new values where available
    for col in update_columns:
        new_col = f"{col}_new"
        if new_col in merged.columns:
            # Update only non-null new values
            mask = merged[new_col].notna()
            merged.loc[mask, col] = merged.loc[mask, new_col]
            merged.drop(columns=[new_col], inplace=True)
    
    return merged


def split_delimited_column(df: pd.DataFrame, 
                          column: str, 
                          delimiter: Optional[str] = None) -> List[str]:
    """
    Split delimited column values into list.
    
    Consolidates column splitting patterns found in 15+ files.
    
    Args:
        df: DataFrame
        column: Column name to split
        delimiter: Delimiter (uses config default if None)
        
    Returns:
        List of all unique values from split column
        
    Example:
        urls = split_delimited_column(df, 's3_youtube_urls', delimiter='|')
    """
    if delimiter is None:
        delimiter = get_csv_delimiter()
    
    all_values = []
    
    if column in df.columns:
        for value in df[column].dropna():
            if isinstance(value, str) and value.strip():
                values = [v.strip() for v in value.split(delimiter) if v.strip()]
                all_values.extend(values)
    
    return list(set(all_values))  # Return unique values


def join_values_to_column(values: List[str], 
                         delimiter: Optional[str] = None) -> str:
    """
    Join list of values into delimited string for CSV storage.
    
    Consolidates value joining patterns.
    
    Args:
        values: List of values to join
        delimiter: Delimiter (uses config default if None)
        
    Returns:
        Delimited string
        
    Example:
        csv_value = join_values_to_column(['url1', 'url2'], delimiter='|')
    """
    if delimiter is None:
        delimiter = get_csv_delimiter()
    
    # Filter out empty values
    clean_values = [str(v).strip() for v in values if v and str(v).strip()]
    
    return delimiter.join(clean_values)


# ============================================================================
# JSON PROCESSING UTILITIES
# ============================================================================

@handle_file_operations("read_json_safe", return_on_error={})
def read_json_safe(file_path: Union[str, Path], 
                   default: Any = None,
                   encoding: str = 'utf-8') -> Any:
    """
    Read JSON file with consistent error handling.
    
    Consolidates JSON reading patterns found in 40+ files.
    
    Args:
        file_path: Path to JSON file
        default: Default value if file doesn't exist or is invalid
        encoding: File encoding
        
    Returns:
        Parsed JSON or default value
        
    Example:
        config = read_json_safe('config.json', default={})
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        logger.debug(f"JSON file not found: {file_path}")
        return default if default is not None else {}
    
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {file_path}: {e}")
        return default if default is not None else {}


@handle_file_operations("write_json_safe", return_on_error=False)
def write_json_safe(data: Any, 
                    file_path: Union[str, Path],
                    indent: int = 2,
                    encoding: str = 'utf-8',
                    sort_keys: bool = True) -> bool:
    """
    Write data to JSON file with consistent error handling.
    
    Consolidates JSON writing patterns found in 30+ files.
    
    Args:
        data: Data to serialize
        file_path: Output file path
        indent: JSON indentation
        encoding: File encoding
        sort_keys: Whether to sort dictionary keys
        
    Returns:
        True if successful, False otherwise
        
    Example:
        success = write_json_safe(metadata, 'metadata.json')
    """
    file_path = Path(file_path)
    
    # Ensure parent directory exists
    ensure_parent_dir(file_path, logger=logger)
    
    with open(file_path, 'w', encoding=encoding) as f:
        json.dump(data, f, indent=indent, sort_keys=sort_keys, default=str)
    
    logger.info(f"Successfully wrote JSON: {file_path}")
    return True


def merge_json_configs(base_config: Dict[str, Any], 
                       updates: Dict[str, Any],
                       deep: bool = True) -> Dict[str, Any]:
    """
    Merge JSON configurations with deep update support.
    
    Consolidates config merging patterns.
    
    Args:
        base_config: Base configuration
        updates: Updates to apply
        deep: Whether to do deep merge for nested dicts
        
    Returns:
        Merged configuration
        
    Example:
        config = merge_json_configs(default_config, user_config)
    """
    if not deep:
        return {**base_config, **updates}
    
    result = base_config.copy()
    
    for key, value in updates.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_json_configs(result[key], value, deep=True)
        else:
            result[key] = value
    
    return result


# ============================================================================
# DATA CLEANING UTILITIES
# ============================================================================

def clean_string_column(series: pd.Series, 
                       strip: bool = True,
                       lower: bool = False,
                       remove_special: bool = False,
                       normalize_whitespace: bool = True) -> pd.Series:
    """
    Clean string column with common transformations.
    
    Consolidates string cleaning patterns found in 20+ files.
    
    Args:
        series: Pandas Series to clean
        strip: Remove leading/trailing whitespace
        lower: Convert to lowercase
        remove_special: Remove special characters
        normalize_whitespace: Normalize internal whitespace
        
    Returns:
        Cleaned Series
        
    Example:
        df['name'] = clean_string_column(df['name'], lower=True)
    """
    # Convert to string and handle nulls
    series = series.astype(str).replace('nan', '')
    
    if strip:
        series = series.str.strip()
    
    if normalize_whitespace:
        series = series.str.replace(r'\s+', ' ', regex=True)
    
    if lower:
        series = series.str.lower()
    
    if remove_special:
        series = series.str.replace(r'[^a-zA-Z0-9\s_-]', '', regex=True)
    
    # Replace empty strings with NaN
    series = series.replace('', np.nan)
    
    return series


def normalize_email_column(series: pd.Series) -> pd.Series:
    """
    Normalize email addresses in column.
    
    Consolidates email normalization patterns.
    
    Args:
        series: Pandas Series with emails
        
    Returns:
        Normalized email Series
        
    Example:
        df['email'] = normalize_email_column(df['email'])
    """
    # Clean and normalize
    series = clean_string_column(series, lower=True, strip=True)
    
    # Validate email format
    email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    
    def validate_email(email):
        if pd.isna(email) or not email:
            return np.nan
        return email if email_pattern.match(email) else np.nan
    
    return series.apply(validate_email)


def remove_duplicates_preserve_order(items: List[Any]) -> List[Any]:
    """
    Remove duplicates while preserving order.
    
    Consolidates deduplication patterns.
    
    Args:
        items: List with potential duplicates
        
    Returns:
        List with duplicates removed, order preserved
        
    Example:
        unique_urls = remove_duplicates_preserve_order(all_urls)
    """
    seen = set()
    result = []
    
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    
    return result


# ============================================================================
# BATCH PROCESSING UTILITIES
# ============================================================================

def process_dataframe_in_batches(df: pd.DataFrame,
                                batch_size: int,
                                process_func: Callable[[pd.DataFrame], Any],
                                show_progress: bool = True) -> List[Any]:
    """
    Process DataFrame in batches with progress tracking.
    
    Consolidates batch processing patterns found in 15+ files.
    
    Args:
        df: DataFrame to process
        batch_size: Size of each batch
        process_func: Function to process each batch
        show_progress: Whether to show progress
        
    Returns:
        List of results from each batch
        
    Example:
        results = process_dataframe_in_batches(df, 100, process_batch_func)
    """
    total_rows = len(df)
    num_batches = (total_rows + batch_size - 1) // batch_size
    results = []
    
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_rows)
        batch = df.iloc[start_idx:end_idx]
        
        if show_progress:
            logger.info(f"Processing batch {i + 1}/{num_batches} ({len(batch)} rows)")
        
        try:
            result = process_func(batch)
            results.append(result)
        except Exception as e:
            logger.error(f"Error processing batch {i + 1}: {e}")
            results.append(None)
    
    return results


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split list into chunks of specified size.
    
    Consolidates list chunking patterns.
    
    Args:
        items: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
        
    Example:
        chunks = chunk_list(urls, 50)
        for chunk in chunks:
            process_urls(chunk)
    """
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


# ============================================================================
# DATA TRANSFORMATION UTILITIES
# ============================================================================

def flatten_nested_dict(nested_dict: Dict[str, Any], 
                       parent_key: str = '',
                       separator: str = '_') -> Dict[str, Any]:
    """
    Flatten nested dictionary structure.
    
    Consolidates dict flattening patterns found in JSON processing.
    
    Args:
        nested_dict: Dictionary to flatten
        parent_key: Parent key prefix
        separator: Key separator
        
    Returns:
        Flattened dictionary
        
    Example:
        flat = flatten_nested_dict({'a': {'b': 1, 'c': 2}})
        # Returns: {'a_b': 1, 'a_c': 2}
    """
    items = []
    
    for key, value in nested_dict.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        
        if isinstance(value, dict):
            items.extend(flatten_nested_dict(value, new_key, separator).items())
        else:
            items.append((new_key, value))
    
    return dict(items)


def safe_get_nested(data: Dict[str, Any], 
                   keys: Union[str, List[str]], 
                   default: Any = None) -> Any:
    """
    Safely get nested dictionary value.
    
    Consolidates nested dict access patterns.
    
    Args:
        data: Dictionary to access
        keys: Key path (string with dots or list)
        default: Default value if not found
        
    Returns:
        Value at key path or default
        
    Example:
        value = safe_get_nested(config, 'aws.s3.bucket_name', default='my-bucket')
    """
    if isinstance(keys, str):
        keys = keys.split('.')
    
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    
    return current


def group_by_attribute(items: List[Dict[str, Any]], 
                      attribute: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group list of dictionaries by attribute value.
    
    Consolidates grouping patterns.
    
    Args:
        items: List of dictionaries
        attribute: Attribute to group by
        
    Returns:
        Dictionary mapping attribute values to lists of items
        
    Example:
        grouped = group_by_attribute(downloads, 'status')
        failed = grouped.get('failed', [])
    """
    grouped = {}
    
    for item in items:
        key = item.get(attribute, 'unknown')
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(item)
    
    return grouped


# ============================================================================
# DATA VALIDATION UTILITIES
# ============================================================================

def validate_dataframe_schema(df: pd.DataFrame,
                            required_columns: List[str],
                            column_types: Optional[Dict[str, type]] = None) -> Tuple[bool, List[str]]:
    """
    Validate DataFrame schema against requirements.
    
    Consolidates DataFrame validation patterns.
    
    Args:
        df: DataFrame to validate
        required_columns: Required column names
        column_types: Expected column types
        
    Returns:
        Tuple of (is_valid, error_messages)
        
    Example:
        is_valid, errors = validate_dataframe_schema(df, ['row_id', 'email'])
    """
    errors = []
    
    # Check required columns
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        errors.append(f"Missing required columns: {missing_columns}")
    
    # Check column types if specified
    if column_types:
        for col, expected_type in column_types.items():
            if col in df.columns:
                actual_type = df[col].dtype
                if not np.issubdtype(actual_type, expected_type):
                    errors.append(f"Column '{col}' has type {actual_type}, expected {expected_type}")
    
    return len(errors) == 0, errors


def find_invalid_values(series: pd.Series, 
                       valid_pattern: Optional[str] = None,
                       valid_values: Optional[List[Any]] = None) -> pd.Series:
    """
    Find invalid values in a Series.
    
    Consolidates validation patterns.
    
    Args:
        series: Series to validate
        valid_pattern: Regex pattern for valid values
        valid_values: List of valid values
        
    Returns:
        Series of invalid values
        
    Example:
        invalid_emails = find_invalid_values(df['email'], valid_pattern=r'^[\w\.-]+@[\w\.-]+\.\w+$')
    """
    mask = pd.Series([False] * len(series), index=series.index)
    
    if valid_pattern:
        pattern = re.compile(valid_pattern)
        mask |= ~series.astype(str).str.match(pattern)
    
    if valid_values:
        mask |= ~series.isin(valid_values)
    
    return series[mask]


# ============================================================================
# STATISTICS AND REPORTING UTILITIES
# ============================================================================

def generate_summary_stats(df: pd.DataFrame, 
                          columns: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Generate summary statistics for DataFrame.
    
    Consolidates statistics generation patterns.
    
    Args:
        df: DataFrame to analyze
        columns: Specific columns to analyze (all if None)
        
    Returns:
        Dictionary of statistics
        
    Example:
        stats = generate_summary_stats(df, columns=['youtube_playlist', 'google_drive'])
    """
    if columns is None:
        columns = df.columns.tolist()
    
    stats = {
        'total_rows': len(df),
        'columns': {}
    }
    
    for col in columns:
        if col not in df.columns:
            continue
        
        col_stats = {
            'non_null_count': df[col].notna().sum(),
            'null_count': df[col].isna().sum(),
            'unique_count': df[col].nunique(),
            'null_percentage': (df[col].isna().sum() / len(df) * 100) if len(df) > 0 else 0
        }
        
        # Add type-specific stats
        if pd.api.types.is_numeric_dtype(df[col]):
            col_stats.update({
                'mean': df[col].mean(),
                'median': df[col].median(),
                'min': df[col].min(),
                'max': df[col].max()
            })
        
        stats['columns'][col] = col_stats
    
    return stats


def create_completion_report(df: pd.DataFrame, 
                           url_columns: List[str]) -> Dict[str, Any]:
    """
    Create completion report for URL columns.
    
    Consolidates completion reporting patterns.
    
    Args:
        df: DataFrame to analyze
        url_columns: Columns containing URLs
        
    Returns:
        Completion report dictionary
        
    Example:
        report = create_completion_report(df, ['youtube_playlist', 'google_drive'])
    """
    report = {
        'summary': {
            'total_rows': len(df),
            'complete_rows': 0,
            'partial_rows': 0,
            'empty_rows': 0
        },
        'by_column': {}
    }
    
    # Check each row for completeness
    for idx, row in df.iterrows():
        filled_columns = sum(1 for col in url_columns if pd.notna(row.get(col)) and str(row.get(col)).strip())
        
        if filled_columns == len(url_columns):
            report['summary']['complete_rows'] += 1
        elif filled_columns > 0:
            report['summary']['partial_rows'] += 1
        else:
            report['summary']['empty_rows'] += 1
    
    # Column-specific stats
    for col in url_columns:
        if col in df.columns:
            report['by_column'][col] = {
                'filled': df[col].notna().sum(),
                'empty': df[col].isna().sum(),
                'percentage': (df[col].notna().sum() / len(df) * 100) if len(df) > 0 else 0
            }
    
    return report


# ============================================================================
# UTILITY DECORATORS FOR DATA PROCESSING
# ============================================================================

def with_dataframe_validation(required_columns: List[str]):
    """
    Decorator to validate DataFrame before processing.
    
    Args:
        required_columns: Columns that must exist
        
    Example:
        @with_dataframe_validation(['row_id', 'email'])
        def process_data(df):
            return df
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(df: pd.DataFrame, *args, **kwargs):
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"Expected DataFrame, got {type(df)}")
            
            missing = set(required_columns) - set(df.columns)
            if missing:
                raise ValueError(f"Missing required columns: {missing}")
            
            return func(df, *args, **kwargs)
        return wrapper
    return decorator


def with_progress_tracking(task_name: str):
    """
    Decorator to add progress tracking to data processing functions.
    
    Args:
        task_name: Name of the task for logging
        
    Example:
        @with_progress_tracking("Processing URLs")
        def process_urls(urls):
            return [process_url(url) for url in urls]
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"Starting {task_name}...")
            start_time = datetime.now()
            
            try:
                result = func(*args, **kwargs)
                elapsed = (datetime.now() - start_time).total_seconds()
                logger.info(f"Completed {task_name} in {elapsed:.2f} seconds")
                return result
            except Exception as e:
                elapsed = (datetime.now() - start_time).total_seconds()
                logger.error(f"Failed {task_name} after {elapsed:.2f} seconds: {e}")
                raise
        return wrapper
    return decorator


# Example usage
if __name__ == "__main__":
    # Test CSV operations
    df = read_csv_safe('outputs/output.csv', required_columns=['row_id'])
    if not df.empty:
        print(f"Loaded {len(df)} rows")
        
        # Generate summary
        stats = generate_summary_stats(df)
        print(json.dumps(stats, indent=2))
        
        # Create completion report
        report = create_completion_report(df, ['youtube_playlist', 'google_drive'])
        print(json.dumps(report, indent=2))

# ============================================================================
# UNIFIED DATA TRANSFORMATION WORKFLOWS (DRY ITERATION 2 - Step 5)
# ============================================================================

class DataTransformationPipeline:
    """
    Unified data transformation pipeline (DRY CONSOLIDATION - Step 5).
    
    CONSOLIDATES TRANSFORMATION PATTERNS FROM:
    - Multiple CSV processing scripts with similar transformation steps  
    - Repeated data cleaning operations across files
    - Inconsistent error handling in transformation workflows
    
    BUSINESS IMPACT: Prevents data corruption and ensures consistent transformations
    """
    
    def __init__(self, name: str = "default_pipeline"):
        self.name = name
        self.steps = []
        self.logger = get_logger(f"{__name__}.{name}")
        self.stats = {
            'processed_rows': 0,
            'errors': 0,
            'transformations_applied': 0,
            'start_time': None,
            'end_time': None
        }
    
    def add_step(self, func: Callable, description: str = None, **kwargs) -> 'DataTransformationPipeline':
        """Add a transformation step to the pipeline."""
        step = {
            'function': func,
            'description': description or func.__name__,
            'kwargs': kwargs,
            'applied_count': 0,
            'error_count': 0
        }
        self.steps.append(step)
        return self
    
    def transform(self, data: Union[pd.DataFrame, Dict, List], 
                 progress_callback: Optional[Callable] = None) -> Union[pd.DataFrame, Dict, List]:
        """
        Execute the transformation pipeline.
        
        Args:
            data: Input data (DataFrame, dict, or list)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Transformed data
        """
        self.stats['start_time'] = datetime.now()
        self.logger.info(f"🔄 Starting transformation pipeline '{self.name}' with {len(self.steps)} steps")
        
        current_data = data
        total_steps = len(self.steps)
        
        for i, step in enumerate(self.steps):
            step_start = datetime.now()
            
            try:
                self.logger.info(f"Step {i+1}/{total_steps}: {step['description']}")
                
                # Apply transformation step
                if isinstance(current_data, pd.DataFrame):
                    original_length = len(current_data)
                else:
                    original_length = len(current_data) if hasattr(current_data, '__len__') else 1
                
                current_data = step['function'](current_data, **step['kwargs'])
                
                # Update statistics
                step['applied_count'] += 1
                self.stats['transformations_applied'] += 1
                
                if isinstance(current_data, pd.DataFrame):
                    new_length = len(current_data)
                    self.logger.info(f"  ✅ Transformed {original_length} → {new_length} rows")
                else:
                    self.logger.info(f"  ✅ Step completed")
                
                # Progress callback
                if progress_callback:
                    progress_callback(i + 1, total_steps, step['description'])
                
                step_duration = (datetime.now() - step_start).total_seconds()
                self.logger.debug(f"  ⏱️ Step duration: {step_duration:.2f}s")
                
            except Exception as e:
                step['error_count'] += 1
                self.stats['errors'] += 1
                error_msg = f"Step {i+1} failed: {step['description']} - {str(e)}"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)
        
        # Final statistics
        self.stats['end_time'] = datetime.now()
        total_duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        if isinstance(current_data, pd.DataFrame):
            self.stats['processed_rows'] = len(current_data)
        
        self.logger.info(f"✅ Pipeline '{self.name}' completed in {total_duration:.2f}s")
        self.logger.info(f"📊 Stats: {self.stats['processed_rows']} rows, {self.stats['transformations_applied']} transformations")
        
        return current_data
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline execution statistics."""
        return self.stats.copy()


class CSVTransformationWorkflow:
    """
    Specialized CSV transformation workflow (DRY CONSOLIDATION - Step 5).
    
    ELIMINATES REPETITIVE PATTERNS:
    - CSV loading with error handling
    - Column validation and normalization  
    - Data cleaning and validation
    - Safe CSV writing with backups
    
    BUSINESS IMPACT: Standardizes CSV processing to prevent data corruption
    """
    
    @staticmethod
    def create_standard_csv_pipeline(required_columns: List[str] = None) -> DataTransformationPipeline:
        """Create a standard CSV processing pipeline."""
        pipeline = DataTransformationPipeline("standard_csv")
        
        # Step 1: Validate required columns
        if required_columns:
            pipeline.add_step(
                CSVTransformationWorkflow._validate_columns,
                f"Validate required columns: {required_columns}",
                required_columns=required_columns
            )
        
        # Step 2: Clean string columns
        pipeline.add_step(
            CSVTransformationWorkflow._clean_string_data,
            "Clean string data and remove invalid characters"
        )
        
        # Step 3: Normalize data types
        pipeline.add_step(
            CSVTransformationWorkflow._normalize_data_types,
            "Normalize data types and handle missing values"
        )
        
        return pipeline
    
    @staticmethod
    def _validate_columns(df: pd.DataFrame, required_columns: List[str]) -> pd.DataFrame:
        """Validate that required columns exist."""
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        return df
    
    @staticmethod
    def _clean_string_data(df: pd.DataFrame) -> pd.DataFrame:
        """Clean string data in all object columns."""
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
            # Remove null string representations
            df[col] = df[col].replace(['nan', 'None', 'null', ''], pd.NA)
        return df
    
    @staticmethod
    def _normalize_data_types(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize data types and handle missing values."""
        # Convert numeric columns
        for col in df.columns:
            if col.endswith('_id') or col in ['row_id']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif 'date' in col.lower() or 'time' in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except:
                    pass  # Keep as string if conversion fails
        
        return df


def batch_process_files(file_pattern: str, 
                       transformation_func: Callable,
                       output_pattern: str = None,
                       progress_callback: Callable = None) -> Dict[str, Any]:
    """
    Batch process multiple files with consistent error handling.
    
    CONSOLIDATES PATTERN FROM:
    - Multiple scripts that process batches of CSV files
    - Inconsistent error handling across batch operations
    - Repeated file discovery and output naming logic
    
    Args:
        file_pattern: Glob pattern for input files
        transformation_func: Function to apply to each file
        output_pattern: Pattern for output files (optional)
        progress_callback: Progress callback function
        
    Returns:
        Dictionary with processing results and statistics
    """
    import glob
    
    files = glob.glob(file_pattern)
    if not files:
        raise ValueError(f"No files found matching pattern: {file_pattern}")
    
    results = {
        'processed_files': [],
        'failed_files': [],
        'total_files': len(files),
        'start_time': datetime.now(),
        'end_time': None
    }
    
    logger.info(f"🚀 Starting batch processing of {len(files)} files")
    
    for i, file_path in enumerate(files):
        try:
            logger.info(f"📁 Processing file {i+1}/{len(files)}: {Path(file_path).name}")
            
            # Apply transformation
            result = transformation_func(file_path)
            
            # Save output if pattern provided
            if output_pattern:
                output_path = output_pattern.format(
                    name=Path(file_path).stem,
                    timestamp=datetime.now().strftime('%Y%m%d_%H%M%S')
                )
                
                if hasattr(result, 'to_csv'):
                    write_csv_safe(result, output_path)
                elif isinstance(result, dict):
                    write_json_safe(result, output_path)
            
            results['processed_files'].append({
                'input_file': file_path,
                'output_file': output_path if output_pattern else None,
                'success': True
            })
            
            # Progress callback
            if progress_callback:
                progress_callback(i + 1, len(files), Path(file_path).name)
                
        except Exception as e:
            error_msg = f"Failed to process {file_path}: {str(e)}"
            logger.error(error_msg)
            
            results['failed_files'].append({
                'input_file': file_path,
                'error': error_msg
            })
    
    results['end_time'] = datetime.now()
    duration = (results['end_time'] - results['start_time']).total_seconds()
    
    logger.info(f"✅ Batch processing completed in {duration:.2f}s")
    logger.info(f"📊 Results: {len(results['processed_files'])} succeeded, {len(results['failed_files'])} failed")
    
    return results


def aggregate_csv_data(file_paths: List[str], 
                      key_column: str = 'row_id',
                      aggregation_rules: Dict[str, str] = None) -> pd.DataFrame:
    """
    Aggregate data from multiple CSV files with consistent handling.
    
    CONSOLIDATES PATTERN FROM:
    - Multiple scripts that combine CSV data from different sources
    - Inconsistent aggregation logic across files
    - Different approaches to handling duplicate keys
    
    Args:
        file_paths: List of CSV file paths to aggregate
        key_column: Column to use as aggregation key
        aggregation_rules: Rules for handling column conflicts
        
    Returns:
        Aggregated DataFrame
    """
    if not file_paths:
        raise ValueError("No file paths provided for aggregation")
    
    aggregation_rules = aggregation_rules or {}
    logger.info(f"🔗 Aggregating data from {len(file_paths)} CSV files")
    
    # Load all DataFrames
    dataframes = []
    for file_path in file_paths:
        try:
            df = read_csv_safe(file_path, required_columns=[key_column])
            if not df.empty:
                df['_source_file'] = Path(file_path).name
                dataframes.append(df)
                logger.info(f"  📄 Loaded {len(df)} rows from {Path(file_path).name}")
        except Exception as e:
            logger.warning(f"  ⚠️ Skipped {file_path}: {str(e)}")
    
    if not dataframes:
        logger.warning("No valid DataFrames to aggregate")
        return pd.DataFrame()
    
    # Perform aggregation
    logger.info("🔄 Performing data aggregation...")
    
    # Start with the first DataFrame
    result_df = dataframes[0].copy()
    
    # Merge subsequent DataFrames
    for df in dataframes[1:]:
        # Merge on key column
        result_df = result_df.merge(
            df, 
            on=key_column, 
            how='outer', 
            suffixes=('', '_new')
        )
        
        # Apply aggregation rules for duplicate columns
        for col in df.columns:
            if col != key_column and col in result_df.columns:
                new_col = f"{col}_new"
                if new_col in result_df.columns:
                    rule = aggregation_rules.get(col, 'keep_first')
                    
                    if rule == 'keep_first':
                        result_df[col] = result_df[col].fillna(result_df[new_col])
                    elif rule == 'keep_last':
                        result_df[col] = result_df[new_col].fillna(result_df[col])
                    elif rule == 'combine':
                        # Combine non-null values
                        mask = result_df[col].isna() | (result_df[col] == '')
                        result_df.loc[mask, col] = result_df.loc[mask, new_col]
                    
                    # Remove the temporary column
                    result_df = result_df.drop(columns=[new_col])
    
    logger.info(f"✅ Aggregation complete: {len(result_df)} total rows")
    return result_df
