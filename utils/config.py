"""Configuration management module for centralized settings."""
import os
import yaml
import importlib
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
import logging
from datetime import datetime
import functools

# Singleton pattern for configuration
_config = None
_config_path = None

class Config:
    """Configuration manager that loads settings from YAML file."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration from YAML file.
        
        Args:
            config_path: Path to config file. If None, looks for config.yaml in project root.
        """
        if config_path is None:
            # Look for config.yaml in config directory (relative to project root)
            utils_dir = Path(__file__).parent
            project_root = utils_dir.parent
            config_path = project_root / "config" / "config.yaml"
        
        self.config_path = Path(config_path)
        self._data = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            self._data = yaml.safe_load(f)
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to config value (e.g., "downloads.youtube.max_workers")
            default: Default value if key not found
        
        Returns:
            Configuration value or default
        """
        keys = key_path.split('.')
        value = self._data
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """
        Get entire configuration section.
        
        Args:
            section: Section name (e.g., "downloads", "retry")
        
        Returns:
            Dictionary containing section configuration
        """
        return self._data.get(section, {})
    
    def reload(self):
        """Reload configuration from file."""
        self._load_config()
    
    @property
    def all(self) -> Dict[str, Any]:
        """Get all configuration data."""
        return self._data.copy()


def get_config(config_path: Optional[str] = None) -> Config:
    """
    Get singleton configuration instance.
    
    Args:
        config_path: Path to config file (only used on first call)
    
    Returns:
        Configuration instance
    """
    global _config, _config_path
    
    if _config is None or (config_path and config_path != _config_path):
        _config = Config(config_path)
        _config_path = config_path
    
    return _config


# DRY CONSOLIDATION: S3 Configuration
S3_CONFIG = {
    'default_bucket': 'typing-clients-uuid-system',
    'backup_bucket': 'typing-clients-backups',
    'temp_prefix': 'temp/',
    'files_prefix': 'files/'
}

def get_s3_bucket(bucket_type: str = 'default') -> str:
    """Get S3 bucket name for specified type."""
    return S3_CONFIG.get(f'{bucket_type}_bucket', S3_CONFIG['default_bucket'])

def get_s3_prefix(prefix_type: str = 'files') -> str:
    """Get S3 prefix for specified type."""
    return S3_CONFIG.get(f'{prefix_type}_prefix', '')

# Convenience functions for common config values
def get_youtube_downloads_dir() -> str:
    """Get YouTube downloads directory."""
    return get_config().get("paths.youtube_downloads", "youtube_downloads")


def get_drive_downloads_dir() -> str:
    """Get Drive downloads directory."""
    return get_config().get("paths.drive_downloads", "drive_downloads")


def get_output_csv_path() -> str:
    """Get output CSV file path."""
    return get_config().get("paths.output_csv", "output.csv")


def get_google_sheets_url() -> str:
    """Get Google Sheets URL."""
    return get_config().get("google_sheets.url", "")


def get_retry_config() -> Dict[str, Any]:
    """Get retry configuration."""
    return get_config().get_section("retry")


def get_timeout(timeout_type: str = "default") -> float:
    """
    Get timeout value.
    
    Args:
        timeout_type: Type of timeout (default, file_lock, http_request, etc.)
    
    Returns:
        Timeout value in seconds
    """
    return get_config().get(f"timeouts.{timeout_type}", 30.0)


def create_download_dir(download_dir: str, logger=None) -> Path:
    """
    Create download directory if it doesn't exist.
    
    Args:
        download_dir: Directory path to create
        logger: Optional logger instance
    
    Returns:
        Path object for the directory
    """
    downloads_path = Path(download_dir)
    if not downloads_path.exists():
        downloads_path.mkdir(parents=True)
        if logger:
            logger.info(f"Created downloads directory: {download_dir}")
    return downloads_path


def ensure_directory(dir_path: Union[str, Path], parents: bool = True, exist_ok: bool = True, logger=None) -> Path:
    """
    Ensure directory exists, creating it if necessary (DRY path utility).
    Consolidates various directory creation patterns throughout codebase.
    
    Args:
        dir_path: Directory path to ensure exists
        parents: Create parent directories if needed
        exist_ok: Don't raise error if directory already exists
        logger: Optional logger instance
    
    Returns:
        Path object for the directory
    """
    path = Path(dir_path)
    if not path.exists():
        path.mkdir(parents=parents, exist_ok=exist_ok)
        if logger:
            logger.info(f"Created directory: {dir_path}")
    return path


def ensure_parent_dir(file_path: Union[str, Path], logger=None) -> Path:
    """
    Ensure parent directory of a file exists (DRY path utility).
    Common pattern for ensuring output files can be written.
    
    Args:
        file_path: File path whose parent directory should exist
        logger: Optional logger instance
    
    Returns:
        Path object for the parent directory
    """
    file_path = Path(file_path)
    parent_dir = file_path.parent
    if not parent_dir.exists():
        parent_dir.mkdir(parents=True, exist_ok=True)
        if logger:
            logger.info(f"Created parent directory: {parent_dir}")
    return parent_dir


def get_project_root() -> Path:
    """
    Get project root directory (DRY path utility).
    
    Returns:
        Path object for project root
    """
    return Path(__file__).parent.parent


def setup_project_imports() -> None:
    """
    Set up project imports by adding necessary directories to sys.path.
    This eliminates the need for sys.path manipulation in individual files.
    
    Usage:
        from utils.config import setup_project_imports
        setup_project_imports()
        
        # Now you can import project modules normally
        from utils.logging_config import setup_logging
        from utils.s3_manager import S3Manager
    """
    import sys
    project_root = get_project_root()
    
    # Add project root to sys.path if not already there
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    
    # Add utils directory to sys.path if not already there
    utils_dir = str(project_root / 'utils')
    if utils_dir not in sys.path:
        sys.path.insert(0, utils_dir)


def get_outputs_dir() -> Path:
    """
    Get outputs directory, creating if necessary (DRY path utility).
    
    Returns:
        Path object for outputs directory
    """
    config = get_config()
    outputs_dir = get_project_root() / config.get("paths.output_dir", "outputs")
    return ensure_directory(outputs_dir)


def get_logs_dir() -> Path:
    """
    Get logs directory, creating if necessary (DRY path utility).
    
    Returns:
        Path object for logs directory
    """
    config = get_config()
    logs_dir = get_project_root() / config.get("paths.logs_dir", "logs")
    return ensure_directory(logs_dir)


def get_download_chunk_size(file_size: int) -> int:
    """
    Get appropriate chunk size based on file size.
    
    Args:
        file_size: Size of file in bytes
    
    Returns:
        Chunk size in bytes
    """
    config = get_config()
    thresholds = config.get_section("downloads").get("drive", {})
    
    if file_size > thresholds.get("size_thresholds", {}).get("large", 104857600):
        return thresholds.get("chunk_sizes", {}).get("large", 8388608)
    elif file_size > thresholds.get("size_thresholds", {}).get("medium", 10485760):
        return thresholds.get("chunk_sizes", {}).get("medium", 2097152)
    else:
        return thresholds.get("chunk_sizes", {}).get("small", 1048576)


def get_parallel_config() -> Dict[str, Any]:
    """Get parallel processing configuration."""
    return get_config().get_section("parallel")


def get_logging_config() -> Dict[str, Any]:
    """Get logging configuration."""
    return get_config().get_section("logging")


def is_ssl_verify_enabled() -> bool:
    """Check if SSL verification is enabled."""
    return get_config().get("security.ssl_verify", True)


def get_allowed_domains(service: str) -> list:
    """
    Get allowed domains for a service.
    
    Args:
        service: Service name (youtube, drive)
    
    Returns:
        List of allowed domains
    """
    return get_config().get(f"security.allowed_domains.{service}", [])


def get_streaming_threshold() -> int:
    """Get file size threshold for streaming operations."""
    return get_config().get("file_processing.streaming_threshold", 5242880)


def safe_import(module_names: Union[str, List[str]], from_items: Optional[Union[str, List[str]]] = None, 
                package: Optional[str] = None) -> Any:
    """
    Centralized import management with automatic fallback for relative/absolute imports.
    
    Eliminates the need for try/except ImportError blocks throughout the codebase.
    
    Args:
        module_names: Module name(s) to import from
        from_items: Specific items to import (functions, classes, etc.)
        package: Package name for relative imports
    
    Returns:
        Imported module or specific items
        
    Examples:
        # Import entire module
        csv_tracker = safe_import('csv_tracker')
        
        # Import specific functions
        reset_func = safe_import('csv_tracker', 'reset_all_download_status')
        
        # Import multiple items
        funcs = safe_import('csv_tracker', ['reset_all_download_status', 'ensure_tracking_columns'])
    """
    if isinstance(module_names, str):
        module_names = [module_names]
    if isinstance(from_items, str):
        from_items = [from_items]
    
    last_error = None
    
    # Try absolute import first
    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
            if from_items:
                if len(from_items) == 1:
                    return getattr(module, from_items[0])
                else:
                    return [getattr(module, item) for item in from_items]
            return module
        except ImportError as e:
            last_error = e
            continue
    
    # Try relative import as fallback
    if package:
        for module_name in module_names:
            try:
                module = importlib.import_module(f'.{module_name}', package=package)
                if from_items:
                    if len(from_items) == 1:
                        return getattr(module, from_items[0])
                    else:
                        return [getattr(module, item) for item in from_items]
                return module
            except ImportError as e:
                last_error = e
                continue
    
    # If all imports fail, raise the last error
    raise last_error


# Simple error categorization utilities (DRY)
def categorize_error(error: Exception) -> str:
    """
    Simple error categorization for consistent error handling (DRY).
    Consolidates scattered error type checking throughout codebase.
    
    Args:
        error: Exception to categorize
    
    Returns:
        Error category string
    """
    error_str = str(error).lower()
    
    # Network-related errors
    if any(keyword in error_str for keyword in ['timeout', 'connection', 'network', 'dns', 'ssl']):
        return 'network'
    elif any(keyword in error_str for keyword in ['rate limit', 'quota', '429', 'too many requests']):
        return 'rate_limit'
    elif any(keyword in error_str for keyword in ['http', '404', '403', '401', '500', '502', '503']):
        return 'http'
    
    # File I/O errors
    elif any(keyword in error_str for keyword in ['file not found', 'no such file', 'permission denied', 'access denied']):
        return 'file_io'
    elif any(keyword in error_str for keyword in ['disk', 'space', 'full']):
        return 'disk_space'
    
    # Data errors
    elif any(keyword in error_str for keyword in ['csv', 'parsing', 'format', 'decode', 'encode']):
        return 'data_format'
    
    # System errors
    elif any(keyword in error_str for keyword in ['memory', 'resource', 'system']):
        return 'system'
    
    # Default category
    else:
        return 'unknown'


def format_error_message(operation: str, error: Exception, context: str = None) -> str:
    """
    Format error message consistently (DRY).
    Consolidates scattered error message formatting patterns.
    
    Args:
        operation: Operation that failed
        error: Exception that occurred
        context: Optional context information
    
    Returns:
        Formatted error message
    """
    category = categorize_error(error)
    base_msg = f"✗ {operation} failed ({category}): {str(error)}"
    
    if context:
        base_msg += f" | Context: {context}"
    
    return base_msg


def load_json_state(filename: str, default: Optional[dict] = None) -> dict:
    """
    Load JSON state file with default fallback (DRY state management).
    Consolidates progress/state loading patterns throughout codebase.
    
    Args:
        filename: Path to JSON file
        default: Default dict to return if file doesn't exist
    
    Returns:
        Loaded JSON data or default
    """
    import json
    filepath = Path(filename)
    if filepath.exists():
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Failed to load JSON state from {filename}: {e}")
            return default or {}
    return default or {}


def save_json_state(filename: str, data: dict) -> None:
    """
    Save state to JSON file (DRY state management).
    Consolidates progress/state saving patterns throughout codebase.
    
    Args:
        filename: Path to JSON file
        data: Dict to save as JSON
    """
    import json
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
    except IOError as e:
        logging.error(f"Failed to save JSON state to {filename}: {e}")
        raise


# === STATUS FORMATTING FUNCTIONS (DRY) ===

class StatusIcons:
    """Centralized status icons for consistent output formatting"""
    SUCCESS = "✓"
    FAILURE = "✗"
    WARNING = "⚠"
    ERROR = "❌"
    COMPLETE = "✅"
    STATS = "📊"
    CELEBRATE = "🎉"
    FILE = "📄"
    DATABASE = "🗄️"
    PACKAGE = "📦"
    ROCKET = "🚀"
    
    # Status prefixes
    SUCCESS_PREFIX = "✓"
    ERROR_PREFIX = "✗"
    WARNING_PREFIX = "⚠️"
    INFO_PREFIX = "📊"
    

def format_success(message: str, icon: bool = True) -> str:
    """Format a success message (DRY)"""
    return f"{StatusIcons.SUCCESS} {message}" if icon else message


def format_error(message: str, icon: bool = True) -> str:
    """Format an error message (DRY)"""
    return f"{StatusIcons.ERROR} {message}" if icon else message


def format_warning(message: str, icon: bool = True) -> str:
    """Format a warning message (DRY)"""
    return f"{StatusIcons.WARNING} {message}" if icon else message


# === DRY PHASE 2: CONSOLIDATED FILE PATH OPERATIONS ===

def ensure_download_directory(name: str = "downloads") -> Path:
    """
    Ensure download directory exists with standard name.
    
    Consolidates the repeated pattern found in 12+ files:
        from pathlib import Path
        path = Path("downloads")
        path.mkdir(exist_ok=True)
    
    Args:
        name: Directory name (default: "downloads")
        
    Returns:
        Path object for the directory
        
    Example:
        downloads_dir = ensure_download_directory()
        youtube_dir = ensure_download_directory("youtube_downloads")
    """
    download_dir = get_project_root() / name
    return ensure_directory(download_dir)


def ensure_output_directory(name: str = "outputs") -> Path:
    """
    Ensure output directory exists with standard name.
    
    Consolidates the repeated pattern for output directories.
    
    Args:
        name: Directory name (default: "outputs")
        
    Returns:
        Path object for the directory
        
    Example:
        outputs_dir = ensure_output_directory()
        results_dir = ensure_output_directory("results")
    """
    output_dir = get_project_root() / name
    return ensure_directory(output_dir)


def ensure_logs_directory(name: str = "logs") -> Path:
    """
    Ensure logs directory exists with standard name.
    
    Consolidates the repeated pattern for log directories.
    
    Args:
        name: Directory name (default: "logs")
        
    Returns:
        Path object for the directory
        
    Example:
        logs_dir = ensure_logs_directory()
        debug_logs_dir = ensure_logs_directory("debug_logs")
    """
    logs_dir = get_project_root() / name
    return ensure_directory(logs_dir)


def get_standard_directories() -> Dict[str, Path]:
    """
    Get all standard directories used throughout the application.
    
    Consolidates directory access patterns and provides single source of truth.
    
    Returns:
        Dictionary mapping directory names to Path objects
        
    Example:
        dirs = get_standard_directories()
        csv_path = dirs['outputs'] / 'output.csv'
        log_path = dirs['logs'] / 'download.log'
    """
    return {
        'project_root': get_project_root(),
        'outputs': ensure_output_directory(),
        'logs': ensure_logs_directory(),
        'downloads': ensure_download_directory(),
        'youtube_downloads': ensure_download_directory('youtube_downloads'),
        'drive_downloads': ensure_download_directory('drive_downloads'),
        'backups': ensure_output_directory('backups'),
        'temp': ensure_download_directory('temp')
    }


def get_standard_file_paths() -> Dict[str, Path]:
    """
    Get standard file paths used throughout the application.
    
    Consolidates file path patterns and provides single source of truth.
    
    Returns:
        Dictionary mapping file names to Path objects
        
    Example:
        files = get_standard_file_paths()
        df = pd.read_csv(files['output_csv'])
    """
    dirs = get_standard_directories()
    
    return {
        'output_csv': dirs['outputs'] / 'output.csv',
        'config_yaml': dirs['project_root'] / 'config' / 'config.yaml',
        'download_log': dirs['logs'] / 'download.log',
        'error_log': dirs['logs'] / 'error.log',
        'status_json': dirs['outputs'] / 'status.json',
        'progress_json': dirs['outputs'] / 'progress.json',
        'stats_json': dirs['outputs'] / 'stats.json'
    }


def create_file_with_parents(file_path: Union[str, Path], content: str = "", 
                           encoding: str = "utf-8") -> Path:
    """
    Create file with parent directories if they don't exist.
    
    Consolidates the repeated pattern:
        from pathlib import Path
        file_path = Path(filename)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content)
    
    Args:
        file_path: Path to file to create
        content: Content to write to file (default: empty string)
        encoding: File encoding (default: utf-8)
        
    Returns:
        Path object for the created file
        
    Example:
        log_file = create_file_with_parents('logs/download.log')
        csv_file = create_file_with_parents('outputs/results.csv', 'header1,header2\n')
    """
    file_path = Path(file_path)
    ensure_parent_dir(file_path)
    
    with open(file_path, 'w', encoding=encoding) as f:
        f.write(content)
    
    return file_path


def safe_file_path(file_path: Union[str, Path], base_dir: Optional[Union[str, Path]] = None) -> Path:
    """
    Create safe file path within project boundaries.
    
    Prevents path traversal attacks and ensures files stay within project.
    
    Args:
        file_path: File path to normalize
        base_dir: Base directory to restrict to (default: project root)
        
    Returns:
        Safe Path object within project boundaries
        
    Example:
        safe_path = safe_file_path('../../../etc/passwd')  # Returns project_root/etc/passwd
        safe_path = safe_file_path('outputs/data.csv')     # Returns project_root/outputs/data.csv
    """
    if base_dir is None:
        base_dir = get_project_root()
    
    base_dir = Path(base_dir).resolve()
    file_path = Path(file_path)
    
    # If absolute path, make it relative to base_dir
    if file_path.is_absolute():
        try:
            file_path = file_path.relative_to(base_dir)
        except ValueError:
            # Path is outside base_dir, use just the filename
            file_path = file_path.name
    
    # Resolve the path within base_dir
    resolved_path = (base_dir / file_path).resolve()
    
    # Ensure the resolved path is still within base_dir
    try:
        resolved_path.relative_to(base_dir)
        return resolved_path
    except ValueError:
        # Path escaped base_dir, return safe fallback
        return base_dir / file_path.name


def format_stats(label: str, value: Any, icon: bool = True) -> str:
    """Format a statistics message (DRY)"""
    return f"{StatusIcons.STATS} {label}: {value}" if icon else f"{label}: {value}"


def format_status_line(status: str, message: str) -> str:
    """Format a status line with appropriate icon (DRY)
    
    Args:
        status: Status type ('success', 'error', 'warning', 'info', 'complete')
        message: The message to format
        
    Returns:
        Formatted status line
    """
    status_map = {
        'success': StatusIcons.SUCCESS,
        'error': StatusIcons.ERROR,
        'warning': StatusIcons.WARNING,
        'info': StatusIcons.STATS,
        'complete': StatusIcons.COMPLETE,
        'failure': StatusIcons.FAILURE,
        'celebrate': StatusIcons.CELEBRATE,
        'file': StatusIcons.FILE,
        'database': StatusIcons.DATABASE,
        'package': StatusIcons.PACKAGE,
        'rocket': StatusIcons.ROCKET
    }
    
    icon = status_map.get(status.lower(), '')
    return f"{icon} {message}" if icon else message


def format_batch_header(batch_num: int, total_batches: int, batch_size: int) -> str:
    """Format batch processing header (DRY)"""
    return f"\n{StatusIcons.PACKAGE} BATCH {batch_num}/{total_batches} ({batch_size} documents)"


def format_progress_indicator(current: int, total: int, label: str = "Processing") -> str:
    """Format progress indicator (DRY)"""
    return f"[{current}/{total}] {label}"


# Example usage
if __name__ == "__main__":
    # Test configuration loading
    config = get_config()
    
    print("Configuration loaded successfully!")
    print(f"YouTube downloads directory: {get_youtube_downloads_dir()}")
    print(f"Default timeout: {get_timeout()} seconds")
    print(f"Retry config: {get_retry_config()}")
    print(f"Parallel workers: {get_parallel_config().get('max_workers', 4)}")
    print(f"SSL verification: {is_ssl_verify_enabled()}")
    
    # Test getting nested values
    print(f"YouTube max workers: {config.get('downloads.youtube.max_workers', 4)}")
    print(f"Large file chunk size: {config.get('downloads.drive.chunk_sizes.large', 8388608)}")


# ============================================================================
# CLI ARGUMENT PARSER CONSOLIDATION (DRY Phase 2)
# ============================================================================

def create_standard_parser(description: str, **standard_args) -> 'argparse.ArgumentParser':
    """
    Create standardized ArgumentParser with common arguments (DRY consolidation).
    
    Eliminates duplicate argparse setup patterns across 20+ scripts.
    
    Args:
        description: Description for the parser
        **standard_args: Standard argument flags to include:
            - csv: bool - Add --csv argument for CSV file path
            - max_rows: bool - Add --max-rows argument  
            - directory: bool - Add --directory argument
            - debug: bool - Add --debug/--verbose arguments
            - output: bool - Add --output argument
            - dry_run: bool - Add --dry-run argument
            
    Returns:
        Configured ArgumentParser instance
    """
    import argparse
    
    parser = argparse.ArgumentParser(description=description)
    
    # Standard CSV file argument
    if standard_args.get('csv', False):
        default_csv = get_config().get('paths.output_csv', 'outputs/output.csv')
        parser.add_argument('--csv', default=default_csv,
                          help=f'Path to CSV file (default: {default_csv})')
    
    # Standard max rows argument
    if standard_args.get('max_rows', False):
        parser.add_argument('--max-rows', type=int, default=None,
                          help='Maximum number of rows to process')
    
    # Standard directory argument
    if standard_args.get('directory', False):
        parser.add_argument('--directory', default='youtube_downloads',
                          help='Directory path (default: youtube_downloads)')
    
    # Standard debug/verbose arguments
    if standard_args.get('debug', False):
        parser.add_argument('--debug', action='store_true',
                          help='Enable debug output')
        parser.add_argument('--verbose', '-v', action='store_true',
                          help='Enable verbose output')
    
    # Standard output argument
    if standard_args.get('output', False):
        parser.add_argument('--output', '-o', type=str,
                          help='Output file path')
    
    # Standard dry run argument
    if standard_args.get('dry_run', False):
        parser.add_argument('--dry-run', action='store_true',
                          help='Show what would be done without executing')
    
    return parser


def standard_script_main(main_func, description: str, **parser_args):
    """
    Standard script entry point wrapper with error handling (DRY consolidation).
    
    Eliminates duplicate main() patterns across 78+ script files.
    
    Args:
        main_func: Function to call with parsed args
        description: Description for argument parser
        **parser_args: Arguments to pass to create_standard_parser()
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        parser = create_standard_parser(description, **parser_args)
        args = parser.parse_args()
        result = main_func(args)
        return result if isinstance(result, int) else 0
    except KeyboardInterrupt:
        print("\n⚠️  Script interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

# ============================================================================
# CONSOLIDATED CONSTANTS AND MAGIC NUMBERS (DRY Phase 2)
# ============================================================================

class Constants:
    """Centralized constants to eliminate magic numbers across the codebase (DRY consolidation)."""
    
    # File size constants (bytes)
    BYTES_PER_KB = 1024
    BYTES_PER_MB = 1024 * 1024
    BYTES_PER_GB = 1024 * 1024 * 1024
    
    # Progress display constants
    DEFAULT_PROGRESS_BAR_WIDTH = 40
    PROGRESS_UPDATE_INTERVAL = 1.0  # seconds
    
    # Default delays (seconds)
    DEFAULT_DELAY = 2.0
    SHORT_DELAY = 1.0
    LONG_DELAY = 5.0

class CoreDefaults:
    """
    Single source of truth for all core operational defaults (DRY CONSOLIDATION STEP 3).
    
    ELIMINATES HARDCODED VALUES FROM:
    - download_drive.py: timeout=30
    - download_youtube.py: timeout=300.0
    - Multiple files: chunk_size=1024*1024
    
    BUSINESS IMPACT: Prevents configuration drift and deployment inconsistencies
    """
    
    # Download settings (consolidates scattered values)
    DOWNLOAD_TIMEOUT = 300.0  # Standard download timeout
    DOWNLOAD_RETRY_ATTEMPTS = 3  
    DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1MB
    
    # File locking (consolidates scattered timeouts)
    FILE_LOCK_TIMEOUT = 30.0  # Quick operations
    DOWNLOAD_LOCK_TIMEOUT = 300.0  # Long downloads
    
    # Progress reporting
    PROGRESS_UPDATE_INTERVAL = 5.0  # seconds
    LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100MB
    
    # Error handling
    MAX_ERROR_MESSAGE_LENGTH = 500
    ERROR_RETRY_DELAY = 2.0
    
    # HTTP settings
    HTTP_REQUEST_TIMEOUT = 60.0
    HTTP_CONNECT_TIMEOUT = 30.0
    
    # Directory defaults
    DEFAULT_DOWNLOADS_DIR = 'downloads'
    DEFAULT_DRIVE_DOWNLOADS_DIR = 'drive_downloads'
    DEFAULT_YOUTUBE_DOWNLOADS_DIR = 'youtube_downloads'


def bytes_to_mb(bytes_value: int) -> float:
    """Convert bytes to megabytes (DRY utility)."""
    return bytes_value / Constants.BYTES_PER_MB


def bytes_to_gb(bytes_value: int) -> float:
    """Convert bytes to gigabytes (DRY utility)."""
    return bytes_value / Constants.BYTES_PER_GB


# ============================================================================
# CONSOLIDATED CONFIGURATION VALUES (DRY Phase 2)
# ============================================================================

# AWS S3 Configuration
def get_s3_config() -> Dict[str, Any]:
    """Get S3 configuration with fallback defaults."""
    return {
        'bucket_name': get_config().get('aws.s3.bucket_name', 'typing-clients-storage-2025'),
        'region': get_config().get('aws.s3.region', 'us-east-1'),
        'create_public_urls': get_config().get('aws.s3.create_public_urls', True),
        'organize_by_person': get_config().get('aws.s3.organize_by_person', True),
        'add_metadata': get_config().get('aws.s3.add_metadata', True)
    }


def get_s3_bucket_name() -> str:
    """Get S3 bucket name."""
    return get_config().get('aws.s3.bucket_name', 'typing-clients-storage-2025')


def get_s3_region() -> str:
    """Get S3 region."""
    return get_config().get('aws.s3.region', 'us-east-1')


# Download Configuration
def get_download_config() -> Dict[str, Any]:
    """
    Get download configuration with all consolidated settings (DRY CONSOLIDATION STEP 3).
    
    ELIMINATES: Hardcoded defaults scattered across download modules
    STANDARDIZES: All download configuration access through single function
    """
    config = get_config()
    return {
        'output_dir': config.get('downloads.output_dir', CoreDefaults.DEFAULT_DOWNLOADS_DIR),
        'timeout': config.get('downloads.timeout', CoreDefaults.DOWNLOAD_TIMEOUT),
        'max_retries': config.get('downloads.max_retries', CoreDefaults.DOWNLOAD_RETRY_ATTEMPTS),
        'retry_delay': config.get('downloads.retry_delay', CoreDefaults.ERROR_RETRY_DELAY),
        'chunk_size': config.get('downloads.chunk_size', CoreDefaults.DOWNLOAD_CHUNK_SIZE),
        'file_lock_timeout': config.get('downloads.file_lock_timeout', CoreDefaults.FILE_LOCK_TIMEOUT),
        'download_lock_timeout': config.get('downloads.download_lock_timeout', CoreDefaults.DOWNLOAD_LOCK_TIMEOUT),
        'progress_interval': config.get('downloads.progress_interval', CoreDefaults.PROGRESS_UPDATE_INTERVAL),
        'create_metadata': config.get('downloads.create_metadata', True),
        'show_progress': config.get('downloads.show_progress', True),
        'youtube': get_youtube_download_config(),
        'drive': get_drive_download_config()
    }


def get_youtube_download_config() -> Dict[str, Any]:
    """Get YouTube-specific download configuration."""
    return {
        'default_format': get_config().get('downloads.youtube.default_format', 'mp3'),
        'default_quality': get_config().get('downloads.youtube.default_quality', '128K'),
        'audio_format': get_config().get('downloads.youtube.audio_format', 'mp3'),
        'video_format': get_config().get('downloads.youtube.video_format', 'mp4'),
        'extract_audio': get_config().get('downloads.youtube.extract_audio', True),
        'no_playlist': get_config().get('downloads.youtube.no_playlist', True),
        'quiet': get_config().get('downloads.youtube.quiet', True),
        'no_warnings': get_config().get('downloads.youtube.no_warnings', True),
        'timeout': get_config().get('downloads.youtube.timeout', 60)
    }


def get_drive_download_config() -> Dict[str, Any]:
    """Get Google Drive download configuration."""
    return {
        'use_gdown': get_config().get('downloads.drive.use_gdown', True),
        'fuzzy_matching': get_config().get('downloads.drive.fuzzy_matching', True),
        'save_info_only': get_config().get('downloads.drive.save_info_only', False),
        'timeout': get_config().get('downloads.drive.timeout', 120),
        'chunk_size': get_config().get('downloads.drive.chunk_size', 1024*1024)
    }


# CSV Configuration
def get_csv_config() -> Dict[str, Any]:
    """Get CSV file configuration."""
    return {
        'input_file': get_config().get('csv.input_file', 'outputs/output.csv'),
        'output_file': get_config().get('csv.output_file', 'outputs/output.csv'),
        'backup_enabled': get_config().get('csv.backup_enabled', True),
        'backup_dir': get_config().get('csv.backup_dir', 'backups'),
        'columns': get_csv_columns_config()
    }


def get_csv_columns_config() -> Dict[str, Any]:
    """Get CSV column configuration."""
    return {
        'youtube_column': get_config().get('csv.columns.youtube', 'youtube_playlist'),
        'drive_column': get_config().get('csv.columns.drive', 'google_drive'),
        's3_youtube_column': get_config().get('csv.columns.s3_youtube', 's3_youtube_urls'),
        's3_drive_column': get_config().get('csv.columns.s3_drive', 's3_drive_urls'),
        's3_all_column': get_config().get('csv.columns.s3_all', 's3_all_files'),
        'delimiter': get_config().get('csv.columns.delimiter', '|')
    }


# Retry Strategy Configuration
def get_retry_strategies() -> Dict[str, Any]:
    """Get retry strategies configuration."""
    return {
        'no_retry': {
            'max_attempts': 1,
            'delay': 0,
            'backoff_factor': 1.0
        },
        'basic_retry': {
            'max_attempts': get_config().get('retry.basic.max_attempts', 3),
            'delay': get_config().get('retry.basic.delay', 2),
            'backoff_factor': get_config().get('retry.basic.backoff_factor', 1.0)
        },
        'aggressive_retry': {
            'max_attempts': get_config().get('retry.aggressive.max_attempts', 5),
            'delay': get_config().get('retry.aggressive.delay', 5),
            'backoff_factor': get_config().get('retry.aggressive.backoff_factor', 2.0)
        },
        'no_timeout': {
            'max_attempts': get_config().get('retry.no_timeout.max_attempts', 3),
            'delay': get_config().get('retry.no_timeout.delay', 2),
            'timeout': None
        }
    }


# Error Handling Configuration
def get_error_handling_config() -> Dict[str, Any]:
    """Get error handling configuration."""
    return {
        'max_error_length': get_config().get('error_handling.max_error_length', 100),
        'sanitize_errors': get_config().get('error_handling.sanitize_errors', True),
        'log_full_errors': get_config().get('error_handling.log_full_errors', True),
        'continue_on_error': get_config().get('error_handling.continue_on_error', True)
    }


# Progress Reporting Configuration
def get_progress_config() -> Dict[str, Any]:
    """Get progress reporting configuration."""
    return {
        'show_progress_bars': get_config().get('progress.show_bars', True),
        'show_file_sizes': get_config().get('progress.show_file_sizes', True),
        'show_download_speeds': get_config().get('progress.show_speeds', True),
        'update_interval': get_config().get('progress.update_interval', 1.0),
        'bar_width': get_config().get('progress.bar_width', 40)
    }


# Metadata Configuration
def get_metadata_config() -> Dict[str, Any]:
    """Get metadata configuration."""
    return {
        'create_metadata': get_config().get('metadata.create_metadata', True),
        'include_timestamps': get_config().get('metadata.include_timestamps', True),
        'include_file_sizes': get_config().get('metadata.include_file_sizes', True),
        'include_source_urls': get_config().get('metadata.include_source_urls', True),
        'include_download_config': get_config().get('metadata.include_download_config', True)
    }


# Directory Structure Configuration
def get_directory_config() -> Dict[str, Any]:
    """Get directory structure configuration."""
    return {
        'downloads_dir': get_config().get('directories.downloads', 'downloads'),
        'outputs_dir': get_config().get('directories.outputs', 'outputs'),
        'logs_dir': get_config().get('directories.logs', 'logs'),
        'backups_dir': get_config().get('directories.backups', 'backups'),
        'temp_dir': get_config().get('directories.temp', '/tmp'),
        'organize_by_person': get_config().get('directories.organize_by_person', True),
        'sanitize_names': get_config().get('directories.sanitize_names', True)
    }


# File Pattern Configuration
def get_file_patterns() -> Dict[str, Any]:
    """Get file patterns for different operations."""
    return {
        'youtube_video_pattern': get_config().get('patterns.youtube_video', r'youtube_([a-zA-Z0-9_-]{11})\.'),
        'youtube_playlist_pattern': get_config().get('patterns.youtube_playlist', r'playlist_([a-zA-Z0-9_-]+)_info\.json'),
        'drive_file_pattern': get_config().get('patterns.drive_file', r'drive_file_([a-zA-Z0-9_-]+)'),
        'drive_folder_pattern': get_config().get('patterns.drive_folder', r'drive_folder_([a-zA-Z0-9_-]+)_info\.json'),
        's3_key_pattern': get_config().get('patterns.s3_key', r'{row_id}/{person_name}/{filename}'),
        'metadata_suffix': get_config().get('patterns.metadata_suffix', '_metadata.json')
    }


# Quality and Format Options
def get_quality_options() -> Dict[str, Any]:
    """Get quality and format options."""
    return {
        'youtube_audio_qualities': get_config().get('quality.youtube.audio', ['128K', '192K', '256K', '320K']),
        'youtube_video_qualities': get_config().get('quality.youtube.video', ['720p', '1080p', '480p', '360p']),
        'audio_formats': get_config().get('quality.audio_formats', ['mp3', 'm4a', 'wav']),
        'video_formats': get_config().get('quality.video_formats', ['mp4', 'webm', 'mkv']),
        'default_audio_quality': get_config().get('quality.default_audio', '128K'),
        'default_video_quality': get_config().get('quality.default_video', '720p')
    }


# Utility Functions for Configuration Access
def get_default_downloads_dir() -> str:
    """Get default downloads directory."""
    return get_config().get('directories.downloads', 'downloads')


def get_default_csv_file() -> str:
    """Get default CSV file path."""
    return get_config().get('csv.input_file', 'outputs/output.csv')


def get_default_timeout() -> int:
    """Get default timeout in seconds."""
    return get_config().get('downloads.timeout', 120)


def get_youtube_audio_format() -> str:
    """Get YouTube audio format."""
    return get_config().get('downloads.youtube.audio_format', 'mp3')


def get_youtube_audio_quality() -> str:
    """Get YouTube audio quality."""
    return get_config().get('downloads.youtube.default_quality', '128K')


def get_drive_chunk_size() -> int:
    """Get Drive download chunk size."""
    return get_config().get('downloads.drive.chunk_size', 1024*1024)


def should_create_metadata() -> bool:
    """Check if metadata should be created."""
    return get_config().get('metadata.create_metadata', True)


def should_show_progress() -> bool:
    """Check if progress should be shown."""
    return get_config().get('progress.show_bars', True)


def should_organize_by_person() -> bool:
    """Check if files should be organized by person."""
    return get_config().get('directories.organize_by_person', True)


def get_csv_delimiter() -> str:
    """Get CSV column delimiter for multiple values."""
    return get_config().get('csv.columns.delimiter', '|')


# Configuration Validation
def validate_config() -> List[str]:
    """Validate configuration and return list of issues."""
    issues = []
    config = get_config()
    
    # Check required directories
    required_dirs = ['downloads', 'outputs', 'logs']
    for dir_name in required_dirs:
        if not config.get(f'directories.{dir_name}'):
            issues.append(f"Missing required directory configuration: directories.{dir_name}")
    
    # Check S3 configuration
    if not get_s3_bucket_name():
        issues.append("Missing S3 bucket name configuration")
    
    # Check timeout values
    timeout = get_default_timeout()
    if timeout <= 0:
        issues.append("Invalid timeout value: must be positive")
    
    # Check retry configuration
    retry_config = get_retry_strategies()
    for strategy_name, strategy_config in retry_config.items():
        if strategy_config.get('max_attempts', 0) <= 0:
            issues.append(f"Invalid retry configuration for {strategy_name}: max_attempts must be positive")
    
    return issues


# Configuration Defaults Creation
def create_default_config_dict() -> Dict[str, Any]:
    """Create default configuration dictionary for new installations."""
    return {
        'aws': {
            's3': {
                'bucket_name': 'typing-clients-storage-2025',
                'region': 'us-east-1',
                'create_public_urls': True,
                'organize_by_person': True,
                'add_metadata': True
            }
        },
        'downloads': {
            'output_dir': 'downloads',
            'timeout': 120,
            'max_retries': 3,
            'retry_delay': 2,
            'create_metadata': True,
            'show_progress': True,
            'youtube': {
                'default_format': 'mp3',
                'default_quality': '128K',
                'audio_format': 'mp3',
                'video_format': 'mp4',
                'extract_audio': True,
                'no_playlist': True,
                'quiet': True,
                'no_warnings': True,
                'timeout': 60
            },
            'drive': {
                'use_gdown': True,
                'fuzzy_matching': True,
                'save_info_only': False,
                'timeout': 120,
                'chunk_size': 1048576
            }
        },
        'csv': {
            'input_file': 'outputs/output.csv',
            'output_file': 'outputs/output.csv',
            'backup_enabled': True,
            'backup_dir': 'backups',
            'columns': {
                'youtube': 'youtube_playlist',
                'drive': 'google_drive',
                's3_youtube': 's3_youtube_urls',
                's3_drive': 's3_drive_urls',
                's3_all': 's3_all_files',
                'delimiter': '|'
            }
        },
        'directories': {
            'downloads': 'downloads',
            'outputs': 'outputs',
            'logs': 'logs',
            'backups': 'backups',
            'temp': '/tmp',
            'organize_by_person': True,
            'sanitize_names': True
        },
        'retry': {
            'basic': {
                'max_attempts': 3,
                'delay': 2,
                'backoff_factor': 1.0
            },
            'aggressive': {
                'max_attempts': 5,
                'delay': 5,
                'backoff_factor': 2.0
            },
            'no_timeout': {
                'max_attempts': 3,
                'delay': 2,
                'timeout': None
            }
        },
        'error_handling': {
            'max_error_length': 100,
            'sanitize_errors': True,
            'log_full_errors': True,
            'continue_on_error': True
        },
        'progress': {
            'show_bars': True,
            'show_file_sizes': True,
            'show_speeds': True,
            'update_interval': 1.0,
            'bar_width': 40
        },
        'metadata': {
            'create_metadata': True,
            'include_timestamps': True,
            'include_file_sizes': True,
            'include_source_urls': True,
            'include_download_config': True
        },
        'patterns': {
            'youtube_video': r'youtube_([a-zA-Z0-9_-]{11})\.',
            'youtube_playlist': r'playlist_([a-zA-Z0-9_-]+)_info\.json',
            'drive_file': r'drive_file_([a-zA-Z0-9_-]+)',
            'drive_folder': r'drive_folder_([a-zA-Z0-9_-]+)_info\.json',
            's3_key': '{row_id}/{person_name}/{filename}',
            'metadata_suffix': '_metadata.json'
        },
        'quality': {
            'youtube': {
                'audio': ['128K', '192K', '256K', '320K'],
                'video': ['720p', '1080p', '480p', '360p']
            },
            'audio_formats': ['mp3', 'm4a', 'wav'],
            'video_formats': ['mp4', 'webm', 'mkv'],
            'default_audio': '128K',
            'default_video': '720p'
        }
    }


def save_default_config(config_path: Optional[str] = None) -> bool:
    """Save default configuration to file."""
    if not config_path:
        config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    
    config_path = Path(config_path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        import yaml
        with open(config_path, 'w') as f:
            yaml.dump(create_default_config_dict(), f, default_flow_style=False, indent=2)
        return True
    except Exception as e:
        logging.error(f"Failed to save default configuration: {e}")
        return False




# ============================================================================
# DRY CONSOLIDATION: COMMON IMPORT PATTERNS
# ============================================================================

def setup_utils_imports(**import_map):
    """
    Consolidates repeated try/except ImportError patterns found across utils modules.
    
    Args:
        **import_map: Dictionary mapping local names to module.function paths
        
    Returns:
        Dictionary of imported functions/classes mapped to local names
        
    Example:
        # Replace this repeated pattern:
        try:
            from logging_config import get_logger
            from validation import validate_youtube_url
        except ImportError:
            from .logging_config import get_logger
            from .validation import validate_youtube_url
            
        # With this:
        imports = setup_utils_imports(
            logger="logging_config.get_logger",
            validate_url="validation.validate_youtube_url"
        )
        logger = imports["logger"](__name__)
        validate_url = imports["validate_url"]
    """
    imports = {}
    
    for local_name, module_path in import_map.items():
        if "." in module_path:
            module_name, function_name = module_path.rsplit(".", 1)
            try:
                # Try absolute import first
                module = importlib.import_module(module_name)
                imports[local_name] = getattr(module, function_name)
            except ImportError:
                try:
                    # Try relative import
                    module = importlib.import_module(f".{module_name}", package="utils")
                    imports[local_name] = getattr(module, function_name)
                except ImportError as e:
                    logging.warning(f"Failed to import {module_path}: {e}")
                    imports[local_name] = None
        else:
            # Import entire module
            try:
                imports[local_name] = importlib.import_module(module_path)
            except ImportError:
                try:
                    imports[local_name] = importlib.import_module(f".{module_path}", package="utils")
                except ImportError as e:
                    logging.warning(f"Failed to import {module_path}: {e}")
                    imports[local_name] = None
    
    return imports


def get_common_utils():
    """Get commonly used utility functions across the codebase."""
    return setup_utils_imports(
        get_logger="logging_config.get_logger",
        # get_config - use direct import to avoid circular reference 
        handle_file_operations="error_handling.handle_file_operations",
        handle_network_operations="error_handling.handle_network_operations",
        sanitize_error_message="sanitization.sanitize_error_message",
        clean_url="patterns.clean_url",
        retry_with_backoff="retry_utils.retry_with_backoff",
        validate_url="validation.validate_url"
    )


# ============================================================================
# PROGRESS TRACKING CONSOLIDATION (DRY CONSOLIDATION - Step 4)
# ============================================================================

class ProgressTracker:
    """
    Centralized progress tracking for state management (DRY CONSOLIDATION - Step 4).
    
    Consolidates progress tracking patterns from:
    - simple_workflow.py: Manual dictionary updates and JSON file handling
    - core/process_pending_metadata_downloads.py: Custom progress loading/saving
    - utils/streaming_integration.py: StreamingProgress class patterns
    
    Builds on existing load_json_state/save_json_state utilities.
    """
    
    def __init__(self, progress_file: str, default_structure: Optional[Dict] = None):
        """
        Initialize progress tracker.
        
        Args:
            progress_file: Path to progress JSON file
            default_structure: Default progress structure if file doesn't exist
        """
        self.progress_file = progress_file
        self.default_structure = default_structure or {
            "completed": [],
            "failed": [],
            "total_processed": 0,
            "last_batch": 0,
            "start_time": None,
            "last_update": None
        }
        self._data = self.load()
    
    def load(self) -> Dict:
        """Load progress from file."""
        return load_json_state(self.progress_file, self.default_structure.copy())
    
    def save(self) -> None:
        """Save progress to file."""
        from datetime import datetime
        self._data["last_update"] = datetime.now().isoformat()
        save_json_state(self.progress_file, self._data)
    
    def mark_completed(self, item: str) -> None:
        """Mark item as completed."""
        if item not in self._data["completed"]:
            self._data["completed"].append(item)
            self._data["total_processed"] += 1
            # Remove from failed if it was there
            if item in self._data["failed"]:
                self._remove_failed(item)
    
    def mark_failed(self, item: str, error: Optional[str] = None) -> None:
        """Mark item as failed with optional error message."""
        if isinstance(self._data["failed"], list):
            if item not in self._data["failed"]:
                self._data["failed"].append(item)
        else:
            # Dictionary format for detailed error tracking
            from datetime import datetime
            self._data["failed"][item] = {
                "error": error or "Unknown error",
                "timestamp": datetime.now().isoformat()
            }
    
    def _remove_failed(self, item: str) -> None:
        """Remove item from failed list/dict."""
        if isinstance(self._data["failed"], list):
            if item in self._data["failed"]:
                self._data["failed"].remove(item)
        else:
            self._data["failed"].pop(item, None)
    
    def is_completed(self, item: str) -> bool:
        """Check if item is completed."""
        return item in self._data["completed"]
    
    def is_failed(self, item: str) -> bool:
        """Check if item is failed."""
        if isinstance(self._data["failed"], list):
            return item in self._data["failed"]
        else:
            return item in self._data["failed"]
    
    def get_completed_count(self) -> int:
        """Get number of completed items."""
        return len(self._data["completed"])
    
    def get_failed_count(self) -> int:
        """Get number of failed items."""
        if isinstance(self._data["failed"], list):
            return len(self._data["failed"])
        else:
            return len(self._data["failed"])
    
    def get_total_processed(self) -> int:
        """Get total number of processed items."""
        return self._data.get("total_processed", 0)
    
    def set_batch_position(self, batch: int) -> None:
        """Set current batch position for resumable processing."""
        self._data["last_batch"] = batch
    
    def get_batch_position(self) -> int:
        """Get current batch position."""
        return self._data.get("last_batch", 0)
    
    def reset(self) -> None:
        """Reset progress to initial state."""
        self._data = self.default_structure.copy()
        from datetime import datetime
        self._data["start_time"] = datetime.now().isoformat()
    
    def get_progress_summary(self) -> Dict:
        """Get summary of current progress."""
        total = self.get_completed_count() + self.get_failed_count()
        return {
            "completed": self.get_completed_count(),
            "failed": self.get_failed_count(),
            "total_processed": total,
            "success_rate": (self.get_completed_count() / total * 100) if total > 0 else 0,
            "last_batch": self.get_batch_position(),
            "start_time": self._data.get("start_time"),
            "last_update": self._data.get("last_update")
        }
    
    def get_failed_items(self) -> Union[List[str], Dict[str, Dict]]:
        """Get list or dict of failed items."""
        return self._data["failed"]
    
    def get_completed_items(self) -> List[str]:
        """Get list of completed items."""
        return self._data["completed"]
    
    @property
    def data(self) -> Dict:
        """Access to raw progress data."""
        return self._data


# ============================================================================
# WORKFLOW ORCHESTRATION CONSOLIDATION (DRY ITERATION 5 - Step 1)
# ============================================================================

class WorkflowOrchestrator:
    """
    Unified workflow orchestration system (DRY CONSOLIDATION ITERATION 5 - Step 1).
    
    Consolidates the step1-step6 workflow pattern from simple_workflow.py and similar
    sequential processing patterns found across the codebase.
    
    ELIMINATES DUPLICATION OF:
    - Sequential step execution with data flow between steps
    - Progress tracking and error handling across workflow steps
    - Different processing modes (basic, text, full)
    - State management and resume capabilities
    - Result aggregation and final output generation
    
    BUSINESS IMPACT: Prevents workflow inconsistencies and enables reusable workflows
    """
    
    def __init__(self, workflow_name: str, config: Optional[Config] = None,
                 progress_file: Optional[str] = None):
        """
        Initialize workflow orchestrator.
        
        Args:
            workflow_name: Name of the workflow for logging and progress tracking
            config: Configuration instance (uses default if None)
            progress_file: Path to progress file (auto-generated if None)
        """
        self.workflow_name = workflow_name
        self.config = config or get_config()
        
        # Setup progress tracking
        if not progress_file:
            progress_file = f"{workflow_name}_progress.json"
        self.progress = ProgressTracker(progress_file)
        
        # Setup logging
        from .logging_config import get_logger
        self.logger = get_logger(f"workflow.{workflow_name}")
        
        # Workflow state
        self.steps = []
        self.results = {}
        self.current_step = 0
        self.processing_mode = 'full'
        self.error_handling = 'continue'  # 'continue', 'stop', 'skip'
        
        # Performance tracking
        from datetime import datetime
        self.start_time = None
        self.step_times = {}
    
    def add_step(self, step_name: str, step_function: callable, 
                 dependencies: Optional[List[str]] = None,
                 required_for_modes: Optional[List[str]] = None,
                 error_handling: Optional[str] = None) -> 'WorkflowOrchestrator':
        """
        Add a step to the workflow.
        
        Args:
            step_name: Unique name for the step
            step_function: Function to execute for this step
            dependencies: List of step names that must complete before this step
            required_for_modes: List of processing modes that require this step
            error_handling: Override error handling for this step
            
        Returns:
            Self for method chaining
        """
        step_config = {
            'name': step_name,
            'function': step_function,
            'dependencies': dependencies or [],
            'required_for_modes': required_for_modes or ['full'],
            'error_handling': error_handling or self.error_handling,
            'completed': False,
            'result': None,
            'error': None,
            'execution_time': None
        }
        
        self.steps.append(step_config)
        return self
    
    def set_processing_mode(self, mode: str) -> 'WorkflowOrchestrator':
        """
        Set the processing mode for the workflow.
        
        Args:
            mode: Processing mode ('basic', 'text', 'full')
            
        Returns:
            Self for method chaining
        """
        self.processing_mode = mode
        return self
    
    def set_error_handling(self, strategy: str) -> 'WorkflowOrchestrator':
        """
        Set error handling strategy.
        
        Args:
            strategy: Error handling strategy ('continue', 'stop', 'skip')
            
        Returns:
            Self for method chaining
        """
        self.error_handling = strategy
        return self
    
    def execute_workflow(self, initial_data: Optional[Dict] = None,
                        resume: bool = False, **kwargs) -> Dict:
        """
        Execute the complete workflow.
        
        Args:
            initial_data: Initial data to pass to first step
            resume: Whether to resume from previous progress
            **kwargs: Additional arguments to pass to all steps
            
        Returns:
            Dictionary containing all step results and workflow summary
        """
        from datetime import datetime
        
        self.start_time = datetime.now()
        self.logger.info(f"🚀 Starting workflow: {self.workflow_name} (mode: {self.processing_mode})")
        
        # Filter steps based on processing mode
        active_steps = [
            step for step in self.steps 
            if self.processing_mode in step['required_for_modes']
        ]
        
        self.logger.info(f"📋 Executing {len(active_steps)} steps for {self.processing_mode} mode")
        
        # Initialize results with initial data
        self.results = initial_data.copy() if initial_data else {}
        workflow_data = self.results.copy()
        
        # Execute steps in sequence
        for step_index, step in enumerate(active_steps):
            step_name = step['name']
            
            # Check if we should skip this step based on resume logic
            if resume and self.progress.is_completed(step_name):
                self.logger.info(f"⏭️  Skipping completed step: {step_name}")
                continue
            
            # Check dependencies
            if not self._check_dependencies(step, active_steps):
                error_msg = f"Dependencies not met for step: {step_name}"
                self._handle_step_error(step, error_msg)
                if step['error_handling'] == 'stop':
                    break
                continue
            
            # Execute the step
            self.current_step = step_index
            step_result = self._execute_step(step, workflow_data, **kwargs)
            
            # Handle step result
            if step_result is not None:
                step['result'] = step_result
                step['completed'] = True
                
                # Update workflow data with step result
                if isinstance(step_result, dict):
                    workflow_data.update(step_result)
                else:
                    workflow_data[f"{step_name}_result"] = step_result
                
                # Mark progress
                self.progress.mark_completed(step_name)
                self.progress.save()
                
                self.logger.info(f"✅ Step completed: {step_name}")
            else:
                # Step failed or returned None
                if step['error_handling'] == 'stop':
                    self.logger.error(f"❌ Workflow stopped due to step failure: {step_name}")
                    break
                elif step['error_handling'] == 'skip':
                    self.logger.warning(f"⚠️  Skipping failed step: {step_name}")
                    continue
        
        # Generate final results
        workflow_results = self._generate_workflow_results(workflow_data)
        
        elapsed_time = (datetime.now() - self.start_time).total_seconds()
        self.logger.info(f"🎉 Workflow completed: {self.workflow_name} ({elapsed_time:.1f}s)")
        
        return workflow_results
    
    def _execute_step(self, step: Dict, workflow_data: Dict, **kwargs) -> Any:
        """Execute a single workflow step with error handling and timing."""
        from datetime import datetime
        
        step_name = step['name']
        step_function = step['function']
        
        self.logger.info(f"▶️  Executing step: {step_name}")
        
        step_start = datetime.now()
        
        try:
            # Call step function with current workflow data
            step_result = step_function(workflow_data, **kwargs)
            
            step_time = (datetime.now() - step_start).total_seconds()
            step['execution_time'] = step_time
            self.step_times[step_name] = step_time
            
            self.logger.info(f"   ⏱️  Step duration: {step_time:.2f}s")
            
            return step_result
            
        except Exception as e:
            step_time = (datetime.now() - step_start).total_seconds()
            step['execution_time'] = step_time
            
            error_msg = f"Step {step_name} failed: {str(e)}"
            self._handle_step_error(step, error_msg)
            
            return None
    
    def _handle_step_error(self, step: Dict, error_msg: str) -> None:
        """Handle step execution errors."""
        step['error'] = error_msg
        step['completed'] = False
        
        self.progress.mark_failed(step['name'], error_msg)
        self.progress.save()
        
        # Log error with appropriate level based on error handling strategy
        if step['error_handling'] == 'stop':
            self.logger.error(f"❌ {error_msg}")
        elif step['error_handling'] == 'skip':
            self.logger.warning(f"⚠️  {error_msg} (skipping)")
        else:  # continue
            self.logger.warning(f"⚠️  {error_msg} (continuing)")
    
    def _check_dependencies(self, step: Dict, active_steps: List[Dict]) -> bool:
        """Check if step dependencies are satisfied."""
        for dep_name in step['dependencies']:
            # Find dependency step
            dep_step = next((s for s in active_steps if s['name'] == dep_name), None)
            if not dep_step or not dep_step['completed']:
                return False
        return True
    
    def _generate_workflow_results(self, workflow_data: Dict) -> Dict:
        """Generate final workflow results summary."""
        from datetime import datetime
        
        # Calculate summary statistics
        completed_steps = [s for s in self.steps if s['completed']]
        failed_steps = [s for s in self.steps if s['error']]
        total_time = (datetime.now() - self.start_time).total_seconds()
        
        # Build results dictionary
        results = {
            'workflow_name': self.workflow_name,
            'processing_mode': self.processing_mode,
            'status': 'completed' if len(failed_steps) == 0 else 'partial',
            'summary': {
                'total_steps': len(self.steps),
                'completed_steps': len(completed_steps),
                'failed_steps': len(failed_steps),
                'execution_time': total_time,
                'start_time': self.start_time.isoformat(),
                'end_time': datetime.now().isoformat()
            },
            'step_results': {
                step['name']: {
                    'result': step['result'],
                    'completed': step['completed'],
                    'error': step['error'],
                    'execution_time': step['execution_time']
                }
                for step in self.steps
            },
            'step_times': self.step_times,
            'workflow_data': workflow_data,
            'progress_summary': self.progress.get_progress_summary()
        }
        
        return results
    
    def get_step_result(self, step_name: str) -> Any:
        """Get result from a specific step."""
        step = next((s for s in self.steps if s['name'] == step_name), None)
        return step['result'] if step else None
    
    def get_workflow_summary(self) -> Dict:
        """Get current workflow execution summary."""
        completed = len([s for s in self.steps if s['completed']])
        failed = len([s for s in self.steps if s['error']])
        
        return {
            'workflow_name': self.workflow_name,
            'total_steps': len(self.steps),
            'completed': completed,
            'failed': failed,
            'current_step': self.current_step,
            'processing_mode': self.processing_mode,
            'total_execution_time': sum(self.step_times.values())
        }
    
    def reset_workflow(self) -> None:
        """Reset workflow state for re-execution."""
        for step in self.steps:
            step['completed'] = False
            step['result'] = None
            step['error'] = None
            step['execution_time'] = None
        
        self.results = {}
        self.current_step = 0
        self.step_times = {}
        self.start_time = None
        self.progress.reset()


def create_standard_workflow(workflow_name: str, processing_mode: str = 'full') -> WorkflowOrchestrator:
    """
    Create a workflow orchestrator with standard configuration.
    
    Convenient factory function for common workflow patterns.
    
    Args:
        workflow_name: Name of the workflow
        processing_mode: Processing mode ('basic', 'text', 'full')
        
    Returns:
        Configured WorkflowOrchestrator instance
        
    Example:
        workflow = create_standard_workflow("data_extraction", "full")
        workflow.add_step("download", download_function)
        workflow.add_step("process", process_function, dependencies=["download"])
        results = workflow.execute_workflow({"source": "google_sheets"})
    """
    return (WorkflowOrchestrator(workflow_name)
            .set_processing_mode(processing_mode)
            .set_error_handling('continue'))


def create_simple_six_step_workflow() -> WorkflowOrchestrator:
    """
    Create the standard 6-step workflow from simple_workflow.py.
    
    This consolidates the specific step1-step6 pattern and makes it reusable.
    
    Returns:
        WorkflowOrchestrator configured with the 6 standard steps
        
    Example:
        workflow = create_simple_six_step_workflow()
        results = workflow.execute_workflow({"test_limit": 10, "basic_mode": True})
    """
    from datetime import datetime
    
    def step1_wrapper(workflow_data, **kwargs):
        """Wrapper for step1_download_sheet to integrate with orchestrator"""
        # Dynamic import to avoid circular dependencies
        from simple_workflow import step1_download_sheet
        html_content = step1_download_sheet()
        return {"html_content": html_content}
    
    def step2_wrapper(workflow_data, **kwargs):
        """Wrapper for step2_extract_people_and_docs"""
        from simple_workflow import step2_extract_people_and_docs
        html_content = workflow_data.get("html_content", "")
        all_people, people_with_docs = step2_extract_people_and_docs(html_content)
        return {
            "all_people": all_people,
            "people_with_docs": people_with_docs
        }
    
    def step3_wrapper(workflow_data, **kwargs):
        """Wrapper for document scraping step"""
        from simple_workflow import step3_scrape_doc_contents
        # This step processes individual documents, handled in step5
        return {"document_scraping_ready": True}
    
    def step4_wrapper(workflow_data, **kwargs):
        """Wrapper for link extraction step"""
        from simple_workflow import step4_extract_links
        # This step processes individual documents, handled in step5
        return {"link_extraction_ready": True}
    
    def step5_wrapper(workflow_data, **kwargs):
        """Wrapper for data processing step - main processing logic"""
        from simple_workflow import (
            step3_scrape_doc_contents, step4_extract_links, 
            step5_process_extracted_data, CSVManager
        )
        
        all_people = workflow_data.get("all_people", [])
        people_with_docs = workflow_data.get("people_with_docs", [])
        basic_mode = kwargs.get("basic_mode", False)
        text_mode = kwargs.get("text_mode", False)
        test_limit = kwargs.get("test_limit")
        
        processed_records = []
        
        # Create lookup for people with docs
        people_with_docs_dict = {person['row_id']: person for person in people_with_docs}
        
        # Process based on mode
        if basic_mode:
            # Basic processing - just extract core data
            for i, person in enumerate(all_people):
                if test_limit and i >= test_limit:
                    break
                record = CSVManager.create_record(person, mode='basic')
                processed_records.append(record)
        
        elif text_mode:
            # Text extraction mode - process documents for text only
            for person in all_people:
                if not person.get('doc_link'):
                    record = CSVManager.create_record(person, mode='text')
                    processed_records.append(record)
            
            # Process documents (simplified for orchestrator)
            docs_to_process = people_with_docs[:test_limit] if test_limit else people_with_docs
            
            for person in docs_to_process:
                from utils.extract_links import extract_text_with_retry
                doc_text, error = extract_text_with_retry(person['doc_link'])
                
                if error:
                    record = CSVManager.create_error_record(person, mode='text', error_message=error)
                else:
                    record = CSVManager.create_record(person, mode='text', doc_text=doc_text)
                
                processed_records.append(record)
        
        else:
            # Full processing of all people
            people_to_process = all_people[:test_limit] if test_limit else all_people
            
            for person in people_to_process:
                if person.get('doc_link') and person.get('row_id') in people_with_docs_dict:
                    # Process document
                    doc_content, doc_text = step3_scrape_doc_contents(person['doc_link'])
                    links = step4_extract_links(doc_content, doc_text)
                    record = step5_process_extracted_data(person, links, doc_text)
                    processed_records.append(record)
                
                elif person.get('doc_link'):
                    # Handle direct links
                    link = person['doc_link'].lower()
                    links = {
                        'youtube': [],
                        'drive_files': [],
                        'drive_folders': [],
                        'all_links': []
                    }
                    
                    if "youtube.com" in link or "youtu.be" in link:
                        links['youtube'].append(person['doc_link'])
                    elif "drive.google.com/file" in link:
                        links['drive_files'].append(person['doc_link'])
                    
                    links['all_links'].append(person['doc_link'])
                    record = step5_process_extracted_data(person, links, '')
                    processed_records.append(record)
                
                else:
                    # No document
                    record = CSVManager.create_record(person, mode='full', doc_text='', links=None)
                    processed_records.append(record)
        
        return {"processed_records": processed_records}
    
    def step6_wrapper(workflow_data, **kwargs):
        """Wrapper for step6_map_data"""
        from simple_workflow import step6_map_data
        
        processed_records = workflow_data.get("processed_records", [])
        basic_mode = kwargs.get("basic_mode", False)
        text_mode = kwargs.get("text_mode", False)
        output_file = kwargs.get("output_file")
        
        if processed_records:
            df = step6_map_data(processed_records, basic_mode=basic_mode, 
                              text_mode=text_mode, output_file=output_file)
            return {"output_dataframe": df, "record_count": len(processed_records)}
        else:
            return {"error": "No records to map"}
    
    # Create and configure the workflow
    workflow = (WorkflowOrchestrator("simple_six_step_workflow")
                .set_error_handling('continue'))
    
    # Add all 6 steps with proper dependencies and mode requirements
    workflow.add_step("step1_download_sheet", step1_wrapper, 
                     required_for_modes=['basic', 'text', 'full'])
    
    workflow.add_step("step2_extract_people", step2_wrapper, 
                     dependencies=["step1_download_sheet"],
                     required_for_modes=['basic', 'text', 'full'])
    
    workflow.add_step("step3_prepare_scraping", step3_wrapper,
                     dependencies=["step2_extract_people"],
                     required_for_modes=['text', 'full'])
    
    workflow.add_step("step4_prepare_extraction", step4_wrapper,
                     dependencies=["step3_prepare_scraping"],
                     required_for_modes=['text', 'full'])
    
    workflow.add_step("step5_process_data", step5_wrapper,
                     dependencies=["step2_extract_people"],
                     required_for_modes=['basic', 'text', 'full'])
    
    workflow.add_step("step6_map_to_csv", step6_wrapper,
                     dependencies=["step5_process_data"],
                     required_for_modes=['basic', 'text', 'full'])
    
    return workflow


# ============================================================================
# RESOURCE LIFECYCLE MANAGEMENT CONSOLIDATION (DRY ITERATION 5 - Step 2)
# ============================================================================

class ResourceManager:
    """
    Unified resource lifecycle management system (DRY CONSOLIDATION ITERATION 5 - Step 2).
    
    Consolidates resource cleanup patterns found throughout the codebase:
    - Selenium WebDriver cleanup (utils/patterns.py)
    - HTTP session cleanup (utils/http_pool.py)
    - Database connection cleanup (utils/database_operations.py)
    - Context manager patterns for file operations
    - atexit registration for global resource cleanup
    
    ELIMINATES DUPLICATION OF:
    - Manual atexit.register() calls for each resource type
    - Scattered try/finally blocks for resource cleanup
    - Inconsistent error handling during resource disposal
    - Resource leak potential from missing cleanup calls
    
    BUSINESS IMPACT: Prevents resource leaks and ensures consistent cleanup
    """
    
    _instance = None
    _resources = {}
    _cleanup_functions = []
    _atexit_registered = False
    
    def __new__(cls):
        """Singleton pattern for global resource management."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize resource manager (singleton safe)."""
        if self._initialized:
            return
        
        try:
            from .logging_config import get_logger
        except ImportError:
            import logging
            get_logger = lambda name: logging.getLogger(name)
        
        self.logger = get_logger("resource_manager")
        
        # Register global cleanup on first initialization
        if not self._atexit_registered:
            import atexit
            atexit.register(self.cleanup_all_resources)
            self._atexit_registered = True
            self.logger.info("🔧 Resource manager initialized with atexit cleanup")
        
        self._initialized = True
    
    def register_resource(self, name: str, resource: Any, 
                         cleanup_method: str = 'close',
                         cleanup_function: Optional[callable] = None) -> None:
        """
        Register a resource for automatic cleanup.
        
        Args:
            name: Unique name for the resource
            resource: The resource object to manage
            cleanup_method: Method name to call for cleanup (default: 'close')
            cleanup_function: Custom cleanup function (overrides cleanup_method)
            
        Example:
            manager = ResourceManager()
            manager.register_resource('http_session', session, 'close')
            manager.register_resource('driver', driver, cleanup_function=lambda: driver.quit())
        """
        if cleanup_function:
            cleanup_func = cleanup_function
        else:
            # Create cleanup function from method name
            cleanup_func = lambda: getattr(resource, cleanup_method, lambda: None)()
        
        self._resources[name] = {
            'resource': resource,
            'cleanup': cleanup_func,
            'cleaned': False
        }
        
        self.logger.debug(f"📌 Registered resource: {name}")
    
    def register_cleanup_function(self, name: str, cleanup_function: callable) -> None:
        """
        Register a standalone cleanup function.
        
        Args:
            name: Unique name for the cleanup function
            cleanup_function: Function to call during cleanup
            
        Example:
            manager = ResourceManager()
            manager.register_cleanup_function('temp_files', lambda: shutil.rmtree('/tmp/myapp'))
        """
        self._cleanup_functions.append({
            'name': name,
            'function': cleanup_function,
            'executed': False
        })
        
        self.logger.debug(f"📌 Registered cleanup function: {name}")
    
    def cleanup_resource(self, name: str) -> bool:
        """
        Clean up a specific resource.
        
        Args:
            name: Name of the resource to clean up
            
        Returns:
            True if cleanup was successful, False otherwise
        """
        if name not in self._resources:
            self.logger.warning(f"⚠️  Resource not found: {name}")
            return False
        
        resource_info = self._resources[name]
        
        if resource_info['cleaned']:
            self.logger.debug(f"🔄 Resource already cleaned: {name}")
            return True
        
        try:
            resource_info['cleanup']()
            resource_info['cleaned'] = True
            self.logger.info(f"✅ Cleaned up resource: {name}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to clean up resource {name}: {e}")
            return False
    
    def cleanup_all_resources(self) -> Dict[str, bool]:
        """
        Clean up all registered resources and execute cleanup functions.
        
        Returns:
            Dictionary mapping resource/function names to cleanup success status
        """
        results = {}
        
        self.logger.info("🧹 Starting cleanup of all resources...")
        
        # Clean up registered resources
        for name in list(self._resources.keys()):
            results[name] = self.cleanup_resource(name)
        
        # Execute cleanup functions
        for cleanup_info in self._cleanup_functions:
            if cleanup_info['executed']:
                continue
                
            try:
                cleanup_info['function']()
                cleanup_info['executed'] = True
                results[cleanup_info['name']] = True
                self.logger.info(f"✅ Executed cleanup function: {cleanup_info['name']}")
                
            except Exception as e:
                results[cleanup_info['name']] = False
                self.logger.error(f"❌ Failed to execute cleanup function {cleanup_info['name']}: {e}")
        
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        self.logger.info(f"🎯 Cleanup complete: {successful}/{total} successful")
        
        return results
    
    def get_resource(self, name: str) -> Optional[Any]:
        """
        Get a registered resource.
        
        Args:
            name: Name of the resource
            
        Returns:
            The resource object or None if not found
        """
        resource_info = self._resources.get(name)
        return resource_info['resource'] if resource_info else None
    
    def is_resource_active(self, name: str) -> bool:
        """
        Check if a resource is active (registered and not cleaned up).
        
        Args:
            name: Name of the resource
            
        Returns:
            True if resource is active, False otherwise
        """
        resource_info = self._resources.get(name)
        return resource_info is not None and not resource_info['cleaned']
    
    def list_resources(self) -> List[str]:
        """
        Get list of all registered resource names.
        
        Returns:
            List of resource names
        """
        return list(self._resources.keys())
    
    def reset(self) -> None:
        """
        Reset the resource manager (for testing).
        
        WARNING: This cleans up all resources and clears the registry.
        """
        self.cleanup_all_resources()
        self._resources.clear()
        self._cleanup_functions.clear()
        self.logger.info("🔄 Resource manager reset")


class ManagedResource:
    """
    Context manager for automatic resource lifecycle management.
    
    Integrates with ResourceManager to provide automatic cleanup
    for resources used in with statements.
    
    Example:
        with ManagedResource('selenium_driver', get_selenium_driver()) as driver:
            driver.get('https://example.com')
            # driver is automatically cleaned up on exit
    """
    
    def __init__(self, name: str, resource: Any, 
                 cleanup_method: str = 'close',
                 cleanup_function: Optional[callable] = None):
        """
        Initialize managed resource.
        
        Args:
            name: Unique name for the resource
            resource: The resource object to manage
            cleanup_method: Method name to call for cleanup (default: 'close')
            cleanup_function: Custom cleanup function (overrides cleanup_method)
        """
        self.name = name
        self.resource = resource
        self.cleanup_method = cleanup_method
        self.cleanup_function = cleanup_function
        self.manager = ResourceManager()
    
    def __enter__(self):
        """Enter context - register resource with manager."""
        self.manager.register_resource(
            self.name, 
            self.resource, 
            self.cleanup_method,
            self.cleanup_function
        )
        return self.resource
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context - cleanup resource."""
        self.manager.cleanup_resource(self.name)


def get_resource_manager() -> ResourceManager:
    """
    Get the global resource manager instance.
    
    Returns:
        ResourceManager singleton instance
        
    Example:
        manager = get_resource_manager()
        manager.register_resource('my_connection', conn, 'close')
    """
    return ResourceManager()


def cleanup_all_resources() -> Dict[str, bool]:
    """
    Convenience function to clean up all registered resources.
    
    Returns:
        Dictionary mapping resource names to cleanup success status
        
    Example:
        results = cleanup_all_resources()
        print(f"Cleanup results: {results}")
    """
    return ResourceManager().cleanup_all_resources()


class SafeFileManager:
    """
    Enhanced file context manager with automatic error handling and resource tracking.
    
    Consolidates file operation patterns found throughout the codebase.
    """
    
    def __init__(self, file_path: Union[str, Path], mode: str = 'r', 
                 encoding: str = 'utf-8', ensure_parent: bool = True):
        """
        Initialize safe file manager.
        
        Args:
            file_path: Path to the file
            mode: File mode ('r', 'w', 'a', etc.)
            encoding: File encoding (default: utf-8)
            ensure_parent: Create parent directory if it doesn't exist
        """
        self.file_path = Path(file_path)
        self.mode = mode
        self.encoding = encoding
        self.ensure_parent = ensure_parent
        self.file_handle = None
        self.manager = get_resource_manager()
    
    def __enter__(self):
        """Enter context - open file with error handling."""
        try:
            # Ensure parent directory exists if needed
            if self.ensure_parent and ('w' in self.mode or 'a' in self.mode):
                ensure_parent_dir(self.file_path)
            
            # Open file
            if 'b' in self.mode:
                self.file_handle = open(self.file_path, self.mode)
            else:
                self.file_handle = open(self.file_path, self.mode, encoding=self.encoding)
            
            # Register with resource manager
            resource_name = f"file_{id(self.file_handle)}"
            self.manager.register_resource(resource_name, self.file_handle, 'close')
            
            return self.file_handle
            
        except Exception as e:
            raise IOError(f"Failed to open file {self.file_path}: {e}")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context - cleanup file handle."""
        if self.file_handle:
            try:
                self.file_handle.close()
            except Exception:
                pass  # Already closed or error - ignore


def register_selenium_cleanup():
    """
    Register Selenium driver cleanup with ResourceManager.
    
    Replaces direct atexit.register(cleanup_selenium_driver) calls.
    """
    manager = get_resource_manager()
    
    # Import cleanup function
    try:
        from .patterns import cleanup_selenium_driver
    except ImportError:
        try:
            from patterns import cleanup_selenium_driver
        except ImportError:
            return  # Selenium not available
    
    manager.register_cleanup_function('selenium_driver_global', cleanup_selenium_driver)


def register_http_pool_cleanup():
    """
    Register HTTP pool cleanup with ResourceManager.
    
    Replaces direct atexit.register(close_http_pool) calls.
    """
    manager = get_resource_manager()
    
    # Import cleanup function
    try:
        from .http_pool import close_http_pool
    except ImportError:
        try:
            from http_pool import close_http_pool
        except ImportError:
            return  # HTTP pool not available
    
    manager.register_cleanup_function('http_pool_global', close_http_pool)


def setup_standard_resource_cleanup():
    """
    Set up standard resource cleanup for the application.
    
    Call this once during application initialization to register
    all standard cleanup functions.
    """
    register_selenium_cleanup()
    register_http_pool_cleanup()
    
    # Log setup completion
    logger = get_resource_manager().logger
    logger.info("🛠️  Standard resource cleanup configured")


# ============================================================================
# PERFORMANCE MONITORING AND TIMING CONSOLIDATION (DRY ITERATION 5 - Step 3)
# ============================================================================

class PerformanceTimer:
    """
    Unified performance timing system (DRY CONSOLIDATION ITERATION 5 - Step 3).
    
    Consolidates timing patterns found throughout the codebase:
    - StreamingProgress class (utils/streaming_integration.py)  
    - Manual datetime.now() and elapsed time calculations
    - WorkflowOrchestrator step timing patterns
    - Performance metrics in doc_templates.py
    - Duration tracking in logging and error formatting
    
    ELIMINATES DUPLICATION OF:
    - Scattered start_time = datetime.now() patterns
    - Inconsistent elapsed time calculations
    - Manual progress percentage calculations
    - Different timing result formats across modules
    
    BUSINESS IMPACT: Standardizes performance monitoring and enables consistent reporting
    """
    
    def __init__(self, operation_name: str, total_items: Optional[int] = None,
                 log_interval: float = 5.0, auto_log: bool = True):
        """
        Initialize performance timer.
        
        Args:
            operation_name: Name of the operation being timed
            total_items: Total number of items to process (for progress tracking)
            log_interval: Interval in seconds between progress logs
            auto_log: Whether to automatically log progress updates
        """
        from datetime import datetime
        
        self.operation_name = operation_name
        self.total_items = total_items
        self.log_interval = log_interval
        self.auto_log = auto_log
        
        # Timing data
        self.start_time = datetime.now()
        self.end_time = None
        self.last_log_time = self.start_time
        
        # Progress tracking
        self.completed_items = 0
        self.failed_items = 0
        self.current_item = None
        
        # Performance metrics
        self.metrics = {}
        self.checkpoints = {}
        
        # Logging
        try:
            from .logging_config import get_logger
        except ImportError:
            import logging
            get_logger = lambda name: logging.getLogger(name)
        
        self.logger = get_logger(f"performance.{operation_name}")
        
        if self.auto_log:
            self.logger.info(f"⏱️  Started timing: {operation_name}")
    
    def start(self) -> 'PerformanceTimer':
        """
        Start or restart the timer.
        
        Returns:
            Self for method chaining
        """
        from datetime import datetime
        
        self.start_time = datetime.now()
        self.end_time = None
        self.last_log_time = self.start_time
        
        if self.auto_log:
            self.logger.info(f"⏱️  Started timing: {self.operation_name}")
        
        return self
    
    def checkpoint(self, name: str, auto_log: bool = None) -> float:
        """
        Create a checkpoint and return elapsed time since start.
        
        Args:
            name: Name of the checkpoint
            auto_log: Override auto_log setting for this checkpoint
            
        Returns:
            Elapsed time in seconds since start
        """
        from datetime import datetime
        
        now = datetime.now()
        elapsed = (now - self.start_time).total_seconds()
        
        self.checkpoints[name] = {
            'timestamp': now.isoformat(),
            'elapsed_since_start': elapsed,
            'elapsed_since_last': (now - self.last_log_time).total_seconds()
        }
        
        should_log = auto_log if auto_log is not None else self.auto_log
        if should_log:
            self.logger.info(f"📍 Checkpoint '{name}': {elapsed:.2f}s")
        
        self.last_log_time = now
        return elapsed
    
    def update_progress(self, completed: Optional[int] = None, failed: Optional[int] = None,
                       current_item: Optional[str] = None, force_log: bool = False) -> None:
        """
        Update progress tracking.
        
        Args:
            completed: Number of completed items (incremental if None)
            failed: Number of failed items (incremental if None)
            current_item: Description of current item being processed
            force_log: Force logging regardless of log_interval
        """
        from datetime import datetime
        
        # Update counters
        if completed is not None:
            self.completed_items = completed
        else:
            self.completed_items += 1
            
        if failed is not None:
            self.failed_items = failed
        else:
            if current_item and 'failed' in str(current_item).lower():
                self.failed_items += 1
        
        if current_item is not None:
            self.current_item = current_item
        
        # Check if we should log
        now = datetime.now()
        should_log = (force_log or 
                     (now - self.last_log_time).total_seconds() >= self.log_interval)
        
        if should_log and self.auto_log:
            self._log_progress()
            self.last_log_time = now
    
    def _log_progress(self) -> None:
        """Log current progress status."""
        elapsed = self.get_elapsed_time()
        total_processed = self.completed_items + self.failed_items
        
        if self.total_items:
            progress_pct = (total_processed / self.total_items) * 100
            progress_str = f"{progress_pct:.1f}% ({total_processed}/{self.total_items})"
        else:
            progress_str = f"{total_processed} items"
        
        # Calculate rate
        rate = total_processed / elapsed if elapsed > 0 else 0
        
        log_message = f"📊 {self.operation_name}: {progress_str}"
        log_message += f" | Elapsed: {elapsed:.1f}s"
        log_message += f" | Rate: {rate:.1f} items/s"
        log_message += f" | Success: {self.completed_items}, Failed: {self.failed_items}"
        
        if self.current_item:
            log_message += f" | Current: {self.current_item}"
        
        self.logger.info(log_message)
    
    def add_metric(self, name: str, value: Union[int, float, str], 
                   unit: Optional[str] = None) -> None:
        """
        Add a performance metric.
        
        Args:
            name: Metric name
            value: Metric value
            unit: Optional unit description
        """
        self.metrics[name] = {
            'value': value,
            'unit': unit,
            'timestamp': datetime.now().isoformat()
        }
    
    def stop(self, auto_log: bool = None) -> float:
        """
        Stop the timer and return total elapsed time.
        
        Args:
            auto_log: Override auto_log setting for final summary
            
        Returns:
            Total elapsed time in seconds
        """
        from datetime import datetime
        
        self.end_time = datetime.now()
        elapsed = self.get_elapsed_time()
        
        should_log = auto_log if auto_log is not None else self.auto_log
        if should_log:
            self._log_final_summary(elapsed)
        
        return elapsed
    
    def _log_final_summary(self, elapsed: float) -> None:
        """Log final timing summary."""
        total_processed = self.completed_items + self.failed_items
        success_rate = (self.completed_items / total_processed * 100) if total_processed > 0 else 0
        
        self.logger.info(f"🎯 Completed: {self.operation_name}")
        self.logger.info(f"   Total time: {elapsed:.2f}s")
        self.logger.info(f"   Items processed: {total_processed}")
        self.logger.info(f"   Success rate: {success_rate:.1f}%")
        
        if total_processed > 0:
            rate = total_processed / elapsed
            self.logger.info(f"   Average rate: {rate:.2f} items/s")
        
        if self.checkpoints:
            self.logger.info(f"   Checkpoints: {len(self.checkpoints)}")
        
        if self.metrics:
            self.logger.info(f"   Metrics: {len(self.metrics)}")
    
    def get_elapsed_time(self) -> float:
        """
        Get elapsed time in seconds.
        
        Returns:
            Elapsed time since start (or total time if stopped)
        """
        from datetime import datetime
        
        end_time = self.end_time or datetime.now()
        return (end_time - self.start_time).total_seconds()
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive timing and performance summary.
        
        Returns:
            Dictionary containing all timing data and metrics
        """
        elapsed = self.get_elapsed_time()
        total_processed = self.completed_items + self.failed_items
        
        summary = {
            'operation_name': self.operation_name,
            'timing': {
                'start_time': self.start_time.isoformat(),
                'end_time': self.end_time.isoformat() if self.end_time else None,
                'elapsed_seconds': elapsed,
                'is_completed': self.end_time is not None
            },
            'progress': {
                'total_items': self.total_items,
                'completed_items': self.completed_items,
                'failed_items': self.failed_items,
                'total_processed': total_processed,
                'success_rate': (self.completed_items / total_processed * 100) if total_processed > 0 else 0,
                'current_item': self.current_item
            },
            'performance': {
                'items_per_second': total_processed / elapsed if elapsed > 0 else 0,
                'seconds_per_item': elapsed / total_processed if total_processed > 0 else 0
            },
            'checkpoints': self.checkpoints,
            'metrics': self.metrics
        }
        
        return summary
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - automatically stop timer."""
        if exc_type:
            self.add_metric('exit_with_exception', str(exc_type.__name__))
        
        self.stop()


class BatchProgressTracker:
    """
    Specialized progress tracker for batch operations.
    
    Consolidates batch processing patterns from simple_workflow.py and other batch scripts.
    """
    
    def __init__(self, operation_name: str, total_batches: int, batch_size: int):
        """
        Initialize batch progress tracker.
        
        Args:
            operation_name: Name of the batch operation
            total_batches: Total number of batches
            batch_size: Size of each batch
        """
        self.operation_name = operation_name
        self.total_batches = total_batches
        self.batch_size = batch_size
        self.current_batch = 0
        
        self.timer = PerformanceTimer(f"{operation_name}_batch", total_batches)
        
        # Batch-specific tracking
        self.batch_results = []
        self.successful_batches = 0
        self.failed_batches = 0
    
    def start_batch(self, batch_num: int, batch_items: int) -> None:
        """
        Start processing a new batch.
        
        Args:
            batch_num: Current batch number (1-indexed)
            batch_items: Number of items in this batch
        """
        self.current_batch = batch_num
        
        self.timer.checkpoint(f"batch_{batch_num}_start")
        self.timer.logger.info(f"📦 BATCH {batch_num}/{self.total_batches} ({batch_items} items)")
    
    def complete_batch(self, batch_num: int, successful_items: int, 
                      failed_items: int) -> None:
        """
        Complete a batch and update progress.
        
        Args:
            batch_num: Batch number that was completed
            successful_items: Number of successful items in batch
            failed_items: Number of failed items in batch
        """
        total_items = successful_items + failed_items
        success_rate = (successful_items / total_items * 100) if total_items > 0 else 0
        
        batch_result = {
            'batch_number': batch_num,
            'total_items': total_items,
            'successful_items': successful_items,
            'failed_items': failed_items,
            'success_rate': success_rate,
            'timestamp': datetime.now().isoformat()
        }
        
        self.batch_results.append(batch_result)
        
        if failed_items == 0:
            self.successful_batches += 1
        else:
            self.failed_batches += 1
        
        # Update main timer
        self.timer.update_progress(
            completed=self.successful_batches,
            failed=self.failed_batches,
            current_item=f"Batch {batch_num}",
            force_log=True
        )
        
        self.timer.checkpoint(f"batch_{batch_num}_complete")
        
        # Log batch completion
        self.timer.logger.info(f"✅ Batch {batch_num} complete: {successful_items} success, {failed_items} failed ({success_rate:.1f}%)")
    
    def get_batch_summary(self) -> Dict[str, Any]:
        """Get summary of all batch processing."""
        overall_successful = sum(b['successful_items'] for b in self.batch_results)
        overall_failed = sum(b['failed_items'] for b in self.batch_results)
        overall_total = overall_successful + overall_failed
        
        summary = self.timer.get_summary()
        summary.update({
            'batch_info': {
                'total_batches': self.total_batches,
                'completed_batches': len(self.batch_results),
                'successful_batches': self.successful_batches,
                'failed_batches': self.failed_batches,
                'average_batch_size': overall_total / len(self.batch_results) if self.batch_results else 0
            },
            'item_totals': {
                'total_items': overall_total,
                'successful_items': overall_successful,
                'failed_items': overall_failed,
                'overall_success_rate': (overall_successful / overall_total * 100) if overall_total > 0 else 0
            },
            'batch_results': self.batch_results
        })
        
        return summary


def create_performance_timer(operation_name: str, total_items: Optional[int] = None,
                           **kwargs) -> PerformanceTimer:
    """
    Factory function to create a performance timer.
    
    Args:
        operation_name: Name of the operation
        total_items: Total number of items (for progress tracking)
        **kwargs: Additional arguments for PerformanceTimer
        
    Returns:
        PerformanceTimer instance
        
    Example:
        timer = create_performance_timer("data_processing", total_items=1000)
        timer.start()
        # ... do work ...
        timer.update_progress(current_item="processing item 1")
        timer.stop()
    """
    return PerformanceTimer(operation_name, total_items, **kwargs)


def create_batch_tracker(operation_name: str, total_batches: int, 
                        batch_size: int) -> BatchProgressTracker:
    """
    Factory function to create a batch progress tracker.
    
    Args:
        operation_name: Name of the batch operation
        total_batches: Total number of batches
        batch_size: Size of each batch
        
    Returns:
        BatchProgressTracker instance
        
    Example:
        tracker = create_batch_tracker("document_processing", 10, 100)
        tracker.start_batch(1, 100)
        # ... process batch ...
        tracker.complete_batch(1, 95, 5)
    """
    return BatchProgressTracker(operation_name, total_batches, batch_size)


def time_operation(operation_name: str, auto_log: bool = True):
    """
    Decorator to automatically time a function execution.
    
    Args:
        operation_name: Name of the operation
        auto_log: Whether to automatically log timing info
        
    Example:
        @time_operation("data_processing")
        def process_data(data):
            # ... processing logic ...
            return results
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            with PerformanceTimer(operation_name, auto_log=auto_log) as timer:
                result = func(*args, **kwargs)
                
                # Add result size as metric if it's a collection
                if hasattr(result, '__len__'):
                    timer.add_metric('result_size', len(result), 'items')
                
                return result
        return wrapper
    return decorator


# ============================================================================
# LEGACY TIMING PATTERN MIGRATION UTILITIES
# ============================================================================

def replace_manual_timing(start_time_var: datetime, operation_name: str) -> PerformanceTimer:
    """
    Replace manual timing patterns with PerformanceTimer.
    
    Helper function for migrating existing code that uses manual timing.
    
    Args:
        start_time_var: Existing start_time variable
        operation_name: Name for the timer
        
    Returns:
        PerformanceTimer instance with start time set
        
    Example:
        # Replace this pattern:
        start_time = datetime.now()
        # ... do work ...
        elapsed = (datetime.now() - start_time).total_seconds()
        
        # With this:
        timer = replace_manual_timing(start_time, "operation")
        # ... do work ...
        elapsed = timer.stop()
    """
    timer = PerformanceTimer(operation_name, auto_log=False)
    timer.start_time = start_time_var
    return timer


def consolidate_progress_tracking(total_items: int, operation_name: str = "progress") -> PerformanceTimer:
    """
    Consolidate manual progress tracking patterns.
    
    Replaces scattered progress calculation and logging patterns.
    
    Args:
        total_items: Total number of items to process
        operation_name: Name of the operation
        
    Returns:
        PerformanceTimer configured for progress tracking
        
    Example:
        # Replace manual progress patterns:
        total = 1000
        completed = 0
        for item in items:
            # ... process item ...
            completed += 1
            if completed % 100 == 0:
                print(f"Progress: {completed/total*100:.1f}%")
        
        # With this:
        timer = consolidate_progress_tracking(1000, "item_processing")
        for item in items:
            # ... process item ...
            timer.update_progress()
    """
    return PerformanceTimer(operation_name, total_items=total_items, log_interval=5.0)


# ============================================================================
# DATABASE AND STORAGE OPERATION CONSOLIDATION (DRY ITERATION 5 - Step 4)
# ============================================================================

class UnifiedDataAccessLayer:
    """
    Unified data access layer (DRY CONSOLIDATION ITERATION 5 - Step 4).
    
    Consolidates database and storage operation patterns found throughout the codebase:
    - Multiple DatabaseManager classes (database_operations.py, database_manager.py)
    - Connection management patterns with context managers
    - Transaction management with commit/rollback patterns
    - Storage operation patterns for different backends (S3, file system)
    - CRUD operation patterns scattered across modules
    
    ELIMINATES DUPLICATION OF:
    - Multiple database connection implementations
    - Scattered transaction management code
    - Inconsistent storage operation patterns
    - Duplicate CRUD operation helpers
    - Various connection pooling implementations
    
    BUSINESS IMPACT: Standardizes all data access and prevents connection leaks
    """
    
    _instance = None
    _backends = {}
    
    def __new__(cls):
        """Singleton pattern for unified data access."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize unified data access layer."""
        if self._initialized:
            return
        
        try:
            from .logging_config import get_logger
        except ImportError:
            import logging
            get_logger = lambda name: logging.getLogger(name)
        
        self.logger = get_logger("data_access")
        self.resource_manager = get_resource_manager()
        self._initialized = True
        
        self.logger.info("🗄️  Unified data access layer initialized")
    
    def register_backend(self, name: str, backend: 'DataBackend') -> None:
        """
        Register a data backend.
        
        Args:
            name: Backend name (e.g., 'database', 's3', 'filesystem')
            backend: Backend implementation
        """
        self._backends[name] = backend
        self.logger.info(f"📌 Registered data backend: {name}")
    
    def get_backend(self, name: str) -> Optional['DataBackend']:
        """
        Get a registered data backend.
        
        Args:
            name: Backend name
            
        Returns:
            Backend instance or None if not found
        """
        return self._backends.get(name)
    
    def execute_with_backend(self, backend_name: str, operation: str, 
                           **kwargs) -> Any:
        """
        Execute an operation with a specific backend.
        
        Args:
            backend_name: Name of the backend to use
            operation: Operation to execute
            **kwargs: Operation parameters
            
        Returns:
            Operation result
            
        Example:
            result = dal.execute_with_backend('database', 'select', 
                                            table='users', where={'active': True})
        """
        backend = self.get_backend(backend_name)
        if not backend:
            raise ValueError(f"Backend not found: {backend_name}")
        
        return backend.execute(operation, **kwargs)


class DataBackend:
    """
    Base class for data backends.
    
    Provides standardized interface for different storage systems.
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize data backend.
        
        Args:
            name: Backend name
            config: Backend configuration
        """
        self.name = name
        self.config = config
        self.is_connected = False
        
        try:
            from .logging_config import get_logger
        except ImportError:
            import logging
            get_logger = lambda name: logging.getLogger(name)
        
        self.logger = get_logger(f"backend.{name}")
    
    def connect(self) -> None:
        """Establish connection to the backend."""
        raise NotImplementedError("Subclasses must implement connect()")
    
    def disconnect(self) -> None:
        """Close connection to the backend."""
        raise NotImplementedError("Subclasses must implement disconnect()")
    
    def execute(self, operation: str, **kwargs) -> Any:
        """
        Execute an operation.
        
        Args:
            operation: Operation to execute
            **kwargs: Operation parameters
            
        Returns:
            Operation result
        """
        raise NotImplementedError("Subclasses must implement execute()")
    
    def health_check(self) -> bool:
        """
        Check if backend is healthy.
        
        Returns:
            True if healthy, False otherwise
        """
        return self.is_connected


class DatabaseBackend(DataBackend):
    """
    Database backend with unified connection management.
    
    Consolidates database patterns from database_operations.py and database_manager.py.
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """Initialize database backend."""
        super().__init__(name, config)
        
        self.db_type = config.get('type', 'sqlite')
        self.connection_string = config.get('connection_string')
        self.pool_size = config.get('pool_size', 5)
        
        self._connection = None
        self._transaction_active = False
        
        import threading
        self._lock = threading.Lock()
    
    def connect(self) -> None:
        """Establish database connection."""
        with self._lock:
            if self.is_connected:
                return
            
            try:
                if self.db_type == 'sqlite':
                    self._connection = self._connect_sqlite()
                elif self.db_type == 'postgresql':
                    self._connection = self._connect_postgresql()
                elif self.db_type == 'mysql':
                    self._connection = self._connect_mysql()
                else:
                    raise ValueError(f"Unsupported database type: {self.db_type}")
                
                self.is_connected = True
                self.logger.info(f"✅ Connected to {self.db_type} database")
                
                # Register with resource manager for cleanup
                resource_manager = get_resource_manager()
                resource_manager.register_resource(
                    f"db_connection_{id(self._connection)}",
                    self._connection,
                    cleanup_function=self.disconnect
                )
                
            except Exception as e:
                self.logger.error(f"❌ Failed to connect to database: {e}")
                raise
    
    def _connect_sqlite(self):
        """Connect to SQLite database."""
        import sqlite3
        
        db_path = self.config.get('path', 'app.db')
        
        # Ensure parent directory exists
        ensure_parent_dir(db_path)
        
        conn = sqlite3.connect(
            db_path,
            timeout=self.config.get('timeout', 30.0),
            check_same_thread=False
        )
        
        # Enable foreign keys and WAL mode for better performance
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        
        return conn
    
    def _connect_postgresql(self):
        """Connect to PostgreSQL database."""
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            conn = psycopg2.connect(
                self.connection_string,
                cursor_factory=RealDictCursor
            )
            return conn
            
        except ImportError:
            raise ImportError("psycopg2 required for PostgreSQL connections")
    
    def _connect_mysql(self):
        """Connect to MySQL database."""
        try:
            import mysql.connector
            
            conn = mysql.connector.connect(
                **self.config.get('connection_params', {})
            )
            return conn
            
        except ImportError:
            raise ImportError("mysql-connector-python required for MySQL connections")
    
    def disconnect(self) -> None:
        """Close database connection."""
        with self._lock:
            if self._connection:
                try:
                    # Rollback any pending transaction
                    if self._transaction_active:
                        self._connection.rollback()
                        self._transaction_active = False
                    
                    self._connection.close()
                    self.is_connected = False
                    self.logger.info(f"🔒 Disconnected from {self.db_type} database")
                    
                except Exception as e:
                    self.logger.error(f"❌ Error disconnecting from database: {e}")
                finally:
                    self._connection = None
    
    def execute(self, operation: str, **kwargs) -> Any:
        """
        Execute database operation.
        
        Args:
            operation: Operation type ('select', 'insert', 'update', 'delete', 'execute')
            **kwargs: Operation parameters
            
        Returns:
            Operation result
        """
        if not self.is_connected:
            self.connect()
        
        if operation == 'select':
            return self._execute_select(**kwargs)
        elif operation == 'insert':
            return self._execute_insert(**kwargs)
        elif operation == 'update':
            return self._execute_update(**kwargs)
        elif operation == 'delete':
            return self._execute_delete(**kwargs)
        elif operation == 'execute':
            return self._execute_raw(**kwargs)
        elif operation == 'transaction':
            return self._execute_transaction(**kwargs)
        else:
            raise ValueError(f"Unsupported operation: {operation}")
    
    def _execute_select(self, table: str, columns: List[str] = None, 
                       where: Dict[str, Any] = None, order_by: str = None,
                       limit: int = None) -> List[Dict[str, Any]]:
        """Execute SELECT query."""
        columns_str = ', '.join(columns) if columns else '*'
        query = f"SELECT {columns_str} FROM {table}"
        params = []
        
        if where:
            where_clauses = []
            for key, value in where.items():
                where_clauses.append(f"{key} = ?")
                params.append(value)
            query += " WHERE " + " AND ".join(where_clauses)
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor = self._connection.cursor()
        cursor.execute(query, params)
        
        results = []
        for row in cursor.fetchall():
            if hasattr(row, '_asdict'):  # namedtuple
                results.append(row._asdict())
            elif hasattr(row, 'keys'):  # sqlite3.Row or dict-like
                results.append(dict(row))
            else:  # regular tuple
                results.append(row)
        
        return results
    
    def _execute_insert(self, table: str, data: Dict[str, Any]) -> int:
        """Execute INSERT query."""
        columns = list(data.keys())
        placeholders = ['?' for _ in columns]
        values = list(data.values())
        
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        cursor = self._connection.cursor()
        cursor.execute(query, values)
        
        if not self._transaction_active:
            self._connection.commit()
        
        return cursor.lastrowid or cursor.rowcount
    
    def _execute_update(self, table: str, data: Dict[str, Any], 
                       where: Dict[str, Any]) -> int:
        """Execute UPDATE query."""
        set_clauses = []
        params = []
        
        for key, value in data.items():
            set_clauses.append(f"{key} = ?")
            params.append(value)
        
        where_clauses = []
        for key, value in where.items():
            where_clauses.append(f"{key} = ?")
            params.append(value)
        
        query = f"UPDATE {table} SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)}"
        
        cursor = self._connection.cursor()
        cursor.execute(query, params)
        
        if not self._transaction_active:
            self._connection.commit()
        
        return cursor.rowcount
    
    def _execute_delete(self, table: str, where: Dict[str, Any]) -> int:
        """Execute DELETE query."""
        where_clauses = []
        params = []
        
        for key, value in where.items():
            where_clauses.append(f"{key} = ?")
            params.append(value)
        
        query = f"DELETE FROM {table} WHERE {' AND '.join(where_clauses)}"
        
        cursor = self._connection.cursor()
        cursor.execute(query, params)
        
        if not self._transaction_active:
            self._connection.commit()
        
        return cursor.rowcount
    
    def _execute_raw(self, query: str, params: List[Any] = None) -> Any:
        """Execute raw SQL query."""
        cursor = self._connection.cursor()
        cursor.execute(query, params or [])
        
        if not self._transaction_active:
            self._connection.commit()
        
        # Return results for SELECT queries
        if query.strip().upper().startswith('SELECT'):
            results = []
            for row in cursor.fetchall():
                if hasattr(row, '_asdict'):
                    results.append(row._asdict())
                elif hasattr(row, 'keys'):
                    results.append(dict(row))
                else:
                    results.append(row)
            return results
        
        return cursor.rowcount
    
    def _execute_transaction(self, operations: List[Dict[str, Any]]) -> List[Any]:
        """Execute multiple operations in a transaction."""
        results = []
        
        try:
            self._transaction_active = True
            
            for op in operations:
                operation_type = op.pop('operation')
                result = self.execute(operation_type, **op)
                results.append(result)
            
            self._connection.commit()
            self._transaction_active = False
            
            return results
            
        except Exception as e:
            self._connection.rollback()
            self._transaction_active = False
            self.logger.error(f"❌ Transaction failed: {e}")
            raise


class StorageBackend(DataBackend):
    """
    Storage backend for file-based operations.
    
    Consolidates storage patterns for S3, filesystem, and other storage systems.
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """Initialize storage backend."""
        super().__init__(name, config)
        
        self.storage_type = config.get('type', 'filesystem')
        self.base_path = config.get('base_path', '.')
        
        self._client = None
    
    def connect(self) -> None:
        """Establish storage connection."""
        if self.is_connected:
            return
        
        try:
            if self.storage_type == 'filesystem':
                self._connect_filesystem()
            elif self.storage_type == 's3':
                self._connect_s3()
            else:
                raise ValueError(f"Unsupported storage type: {self.storage_type}")
            
            self.is_connected = True
            self.logger.info(f"✅ Connected to {self.storage_type} storage")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to storage: {e}")
            raise
    
    def _connect_filesystem(self):
        """Connect to filesystem storage."""
        # Ensure base directory exists
        ensure_directory(self.base_path)
        self._client = Path(self.base_path)
    
    def _connect_s3(self):
        """Connect to S3 storage."""
        try:
            import boto3
            
            self._client = boto3.client(
                's3',
                region_name=self.config.get('region', 'us-east-1'),
                aws_access_key_id=self.config.get('access_key_id'),
                aws_secret_access_key=self.config.get('secret_access_key')
            )
            
        except ImportError:
            raise ImportError("boto3 required for S3 connections")
    
    def disconnect(self) -> None:
        """Close storage connection."""
        if self._client:
            # No explicit disconnect needed for most storage backends
            self._client = None
            self.is_connected = False
            self.logger.info(f"🔒 Disconnected from {self.storage_type} storage")
    
    def execute(self, operation: str, **kwargs) -> Any:
        """
        Execute storage operation.
        
        Args:
            operation: Operation type ('store', 'retrieve', 'delete', 'list')
            **kwargs: Operation parameters
            
        Returns:
            Operation result
        """
        if not self.is_connected:
            self.connect()
        
        if operation == 'store':
            return self._store(**kwargs)
        elif operation == 'retrieve':
            return self._retrieve(**kwargs)
        elif operation == 'delete':
            return self._delete(**kwargs)
        elif operation == 'list':
            return self._list(**kwargs)
        elif operation == 'exists':
            return self._exists(**kwargs)
        else:
            raise ValueError(f"Unsupported storage operation: {operation}")
    
    def _store(self, key: str, data: Union[str, bytes, Dict], 
              metadata: Dict[str, str] = None) -> bool:
        """Store data."""
        if self.storage_type == 'filesystem':
            return self._store_filesystem(key, data, metadata)
        elif self.storage_type == 's3':
            return self._store_s3(key, data, metadata)
    
    def _retrieve(self, key: str) -> Any:
        """Retrieve data."""
        if self.storage_type == 'filesystem':
            return self._retrieve_filesystem(key)
        elif self.storage_type == 's3':
            return self._retrieve_s3(key)
    
    def _delete(self, key: str) -> bool:
        """Delete data."""
        if self.storage_type == 'filesystem':
            return self._delete_filesystem(key)
        elif self.storage_type == 's3':
            return self._delete_s3(key)
    
    def _list(self, prefix: str = "") -> List[str]:
        """List stored items."""
        if self.storage_type == 'filesystem':
            return self._list_filesystem(prefix)
        elif self.storage_type == 's3':
            return self._list_s3(prefix)
    
    def _exists(self, key: str) -> bool:
        """Check if item exists."""
        if self.storage_type == 'filesystem':
            return (self._client / key).exists()
        elif self.storage_type == 's3':
            try:
                self._client.head_object(
                    Bucket=self.config['bucket'],
                    Key=key
                )
                return True
            except Exception:
                return False
    
    def _store_filesystem(self, key: str, data: Union[str, bytes, Dict], 
                         metadata: Dict[str, str] = None) -> bool:
        """Store to filesystem."""
        file_path = self._client / key
        ensure_parent_dir(file_path)
        
        try:
            if isinstance(data, dict):
                # Store as JSON
                import json
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
            elif isinstance(data, str):
                # Store as text
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(data)
            else:
                # Store as binary
                with open(file_path, 'wb') as f:
                    f.write(data)
            
            # Store metadata if provided
            if metadata:
                metadata_path = file_path.with_suffix(file_path.suffix + '.meta')
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    import json
                    json.dump(metadata, f, indent=2)
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to store {key}: {e}")
            return False
    
    def _retrieve_filesystem(self, key: str) -> Any:
        """Retrieve from filesystem."""
        file_path = self._client / key
        
        if not file_path.exists():
            return None
        
        try:
            # Try to load as JSON first
            if file_path.suffix.lower() == '.json':
                import json
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            # Load as text
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
                
        except UnicodeDecodeError:
            # Load as binary
            with open(file_path, 'rb') as f:
                return f.read()
        
        except Exception as e:
            self.logger.error(f"❌ Failed to retrieve {key}: {e}")
            return None
    
    def _delete_filesystem(self, key: str) -> bool:
        """Delete from filesystem."""
        file_path = self._client / key
        
        try:
            if file_path.exists():
                file_path.unlink()
            
            # Also delete metadata if it exists
            metadata_path = file_path.with_suffix(file_path.suffix + '.meta')
            if metadata_path.exists():
                metadata_path.unlink()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to delete {key}: {e}")
            return False
    
    def _list_filesystem(self, prefix: str = "") -> List[str]:
        """List filesystem items."""
        try:
            base_path = self._client / prefix if prefix else self._client
            
            items = []
            if base_path.is_dir():
                for item in base_path.rglob('*'):
                    if item.is_file() and not item.name.endswith('.meta'):
                        # Return relative path
                        relative_path = item.relative_to(self._client)
                        items.append(str(relative_path))
            
            return sorted(items)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to list items: {e}")
            return []


def get_data_access_layer() -> UnifiedDataAccessLayer:
    """
    Get the global unified data access layer.
    
    Returns:
        UnifiedDataAccessLayer singleton instance
        
    Example:
        dal = get_data_access_layer()
        dal.register_backend('database', DatabaseBackend('db', db_config))
        result = dal.execute_with_backend('database', 'select', table='users')
    """
    return UnifiedDataAccessLayer()


def create_database_backend(name: str, db_type: str = 'sqlite', 
                          **config) -> DatabaseBackend:
    """
    Create a database backend.
    
    Args:
        name: Backend name
        db_type: Database type ('sqlite', 'postgresql', 'mysql')
        **config: Database configuration
        
    Returns:
        DatabaseBackend instance
        
    Example:
        backend = create_database_backend('main_db', 'sqlite', path='app.db')
        dal = get_data_access_layer()
        dal.register_backend('database', backend)
    """
    config['type'] = db_type
    return DatabaseBackend(name, config)


def create_storage_backend(name: str, storage_type: str = 'filesystem',
                          **config) -> StorageBackend:
    """
    Create a storage backend.
    
    Args:
        name: Backend name
        storage_type: Storage type ('filesystem', 's3')
        **config: Storage configuration
        
    Returns:
        StorageBackend instance
        
    Example:
        backend = create_storage_backend('file_storage', 'filesystem', 
                                       base_path='./data')
        dal = get_data_access_layer()
        dal.register_backend('storage', backend)
    """
    config['type'] = storage_type
    return StorageBackend(name, config)


# ============================================================================
# LEGACY DATABASE PATTERN MIGRATION UTILITIES
# ============================================================================

def setup_standard_data_backends():
    """
    Set up standard data backends for the application.
    
    Replaces scattered database and storage initialization patterns.
    """
    dal = get_data_access_layer()
    
    # Set up SQLite database backend
    db_backend = create_database_backend(
        'main_database',
        'sqlite',
        path='app.db',
        timeout=30.0
    )
    dal.register_backend('database', db_backend)
    
    # Set up filesystem storage backend
    storage_backend = create_storage_backend(
        'file_storage',
        'filesystem',
        base_path='./data'
    )
    dal.register_backend('storage', storage_backend)
    
    dal.logger.info("🏗️  Standard data backends configured")


# Convenience functions for common operations
def db_select(table: str, **kwargs) -> List[Dict[str, Any]]:
    """Convenience function for database SELECT."""
    dal = get_data_access_layer()
    return dal.execute_with_backend('database', 'select', table=table, **kwargs)


def db_insert(table: str, data: Dict[str, Any]) -> int:
    """Convenience function for database INSERT."""
    dal = get_data_access_layer()
    return dal.execute_with_backend('database', 'insert', table=table, data=data)


def storage_store(key: str, data: Any, **kwargs) -> bool:
    """Convenience function for storage store."""
    dal = get_data_access_layer()
    return dal.execute_with_backend('storage', 'store', key=key, data=data, **kwargs)


def storage_retrieve(key: str) -> Any:
    """Convenience function for storage retrieve."""
    dal = get_data_access_layer()
    return dal.execute_with_backend('storage', 'retrieve', key=key)


# ============================================================================
# DRY CONSOLIDATION ITERATION 5 - STEP 5: UNIFIED EXCEPTION CONTEXT & RECOVERY
# ============================================================================

class ExceptionRecoveryContext:
    """
    Unified exception context and recovery management system.
    
    Consolidates scattered exception handling patterns:
    - Exception context preservation 
    - Recovery attempt tracking
    - Intelligent retry logic
    - Context-aware fallback strategies
    """
    
    def __init__(self, operation_name: str, max_attempts: int = 3, 
                 backoff_factor: float = 1.5, timeout: float = 30.0):
        self.operation_name = operation_name
        self.max_attempts = max_attempts
        self.backoff_factor = backoff_factor
        self.timeout = timeout
        self.attempt_count = 0
        self.exceptions_encountered = []
        self.recovery_strategies = []
        self.context_data = {}
        self.start_time = None
        self.last_attempt_time = None
        
    def add_context(self, **kwargs):
        """Add contextual data for debugging and recovery"""
        self.context_data.update(kwargs)
        
    def add_recovery_strategy(self, strategy_func, priority: int = 0):
        """Add a recovery strategy function with priority"""
        self.recovery_strategies.append((priority, strategy_func))
        self.recovery_strategies.sort(key=lambda x: x[0])
        
    def should_retry(self, exception: Exception) -> bool:
        """Determine if operation should be retried based on exception type"""
        if self.attempt_count >= self.max_attempts:
            return False
            
        # Check timeout
        if self.start_time and (time.time() - self.start_time) > self.timeout:
            return False
            
        # Classify exception for retry decision
        exception_type = type(exception).__name__
        error_msg = str(exception).lower()
        
        # Network errors - usually retriable
        if any(keyword in error_msg for keyword in ['timeout', 'connection', 'network', 'dns']):
            return True
            
        # Rate limiting - retriable with backoff
        if any(keyword in error_msg for keyword in ['rate limit', '429', 'too many requests']):
            return True
            
        # Temporary service issues
        if any(keyword in error_msg for keyword in ['503', '502', '500', 'service unavailable']):
            return True
            
        # Don't retry validation or permission errors
        if any(keyword in error_msg for keyword in ['invalid', 'permission', 'access denied', 'not found']):
            return False
            
        # Don't retry critical system errors
        if exception_type in ['MemoryError', 'SystemError']:
            return False
            
        # Default: retry up to limit
        return True
        
    def get_backoff_delay(self) -> float:
        """Calculate backoff delay for current attempt"""
        if self.attempt_count <= 1:
            return 1.0
        return min(60.0, 1.0 * (self.backoff_factor ** (self.attempt_count - 1)))
        
    def record_exception(self, exception: Exception, context: dict = None):
        """Record exception details for analysis"""
        exception_record = {
            'timestamp': time.time(),
            'attempt': self.attempt_count,
            'exception_type': type(exception).__name__,
            'message': str(exception),
            'context': context or {}
        }
        self.exceptions_encountered.append(exception_record)
        
    def try_recovery_strategies(self, exception: Exception):
        """Attempt recovery using registered strategies"""
        for priority, strategy_func in self.recovery_strategies:
            try:
                result = strategy_func(exception, self.context_data)
                if result:  # Strategy succeeded
                    return True
            except Exception:
                continue  # Strategy failed, try next
        return False
        
    def get_summary_report(self) -> dict:
        """Get comprehensive summary of all attempts and context"""
        return {
            'operation': self.operation_name,
            'total_attempts': self.attempt_count,
            'max_attempts': self.max_attempts,
            'duration_seconds': time.time() - self.start_time if self.start_time else 0,
            'success': self.attempt_count > 0 and len(self.exceptions_encountered) < self.attempt_count,
            'exceptions': self.exceptions_encountered,
            'context_data': self.context_data,
            'recovery_strategies_used': len(self.recovery_strategies)
        }


class UnifiedExceptionManager:
    """
    Singleton manager for exception contexts and recovery patterns.
    
    Centralizes exception handling logic scattered across modules.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.active_contexts = {}
            cls._instance.global_recovery_strategies = []
            cls._instance.exception_stats = {
                'total_handled': 0,
                'total_recovered': 0,
                'by_type': {},
                'by_operation': {}
            }
        return cls._instance
        
    def create_context(self, operation_name: str, **kwargs) -> ExceptionRecoveryContext:
        """Create a new exception recovery context"""
        context = ExceptionRecoveryContext(operation_name, **kwargs)
        context.start_time = time.time()
        self.active_contexts[operation_name] = context
        return context
        
    def get_context(self, operation_name: str) -> Optional[ExceptionRecoveryContext]:
        """Get existing context by operation name"""
        return self.active_contexts.get(operation_name)
        
    def complete_context(self, operation_name: str):
        """Mark context as complete and clean up"""
        context = self.active_contexts.pop(operation_name, None)
        if context:
            # Update global stats
            self.exception_stats['total_handled'] += len(context.exceptions_encountered)
            if context.exceptions_encountered and context.attempt_count <= context.max_attempts:
                self.exception_stats['total_recovered'] += 1
                
            # Update by-operation stats
            op_stats = self.exception_stats['by_operation'].get(operation_name, {})
            op_stats['attempts'] = op_stats.get('attempts', 0) + context.attempt_count
            op_stats['exceptions'] = op_stats.get('exceptions', 0) + len(context.exceptions_encountered)
            self.exception_stats['by_operation'][operation_name] = op_stats
            
    def register_global_recovery_strategy(self, strategy_func, priority: int = 0):
        """Register a global recovery strategy for all contexts"""
        self.global_recovery_strategies.append((priority, strategy_func))
        self.global_recovery_strategies.sort(key=lambda x: x[0])
        
    def get_global_stats(self) -> dict:
        """Get global exception handling statistics"""
        return self.exception_stats.copy()


def get_exception_manager() -> UnifiedExceptionManager:
    """Get singleton exception manager instance"""
    return UnifiedExceptionManager()


def with_unified_exception_handling(operation_name: str, max_attempts: int = 3,
                                  timeout: float = 30.0, fallback_value=None,
                                  recovery_strategies: list = None):
    """
    Decorator for unified exception handling with context preservation.
    
    Consolidates patterns like:
        try:
            result = risky_operation()
        except Exception as e:
            logger.error(f"Operation failed: {e}")
            return None
            
    Into standardized context-aware handling with recovery attempts.
    
    Args:
        operation_name: Name for context tracking
        max_attempts: Maximum retry attempts
        timeout: Maximum operation timeout
        fallback_value: Value to return on final failure
        recovery_strategies: List of recovery strategy functions
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            manager = get_exception_manager()
            context = manager.create_context(
                operation_name, 
                max_attempts=max_attempts,
                timeout=timeout
            )
            
            # Add function context
            context.add_context(
                function_name=func.__name__,
                module=func.__module__,
                args_count=len(args),
                kwargs_keys=list(kwargs.keys())
            )
            
            # Register recovery strategies
            if recovery_strategies:
                for strategy in recovery_strategies:
                    context.add_recovery_strategy(strategy)
                    
            # Add global recovery strategies
            for priority, strategy in manager.global_recovery_strategies:
                context.add_recovery_strategy(strategy, priority)
                
            try:
                while context.attempt_count < context.max_attempts:
                    context.attempt_count += 1
                    context.last_attempt_time = time.time()
                    
                    try:
                        result = func(*args, **kwargs)
                        manager.complete_context(operation_name)
                        return result
                        
                    except Exception as e:
                        context.record_exception(e, {
                            'args': str(args)[:100],  # Truncate for safety
                            'kwargs': str(kwargs)[:100]
                        })
                        
                        # Try recovery strategies
                        if context.try_recovery_strategies(e):
                            continue
                            
                        # Check if should retry
                        if not context.should_retry(e):
                            break
                            
                        # Wait before retry
                        if context.attempt_count < context.max_attempts:
                            delay = context.get_backoff_delay()
                            time.sleep(delay)
                            
                # All attempts failed
                summary = context.get_summary_report()
                manager.complete_context(operation_name)
                
                # Log comprehensive failure info
                logger = get_logger()
                logger.error(f"Operation '{operation_name}' failed after {context.attempt_count} attempts")
                logger.error(f"Exceptions encountered: {[e['exception_type'] for e in context.exceptions_encountered]}")
                logger.error(f"Context: {context.context_data}")
                
                return fallback_value
                
            except KeyboardInterrupt:
                manager.complete_context(operation_name)
                raise
                
        return wrapper
    return decorator


def create_recovery_strategy(name: str):
    """
    Decorator to create standardized recovery strategy functions.
    
    Recovery strategies receive (exception, context_data) and return True if recovery succeeded.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(exception, context_data):
            try:
                logger = get_logger()
                logger.info(f"Attempting recovery strategy: {name}")
                result = func(exception, context_data)
                if result:
                    logger.info(f"Recovery strategy '{name}' succeeded")
                return result
            except Exception as e:
                logger = get_logger()
                logger.warning(f"Recovery strategy '{name}' failed: {e}")
                return False
        wrapper.strategy_name = name
        return wrapper
    return decorator


# Common recovery strategies
@create_recovery_strategy("clear_cache_and_retry")
def clear_cache_recovery(exception, context_data):
    """Recovery strategy: Clear caches and temporary files"""
    try:
        # Clear various caches that might be corrupted
        cache_dirs = [
            Path.cwd() / '__pycache__',
            Path.cwd() / '.cache',
            Path.cwd() / 'temp'
        ]
        
        for cache_dir in cache_dirs:
            if cache_dir.exists():
                import shutil
                shutil.rmtree(cache_dir, ignore_errors=True)
                
        return True
    except Exception:
        return False


@create_recovery_strategy("reset_connections")
def reset_connections_recovery(exception, context_data):
    """Recovery strategy: Reset network connections and sessions"""
    try:
        # Reset HTTP pool if available
        try:
            from .http_pool import close_http_pool
            close_http_pool()
        except ImportError:
            pass
            
        # Force garbage collection
        import gc
        gc.collect()
        
        return True
    except Exception:
        return False


@create_recovery_strategy("fallback_to_alternative")
def fallback_alternative_recovery(exception, context_data):
    """Recovery strategy: Use alternative approaches based on context"""
    try:
        operation = context_data.get('operation', '')
        
        # URL-based operations: try alternative domains
        if 'url' in context_data:
            original_url = context_data['url']
            if 'youtube.com' in original_url:
                # Try alternative YouTube domains
                alternatives = [
                    original_url.replace('youtube.com', 'youtu.be'),
                    original_url.replace('www.youtube.com', 'm.youtube.com')
                ]
                context_data['alternative_urls'] = alternatives
                return True
                
        # File operations: try alternative paths
        if 'file_path' in context_data:
            original_path = Path(context_data['file_path'])
            alternatives = [
                original_path.with_suffix('.bak'),
                original_path.parent / f"alt_{original_path.name}",
                Path.cwd() / original_path.name
            ]
            context_data['alternative_paths'] = [str(p) for p in alternatives if p.exists()]
            return bool(context_data['alternative_paths'])
            
        return False
    except Exception:
        return False


# Register default recovery strategies globally
_exception_manager = get_exception_manager()
_exception_manager.register_global_recovery_strategy(clear_cache_recovery, priority=1)
_exception_manager.register_global_recovery_strategy(reset_connections_recovery, priority=2)
_exception_manager.register_global_recovery_strategy(fallback_alternative_recovery, priority=3)


# Convenience functions for common exception patterns
def handle_with_context(operation_name: str, func, *args, **kwargs):
    """Execute function with unified exception context"""
    @with_unified_exception_handling(operation_name)
    def wrapped_func():
        return func(*args, **kwargs)
    return wrapped_func()


def safe_execute(operation_name: str, func, *args, fallback=None, **kwargs):
    """Safely execute function with fallback value"""
    try:
        return handle_with_context(operation_name, func, *args, **kwargs)
    except Exception:
        return fallback


def retry_with_recovery(operation_name: str, max_attempts: int = 3):
    """Simple retry decorator with built-in recovery strategies"""
    return with_unified_exception_handling(
        operation_name=operation_name,
        max_attempts=max_attempts,
        recovery_strategies=[
            clear_cache_recovery,
            reset_connections_recovery,
            fallback_alternative_recovery
        ]
    )


# Migration utilities to help transition existing code
def migrate_simple_try_except(operation_name: str):
    """
    Helper to migrate simple try/except blocks to unified handling.
    
    Before:
        try:
            result = risky_operation()
        except Exception as e:
            logger.error(f"Failed: {e}")
            return None
            
    After:
        @migrate_simple_try_except("risky_operation")
        def risky_operation():
            # original code without try/except
    """
    return with_unified_exception_handling(operation_name, fallback_value=None)


def create_exception_summary_report() -> dict:
    """Create comprehensive exception handling summary for monitoring"""
    manager = get_exception_manager()
    global_stats = manager.get_global_stats()
    
    return {
        'timestamp': datetime.now().isoformat(),
        'global_stats': global_stats,
        'active_contexts': len(manager.active_contexts),
        'recovery_strategies_registered': len(manager.global_recovery_strategies),
        'top_failing_operations': sorted(
            global_stats.get('by_operation', {}).items(),
            key=lambda x: x[1].get('exceptions', 0),
            reverse=True
        )[:10]
    }


# ============================================================================
# STANDARDIZED CONFIGURATION ACCESS (DRY ITERATION 1 - Step 3)
# ============================================================================

def get_config_value(key_path: str, default: Any = None, 
                    env_var: Optional[str] = None,
                    required: bool = False) -> Any:
    """
    Standardized configuration access with environment variable fallback.
    
    CONSOLIDATES PATTERNS:
    - config.get() usage across 50+ files
    - Mixed hardcoded defaults
    - Inconsistent environment variable handling
    
    Args:
        key_path: Dot-separated config path (e.g., "downloads.youtube.max_workers")
        default: Default value if not found
        env_var: Environment variable name to check first
        required: If True, raises ValueError when value not found
        
    Returns:
        Configuration value
        
    Raises:
        ValueError: If required=True and value not found
        
    Example:
        # Replace mixed patterns:
        max_workers = config.get('downloads.youtube.max_workers', 4)
        timeout = config['timeout']  # UNSAFE
        bucket = os.environ.get('S3_BUCKET', 'default-bucket')
        
        # With standardized pattern:
        max_workers = get_config_value('downloads.youtube.max_workers', 4)
        timeout = get_config_value('network.timeout', required=True)
        bucket = get_config_value('s3.bucket', 'default-bucket', env_var='S3_BUCKET')
    """
    # Check environment variable first if provided
    if env_var and env_var in os.environ:
        value = os.environ[env_var]
        # Convert common types
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        try:
            # Try int conversion
            return int(value)
        except ValueError:
            try:
                # Try float conversion
                return float(value)
            except ValueError:
                # Return as string
                return value
    
    # Get from config
    config = get_config()
    value = config.get(key_path, default)
    
    if required and value is None:
        raise ValueError(f"Required configuration key '{key_path}' not found")
    
    return value


def get_path_config(key_path: str, default: Optional[str] = None,
                   create_dirs: bool = False) -> Path:
    """
    Get path configuration with automatic path resolution.
    
    CONSOLIDATES PATH HANDLING patterns across modules.
    
    Args:
        key_path: Config path to directory/file path
        default: Default path if not in config
        create_dirs: Whether to create parent directories
        
    Returns:
        Resolved Path object
        
    Example:
        # Replace patterns like:
        csv_path = config.get("paths.output_csv", "outputs/output.csv")
        output_dir = Path(config.get("paths.outputs", "outputs"))
        
        # With:
        csv_path = get_path_config("paths.output_csv", "outputs/output.csv")
        output_dir = get_path_config("paths.outputs", "outputs", create_dirs=True)
    """
    path_str = get_config_value(key_path, default)
    if path_str is None:
        raise ValueError(f"Path configuration '{key_path}' not found and no default provided")
    
    path = Path(path_str)
    
    # Make relative paths relative to project root
    if not path.is_absolute():
        project_root = Path(__file__).parent.parent
        path = project_root / path
    
    if create_dirs:
        if path.suffix:  # It's a file path, create parent dirs
            path.parent.mkdir(parents=True, exist_ok=True)
        else:  # It's a directory path
            path.mkdir(parents=True, exist_ok=True)
    
    return path


class ConfigSection:
    """
    Wrapper for configuration sections with validation and defaults.
    
    ELIMINATES need for repeated config.get_section() calls and provides
    consistent access patterns with validation.
    """
    
    def __init__(self, section_name: str, defaults: Optional[Dict] = None):
        self.section_name = section_name
        self.defaults = defaults or {}
        self._config = get_config()
        self._section = self._config.get_section(section_name)
    
    def get(self, key: str, default: Any = None, 
           env_var: Optional[str] = None) -> Any:
        """Get value from section with environment fallback."""
        full_key = f"{self.section_name}.{key}"
        return get_config_value(full_key, default, env_var)
    
    def get_all(self) -> Dict[str, Any]:
        """Get all values in section merged with defaults."""
        result = self.defaults.copy()
        result.update(self._section)
        return result
    
    def validate_required(self, required_keys: List[str]) -> None:
        """Validate that all required keys exist in section."""
        missing = []
        for key in required_keys:
            if key not in self._section:
                missing.append(key)
        
        if missing:
            raise ValueError(f"Missing required configuration keys in section '{self.section_name}': {missing}")


# Standard configuration sections
def get_database_config() -> ConfigSection:
    """Get database configuration section."""
    return ConfigSection('database', defaults={
        'host': 'localhost',
        'port': 5432,
        'timeout': 30,
        'max_connections': 10
    })


def get_s3_config() -> ConfigSection:
    """Get S3 configuration section."""
    return ConfigSection('s3', defaults={
        'region': 'us-east-1',
        'timeout': 300,
        'max_retries': 3
    })


def get_download_config() -> ConfigSection:
    """Get download configuration section."""
    return ConfigSection('downloads', defaults={
        'max_workers': 4,
        'timeout': 300,
        'chunk_size': 8192,
        'max_retries': 3
    })


def get_retry_config() -> ConfigSection:
    """Get retry configuration section."""
    return ConfigSection('retry', defaults={
        'max_attempts': 3,
        'initial_delay': 1.0,
        'backoff_factor': 2.0,
        'max_delay': 60.0
    })


# ============================================================================
# MIGRATION HELPERS
# ============================================================================

def migrate_config_access(old_patterns: Dict[str, str]) -> Dict[str, str]:
    """
    Helper to show migration from old config access patterns.
    
    Args:
        old_patterns: Dict of old pattern -> recommended replacement
        
    Returns:
        Migration suggestions
    """
    return {
        # Direct access (UNSAFE)
        "config['key']": "get_config_value('key', default_value)",
        
        # Nested gets
        "config.get('section').get('key')": "get_config_value('section.key', default)",
        
        # Mixed environment
        "os.environ.get('VAR', config.get('key'))": "get_config_value('key', default, env_var='VAR')",
        
        # Path handling
        "Path(config.get('paths.output'))": "get_path_config('paths.output')",
        
        # Section access
        "config.get_section('db')": "get_database_config().get_all()",
    }


def validate_configuration() -> Dict[str, Any]:
    """
    Validate current configuration and return report.
    
    Returns:
        Dictionary with validation results
    """
    report = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'sections_found': [],
        'timestamp': datetime.now().isoformat()
    }
    
    config = get_config()
    
    # Check critical sections exist
    critical_sections = ['paths', 'downloads', 's3']
    for section in critical_sections:
        if section in config.all:
            report['sections_found'].append(section)
        else:
            report['errors'].append(f"Missing critical section: {section}")
            report['valid'] = False
    
    # Check critical paths
    critical_paths = [
        'paths.output_csv',
        'paths.downloads',
        'paths.logs'
    ]
    
    for path_key in critical_paths:
        try:
            path = get_path_config(path_key)
            if not path.parent.exists():
                report['warnings'].append(f"Parent directory doesn't exist for {path_key}: {path.parent}")
        except (ValueError, KeyError):
            report['warnings'].append(f"Path configuration missing: {path_key}")
    
    return report


