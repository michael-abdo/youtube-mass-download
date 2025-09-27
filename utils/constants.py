#!/usr/bin/env python3
"""
Shared Constants Module (DRY Phase 12)

Consolidates constants, magic numbers, and repeated values from throughout the codebase:
- File extensions and formats
- URL patterns and domains
- Timeout values and HTTP status codes
- Encoding and buffer size constants
- Path patterns and directory structures
- Validation patterns and magic numbers

This eliminates hardcoded values scattered across 50+ files and provides
a single source of truth for all system constants.
"""

import re
from enum import Enum
from typing import Dict, List, Set, Pattern, Optional


# ============================================================================
# FILE SYSTEM CONSTANTS
# ============================================================================

class FileExtensions:
    """File extension constants."""
    CSV = '.csv'
    JSON = '.json'
    YAML = '.yaml'
    YML = '.yml'
    TXT = '.txt'
    LOG = '.log'
    PY = '.py'
    HTML = '.html'
    XML = '.xml'
    MD = '.md'
    PDF = '.pdf'
    ZIP = '.zip'
    
    # Media extensions
    MP4 = '.mp4'
    MP3 = '.mp3'
    WAV = '.wav'
    WEBM = '.webm'
    AVI = '.avi'
    MOV = '.mov'
    
    # Image extensions
    JPG = '.jpg'
    JPEG = '.jpeg'
    PNG = '.png'
    GIF = '.gif'
    WEBP = '.webp'
    
    # Document extensions
    DOCX = '.docx'
    DOC = '.doc'
    XLSX = '.xlsx'
    XLS = '.xls'
    PPTX = '.pptx'
    PPT = '.ppt'
    
    @classmethod
    def all_media(cls) -> List[str]:
        """Get all media file extensions."""
        return [cls.MP4, cls.MP3, cls.WAV, cls.WEBM, cls.AVI, cls.MOV]
    
    @classmethod
    def all_images(cls) -> List[str]:
        """Get all image file extensions."""
        return [cls.JPG, cls.JPEG, cls.PNG, cls.GIF, cls.WEBP]
    
    @classmethod
    def all_documents(cls) -> List[str]:
        """Get all document file extensions."""
        return [cls.DOCX, cls.DOC, cls.XLSX, cls.XLS, cls.PPTX, cls.PPT, cls.PDF]


class FilePaths:
    """Standard file path constants."""
    # Default CSV file paths
    DEFAULT_CSV = 'outputs/output.csv'
    DEFAULT_CSV_BACKUP = 'outputs/output.csv.backup'
    DEFAULT_CSV_LOCK = 'outputs/output.csv.lock'
    DEFAULT_CSV_TEMP_SUFFIX = '.temp'
    
    # Configuration paths
    CONFIG_DIR = 'config'
    CONFIG_FILE = 'config/config.yaml'
    
    # Output directories
    OUTPUTS_DIR = 'outputs'
    DOWNLOADS_DIR = 'downloads'
    DRIVE_DOWNLOADS_DIR = 'drive_downloads'
    BACKUPS_DIR = 'outputs/backups'
    LOGS_DIR = 'logs'
    REPORTS_DIR = 'reports'
    TESTS_DIR = 'tests'
    UTILS_DIR = 'utils'
    DOCS_DIR = 'docs'
    
    # Archive directories
    ARCHIVED_DIR = 'archived'
    CLEANUP_DIR = 'archived/cleanup_{timestamp}'
    
    # Log files
    DEFAULT_LOG_FILE = 'logs/app.log'
    ERROR_LOG_FILE = 'logs/error.log'
    DEBUG_LOG_FILE = 'logs/debug.log'
    
    # State files
    EXTRACTION_PROGRESS = 'extraction_progress.json'
    FAILED_EXTRACTIONS = 'failed_extractions.json'
    
    # Temporary file patterns
    TEMP_PREFIX = 'temp_'
    BACKUP_PREFIX = 'backup_'
    LOCK_SUFFIX = '.lock'


class DirectoryStructure:
    """Standard directory structure constants."""
    PERSON_DIR_PATTERN = '{row_id}_{name}'  # e.g., "123_John_Doe"
    MEDIA_DIR_PATTERN = '{row_id}_{name}/media'
    DOCUMENTS_DIR_PATTERN = '{row_id}_{name}/documents'
    METADATA_DIR_PATTERN = '{row_id}_{name}/metadata'


# ============================================================================
# ENCODING AND FORMAT CONSTANTS
# ============================================================================

class Encoding:
    """Text encoding constants."""
    UTF8 = 'utf-8'
    UTF8_BOM = 'utf-8-sig'
    ASCII = 'ascii'
    LATIN1 = 'latin-1'
    
    # CSV specific
    CSV_ENCODING = UTF8
    CSV_NEWLINE = ''  # For Python CSV module
    
    # File operations
    DEFAULT_FILE_ENCODING = UTF8
    LOG_FILE_ENCODING = UTF8


class CSVConstants:
    """CSV file operation constants."""
    DELIMITER = ','
    QUOTECHAR = '"'
    ENCODING = Encoding.CSV_ENCODING
    NEWLINE = Encoding.CSV_NEWLINE
    LINK_SEPARATOR = '|'  # For multiple links in one field
    
    # Field size limits
    MAX_FIELD_SIZE = 100000  # 100KB limit for CSV fields
    
    # Standard column names
    class Columns:
        ROW_ID = 'row_id'
        NAME = 'name'
        EMAIL = 'email'
        TYPE = 'type'
        LINK = 'link'
        YOUTUBE_PLAYLIST = 'youtube_playlist'
        GOOGLE_DRIVE = 'google_drive'
        EXTRACTED_LINKS = 'extracted_links'
        DOCUMENT_TEXT = 'document_text'
        PROCESSED = 'processed'
        DOWNLOAD_STATUS = 'download_status'
        MEDIA_ID = 'media_id'
        FILE_PATH = 'file_path'
        ERROR_MESSAGE = 'error_message'
        LAST_MODIFIED = 'last_modified'
        
        # Additional columns from codebase usage
        CREATED_AT = 'created_at'
        DOWNLOAD_ERRORS = 'download_errors'
        DRIVE_STATUS = 'drive_status'
        FILE_ID = 'file_id'
        FILE_UUIDS = 'file_uuids'
        LAST_DOWNLOAD_ATTEMPT = 'last_download_attempt'
        NOTES = 'notes'
        S3_PATHS = 's3_paths'
        YOUTUBE_STATUS = 'youtube_status'


# ============================================================================
# NETWORK AND HTTP CONSTANTS
# ============================================================================

class HTTPConstants:
    """HTTP related constants."""
    # Status codes
    OK = 200
    CREATED = 201
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    METHOD_NOT_ALLOWED = 405
    REQUEST_TIMEOUT = 408
    TOO_MANY_REQUESTS = 429
    INTERNAL_SERVER_ERROR = 500
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504
    
    # Retry status codes (codes that should trigger retries)
    RETRY_STATUS_CODES = [REQUEST_TIMEOUT, TOO_MANY_REQUESTS, INTERNAL_SERVER_ERROR, 
                         BAD_GATEWAY, SERVICE_UNAVAILABLE, GATEWAY_TIMEOUT]
    
    # Headers
    USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ACCEPT_HTML = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    ACCEPT_JSON = 'application/json'
    ACCEPT_LANGUAGE = 'en-US,en;q=0.5'
    
    # Content types
    CONTENT_TYPE_JSON = 'application/json'
    CONTENT_TYPE_HTML = 'text/html'
    CONTENT_TYPE_XML = 'text/xml'
    CONTENT_TYPE_PLAIN = 'text/plain'
    CONTENT_TYPE_OCTET_STREAM = 'application/octet-stream'


class TimeoutConstants:
    """Timeout values in seconds."""
    # HTTP request timeouts
    HTTP_REQUEST_SHORT = 10
    HTTP_REQUEST_DEFAULT = 30
    HTTP_REQUEST_LONG = 60
    HTTP_REQUEST_VERY_LONG = 120
    
    # Download timeouts
    FILE_DOWNLOAD_DEFAULT = 300  # 5 minutes
    FILE_DOWNLOAD_LARGE = 600    # 10 minutes
    FILE_DOWNLOAD_HUGE = 1800    # 30 minutes
    
    # Database timeouts
    DATABASE_CONNECTION = 30
    DATABASE_QUERY = 60
    
    # Process timeouts
    SUBPROCESS_DEFAULT = 30
    SUBPROCESS_DOWNLOAD = 300
    SUBPROCESS_LONG = 600
    
    # File operation timeouts
    FILE_LOCK_DEFAULT = 5.0
    FILE_LOCK_SHORT = 2.0
    FILE_LOCK_LONG = 10.0
    
    # Retry delays
    RETRY_BASE_DELAY = 1.0
    RETRY_MAX_DELAY = 60.0
    RETRY_BACKOFF_FACTOR = 2.0


class BufferSizes:
    """Buffer and chunk size constants."""
    # Standard buffer sizes
    SMALL_BUFFER = 1024          # 1KB
    DEFAULT_BUFFER = 8192        # 8KB  
    LARGE_BUFFER = 65536         # 64KB
    HUGE_BUFFER = 1048576        # 1MB
    
    # Download chunk sizes
    DOWNLOAD_CHUNK_SMALL = 8192      # 8KB
    DOWNLOAD_CHUNK_DEFAULT = 65536   # 64KB
    DOWNLOAD_CHUNK_LARGE = 1048576   # 1MB
    
    # File processing
    FILE_READ_CHUNK = 8192
    LOG_ROTATION_SIZE = 10485760     # 10MB
    
    # Memory limits
    MAX_MEMORY_USAGE = 1073741824    # 1GB


# ============================================================================
# URL AND DOMAIN CONSTANTS
# ============================================================================

class Domains:
    """Domain name constants."""
    # YouTube domains
    YOUTUBE_MAIN = 'youtube.com'
    YOUTUBE_SHORT = 'youtu.be'
    YOUTUBE_MOBILE = 'm.youtube.com'
    YOUTUBE_MUSIC = 'music.youtube.com'
    
    # Google Drive domains
    DRIVE_MAIN = 'drive.google.com'
    DRIVE_DOCS = 'docs.google.com'
    DRIVE_CONTENT = 'drive.usercontent.google.com'
    
    # Google Sheets domains
    SHEETS_MAIN = 'docs.google.com'
    
    @classmethod
    def youtube_domains(cls) -> List[str]:
        """Get all YouTube domains."""
        return [cls.YOUTUBE_MAIN, cls.YOUTUBE_SHORT, cls.YOUTUBE_MOBILE, cls.YOUTUBE_MUSIC]
    
    @classmethod
    def drive_domains(cls) -> List[str]:
        """Get all Google Drive domains."""
        return [cls.DRIVE_MAIN, cls.DRIVE_DOCS, cls.DRIVE_CONTENT]


class URLPatterns:
    """URL pattern constants and compiled regex patterns."""
    
    # Protocol patterns
    HTTP_PROTOCOL = 'http://'
    HTTPS_PROTOCOL = 'https://'
    
    # YouTube URL patterns
    YOUTUBE_WATCH = 'https://www.youtube.com/watch?v='
    YOUTUBE_SHORT = 'https://youtu.be/'
    YOUTUBE_PLAYLIST = 'https://www.youtube.com/playlist?list='
    YOUTUBE_WATCH_VIDEOS = 'https://www.youtube.com/watch_videos?video_ids='
    
    # Google Drive URL patterns
    DRIVE_FILE = 'https://drive.google.com/file/d/'
    DRIVE_OPEN = 'https://drive.google.com/open?id='
    DRIVE_FOLDER = 'https://drive.google.com/drive/folders/'
    DRIVE_DOCUMENT = 'https://docs.google.com/document/d/'
    DRIVE_SPREADSHEET = 'https://docs.google.com/spreadsheets/d/'
    DRIVE_PRESENTATION = 'https://docs.google.com/presentation/d/'
    DRIVE_DOWNLOAD = 'https://drive.google.com/uc?export=download&id='
    DRIVE_DIRECT_DOWNLOAD = 'https://drive.usercontent.google.com/download'
    
    # Compiled regex patterns (from patterns.py consolidation)
    YOUTUBE_VIDEO_ID = re.compile(r'(?:v=|youtu\.be/)([a-zA-Z0-9_-]{11})')
    DRIVE_FILE_ID = re.compile(r'(?:file/d/|open\?id=|document/d/)([a-zA-Z0-9_-]+)')
    DRIVE_FOLDER_ID = re.compile(r'folders/([a-zA-Z0-9_-]+)')
    
    # Video ID validation
    YOUTUBE_VIDEO_ID_LENGTH = 11
    YOUTUBE_VIDEO_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{11}$')
    
    # Drive ID validation  
    DRIVE_ID_MIN_LENGTH = 25
    DRIVE_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{25,}$')
    
    # DRY CONSOLIDATION - Step 1: Centralized URL construction methods
    @staticmethod
    def youtube_watch_url(video_id: str) -> str:
        """Construct YouTube watch URL from video ID."""
        return f"{URLPatterns.YOUTUBE_WATCH}{video_id}"
    
    @staticmethod
    def youtube_short_url(video_id: str) -> str:
        """Construct YouTube short URL from video ID."""
        return f"{URLPatterns.YOUTUBE_SHORT}{video_id}"
    
    @staticmethod
    def youtube_playlist_url(playlist_id: str) -> str:
        """Construct YouTube playlist URL from playlist ID."""
        return f"{URLPatterns.YOUTUBE_PLAYLIST}{playlist_id}"
    
    @staticmethod
    def drive_file_url(file_id: str, view: bool = True) -> str:
        """Construct Google Drive file URL from file ID."""
        base_url = f"{URLPatterns.DRIVE_FILE}{file_id}"
        return f"{base_url}/view" if view else base_url
    
    @staticmethod
    def drive_open_url(file_id: str) -> str:
        """Construct Google Drive open URL from file ID."""
        return f"{URLPatterns.DRIVE_OPEN}{file_id}"
    
    @staticmethod
    def drive_folder_url(folder_id: str) -> str:
        """Construct Google Drive folder URL from folder ID."""
        return f"{URLPatterns.DRIVE_FOLDER}{folder_id}"
    
    @staticmethod
    def drive_download_url(file_id: str, confirm: Optional[str] = None) -> str:
        """Construct Google Drive download URL from file ID."""
        url = f"{URLPatterns.DRIVE_DOWNLOAD}{file_id}"
        if confirm:
            url += f"&confirm={confirm}"
        return url
    
    @staticmethod
    def drive_direct_download_url(file_id: str, uuid: Optional[str] = None, confirm: Optional[str] = None) -> str:
        """Construct Google Drive direct download URL with parameters."""
        params = [f"id={file_id}", "export=download"]
        if confirm:
            params.append(f"confirm={confirm}")
        if uuid:
            params.append(f"uuid={uuid}")
        return f"{URLPatterns.DRIVE_DIRECT_DOWNLOAD}?{'&'.join(params)}"
    
    @staticmethod
    def docs_document_url(doc_id: str, edit: bool = False) -> str:
        """Construct Google Docs document URL from doc ID."""
        base_url = f"{URLPatterns.DRIVE_DOCUMENT}{doc_id}"
        return f"{base_url}/edit" if edit else base_url
    
    @staticmethod
    def docs_spreadsheet_url(sheet_id: str, edit: bool = False) -> str:
        """Construct Google Sheets spreadsheet URL from sheet ID."""
        base_url = f"{URLPatterns.DRIVE_SPREADSHEET}{sheet_id}"
        return f"{base_url}/edit" if edit else base_url


# ============================================================================
# SIZE AND UNIT CONSTANTS
# ============================================================================

class SizeConstants:
    """File size and unit constants."""
    # Byte conversion factors
    BYTES_PER_KB = 1024
    BYTES_PER_MB = 1024 * 1024
    BYTES_PER_GB = 1024 * 1024 * 1024
    BYTES_PER_TB = 1024 * 1024 * 1024 * 1024
    
    # File size limits
    MAX_CSV_FIELD_SIZE = 100000      # 100KB
    MAX_LOG_FILE_SIZE = 10485760     # 10MB
    MAX_SMALL_FILE = 1048576         # 1MB
    MAX_MEDIUM_FILE = 104857600      # 100MB
    MAX_LARGE_FILE = 1073741824      # 1GB
    
    # Download size limits
    YOUTUBE_MAX_SIZE = '100M'        # For yt-dlp
    DRIVE_LARGE_FILE_THRESHOLD = 104857600  # 100MB (triggers virus scan)
    
    @staticmethod
    def bytes_to_mb(bytes_val: int) -> float:
        """Convert bytes to megabytes."""
        return bytes_val / SizeConstants.BYTES_PER_MB
    
    @staticmethod
    def bytes_to_gb(bytes_val: int) -> float:
        """Convert bytes to gigabytes."""
        return bytes_val / SizeConstants.BYTES_PER_GB
    
    @staticmethod
    def mb_to_bytes(mb_val: float) -> int:
        """Convert megabytes to bytes."""
        return int(mb_val * SizeConstants.BYTES_PER_MB)


# ============================================================================
# VALIDATION CONSTANTS
# ============================================================================

class ValidationConstants:
    """Input validation constants."""
    
    # YouTube validation
    YOUTUBE_VIDEO_ID_LENGTH = 11
    YOUTUBE_VIDEO_ID_CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-'
    
    # Email validation
    EMAIL_MAX_LENGTH = 254
    EMAIL_LOCAL_MAX_LENGTH = 64
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    
    # File name validation
    FILENAME_MAX_LENGTH = 255
    FILENAME_INVALID_CHARS = r'[<>:"/\\|?*\x00-\x1f]'
    FILENAME_RESERVED_NAMES = {
        'CON', 'PRN', 'AUX', 'NUL',
        'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
        'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
    }
    
    # Path validation
    PATH_MAX_LENGTH = 260
    PATH_TRAVERSAL_PATTERNS = ['../', '..\\', '..']
    SUSPICIOUS_PATH_CHARS = ['|', ';', '&', '$', '`', '<', '>', '"', "'"]
    
    # URL validation
    ALLOWED_URL_SCHEMES = ['http', 'https']
    SUSPICIOUS_URL_PATTERNS = [
        r'[;|`$]',      # Command separators
        r'\\.\\./',      # Path traversal
        r'%00',         # Null byte
        r'\\$\\(',      # Command substitution
        r'\\{.*\\}',    # Variable expansion
        r"'.*&&.*'",    # Command injection
        r'".*&&.*"',    # Command injection
        r'javascript:', # JavaScript protocol
        r'data:',       # Data protocol
        r'file:',       # File protocol
        r'ftp:',        # FTP protocol
    ]


# ============================================================================
# ERROR AND LOG CONSTANTS
# ============================================================================

class ErrorCodes:
    """Standard error codes."""
    # System errors
    SUCCESS = 0
    GENERAL_ERROR = 1
    FILE_NOT_FOUND = 2
    PERMISSION_DENIED = 3
    NETWORK_ERROR = 4
    TIMEOUT_ERROR = 5
    VALIDATION_ERROR = 6
    CONFIGURATION_ERROR = 7
    
    # Application specific errors
    CSV_PARSING_ERROR = 100
    DOWNLOAD_ERROR = 101
    UPLOAD_ERROR = 102
    EXTRACTION_ERROR = 103
    PROCESSING_ERROR = 104
    
    # HTTP error codes (mapped from HTTPConstants)
    HTTP_BAD_REQUEST = HTTPConstants.BAD_REQUEST
    HTTP_UNAUTHORIZED = HTTPConstants.UNAUTHORIZED
    HTTP_FORBIDDEN = HTTPConstants.FORBIDDEN
    HTTP_NOT_FOUND = HTTPConstants.NOT_FOUND
    HTTP_TIMEOUT = HTTPConstants.REQUEST_TIMEOUT
    HTTP_SERVER_ERROR = HTTPConstants.INTERNAL_SERVER_ERROR


class LogLevels:
    """Logging level constants."""
    CRITICAL = 'CRITICAL'
    ERROR = 'ERROR'
    WARNING = 'WARNING'
    INFO = 'INFO'
    DEBUG = 'DEBUG'
    
    # Numeric levels
    CRITICAL_NUM = 50
    ERROR_NUM = 40
    WARNING_NUM = 30
    INFO_NUM = 20
    DEBUG_NUM = 10


class LogFormats:
    """Log format constants."""
    DETAILED = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    SIMPLE = '%(asctime)s - %(levelname)s - %(message)s'
    MINIMAL = '%(levelname)s: %(message)s'
    
    # Date formats
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    DATE_FORMAT_FILENAME = '%Y%m%d_%H%M%S'


# ============================================================================
# DATABASE CONSTANTS
# ============================================================================

class DatabaseConstants:
    """Database related constants."""
    # Default database configuration
    DEFAULT_DB_TYPE = 'sqlite'
    DEFAULT_DB_NAME = 'app.db'
    DEFAULT_PORT_POSTGRESQL = 5432
    DEFAULT_PORT_MYSQL = 3306
    
    # Connection pool settings
    DEFAULT_POOL_SIZE = 5
    MAX_POOL_SIZE = 20
    
    # Query limits
    DEFAULT_SELECT_LIMIT = 1000
    MAX_SELECT_LIMIT = 10000
    
    # Table names
    MIGRATIONS_TABLE = 'migrations'
    CONFIG_TABLE = 'config'
    LOGS_TABLE = 'logs'


# ============================================================================
# PERFORMANCE CONSTANTS
# ============================================================================

class PerformanceConstants:
    """Performance tuning constants."""
    # Threading
    DEFAULT_MAX_WORKERS = 4
    MAX_CONCURRENT_DOWNLOADS = 10
    MAX_CONCURRENT_UPLOADS = 5
    
    # Retry configuration
    DEFAULT_MAX_RETRIES = 3
    MAX_RETRY_ATTEMPTS = 10
    
    # Progress reporting intervals
    PROGRESS_REPORT_INTERVAL = 100  # Report every N items
    PROGRESS_UPDATE_FREQUENCY = 5   # Update every N seconds
    
    # Memory management
    MEMORY_CHECK_INTERVAL = 1000    # Check memory every N operations
    MAX_MEMORY_THRESHOLD = 0.8      # 80% of available memory


# ============================================================================
# S3 AND CLOUD CONSTANTS
# ============================================================================

class S3Constants:
    """AWS S3 related constants."""
    # Default settings
    DEFAULT_REGION = 'us-east-1'
    DEFAULT_STORAGE_CLASS = 'STANDARD'
    
    # Upload settings
    MULTIPART_THRESHOLD = 8 * 1024 * 1024  # 8MB
    MULTIPART_CHUNKSIZE = 8 * 1024 * 1024  # 8MB
    MAX_CONCURRENCY = 10
    
    # Content types
    CONTENT_TYPE_VIDEO = 'video/mp4'
    CONTENT_TYPE_AUDIO = 'audio/mpeg'
    CONTENT_TYPE_DOCUMENT = 'application/octet-stream'
    
    # Key patterns
    KEY_PATTERN_PERSON = '{row_id}_{name}'
    KEY_PATTERN_MEDIA = 'media/{uuid}.{ext}'
    KEY_PATTERN_DOCUMENT = 'documents/{uuid}.{ext}'
    
    # UUID-based storage patterns (DRY CONSOLIDATION - Step 1)
    UUID_FILES_PREFIX = 'files/'  # Standard prefix for UUID-based storage
    KEY_PATTERN_UUID_FILE = 'files/{uuid}{ext}'  # e.g., files/abc123.mp3


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_all_constants() -> Dict[str, any]:
    """
    Get all constants as a dictionary for debugging/inspection.
    
    Returns:
        Dictionary of all constant classes and their values
    """
    constants = {}
    
    # Get all classes defined in this module
    import sys
    current_module = sys.modules[__name__]
    
    for name in dir(current_module):
        obj = getattr(current_module, name)
        if isinstance(obj, type) and name.endswith('Constants') or name in [
            'FileExtensions', 'FilePaths', 'Domains', 'URLPatterns', 'Encoding', 
            'CSVConstants', 'HTTPConstants', 'TimeoutConstants', 'BufferSizes',
            'SizeConstants', 'ValidationConstants', 'ErrorCodes', 'LogLevels',
            'LogFormats', 'DatabaseConstants', 'PerformanceConstants', 'S3Constants'
        ]:
            constants[name] = {}
            for attr_name in dir(obj):
                if not attr_name.startswith('_') and not callable(getattr(obj, attr_name)):
                    constants[name][attr_name] = getattr(obj, attr_name)
    
    return constants


def validate_constant_usage():
    """
    Validate that constants are being used correctly.
    This function can be extended to check for deprecated constants.
    """
    # This could be extended to scan codebase for hardcoded values
    # that should use constants instead
    pass


# ============================================================================
# BACKWARDS COMPATIBILITY ALIASES
# ============================================================================

# Common constants that are frequently used
DEFAULT_CSV_FILE = FilePaths.DEFAULT_CSV
YOUTUBE_VIDEO_ID_LENGTH = ValidationConstants.YOUTUBE_VIDEO_ID_LENGTH
HTTP_OK = HTTPConstants.OK
DEFAULT_TIMEOUT = TimeoutConstants.HTTP_REQUEST_DEFAULT
DEFAULT_CHUNK_SIZE = BufferSizes.DOWNLOAD_CHUNK_DEFAULT
UTF8_ENCODING = Encoding.UTF8


if __name__ == "__main__":
    # Test constants module
    print("🔧 Testing Constants Module")
    print("=" * 50)
    
    # Test file extensions
    print(f"Media extensions: {FileExtensions.all_media()}")
    print(f"CSV encoding: {Encoding.CSV_ENCODING}")
    print(f"Default timeout: {TimeoutConstants.HTTP_REQUEST_DEFAULT}s")
    print(f"YouTube domains: {Domains.youtube_domains()}")
    print(f"1MB in bytes: {SizeConstants.BYTES_PER_MB:,}")
    
    # Test conversions
    file_size_mb = SizeConstants.bytes_to_mb(5242880)  # 5MB
    print(f"5MB file size: {file_size_mb} MB")
    
    # Test pattern matching
    test_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    match = URLPatterns.YOUTUBE_VIDEO_ID.search(test_url)
    if match:
        print(f"YouTube video ID: {match.group(1)}")
    
    # Show all constants summary
    all_constants = get_all_constants()
    print(f"\nLoaded {len(all_constants)} constant categories")
    for category, constants in all_constants.items():
        print(f"- {category}: {len(constants)} constants")
    
    print("\n✓ Constants module is ready!")