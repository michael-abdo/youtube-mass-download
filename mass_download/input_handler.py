#!/usr/bin/env python3
"""
Input Handler Module with Format Detection
Phase 3.1: Create input handler module with format detection

Handles multiple input formats for mass download:
- CSV files with channel lists
- JSON files with structured data
- Plain text files with URLs
- Command line arguments

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import os
import json
import csv
import re
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Union, Tuple
from enum import Enum
import logging

# Add parent directory to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

# Initialize logger using standard logging
logger = logging.getLogger(__name__)

# Import from same package
try:
    from .database_schema import PersonRecord
    from .channel_discovery import YouTubeChannelDiscovery
except ImportError:
    # Fallback for testing or direct execution
    try:
        from database_schema import PersonRecord
        from channel_discovery import YouTubeChannelDiscovery
    except ImportError as e:
        print(f"CRITICAL ERROR: Failed to import required modules: {e}")
        print("Ensure all dependencies are properly installed.")
        sys.exit(1)

# Simple validation function (inline implementation)
def validate_youtube_url(url):
    """Simple YouTube URL validation."""
    if not url or not isinstance(url, str):
        return False
    return "youtube.com" in url or "youtu.be" in url

# Simple config loader (inline implementation)
def get_config():
    """Get configuration with fallback values."""
    return {}


class InputFormat(Enum):
    """Supported input formats for mass download."""
    CSV = "csv"
    JSON = "json"
    TEXT = "text"
    UNKNOWN = "unknown"


@dataclass
class ChannelInput:
    """
    Represents a single channel input with metadata.
    
    Implements fail-fast validation on creation.
    """
    name: str
    channel_url: str
    email: Optional[str] = None
    type: Optional[str] = None
    additional_data: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Validate channel input with fail-fast principles."""
        self.validate()
    
    def validate(self) -> None:
        """
        Fail-fast validation of channel input.
        
        Raises:
            ValueError: If validation fails (fail-loud)
        """
        if not self.name or not isinstance(self.name, str):
            raise ValueError(
                f"VALIDATION ERROR: Channel name is required and must be non-empty string. "
                f"Got: {self.name}"
            )
        
        if not self.channel_url or not isinstance(self.channel_url, str):
            raise ValueError(
                f"VALIDATION ERROR: Channel URL is required and must be non-empty string. "
                f"Got: {self.channel_url}"
            )
        
        # Validate YouTube URL format
        if not self.channel_url.startswith(("https://youtube.com/", "https://www.youtube.com/", "youtube.com/", "www.youtube.com/")):
            raise ValueError(
                f"VALIDATION ERROR: Invalid YouTube channel URL format. "
                f"URL must be a YouTube channel URL. Got: {self.channel_url}"
            )
        
        # Email validation if provided
        if self.email:
            if not isinstance(self.email, str):
                raise ValueError(
                    f"VALIDATION ERROR: Email must be a string. "
                    f"Got: {type(self.email).__name__}"
                )
            
            # Basic email validation
            if "@" not in self.email:
                raise ValueError(
                    f"VALIDATION ERROR: Email must contain @. "
                    f"Got: {self.email}"
                )
            
            # Check for spaces (not allowed in email addresses)
            if " " in self.email:
                raise ValueError(
                    f"VALIDATION ERROR: Email cannot contain spaces. "
                    f"Got: {self.email}"
                )
            
            # Check for consecutive dots
            if ".." in self.email:
                raise ValueError(
                    f"VALIDATION ERROR: Email cannot contain consecutive dots. "
                    f"Got: {self.email}"
                )
            
            parts = self.email.split("@")
            if len(parts) != 2 or not parts[0] or not parts[1]:
                raise ValueError(
                    f"VALIDATION ERROR: Invalid email format. "
                    f"Got: {self.email}"
                )
            
            # Check for basic domain structure
            if "." not in parts[1]:
                raise ValueError(
                    f"VALIDATION ERROR: Email domain must contain a dot. "
                    f"Got: {self.email}"
                )
            
            # Check domain parts are not empty
            domain_parts = parts[1].split(".")
            if any(not part for part in domain_parts):
                raise ValueError(
                    f"VALIDATION ERROR: Email domain parts cannot be empty. "
                    f"Got: {self.email}"
                )
    
    def to_person_record(self) -> PersonRecord:
        """Convert to PersonRecord for database storage."""
        return PersonRecord(
            name=self.name,
            email=self.email,
            type=self.type,
            channel_url=self.channel_url
        )


class InputHandler:
    """
    Handles multiple input formats for mass download with fail-safe format detection.
    """
    
    def __init__(self):
        """Initialize input handler with configuration."""
        self.config = get_config()
        self.discovery = None  # Lazy initialization
        
        # Supported file extensions
        self.format_extensions = {
            InputFormat.CSV: [".csv"],
            InputFormat.JSON: [".json"],
            InputFormat.TEXT: [".txt", ".text"]
        }
        
        logger.info("InputHandler initialized with format detection")
    
    def detect_format(self, file_path: str) -> InputFormat:
        """
        Detect input file format based on extension and content.
        
        Args:
            file_path: Path to input file
            
        Returns:
            InputFormat enum value
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"INPUT ERROR: Input file not found: {file_path}"
            )
        
        file_path = Path(file_path)
        extension = file_path.suffix.lower()
        
        # Check extension first
        for format_type, extensions in self.format_extensions.items():
            if extension in extensions:
                logger.info(f"Detected format {format_type.value} based on extension: {extension}")
                return format_type
        
        # If no extension match, try to detect from content
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                
                # Check for JSON
                if first_line.startswith(("{", "[")):
                    try:
                        f.seek(0)
                        json.load(f)
                        logger.info("Detected JSON format from content")
                        return InputFormat.JSON
                    except json.JSONDecodeError:
                        pass
                
                # Check for CSV (has commas and possibly headers)
                if "," in first_line:
                    logger.info("Detected CSV format from content (found commas)")
                    return InputFormat.CSV
                
                # Check for YouTube URLs
                if "youtube.com" in first_line or "youtu.be" in first_line:
                    logger.info("Detected TEXT format with YouTube URLs")
                    return InputFormat.TEXT
        
        except Exception as e:
            logger.warning(f"Error detecting format from content: {e}")
        
        # Default to TEXT if uncertain
        logger.warning(f"Could not detect format for {file_path}, defaulting to TEXT")
        return InputFormat.TEXT
    
    def validate_input_file(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate input file with comprehensive checks.
        
        Args:
            file_path: Path to input file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Check file exists
            if not os.path.exists(file_path):
                return False, f"File not found: {file_path}"
            
            # Check file is readable
            if not os.access(file_path, os.R_OK):
                return False, f"File is not readable: {file_path}"
            
            # Check file size (fail-safely with reasonable limits)
            file_size = os.path.getsize(file_path)
            max_size = 100 * 1024 * 1024  # 100MB limit
            
            if file_size == 0:
                return False, "File is empty"
            
            if file_size > max_size:
                return False, f"File too large: {file_size} bytes (max: {max_size} bytes)"
            
            # Check encoding
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    f.read(1024)  # Read first 1KB to check encoding
            except UnicodeDecodeError:
                return False, "File is not valid UTF-8 encoded"
            
            return True, ""
            
        except Exception as e:
            return False, f"Validation error: {e}"
    
    def parse_input_file(self, file_path: str) -> List[ChannelInput]:
        """
        Parse input file and return list of channel inputs.
        
        Args:
            file_path: Path to input file
            
        Returns:
            List of ChannelInput objects
            
        Raises:
            ValueError: If file validation fails
            RuntimeError: If parsing fails
        """
        # Validate file first (fail-fast)
        is_valid, error_msg = self.validate_input_file(file_path)
        if not is_valid:
            raise ValueError(
                f"INPUT VALIDATION ERROR: {error_msg}"
            )
        
        # Detect format
        format_type = self.detect_format(file_path)
        
        # Parse based on format
        try:
            if format_type == InputFormat.CSV:
                return self._parse_csv_file(file_path)
            elif format_type == InputFormat.JSON:
                return self._parse_json_file(file_path)
            elif format_type == InputFormat.TEXT:
                return self._parse_text_file(file_path)
            else:
                raise RuntimeError(
                    f"PARSE ERROR: Unsupported format: {format_type.value}"
                )
        except Exception as e:
            logger.error(f"Failed to parse {format_type.value} file: {e}")
            raise
    
    def _parse_csv_file(self, file_path: str) -> List[ChannelInput]:
        """
        Parse CSV file with channel information.
        
        Expected CSV columns (flexible order):
        - name: Channel/Person name (required)
        - url/channel_url: YouTube channel URL (required)
        - email: Email address (optional)
        - type: Type/category (optional)
        - Any additional columns stored in additional_data
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            List of ChannelInput objects
            
        Raises:
            ValueError: If required columns are missing
            RuntimeError: If CSV parsing fails
        """
        channel_inputs = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                # Detect CSV dialect
                sample = csvfile.read(1024)
                csvfile.seek(0)
                
                try:
                    dialect = csv.Sniffer().sniff(sample)
                except csv.Error:
                    # Default to standard CSV if detection fails
                    dialect = csv.excel
                
                reader = csv.DictReader(csvfile, dialect=dialect)
                
                # Validate required columns (fail-fast)
                if not reader.fieldnames:
                    raise ValueError("CSV file has no headers")
                
                # Normalize field names (case-insensitive)
                fieldnames_lower = [f.lower().strip() for f in reader.fieldnames]
                
                # Check for required fields
                has_name = any(field in ['name', 'channel_name', 'channel name', 'person_name', 'person name', 'channel'] 
                             for field in fieldnames_lower)
                has_url = any(field in ['url', 'channel_url', 'channel url', 'youtube_url', 'youtube url', 'link'] 
                            for field in fieldnames_lower)
                
                if not has_name:
                    raise ValueError(
                        f"CSV ERROR: Missing required 'name' column. "
                        f"Available columns: {reader.fieldnames}"
                    )
                
                if not has_url:
                    raise ValueError(
                        f"CSV ERROR: Missing required 'url' or 'channel_url' column. "
                        f"Available columns: {reader.fieldnames}"
                    )
                
                # Map normalized field names
                field_mapping = self._create_csv_field_mapping(reader.fieldnames)
                
                # Parse rows
                for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is 1)
                    try:
                        # Skip empty rows
                        if not any(row.values()):
                            continue
                        
                        # Extract fields using mapping
                        name = self._extract_csv_field(row, field_mapping['name'])
                        url = self._extract_csv_field(row, field_mapping['url'])
                        
                        if not name or not url:
                            logger.warning(f"Skipping row {row_num}: missing name or URL")
                            continue
                        
                        email = self._extract_csv_field(row, field_mapping.get('email'))
                        type_field = self._extract_csv_field(row, field_mapping.get('type'))
                        
                        # Collect additional data
                        additional_data = {}
                        for key, value in row.items():
                            normalized_key = key.lower().strip()
                            if normalized_key not in ['name', 'url', 'channel_url', 'email', 'type']:
                                if value and value.strip():
                                    additional_data[key] = value.strip()
                        
                        # Create ChannelInput
                        channel_input = ChannelInput(
                            name=name,
                            channel_url=url,
                            email=email,
                            type=type_field,
                            additional_data=additional_data if additional_data else None
                        )
                        
                        channel_inputs.append(channel_input)
                        
                    except ValueError as e:
                        logger.warning(f"Invalid data in row {row_num}: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"Error parsing row {row_num}: {e}")
                        raise RuntimeError(
                            f"CSV PARSE ERROR: Failed to parse row {row_num}. "
                            f"Error: {e}"
                        ) from e
                
                logger.info(f"Parsed {len(channel_inputs)} channels from CSV file")
                
                if not channel_inputs:
                    raise ValueError("CSV file contains no valid channel entries")
                
                return channel_inputs
                
        except FileNotFoundError:
            raise
        except ValueError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"CSV PARSE ERROR: Failed to parse CSV file {file_path}. "
                f"Error: {e}"
            ) from e
    
    def _create_csv_field_mapping(self, fieldnames: List[str]) -> Dict[str, str]:
        """Create mapping of normalized field names to actual column names."""
        mapping = {}
        
        for field in fieldnames:
            field_lower = field.lower().strip()
            
            # Map name variations
            if field_lower in ['name', 'channel_name', 'channel name', 'person_name', 'person name', 'channel']:
                mapping['name'] = field
            
            # Map URL variations
            elif field_lower in ['url', 'channel_url', 'channel url', 'youtube_url', 'youtube url', 'link']:
                mapping['url'] = field
            
            # Map email variations
            elif field_lower in ['email', 'email_address', 'email address', 'e-mail']:
                mapping['email'] = field
            
            # Map type variations
            elif field_lower in ['type', 'category', 'channel_type']:
                mapping['type'] = field
        
        return mapping
    
    def _extract_csv_field(self, row: Dict[str, str], field_name: Optional[str]) -> Optional[str]:
        """Extract and clean field value from CSV row."""
        if not field_name:
            return None
        
        value = row.get(field_name, '').strip()
        return value if value else None
    
    def _parse_json_file(self, file_path: str) -> List[ChannelInput]:
        """
        Parse JSON file with channel information.
        
        Supports multiple JSON formats:
        1. Array of channel objects: [{"name": "...", "channel_url": "..."}, ...]
        2. Object with channels array: {"channels": [{"name": "...", "channel_url": "..."}, ...]}
        3. Object with named channels: {"channel1": {"name": "...", "channel_url": "..."}, ...}
        
        Required fields for each channel:
        - name: Channel/Person name
        - channel_url or url: YouTube channel URL
        
        Optional fields:
        - email: Email address
        - type: Type/category
        - Any additional fields stored in additional_data
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            List of ChannelInput objects
            
        Raises:
            ValueError: If JSON is invalid or missing required fields
            RuntimeError: If JSON parsing fails
        """
        channel_inputs = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as jsonfile:
                try:
                    data = json.load(jsonfile)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"JSON PARSE ERROR: Invalid JSON format in file. "
                        f"Line {e.lineno}, Column {e.colno}: {e.msg}"
                    )
                
                # Handle different JSON structures
                channels_data = []
                
                if isinstance(data, list):
                    # Format 1: Direct array of channels
                    channels_data = data
                    logger.info(f"Detected JSON array format with {len(data)} entries")
                    
                elif isinstance(data, dict):
                    # Check for Format 2: Object with 'channels' key
                    if "channels" in data:
                        if not isinstance(data["channels"], list):
                            raise ValueError(
                                f"JSON ERROR: 'channels' field must be an array. "
                                f"Got: {type(data['channels']).__name__}"
                            )
                        channels_data = data["channels"]
                        logger.info(f"Detected JSON object with 'channels' array ({len(channels_data)} entries)")
                    
                    # Format 3: Object with named channel entries
                    else:
                        # Look for channel-like objects in the dictionary
                        for key, value in data.items():
                            if isinstance(value, dict) and self._is_channel_object(value):
                                # Add the key as channel name if not present
                                if "name" not in value:
                                    value["name"] = key
                                channels_data.append(value)
                        
                        if channels_data:
                            logger.info(f"Detected JSON object with named channels ({len(channels_data)} entries)")
                        else:
                            raise ValueError(
                                "JSON ERROR: No valid channel data found in JSON object. "
                                "Expected array of channels or object with channel entries."
                            )
                else:
                    raise ValueError(
                        f"JSON ERROR: Invalid root structure. Expected array or object, "
                        f"got: {type(data).__name__}"
                    )
                
                # Parse each channel entry
                for idx, channel_data in enumerate(channels_data):
                    try:
                        if not isinstance(channel_data, dict):
                            logger.warning(f"Skipping non-object entry at index {idx}: {type(channel_data).__name__}")
                            continue
                        
                        # Extract required fields with flexible key mapping
                        name = self._extract_json_field(channel_data, ["name", "channel_name", "person_name", "title"])
                        url = self._extract_json_field(channel_data, ["channel_url", "url", "youtube_url", "link"])
                        
                        if not name:
                            logger.warning(f"Skipping entry {idx}: missing required 'name' field")
                            continue
                        
                        if not url:
                            logger.warning(f"Skipping entry {idx}: missing required 'url' field")
                            continue
                        
                        # Extract optional fields
                        email = self._extract_json_field(channel_data, ["email", "email_address", "contact_email"])
                        type_field = self._extract_json_field(channel_data, ["type", "category", "channel_type"])
                        
                        # Collect additional data
                        additional_data = {}
                        standard_fields = {
                            "name", "channel_name", "person_name", "title",
                            "channel_url", "url", "youtube_url", "link",
                            "email", "email_address", "contact_email",
                            "type", "category", "channel_type"
                        }
                        
                        for key, value in channel_data.items():
                            if key.lower() not in standard_fields and value is not None:
                                # Convert non-string values to strings for consistency
                                if isinstance(value, (int, float, bool)):
                                    additional_data[key] = str(value)
                                elif isinstance(value, str):
                                    additional_data[key] = value
                                # Store complex types as JSON strings
                                elif isinstance(value, (list, dict)):
                                    additional_data[key] = json.dumps(value)
                        
                        # Create ChannelInput with validation
                        channel_input = ChannelInput(
                            name=name,
                            channel_url=url,
                            email=email,
                            type=type_field,
                            additional_data=additional_data if additional_data else None
                        )
                        
                        channel_inputs.append(channel_input)
                        
                    except ValueError as e:
                        logger.warning(f"Invalid channel data at index {idx}: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"Error parsing channel at index {idx}: {e}")
                        raise RuntimeError(
                            f"JSON PARSE ERROR: Failed to parse channel entry {idx}. "
                            f"Error: {e}"
                        ) from e
                
                logger.info(f"Parsed {len(channel_inputs)} channels from JSON file")
                
                if not channel_inputs:
                    raise ValueError("JSON file contains no valid channel entries")
                
                return channel_inputs
                
        except FileNotFoundError:
            raise
        except ValueError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"JSON PARSE ERROR: Failed to parse JSON file {file_path}. "
                f"Error: {e}"
            ) from e
    
    def _is_channel_object(self, obj: Dict[str, Any]) -> bool:
        """Check if a dictionary object looks like a channel entry."""
        # Must have at least a URL-like field
        url_fields = ["channel_url", "url", "youtube_url", "link"]
        has_url = any(field in obj for field in url_fields)
        
        # Should have a name or be able to derive one
        name_fields = ["name", "channel_name", "person_name", "title"]
        has_name = any(field in obj for field in name_fields)
        
        return has_url or (has_name and len(obj) >= 2)
    
    def _extract_json_field(self, data: Dict[str, Any], field_names: List[str]) -> Optional[str]:
        """Extract field value from JSON object using multiple possible field names."""
        for field_name in field_names:
            # Try exact match first
            if field_name in data:
                value = data[field_name]
                if value is not None:
                    return str(value).strip() if str(value).strip() else None
            
            # Try case-insensitive match
            for key in data.keys():
                if key.lower() == field_name.lower():
                    value = data[key]
                    if value is not None:
                        return str(value).strip() if str(value).strip() else None
        
        return None
    
    def _parse_text_file(self, file_path: str) -> List[ChannelInput]:
        """
        Parse text file with channel URLs.
        
        Supports multiple text formats:
        1. One URL per line
        2. Comma-separated URLs
        3. URLs mixed with text (extracts all YouTube URLs)
        4. Named entries like "ChannelName: URL"
        5. Markdown-style links like "[Channel Name](URL)"
        
        The parser is very flexible and extracts all valid YouTube channel URLs
        from the text, regardless of format.
        
        Args:
            file_path: Path to text file
            
        Returns:
            List of ChannelInput objects
            
        Raises:
            ValueError: If no valid URLs found
            RuntimeError: If text parsing fails
        """
        channel_inputs = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as textfile:
                content = textfile.read()
                
                if not content.strip():
                    raise ValueError("TEXT ERROR: File is empty")
                
                # First, try to parse structured formats
                lines = content.strip().split('\n')
                
                for line_num, line in enumerate(lines, 1):
                    line = line.strip()
                    if not line or line.startswith('#'):  # Skip empty lines and comments
                        continue
                    
                    # Check for named entry format: "Name: URL" or "Name - URL"
                    named_match = re.match(r'^([^:,|-]+?)\s*[:|-]\s*(https?://.*youtube\.com/\S+)', line, re.IGNORECASE)
                    if named_match:
                        name = named_match.group(1).strip()
                        url = named_match.group(2).strip()
                        
                        try:
                            channel_input = ChannelInput(
                                name=name,
                                channel_url=url
                            )
                            channel_inputs.append(channel_input)
                            logger.info(f"Parsed named entry: {name} -> {url}")
                            continue
                        except ValueError as e:
                            logger.warning(f"Line {line_num}: Invalid named entry: {e}")
                    
                    # Check for Markdown link format: [Name](URL)
                    markdown_match = re.match(r'^\[([^\]]+)\]\((https?://.*youtube\.com/[^)]+)\)', line)
                    if markdown_match:
                        name = markdown_match.group(1).strip()
                        url = markdown_match.group(2).strip()
                        
                        try:
                            channel_input = ChannelInput(
                                name=name,
                                channel_url=url
                            )
                            channel_inputs.append(channel_input)
                            logger.info(f"Parsed Markdown link: {name} -> {url}")
                            continue
                        except ValueError as e:
                            logger.warning(f"Line {line_num}: Invalid Markdown link: {e}")
                    
                    # Check if line is just a URL
                    urls_in_line = self.extract_youtube_urls(line)
                    if urls_in_line:
                        for url in urls_in_line:
                            # Check if this URL is the main content of the line
                            url_pattern = url.replace("https://", "")
                            if url_pattern.lower() in line.lower() and len(line.strip()) < len(url) + 20:
                                # Line contains mainly a URL, use channel name from URL
                                name = self._extract_channel_name_from_url(url)
                                
                                try:
                                    channel_input = ChannelInput(
                                        name=name,
                                        channel_url=url
                                    )
                                    channel_inputs.append(channel_input)
                                    logger.info(f"Parsed URL-only line: {name} -> {url}")
                                except ValueError as e:
                                    logger.warning(f"Line {line_num}: Invalid URL: {e}")
                
                # If structured parsing didn't find many channels, try extracting all URLs
                if len(channel_inputs) < 2:  # Lower threshold for better URL extraction
                    logger.info("Few structured entries found, extracting all YouTube URLs from text")
                    
                    all_urls = self.extract_youtube_urls(content)
                    existing_urls = {ch.channel_url for ch in channel_inputs}
                    
                    for url in all_urls:
                        if url not in existing_urls:
                            name = self._extract_channel_name_from_url(url)
                            
                            try:
                                channel_input = ChannelInput(
                                    name=name,
                                    channel_url=url
                                )
                                channel_inputs.append(channel_input)
                                logger.info(f"Extracted additional URL: {name} -> {url}")
                            except ValueError as e:
                                logger.warning(f"Invalid extracted URL {url}: {e}")
                
                # Handle comma-separated URLs on single lines
                for line in lines:
                    if ',' in line and line.count('https://') > 1:
                        # Line has multiple URLs separated by commas
                        parts = [p.strip() for p in line.split(',')]
                        for part in parts:
                            urls = self.extract_youtube_urls(part)
                            for url in urls:
                                # Check if already parsed
                                if not any(ch.channel_url == url for ch in channel_inputs):
                                    name = self._extract_channel_name_from_url(url)
                                    try:
                                        channel_input = ChannelInput(
                                            name=name,
                                            channel_url=url
                                        )
                                        channel_inputs.append(channel_input)
                                        logger.info(f"Parsed comma-separated URL: {name} -> {url}")
                                    except ValueError as e:
                                        logger.warning(f"Invalid comma-separated URL {url}: {e}")
                
                logger.info(f"Parsed {len(channel_inputs)} channels from text file")
                
                if not channel_inputs:
                    raise ValueError(
                        "TEXT ERROR: No valid YouTube channel URLs found in file. "
                        "Supported formats: URL per line, 'Name: URL', '[Name](URL)', "
                        "or URLs mixed with text."
                    )
                
                return channel_inputs
                
        except FileNotFoundError:
            raise
        except ValueError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"TEXT PARSE ERROR: Failed to parse text file {file_path}. "
                f"Error: {e}"
            ) from e
    
    def _extract_channel_name_from_url(self, url: str) -> str:
        """
        Extract a reasonable channel name from a YouTube URL.
        
        Examples:
        - https://youtube.com/@MrBeast -> "MrBeast"
        - https://youtube.com/channel/UCX6OQ3DkcsbYNE6H8uQQuVA -> "Channel UCX6OQ3D"
        - https://youtube.com/c/PewDiePie -> "PewDiePie"
        - https://youtube.com/user/SomeUser -> "SomeUser"
        """
        # Try to extract @username (case-insensitive)
        match = re.search(r'/@([^/?]+)', url, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Try to extract from /c/channelname
        match = re.search(r'/c/([^/?]+)', url, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Try to extract from /user/username
        match = re.search(r'/user/([^/?]+)', url, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # For channel IDs, use abbreviated form
        match = re.search(r'/channel/([^/?]+)', url, re.IGNORECASE)
        if match:
            channel_id = match.group(1)
            return f"Channel {channel_id[:8]}"
        
        # Fallback
        return "Unknown Channel"
    
    def extract_youtube_urls(self, text: str) -> List[str]:
        """
        Extract YouTube channel URLs from text.
        
        Args:
            text: Text containing YouTube URLs
            
        Returns:
            List of extracted YouTube channel URLs
        """
        # YouTube URL patterns
        patterns = [
            r'https?://(?:www\.)?youtube\.com/@[\w-]+',
            r'https?://(?:www\.)?youtube\.com/channel/[\w-]+',
            r'https?://(?:www\.)?youtube\.com/c/[\w-]+',
            r'https?://(?:www\.)?youtube\.com/user/[\w-]+',
            r'(?:www\.)?youtube\.com/@[\w-]+',
            r'(?:www\.)?youtube\.com/channel/[\w-]+',
            r'(?:www\.)?youtube\.com/c/[\w-]+',
            r'(?:www\.)?youtube\.com/user/[\w-]+',
        ]
        
        urls = []
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            urls.extend(matches)
        
        # Normalize URLs
        normalized_urls = []
        for url in urls:
            # Handle case-insensitive protocol
            if url.upper().startswith("HTTPS://") or url.upper().startswith("HTTP://"):
                # Already has protocol, just keep it as-is
                normalized_urls.append(url)
            else:
                # Add https:// protocol
                url = "https://" + url
                normalized_urls.append(url)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in normalized_urls:
            url_lower = url.lower()
            if url_lower not in seen:
                seen.add(url_lower)
                unique_urls.append(url)
        
        logger.info(f"Extracted {len(unique_urls)} unique YouTube URLs")
        return unique_urls
    
    def validate_channel_inputs(self, inputs: List[ChannelInput]) -> Tuple[List[ChannelInput], List[str]]:
        """
        Validate all channel inputs and separate valid from invalid.
        
        Args:
            inputs: List of channel inputs to validate
            
        Returns:
            Tuple of (valid_inputs, error_messages)
        """
        valid_inputs = []
        error_messages = []
        
        for i, channel_input in enumerate(inputs):
            try:
                # Validate input
                channel_input.validate()
                
                # Initialize discovery if needed
                if self.discovery is None:
                    self.discovery = YouTubeChannelDiscovery()
                
                # Validate YouTube URL
                normalized_url = self.discovery.validate_channel_url(channel_input.channel_url)
                channel_input.channel_url = normalized_url
                
                valid_inputs.append(channel_input)
                
            except Exception as e:
                error_msg = f"Input #{i+1} ({channel_input.name}): {e}"
                error_messages.append(error_msg)
                logger.warning(f"Invalid channel input: {error_msg}")
        
        logger.info(f"Validated {len(valid_inputs)} valid inputs, {len(error_messages)} invalid")
        
        return valid_inputs, error_messages
    
    def get_format_stats(self) -> Dict[str, Any]:
        """Get statistics about supported formats."""
        return {
            "supported_formats": [f.value for f in InputFormat if f != InputFormat.UNKNOWN],
            "format_extensions": {
                format_type.value: extensions 
                for format_type, extensions in self.format_extensions.items()
            },
            "max_file_size": "100MB",
            "encoding": "UTF-8"
        }


def main():
    """Test input handler functionality."""
    print("🚀 Testing Input Handler Module")
    print("=" * 80)
    
    try:
        # Initialize handler
        handler = InputHandler()
        
        # Show supported formats
        stats = handler.get_format_stats()
        print(f"Supported formats: {stats['supported_formats']}")
        print(f"Extensions: {stats['format_extensions']}")
        
        # Test format detection
        test_files = [
            "channels.csv",
            "channels.json", 
            "urls.txt",
            "unknown.xyz"
        ]
        
        print("\n📁 Format Detection Tests:")
        for test_file in test_files:
            # Create mock file for testing
            mock_path = f"/tmp/{test_file}"
            try:
                with open(mock_path, 'w') as f:
                    if test_file.endswith('.json'):
                        f.write('{"channels": []}')
                    elif test_file.endswith('.csv'):
                        f.write('name,url\n')
                    else:
                        f.write('https://youtube.com/@testchannel\n')
                
                format_type = handler.detect_format(mock_path)
                print(f"  {test_file} -> {format_type.value}")
                
                os.unlink(mock_path)
            except Exception as e:
                print(f"  {test_file} -> Error: {e}")
        
        # Test URL extraction
        print("\n🔗 URL Extraction Test:")
        test_text = """
        Check out these channels:
        https://www.youtube.com/@MrBeast
        youtube.com/channel/UCX6OQ3DkcsbYNE6H8uQQuVA
        Also visit https://youtube.com/c/PewDiePie
        """
        
        urls = handler.extract_youtube_urls(test_text)
        for url in urls:
            print(f"  Found: {url}")
        
        # Test validation
        print("\n✅ Validation Test:")
        test_input = ChannelInput(
            name="Test Channel",
            channel_url="https://youtube.com/@testchannel",
            email="test@example.com"
        )
        
        print(f"  Valid input created: {test_input.name}")
        
        print("\n✅ Input Handler module is ready!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())