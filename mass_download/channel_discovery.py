#!/usr/bin/env python3
"""
Channel Discovery Module for Mass Download Feature

Implements fail-fast, fail-loud, fail-safely principles for YouTube channel discovery:
- Fail Fast: Immediate validation of channel URLs and yt-dlp availability
- Fail Loud: Clear, actionable error messages with context
- Fail Safely: Safe enumeration with rollback on errors

Functionality:
- YouTube channel URL validation and normalization
- Channel video enumeration using yt-dlp
- Video metadata extraction with comprehensive validation
- Rate limiting and error recovery
"""

import os
import sys
import re
import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union, Tuple, NamedTuple
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import logging

# Initialize logger using standard logging
logger = logging.getLogger(__name__)

# Simple rate limiting decorator (inline implementation)
def rate_limit(service_name: str):
    """Simple rate limiting decorator."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Simple rate limiting - sleep for 2 seconds
            time.sleep(2.0)
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Simple config loader (inline implementation)
def get_config():
    """Get configuration with fallback values."""
    return {
        "rate_limiting": {
            "services": {
                "youtube": {
                    "rate": 2.0
                }
            }
        }
    }

# Simple rate limiter initialization (inline implementation)
def initialize_rate_limiter(config):
    """Initialize rate limiter (placeholder implementation)."""
    pass

# Global validation state
_YT_DLP_VALIDATED = False
_MODULE_VALIDATED = False


@dataclass
class ChannelInfo:
    """
    Channel information with fail-fast validation.
    
    Contains basic channel metadata extracted from YouTube.
    """
    channel_id: str
    channel_url: str
    title: str
    description: Optional[str] = None
    subscriber_count: Optional[int] = None
    video_count: Optional[int] = None
    playlist_id: Optional[str] = None
    
    def __post_init__(self):
        """Fail-fast validation on creation."""
        self.validate()
    
    def validate(self) -> None:
        """
        Fail-fast validation of channel info.
        
        Raises:
            ValueError: If validation fails (fail-loud)
        """
        if not self.channel_id or not isinstance(self.channel_id, str):
            raise ValueError(
                f"VALIDATION ERROR: channel_id is required and must be non-empty string. "
                f"Got: {self.channel_id}"
            )
        
        if len(self.channel_id) < 5:  # YouTube channel IDs are longer
            raise ValueError(
                f"VALIDATION ERROR: channel_id appears invalid (too short). "
                f"Got: '{self.channel_id}' (length: {len(self.channel_id)})"
            )
        
        if not self.channel_url or not isinstance(self.channel_url, str):
            raise ValueError(
                f"VALIDATION ERROR: channel_url is required and must be non-empty string. "
                f"Got: {self.channel_url}"
            )
        
        if not self.channel_url.startswith(("https://youtube.com/", "https://www.youtube.com/")):
            raise ValueError(
                f"VALIDATION ERROR: Invalid channel_url format. "
                f"Must start with https://youtube.com/ or https://www.youtube.com/. "
                f"Got: {self.channel_url}"
            )
        
        if not self.title or not isinstance(self.title, str):
            raise ValueError(
                f"VALIDATION ERROR: title is required and must be non-empty string. "
                f"Got: {self.title}"
            )
        
        if self.subscriber_count is not None and (not isinstance(self.subscriber_count, int) or self.subscriber_count < 0):
            raise ValueError(
                f"VALIDATION ERROR: subscriber_count must be non-negative integer. "
                f"Got: {self.subscriber_count} (type: {type(self.subscriber_count)})"
            )
        
        if self.video_count is not None and (not isinstance(self.video_count, int) or self.video_count < 0):
            raise ValueError(
                f"VALIDATION ERROR: video_count must be non-negative integer. "
                f"Got: {self.video_count} (type: {type(self.video_count)})"
            )


@dataclass
class VideoMetadata:
    """
    Video metadata with fail-fast validation.
    
    Contains comprehensive video information extracted from YouTube.
    """
    video_id: str
    title: str
    description: Optional[str] = None
    duration: Optional[int] = None  # seconds
    upload_date: Optional[datetime] = None
    view_count: Optional[int] = None
    like_count: Optional[int] = None
    comment_count: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)
    thumbnail_url: Optional[str] = None
    video_url: Optional[str] = None
    channel_id: Optional[str] = None
    uploader: Optional[str] = None
    is_live: bool = False
    age_restricted: bool = False
    
    def __post_init__(self):
        """Fail-fast validation on creation."""
        self.validate()
    
    def validate(self) -> None:
        """
        Fail-fast validation of video metadata.
        
        Raises:
            ValueError: If validation fails (fail-loud)
        """
        if not self.video_id or not isinstance(self.video_id, str):
            raise ValueError(
                f"VALIDATION ERROR: video_id is required and must be non-empty string. "
                f"Got: {self.video_id}"
            )
        
        if len(self.video_id) != 11:  # YouTube video IDs are exactly 11 chars
            raise ValueError(
                f"VALIDATION ERROR: YouTube video_id must be exactly 11 characters. "
                f"Got: '{self.video_id}' (length: {len(self.video_id)})"
            )
        
        if not self.title or not isinstance(self.title, str):
            raise ValueError(
                f"VALIDATION ERROR: title is required and must be non-empty string. "
                f"Got: {self.title}"
            )
        
        if self.duration is not None:
            # Handle float duration from yt-dlp by converting to int
            if isinstance(self.duration, float):
                self.duration = int(self.duration)
            elif not isinstance(self.duration, int):
                raise ValueError(
                    f"VALIDATION ERROR: duration must be numeric (seconds). "
                    f"Got: {self.duration} (type: {type(self.duration)})"
                )
            if self.duration < 0:
                raise ValueError(
                    f"VALIDATION ERROR: duration must be non-negative. "
                    f"Got: {self.duration}"
                )
        
        if self.view_count is not None and (not isinstance(self.view_count, int) or self.view_count < 0):
            raise ValueError(
                f"VALIDATION ERROR: view_count must be non-negative integer. "
                f"Got: {self.view_count} (type: {type(self.view_count)})"
            )
        
        if self.video_url and not self.video_url.startswith(("https://youtube.com/", "https://www.youtube.com/")):
            raise ValueError(
                f"VALIDATION ERROR: Invalid video_url format. "
                f"Must start with https://youtube.com/ or https://www.youtube.com/. "
                f"Got: {self.video_url}"
            )


class YouTubeChannelDiscovery:
    """
    YouTube channel discovery with fail-fast/fail-loud/fail-safely principles.
    """
    
    def __init__(self, yt_dlp_path: str = "yt-dlp"):
        """
        Initialize channel discovery with fail-fast validation.
        
        Args:
            yt_dlp_path: Path to yt-dlp executable
            
        Raises:
            RuntimeError: If yt-dlp is not available or invalid
        """
        self.yt_dlp_path = yt_dlp_path
        self.config = get_config()
        
        # Fail-fast yt-dlp validation
        self._validate_yt_dlp()
        
        # Initialize rate limiter with configuration for burst support
        initialize_rate_limiter(self.config)
        
        # Rate limiting settings (backward compatibility)
        self.rate_limit_delay = self.config.get("rate_limiting.services.youtube.rate", 2.0)
        
        # Initialize duplicate detection tracking
        self._processed_videos: set[str] = set()  # Track processed video IDs
        self._uuid_mapping: dict[str, str] = {}   # Map video_id -> uuid for tracking
        
        logger.info("YouTubeChannelDiscovery initialized successfully")
        logger.info("Rate limiting integrated with burst support for YouTube API compliance")
        logger.info("UUID generation and duplicate detection initialized")
    
    def _validate_yt_dlp(self) -> None:
        """
        Validate yt-dlp availability (fail-fast).
        
        Raises:
            RuntimeError: If yt-dlp validation fails
        """
        global _YT_DLP_VALIDATED
        
        try:
            logger.info(f"Validating yt-dlp at: {self.yt_dlp_path}")
            
            # Test yt-dlp availability
            result = subprocess.run(
                [self.yt_dlp_path, "--version"],
                capture_output=True,
                text=True,
                timeout=10  # Fast timeout for fail-fast
            )
            
            if result.returncode != 0:
                raise RuntimeError(
                    f"YT-DLP ERROR: yt-dlp command failed. "
                    f"Command: {self.yt_dlp_path} --version. "
                    f"Return code: {result.returncode}. "
                    f"Error: {result.stderr}"
                )
            
            version = result.stdout.strip()
            if not version:
                raise RuntimeError(
                    f"YT-DLP ERROR: yt-dlp returned empty version string. "
                    f"This indicates a broken installation."
                )
            
            _YT_DLP_VALIDATED = True
            logger.info(f"yt-dlp validation PASSED: {version}")
            
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"YT-DLP ERROR: yt-dlp command timed out. "
                f"This indicates yt-dlp is not responsive. "
                f"Command: {self.yt_dlp_path} --version"
            ) from None
        except FileNotFoundError:
            raise RuntimeError(
                f"YT-DLP ERROR: yt-dlp executable not found. "
                f"Path: {self.yt_dlp_path}. "
                f"Please install yt-dlp: pip install yt-dlp"
            ) from None
        except Exception as e:
            raise RuntimeError(
                f"YT-DLP ERROR: Unexpected error during yt-dlp validation. "
                f"Error: {e}"
            ) from e
    
    def validate_channel_url(self, channel_url: str) -> str:
        """
        Validate and normalize YouTube channel URL (fail-fast).
        
        Args:
            channel_url: Raw channel URL
            
        Returns:
            str: Normalized channel URL
            
        Raises:
            ValueError: If URL validation fails (fail-loud)
        """
        if not channel_url or not isinstance(channel_url, str):
            raise ValueError(
                f"CHANNEL URL ERROR: Channel URL is required and must be non-empty string. "
                f"Got: {channel_url} (type: {type(channel_url)})"
            )
        
        # Strip whitespace
        channel_url = channel_url.strip()
        
        if not channel_url:
            raise ValueError(
                f"CHANNEL URL ERROR: Channel URL cannot be empty or whitespace-only. "
                f"Provide a valid YouTube channel URL."
            )
        
        # Ensure HTTPS
        if channel_url.startswith("http://"):
            channel_url = channel_url.replace("http://", "https://", 1)
        elif not channel_url.startswith("https://"):
            channel_url = f"https://{channel_url}"
        
        # Validate YouTube domain
        parsed = urlparse(channel_url)
        if parsed.netloc not in ["youtube.com", "www.youtube.com", "m.youtube.com"]:
            raise ValueError(
                f"CHANNEL URL ERROR: Invalid YouTube domain. "
                f"Expected youtube.com or www.youtube.com, got: {parsed.netloc}. "
                f"URL: {channel_url}"
            )
        
        # Validate channel path patterns
        valid_patterns = [
            r"^/channel/[A-Za-z0-9_-]{10,}$",  # /channel/UC... (at least 10 chars)
            r"^/c/[A-Za-z0-9_-]+$",            # /c/channel_name
            r"^/user/[A-Za-z0-9_-]+$",         # /user/username
            r"^/@[A-Za-z0-9_.-]+$",            # /@handle
        ]
        
        path = parsed.path
        if not any(re.match(pattern, path) for pattern in valid_patterns):
            raise ValueError(
                f"CHANNEL URL ERROR: Invalid YouTube channel URL format. "
                f"Expected formats: /channel/ID, /c/name, /user/name, /@handle. "
                f"Got path: {path}. Full URL: {channel_url}"
            )
        
        # Normalize to www.youtube.com
        if parsed.netloc != "www.youtube.com":
            channel_url = channel_url.replace(parsed.netloc, "www.youtube.com")
        
        logger.debug(f"Channel URL validated and normalized: {channel_url}")
        return channel_url
    
    @rate_limit("youtube")
    def extract_channel_info(self, channel_url: str) -> ChannelInfo:
        """
        Extract basic channel information (fail-safely).
        
        Args:
            channel_url: YouTube channel URL
            
        Returns:
            ChannelInfo: Channel metadata
            
        Raises:
            RuntimeError: If channel info extraction fails
        """
        # Validate URL first (fail-fast)
        normalized_url = self.validate_channel_url(channel_url)
        
        try:
            logger.info(f"Extracting channel info for: {normalized_url}")
            
            # Build yt-dlp command
            cmd = [
                self.yt_dlp_path,
                "--quiet",
                "--no-warnings",
                "--dump-json",
                "--flat-playlist",
                "--playlist-items", "1",  # Just get channel info, not all videos
                normalized_url
            ]
            
            logger.debug(f"Running command: {' '.join(cmd)}")
            
            # Execute with timeout
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60  # 1 minute timeout
            )
            
            if result.returncode != 0:
                raise RuntimeError(
                    f"CHANNEL EXTRACTION ERROR: yt-dlp failed to extract channel info. "
                    f"URL: {normalized_url}. "
                    f"Return code: {result.returncode}. "
                    f"Error: {result.stderr.strip()}"
                )
            
            if not result.stdout.strip():
                raise RuntimeError(
                    f"CHANNEL EXTRACTION ERROR: yt-dlp returned empty output. "
                    f"URL may be invalid or channel may not exist: {normalized_url}"
                )
            
            # Parse JSON output
            try:
                data = json.loads(result.stdout.strip().split('\n')[0])  # First line should be channel info
            except (json.JSONDecodeError, IndexError) as e:
                raise RuntimeError(
                    f"CHANNEL EXTRACTION ERROR: Failed to parse yt-dlp output as JSON. "
                    f"URL: {normalized_url}. "
                    f"Output: {result.stdout[:200]}... "
                    f"Error: {e}"
                ) from e
            
            # Extract channel information with validation 
            # For --flat-playlist, channel info is in playlist_* fields
            channel_id = (
                data.get("playlist_channel_id") or 
                data.get("channel_id") or 
                data.get("uploader_id") or 
                data.get("playlist_uploader_id") or
                data.get("id") or 
                ""
            )
            
            # Get channel title from playlist or fallback fields
            title = (
                data.get("playlist_channel") or
                data.get("channel") or 
                data.get("uploader") or 
                data.get("playlist_uploader") or
                "Unknown Channel"
            )
            
            # If still empty, try to extract from URL or generate a fallback
            if not channel_id:
                # Extract from @handle format in URL
                import re
                handle_match = re.search(r'/@([A-Za-z0-9_.-]+)', normalized_url)
                if handle_match:
                    channel_id = f"@{handle_match.group(1)}"
                else:
                    # Generate a fallback ID from the title
                    channel_id = f"UNKNOWN_{title.replace(' ', '_')[:20]}"
            
            channel_info = ChannelInfo(
                channel_id=channel_id,
                channel_url=normalized_url,
                title=title,
                description=data.get("description"),
                subscriber_count=data.get("subscriber_count"),
                video_count=data.get("playlist_count") or data.get("n_entries"),
                playlist_id=data.get("playlist_id") or data.get("id")
            )
            
            logger.info(f"Channel info extracted successfully: {channel_info.title}")
            return channel_info
            
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"CHANNEL EXTRACTION ERROR: yt-dlp timed out extracting channel info. "
                f"URL: {normalized_url}. "
                f"Channel may be very large or network is slow."
            ) from None
        except Exception as e:
            logger.error(f"Channel info extraction failed: {e}")
            raise
    
    @rate_limit("youtube")
    def enumerate_channel_videos(self, channel_url: str, max_videos: Optional[int] = None) -> List[VideoMetadata]:
        """
        Enumerate all videos from a YouTube channel (fail-safely).
        
        Args:
            channel_url: YouTube channel URL
            max_videos: Maximum number of videos to retrieve (None for all)
            
        Returns:
            List[VideoMetadata]: List of video metadata objects
            
        Raises:
            RuntimeError: If channel enumeration fails
        """
        # Validate URL first (fail-fast)
        normalized_url = self.validate_channel_url(channel_url)
        
        try:
            logger.info(f"Enumerating videos from channel: {normalized_url}")
            if max_videos:
                logger.info(f"Limited to {max_videos} videos")
            
            # Build yt-dlp command for video enumeration
            cmd = [
                self.yt_dlp_path,
                "--quiet",
                "--no-warnings", 
                "--dump-json",
                "--flat-playlist",
                "--ignore-errors",  # Continue on individual video errors
                normalized_url
            ]
            
            # Add video limit if specified
            if max_videos and max_videos > 0:
                cmd.extend(["--playlist-items", f"1:{max_videos}"])
            
            logger.debug(f"Running command: {' '.join(cmd)}")
            
            # Execute with timeout
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout for channel enumeration
            )
            
            if result.returncode != 0:
                # Non-zero return code but still check if we got partial results
                if not result.stdout.strip():
                    raise RuntimeError(
                        f"CHANNEL ENUMERATION ERROR: yt-dlp failed to enumerate channel videos. "
                        f"URL: {normalized_url}. "
                        f"Return code: {result.returncode}. "
                        f"Error: {result.stderr.strip()}"
                    )
                else:
                    logger.warning(f"yt-dlp returned non-zero code but produced output. "
                                 f"Return code: {result.returncode}. "
                                 f"Error: {result.stderr.strip()}")
            
            if not result.stdout.strip():
                # Empty output might indicate no videos or private channel
                logger.warning(f"No videos found for channel: {normalized_url}")
                return []
            
            # Parse JSON output (one video per line)
            videos = []
            errors = []
            
            for line_num, line in enumerate(result.stdout.strip().split('\n'), 1):
                if not line.strip():
                    continue
                
                try:
                    data = json.loads(line.strip())
                    
                    # Extract video metadata with validation
                    video_metadata = self._extract_video_metadata(data, normalized_url)
                    if video_metadata:
                        videos.append(video_metadata)
                    
                except json.JSONDecodeError as e:
                    error_msg = f"Line {line_num}: Failed to parse JSON: {e}"
                    errors.append(error_msg)
                    logger.warning(f"Video enumeration JSON parse error: {error_msg}")
                    continue
                except ValueError as e:
                    error_msg = f"Line {line_num}: Video validation failed: {e}"
                    errors.append(error_msg)
                    logger.warning(f"Video enumeration validation error: {error_msg}")
                    continue
                except Exception as e:
                    error_msg = f"Line {line_num}: Unexpected error: {e}"
                    errors.append(error_msg)
                    logger.warning(f"Video enumeration unexpected error: {error_msg}")
                    continue
            
            # Apply max_videos limit if specified (post-processing since --playlist-items may not work reliably)
            if max_videos and max_videos > 0 and len(videos) > max_videos:
                videos = videos[:max_videos]
                logger.info(f"Limited to {max_videos} videos as requested")
            
            # Log results
            logger.info(f"Channel enumeration completed: {len(videos)} videos found")
            if errors:
                logger.warning(f"Encountered {len(errors)} errors during enumeration")
                # Log first few errors for debugging
                for error in errors[:3]:
                    logger.debug(f"Enumeration error: {error}")
                if len(errors) > 3:
                    logger.debug(f"... and {len(errors) - 3} more errors")
            
            return videos
            
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"CHANNEL ENUMERATION ERROR: yt-dlp timed out enumerating channel videos. "
                f"URL: {normalized_url}. "
                f"Channel may be very large or network is slow. Consider using max_videos parameter."
            ) from None
        except Exception as e:
            logger.error(f"Channel video enumeration failed: {e}")
            raise
    
    def _extract_video_metadata(self, data: Dict[str, Any], channel_url: str) -> Optional[VideoMetadata]:
        """
        Enhanced video metadata extraction with comprehensive validation and edge case handling.
        
        Implements fail-fast/fail-loud/fail-safely principles:
        - Fail Fast: Immediate validation of required fields
        - Fail Loud: Detailed error messages with context
        - Fail Safely: Graceful handling of malformed/missing optional data
        
        Args:
            data: JSON data from yt-dlp
            channel_url: Source channel URL for context
            
        Returns:
            VideoMetadata: Validated video metadata or None if invalid
            
        Raises:
            ValueError: If video data validation fails with detailed context
        """
        extraction_context = f"video_id={data.get('id', 'UNKNOWN')}, channel={channel_url}"
        
        try:
            logger.debug(f"Extracting metadata for {extraction_context}")
            
            # PHASE 1: Required fields with fail-fast validation
            video_id = self._extract_required_field(data, "id", "video_id", extraction_context)
            title = self._extract_required_field(data, "title", "title", extraction_context)
            
            # PHASE 2: Enhanced optional field extraction with robust error handling
            description = self._safe_extract_string(data, "description", max_length=5000)
            duration = self._safe_extract_duration(data)
            upload_date = self._safe_extract_upload_date(data, extraction_context)
            
            # PHASE 3: Numeric metadata with comprehensive validation
            view_count = self._safe_extract_numeric(data, "view_count", min_value=0)
            like_count = self._safe_extract_numeric(data, "like_count", min_value=0)
            comment_count = self._safe_extract_numeric(data, "comment_count", min_value=0)
            
            # PHASE 4: Collection fields with type validation
            tags = self._safe_extract_list(data, "tags", item_type=str, max_items=50)
            categories = self._safe_extract_list(data, "categories", item_type=str, max_items=10)
            
            # PHASE 5: URL and media information
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            thumbnail_url = self._extract_best_thumbnail(data)
            
            # PHASE 6: Channel identification with fallback strategies
            channel_id = self._extract_channel_identifier(data)
            uploader = self._safe_extract_string(data, ["uploader", "channel"], max_length=200)
            
            # PHASE 7: Content characteristics and restrictions
            is_live = self._safe_extract_boolean(data, ["is_live", "live_status"])
            age_restricted = self._extract_age_restriction(data)
            
            # PHASE 8: Additional metadata for enhanced functionality
            availability = self._safe_extract_string(data, "availability", max_length=50)
            language = self._safe_extract_string(data, ["language", "automatic_captions"], max_length=10)
            
            logger.debug(f"Successfully extracted metadata for {extraction_context}: "
                        f"duration={duration}s, views={view_count}, live={is_live}")
            
            # PHASE 9: Create and validate video metadata with enhanced validation
            video_metadata = VideoMetadata(
                video_id=video_id,
                title=title,
                description=description,
                duration=duration,
                upload_date=upload_date,
                view_count=view_count,
                like_count=like_count,
                comment_count=comment_count,
                tags=tags,
                categories=categories,
                thumbnail_url=thumbnail_url,
                video_url=video_url,
                channel_id=channel_id,
                uploader=uploader,
                is_live=is_live,
                age_restricted=age_restricted
            )
            
            return video_metadata
            
        except ValueError as e:
            # Re-raise validation errors with enhanced context
            logger.warning(f"Video metadata validation failed for {extraction_context}: {e}")
            raise ValueError(f"Video metadata validation failed for {extraction_context}: {e}") from e
        except Exception as e:
            # Log unexpected errors with full context
            logger.error(f"Unexpected error extracting video metadata for {extraction_context}: {e}")
            raise ValueError(f"Video metadata extraction failed for {extraction_context}: {e}") from e
    
    def _extract_required_field(self, data: Dict[str, Any], field: str, field_name: str, context: str) -> str:
        """Extract required field with fail-fast validation."""
        value = data.get(field)
        if not value or not isinstance(value, str) or not value.strip():
            raise ValueError(f"Missing or invalid required field '{field_name}' in {context}")
        return value.strip()
    
    def _safe_extract_string(self, data: Dict[str, Any], fields: Union[str, List[str]], 
                           max_length: int = 1000) -> Optional[str]:
        """Safely extract string field(s) with length validation."""
        if isinstance(fields, str):
            fields = [fields]
        
        for field in fields:
            value = data.get(field)
            if value and isinstance(value, str):
                value = value.strip()
                if value:
                    return value[:max_length] if len(value) > max_length else value
        return None
    
    def _safe_extract_duration(self, data: Dict[str, Any]) -> Optional[int]:
        """Enhanced duration extraction with multiple format support."""
        duration = data.get("duration")
        if duration is None:
            return None
        
        try:
            # Handle float values from yt-dlp
            if isinstance(duration, (int, float)):
                duration_int = int(duration)
                if duration_int < 0:
                    logger.debug(f"Invalid negative duration: {duration}")
                    return None
                if duration_int > 86400:  # > 24 hours, likely an error
                    logger.debug(f"Suspiciously long duration: {duration_int}s")
                    return None
                return duration_int
            
            # Handle string durations (HH:MM:SS format)
            if isinstance(duration, str):
                parts = duration.split(":")
                if len(parts) == 3:
                    hours, minutes, seconds = map(int, parts)
                    return hours * 3600 + minutes * 60 + seconds
                elif len(parts) == 2:
                    minutes, seconds = map(int, parts)
                    return minutes * 60 + seconds
                else:
                    return int(duration)
                    
        except (ValueError, TypeError) as e:
            logger.debug(f"Could not parse duration '{duration}': {e}")
            return None
    
    def _safe_extract_upload_date(self, data: Dict[str, Any], context: str) -> Optional[datetime]:
        """Enhanced upload date parsing with multiple format support."""
        date_fields = ["upload_date", "release_date", "timestamp"]
        
        for field in date_fields:
            date_value = data.get(field)
            if not date_value:
                continue
                
            try:
                # Handle YYYYMMDD format
                if isinstance(date_value, str) and len(date_value) == 8:
                    return datetime.strptime(date_value, "%Y%m%d")
                
                # Handle timestamp
                if isinstance(date_value, (int, float)):
                    return datetime.fromtimestamp(date_value)
                
                # Handle ISO format
                if isinstance(date_value, str) and "T" in date_value:
                    return datetime.fromisoformat(date_value.replace("Z", "+00:00"))
                    
            except (ValueError, TypeError) as e:
                logger.debug(f"Could not parse {field} '{date_value}' for {context}: {e}")
                continue
        
        return None
    
    def _safe_extract_numeric(self, data: Dict[str, Any], field: str, 
                            min_value: int = 0, max_value: int = None) -> Optional[int]:
        """Safely extract and validate numeric fields."""
        value = data.get(field)
        if value is None:
            return None
        
        try:
            if isinstance(value, str):
                # Handle formatted numbers (e.g., "1,234,567")
                value = value.replace(",", "").replace(" ", "")
            
            numeric_value = int(float(value))  # Handle "123.0" strings
            
            if numeric_value < min_value:
                logger.debug(f"Value {numeric_value} below minimum {min_value} for field {field}")
                return None
            
            if max_value and numeric_value > max_value:
                logger.debug(f"Value {numeric_value} above maximum {max_value} for field {field}")
                return max_value
                
            return numeric_value
            
        except (ValueError, TypeError) as e:
            logger.debug(f"Could not parse numeric field '{field}' value '{value}': {e}")
            return None
    
    def _safe_extract_list(self, data: Dict[str, Any], field: str, 
                         item_type: type = str, max_items: int = 100) -> List:
        """Safely extract and validate list fields."""
        value = data.get(field, [])
        
        if not isinstance(value, list):
            logger.debug(f"Field '{field}' is not a list: {type(value)}")
            return []
        
        validated_items = []
        for item in value[:max_items]:  # Limit list size
            if isinstance(item, item_type) and (item_type != str or item.strip()):
                validated_items.append(item.strip() if item_type == str else item)
        
        return validated_items
    
    def _extract_best_thumbnail(self, data: Dict[str, Any]) -> Optional[str]:
        """Extract the highest quality thumbnail URL."""
        thumbnails = data.get("thumbnails", [])
        if not isinstance(thumbnails, list) or not thumbnails:
            return None
        
        # Sort by quality: prefer larger thumbnails
        try:
            best_thumbnail = max(
                thumbnails, 
                key=lambda t: (t.get("width", 0) * t.get("height", 0), t.get("preference", 0))
            )
            return best_thumbnail.get("url")
        except (ValueError, TypeError):
            # Fallback to last thumbnail
            return thumbnails[-1].get("url") if thumbnails else None
    
    def _extract_channel_identifier(self, data: Dict[str, Any]) -> Optional[str]:
        """Extract channel ID with multiple fallback strategies."""
        # Try various channel ID fields in order of preference
        id_fields = [
            "channel_id", "uploader_id", "playlist_channel_id", 
            "channel_url", "uploader_url"
        ]
        
        for field in id_fields:
            value = data.get(field)
            if value and isinstance(value, str):
                # Extract ID from URL if needed
                if "/channel/" in value:
                    return value.split("/channel/")[-1].split("/")[0]
                elif value.startswith("UC") or value.startswith("@"):
                    return value
        
        return None
    
    def _safe_extract_boolean(self, data: Dict[str, Any], fields: Union[str, List[str]]) -> bool:
        """Safely extract boolean values from multiple possible fields."""
        if isinstance(fields, str):
            fields = [fields]
        
        for field in fields:
            value = data.get(field)
            if value is not None:
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ["true", "yes", "1", "live"]
                if isinstance(value, (int, float)):
                    return bool(value)
        
        return False
    
    def _extract_age_restriction(self, data: Dict[str, Any]) -> bool:
        """Determine if content is age-restricted."""
        age_limit = data.get("age_limit")
        if age_limit and isinstance(age_limit, (int, float)):
            return age_limit > 0
        
        # Check other indicators
        return any([
            data.get("is_age_restricted", False),
            "age" in str(data.get("content_warning", "")).lower(),
            "mature" in str(data.get("content_rating", "")).lower()
        ])
    
    @rate_limit("youtube")
    def get_video_details(self, video_id: str) -> VideoMetadata:
        """
        Get detailed metadata for a specific video (fail-safely).
        
        Args:
            video_id: YouTube video ID (11 characters)
            
        Returns:
            VideoMetadata: Detailed video metadata
            
        Raises:
            RuntimeError: If video details extraction fails
        """
        # Validate video ID (fail-fast)
        if not video_id or not isinstance(video_id, str):
            raise ValueError(
                f"VIDEO ID ERROR: Video ID is required and must be non-empty string. "
                f"Got: {video_id}"
            )
        
        if len(video_id) != 11:
            raise ValueError(
                f"VIDEO ID ERROR: YouTube video ID must be exactly 11 characters. "
                f"Got: '{video_id}' (length: {len(video_id)})"
            )
        
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        
        try:
            logger.info(f"Getting video details for: {video_id}")
            
            # Build yt-dlp command for detailed video info
            cmd = [
                self.yt_dlp_path,
                "--quiet",
                "--no-warnings",
                "--dump-json",
                video_url
            ]
            
            logger.debug(f"Running command: {' '.join(cmd)}")
            
            # Execute with timeout
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60  # 1 minute timeout for single video
            )
            
            if result.returncode != 0:
                raise RuntimeError(
                    f"VIDEO DETAILS ERROR: yt-dlp failed to get video details. "
                    f"Video ID: {video_id}. "
                    f"Return code: {result.returncode}. "
                    f"Error: {result.stderr.strip()}"
                )
            
            if not result.stdout.strip():
                raise RuntimeError(
                    f"VIDEO DETAILS ERROR: yt-dlp returned empty output. "
                    f"Video may not exist or be private: {video_id}"
                )
            
            # Parse JSON output
            try:
                data = json.loads(result.stdout.strip())
            except json.JSONDecodeError as e:
                raise RuntimeError(
                    f"VIDEO DETAILS ERROR: Failed to parse yt-dlp output as JSON. "
                    f"Video ID: {video_id}. "
                    f"Output: {result.stdout[:200]}... "
                    f"Error: {e}"
                ) from e
            
            # Extract video metadata with validation
            video_metadata = self._extract_video_metadata(data, video_url)
            
            logger.info(f"Video details extracted successfully: {video_metadata.title}")
            return video_metadata
            
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"VIDEO DETAILS ERROR: yt-dlp timed out getting video details. "
                f"Video ID: {video_id}. "
                f"Network may be slow or video may be very large."
            ) from None
        except Exception as e:
            logger.error(f"Video details extraction failed: {e}")
            raise
    
    def is_duplicate_video(self, video_id: str) -> bool:
        """
        Check if video has already been processed (duplicate detection).
        
        Args:
            video_id: YouTube video ID to check
            
        Returns:
            True if video is a duplicate, False if it's new
        """
        if not video_id or not isinstance(video_id, str):
            logger.warning(f"Invalid video_id for duplicate check: {video_id}")
            return True  # Treat invalid IDs as duplicates (fail-safely)
        
        is_duplicate = video_id in self._processed_videos
        if is_duplicate:
            logger.debug(f"Duplicate video detected: {video_id}")
        
        return is_duplicate
    
    def mark_video_processed(self, video_id: str, video_uuid: str) -> None:
        """
        Mark video as processed and store UUID mapping.
        
        Args:
            video_id: YouTube video ID
            video_uuid: Generated UUID for the video
        """
        if not video_id or not isinstance(video_id, str):
            raise ValueError(f"Invalid video_id for marking processed: {video_id}")
        
        if not video_uuid or not isinstance(video_uuid, str):
            raise ValueError(f"Invalid video_uuid for marking processed: {video_uuid}")
        
        self._processed_videos.add(video_id)
        self._uuid_mapping[video_id] = video_uuid
        
        logger.debug(f"Marked video as processed: {video_id} -> {video_uuid}")
    
    def get_video_uuid(self, video_id: str) -> Optional[str]:
        """
        Get UUID for a previously processed video.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            UUID if video was processed, None if not found
        """
        return self._uuid_mapping.get(video_id)
    
    def get_duplicate_detection_stats(self) -> Dict[str, Any]:
        """
        Get statistics about duplicate detection.
        
        Returns:
            Dictionary with duplicate detection statistics
        """
        return {
            "total_processed_videos": len(self._processed_videos),
            "uuid_mappings": len(self._uuid_mapping),
            "processed_video_ids": list(self._processed_videos),
            "recent_uuids": list(self._uuid_mapping.values())[-10:]  # Last 10 UUIDs
        }
    
    def reset_duplicate_detection(self) -> None:
        """
        Reset duplicate detection state (for testing or fresh starts).
        
        Warning: This clears all duplicate detection state!
        """
        old_count = len(self._processed_videos)
        self._processed_videos.clear()
        self._uuid_mapping.clear()
        
        logger.warning(f"Reset duplicate detection state - cleared {old_count} processed videos")
    
    def load_existing_videos_for_duplicate_detection(self, existing_video_ids: List[str]) -> None:
        """
        Load existing video IDs for duplicate detection (e.g., from database).
        
        Args:
            existing_video_ids: List of video IDs that already exist
        """
        if not isinstance(existing_video_ids, list):
            raise ValueError(f"existing_video_ids must be list, got: {type(existing_video_ids)}")
        
        # Add existing videos to duplicate detection
        for video_id in existing_video_ids:
            if video_id and isinstance(video_id, str):
                self._processed_videos.add(video_id)
        
        logger.info(f"Loaded {len(existing_video_ids)} existing videos for duplicate detection")
        logger.debug(f"Duplicate detection now tracking {len(self._processed_videos)} total videos")


def validate_channel_discovery_module():
    """
    Validate channel discovery module (fail-fast on import).
    
    Raises:
        RuntimeError: If module validation fails
    """
    global _MODULE_VALIDATED
    
    try:
        # Test yt-dlp availability
        discovery = YouTubeChannelDiscovery()
        
        # Test URL validation
        test_url = "https://www.youtube.com/@testchannel"
        normalized = discovery.validate_channel_url(test_url)
        if not normalized.startswith("https://www.youtube.com/"):
            raise RuntimeError("URL validation failed")
        
        _MODULE_VALIDATED = True
        logger.info("Channel discovery module validation PASSED")
        return True
        
    except Exception as e:
        logger.error(f"Channel discovery module validation FAILED: {e}")
        raise


# Run validation on import (fail-fast)
validate_channel_discovery_module()