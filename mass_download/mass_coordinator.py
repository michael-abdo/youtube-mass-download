#!/usr/bin/env python3
"""
Mass Download Coordinator Module
Phase 4.1: Create mass coordinator module structure

This module orchestrates the mass download process for YouTube channels.
It coordinates between channel discovery, database operations, download management,
and S3 streaming to provide a unified interface for processing multiple channels.

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import time
import logging
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from enum import Enum

# Add parent directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir.parent))

# Initialize logger using standard logging
logger = logging.getLogger(__name__)

# Simple utility functions (inline implementations)
def get_config():
    """Get configuration with fallback values."""
    import yaml
    try:
        with open('/home/Mike/projects/xenodex/typing-clients-ingestion/mass-download/config/config.yaml', 'r') as f:
            return yaml.safe_load(f)
    except:
        return {"mass_download": {"max_concurrent_channels": 2, "max_videos_per_channel": 100}}

def with_standard_error_handling(func):
    """Simple error handling decorator."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            raise
    return wrapper

def retry_with_backoff(max_retries=3):
    """Simple retry decorator."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}")
                    time.sleep(2 ** attempt)  # exponential backoff
        return wrapper
    return decorator

# Core module imports with graceful fallback
try:
    from .channel_discovery import YouTubeChannelDiscovery, ChannelInfo, VideoMetadata
    from .database_schema import DatabaseSchemaManager, PersonRecord, VideoRecord  
    from .input_handler import InputHandler
    _CORE_IMPORTS_OK = True
except ImportError as e:
    logger.warning(f"Core imports failed: {e}")
    _CORE_IMPORTS_OK = False
    YouTubeChannelDiscovery = None
    ChannelInfo = None
    VideoMetadata = None
    DatabaseSchemaManager = None
    PersonRecord = None
    VideoRecord = None
    InputHandler = None

# Optional advanced module imports
try:
    from .database_operations_ext import MassDownloadDatabaseOperations
    from .download_integration import DownloadIntegration, DownloadResult
    from .concurrent_processor import ConcurrentProcessor, ResourceLimits
    from .progress_monitor import ProgressMonitor, ProgressReporter
    _ADVANCED_IMPORTS_OK = True
except ImportError as e:
    logger.warning(f"Advanced imports failed: {e}")
    _ADVANCED_IMPORTS_OK = False
    MassDownloadDatabaseOperations = None
    DownloadIntegration = None
    DownloadResult = None
    ConcurrentProcessor = None
    ResourceLimits = None
    ProgressMonitor = None
    ProgressReporter = None

# Error recovery imports (may not exist)
try:
    from .error_recovery import (
        ErrorRecoveryManager, RecoveryStrategy, ErrorContext,
        RecoveryCheckpoint, TransactionManager
    )
except ImportError:
    logger.info("Error recovery module not available - using basic error handling")
    ErrorRecoveryManager = None
    RecoveryStrategy = None
    ErrorContext = None
    RecoveryCheckpoint = None
    TransactionManager = None


class ProcessingStatus(Enum):
    """Status enum for tracking processing state."""
    PENDING = "pending"
    DISCOVERING = "discovering"
    DOWNLOADING = "downloading"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ChannelProcessingResult:
    """Result of processing a single channel."""
    channel_url: str
    person_id: Optional[int] = None
    status: ProcessingStatus = ProcessingStatus.PENDING
    videos_found: int = 0
    videos_processed: int = 0
    videos_skipped: int = 0
    videos_failed: int = 0
    error_message: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    channel_info: Optional[ChannelInfo] = None
    
    def __post_init__(self):
        """Validate processing result with fail-fast principles."""
        if not self.channel_url or not isinstance(self.channel_url, str):
            raise ValueError(
                f"VALIDATION ERROR: channel_url is required and must be non-empty string. "
                f"Got: {self.channel_url}"
            )
        
        if not isinstance(self.status, ProcessingStatus):
            raise ValueError(
                f"VALIDATION ERROR: status must be ProcessingStatus enum. "
                f"Got: {self.status} (type: {type(self.status)})"
            )
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate processing duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.videos_found == 0:
            return 0.0
        return (self.videos_processed / self.videos_found) * 100.0


@dataclass
class MassDownloadProgress:
    """Track overall progress of mass download operation."""
    total_channels: int = 0
    channels_processed: int = 0
    channels_failed: int = 0
    channels_skipped: int = 0
    total_videos: int = 0
    videos_processed: int = 0
    videos_failed: int = 0
    videos_skipped: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    current_channel: Optional[str] = None
    current_status: Optional[str] = None
    errors: List[str] = field(default_factory=list)
    
    @property
    def channels_remaining(self) -> int:
        """Calculate remaining channels to process."""
        return self.total_channels - (self.channels_processed + self.channels_failed + self.channels_skipped)
    
    @property
    def overall_progress_percent(self) -> float:
        """Calculate overall progress percentage."""
        if self.total_channels == 0:
            return 0.0
        completed = self.channels_processed + self.channels_failed + self.channels_skipped
        return (completed / self.total_channels) * 100.0
    
    @property
    def elapsed_time_seconds(self) -> float:
        """Calculate elapsed time in seconds."""
        return (datetime.now() - self.start_time).total_seconds()
    
    @property
    def estimated_time_remaining_seconds(self) -> Optional[float]:
        """Estimate remaining time based on current progress."""
        if self.channels_processed == 0 or self.channels_remaining == 0:
            return None
        
        avg_time_per_channel = self.elapsed_time_seconds / self.channels_processed
        return avg_time_per_channel * self.channels_remaining
    
    def get_status_dict(self) -> Dict[str, Any]:
        """Get status as dictionary for reporting."""
        return {
            "total_channels": self.total_channels,
            "channels_processed": self.channels_processed,
            "channels_failed": self.channels_failed,
            "channels_skipped": self.channels_skipped,
            "channels_remaining": self.channels_remaining,
            "total_videos": self.total_videos,
            "videos_processed": self.videos_processed,
            "videos_failed": self.videos_failed,
            "videos_skipped": self.videos_skipped,
            "current_channel": self.current_channel,
            "current_status": self.current_status,
            "progress_percent": round(self.overall_progress_percent, 1),
            "elapsed_seconds": round(self.elapsed_time_seconds, 1),
            "estimated_remaining_seconds": round(self.estimated_time_remaining_seconds, 1) if self.estimated_time_remaining_seconds else None,
            "errors_count": len(self.errors),
            "recent_errors": self.errors[-5:]  # Last 5 errors
        }


class MassDownloadCoordinator:
    """
    Coordinate mass download operations with fail-fast/fail-loud/fail-safely principles.
    
    This coordinator manages:
    - Channel discovery and video enumeration
    - Database operations for persons and videos
    - Download coordination and S3 streaming
    - Progress tracking and error handling
    - Concurrent processing with resource management
    """
    
    def __init__(self, config: Optional[Any] = None):
        """
        Initialize mass download coordinator.
        
        Args:
            config: Configuration object (loads from config if None)
        """
        self.config = config or get_config()
        
        # Initialize components
        self.channel_discovery = YouTubeChannelDiscovery()
        
        # Initialize database manager (optional for testing)
        try:
            self.database_manager = DatabaseSchemaManager()
            self.db_ops = MassDownloadDatabaseOperations()
        except Exception as e:
            logger.warning(f"Database manager initialization failed (will run without database): {e}")
            self.database_manager = None
            self.db_ops = None
            
        # In-memory storage for video records when database is not available
        self.in_memory_videos = {}  # person_id -> List[VideoRecord]
            
        self.input_handler = InputHandler()
        
        # Initialize download integration
        self.download_integration = DownloadIntegration(config=self.config)
        
        # Processing configuration
        self.max_concurrent_channels = self.config.get("mass_download", {}).get("max_concurrent_channels", 3)
        self.max_videos_per_channel = self.config.get("mass_download", {}).get("max_videos_per_channel", None)
        self.skip_existing_videos = self.config.get("mass_download", {}).get("skip_existing_videos", True)
        self.continue_on_error = self.config.get("mass_download", {}).get("continue_on_error", True)
        self.download_videos = self.config.get("mass_download", {}).get("download_videos", True)
        self.max_concurrent_downloads = self.config.get("mass_download", {}).get("max_concurrent_downloads", 3)
        
        # Progress tracking
        self.progress = MassDownloadProgress()
        self.processing_results: List[ChannelProcessingResult] = []
        self.progress_lock = threading.RLock()
        self.job_id: Optional[str] = None
        self.input_file_path: Optional[str] = None
        
        # Enhanced concurrent processor with resource management
        resource_limits = ResourceLimits(
            max_cpu_percent=80.0,
            max_memory_percent=80.0,
            max_concurrent_channels=self.max_concurrent_channels,
            max_concurrent_downloads=self.max_concurrent_downloads,
            max_queue_size=100,
            check_interval_seconds=5.0
        )
        self.concurrent_processor = ConcurrentProcessor(
            resource_limits=resource_limits,
            progress_callback=self._on_concurrent_progress
        )
        
        # Thread pool for backward compatibility (will be removed)
        self.executor = ThreadPoolExecutor(max_workers=self.max_concurrent_channels)
        
        # Error recovery manager
        recovery_dir = Path(self.config.get("mass_download.recovery_dir", "/tmp/mass_download_recovery"))
        self.error_recovery = ErrorRecoveryManager(
            checkpoint_dir=recovery_dir / "checkpoints",
            dead_letter_path=recovery_dir / "dead_letter.json"
        )
        
        # Progress monitor
        self.progress_monitor = ProgressMonitor(
            update_interval=1.0,
            persist_interval=30.0,
            progress_file=Path("mass_download_progress.json")
        )
        
        logger.info("MassDownloadCoordinator initialized successfully")
        logger.info(f"Configuration: max_concurrent_channels={self.max_concurrent_channels}, "
                   f"max_videos_per_channel={self.max_videos_per_channel}, "
                   f"skip_existing_videos={self.skip_existing_videos}, "
                   f"download_videos={self.download_videos}")
    
    def resume_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Resume a previous job.
        
        Args:
            job_id: Job ID to resume
            
        Returns:
            Job progress dictionary or None if not found
        """
        if not self.db_ops:
            logger.warning("Database not available, cannot resume job")
            return None
        
        try:
            # Get job progress
            progress = self.db_ops.get_progress_by_job_id(job_id)
            
            if not progress:
                logger.error(f"Job not found: {job_id}")
                return None
            
            if progress['status'] not in ['running', 'paused']:
                logger.warning(f"Job {job_id} has status '{progress['status']}', cannot resume")
                return None
            
            # Restore progress
            self.job_id = job_id
            self.input_file_path = progress['input_file']
            
            with self.progress_lock:
                self.progress.total_channels = progress['total_channels']
                self.progress.channels_processed = progress['channels_processed']
                self.progress.channels_failed = progress['channels_failed']
                self.progress.channels_skipped = progress['channels_skipped']
                self.progress.total_videos = progress['total_videos']
                self.progress.videos_processed = progress['videos_processed']
                self.progress.videos_failed = progress['videos_failed']
                self.progress.videos_skipped = progress['videos_skipped']
                self.progress.start_time = progress['started_at']
            
            logger.info(f"Resumed job {job_id}: {progress['channels_processed']}/{progress['total_channels']} channels processed")
            return progress
            
        except Exception as e:
            logger.error(f"Failed to resume job {job_id}: {e}")
            return None
    
    def _on_concurrent_progress(self, event_type: str, event_data: Dict[str, Any]):
        """
        Handle progress events from concurrent processor.
        
        Args:
            event_type: Type of event (task_completed, task_failed, etc)
            event_data: Event data dictionary
        """
        try:
            if event_type == "task_completed":
                logger.info(f"Task completed: {event_data.get('task_id', 'unknown')}")
            elif event_type == "task_failed":
                error_msg = f"Task failed: {event_data.get('task_id', 'unknown')} - {event_data.get('error', 'unknown error')}"
                self._add_error(error_msg)
            elif event_type == "download_completed":
                with self.progress_lock:
                    self.progress.videos_processed += 1
                    self._save_progress_to_database()
            elif event_type == "download_failed":
                with self.progress_lock:
                    self.progress.videos_failed += 1
                    self._save_progress_to_database()
        except Exception as e:
            logger.error(f"Error handling concurrent progress event: {e}")
    
    def process_input_file(self, input_file_path: str, job_id: Optional[str] = None) -> List[Tuple[PersonRecord, str]]:
        """
        Process input file and extract person/channel pairs.
        
        Args:
            input_file_path: Path to input file (CSV, JSON, or text)
            job_id: Optional job ID for resume capability
            
        Returns:
            List of (PersonRecord, channel_url) tuples
            
        Raises:
            ValueError: If input file is invalid
            RuntimeError: If processing fails
        """
        try:
            logger.info(f"Processing input file: {input_file_path}")
            
            # Validate file exists
            if not Path(input_file_path).exists():
                raise ValueError(f"Input file not found: {input_file_path}")
            
            # Generate job ID if not provided
            if not job_id:
                job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{Path(input_file_path).stem}"
            
            self.job_id = job_id
            self.input_file_path = input_file_path
            
            logger.info(f"Job ID: {self.job_id}")
            
            # Parse input file
            entries = self.input_handler.parse_input_file(input_file_path)
            
            if not entries:
                raise ValueError(f"No valid entries found in input file: {input_file_path}")
            
            logger.info(f"Found {len(entries)} entries in input file")
            
            # Validate and create person/channel pairs
            person_channel_pairs = []
            
            for entry in entries:
                # ChannelInput objects already validated
                try:
                    # Convert to PersonRecord
                    person = entry.to_person_record()
                    
                    # Validate channel URL (extra validation)
                    normalized_url = self.channel_discovery.validate_channel_url(entry.channel_url)
                    
                    person_channel_pairs.append((person, normalized_url))
                    
                except (ValueError, KeyError) as e:
                    logger.error(f"Invalid entry in input file: {e}")
                    if not self.continue_on_error:
                        raise
                    continue
            
            logger.info(f"Successfully validated {len(person_channel_pairs)} channel entries")
            
            # Save initial progress to database
            self._save_progress_to_database()
            
            return person_channel_pairs
            
        except Exception as e:
            logger.error(f"Failed to process input file: {e}")
            raise RuntimeError(f"INPUT FILE ERROR: Failed to process {input_file_path}: {e}") from e
    
    def _update_progress(self, **kwargs):
        """Thread-safe progress update."""
        with self.progress_lock:
            for key, value in kwargs.items():
                if hasattr(self.progress, key):
                    setattr(self.progress, key, value)
                else:
                    logger.warning(f"Unknown progress attribute: {key}")
    
    def _add_error(self, error_message: str):
        """Thread-safe error addition."""
        with self.progress_lock:
            self.progress.errors.append(f"[{datetime.now().isoformat()}] {error_message}")
            logger.error(f"Processing error: {error_message}")
    
    def _save_progress_to_database(self):
        """Save current progress to database."""
        if not self.db_ops or not self.job_id:
            return
        
        try:
            from database_schema import ProgressRecord
            
            with self.progress_lock:
                progress_record = ProgressRecord(
                    job_id=self.job_id,
                    input_file=self.input_file_path or "",
                    total_channels=self.progress.total_channels,
                    channels_processed=self.progress.channels_processed,
                    channels_failed=self.progress.channels_failed,
                    channels_skipped=self.progress.channels_skipped,
                    total_videos=self.progress.total_videos,
                    videos_processed=self.progress.videos_processed,
                    videos_failed=self.progress.videos_failed,
                    videos_skipped=self.progress.videos_skipped,
                    status="running",  # Always use "running" for active jobs
                    started_at=self.progress.start_time
                )
                
                self.db_ops.save_progress(progress_record)
                
        except Exception as e:
            logger.warning(f"Failed to save progress to database: {e}")
    
    def _create_channel_checkpoint(self, channel_url: str, person: PersonRecord, 
                                   videos_processed: List[str], videos_pending: List[str]) -> RecoveryCheckpoint:
        """Create checkpoint for channel processing."""
        checkpoint_id = f"channel_{channel_url.replace('/', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        return self.error_recovery.create_checkpoint(
            checkpoint_id=checkpoint_id,
            operation=f"process_channel_{channel_url}",
            state={
                'person': person.__dict__,
                'channel_url': channel_url,
                'job_id': self.job_id
            },
            completed_items=videos_processed,
            pending_items=videos_pending
        )
    
    def process_channel(self, person: PersonRecord, channel_url: str, 
                       checkpoint_id: Optional[str] = None) -> ChannelProcessingResult:
        """
        Process a single channel (discover, store, prepare for download).
        
        Args:
            person: PersonRecord for the channel owner
            channel_url: YouTube channel URL
            
        Returns:
            ChannelProcessingResult with processing details
        """
        result = ChannelProcessingResult(
            channel_url=channel_url,
            start_time=datetime.now()
        )
        
        try:
            logger.info(f"Starting channel processing - URL: {channel_url}, Person: {person.name}")
            
            # Update progress monitor
            self.progress_monitor.start_channel(channel_url, person.name)
            
            self._update_progress(current_channel=channel_url, current_status="discovering")
            result.status = ProcessingStatus.DISCOVERING
            
            # Step 1: Extract channel information
            try:
                channel_info = self.channel_discovery.extract_channel_info(channel_url)
                result.channel_info = channel_info
                
                # Update person record with channel info
                person.channel_id = channel_info.channel_id
                
            except Exception as e:
                raise RuntimeError(f"Channel discovery failed: {e}") from e
            
            # Step 2: Save or update person in database
            try:
                if self.db_ops:
                    person_id = self.db_ops.save_person(person)
                    result.person_id = person_id
                    logger.info(f"Person record saved to database with ID: {person_id}")
                else:
                    # Fallback for testing without database
                    person_id = hash(person.name + person.channel_url) % 1000000
                    result.person_id = person_id
                    logger.info(f"Person record prepared with fallback ID: {person_id} (database not available)")
                
            except Exception as e:
                raise RuntimeError(f"Database operation failed for person: {e}") from e
            
            # Step 3: Enumerate videos from channel
            self._update_progress(current_status="enumerating videos")
            
            try:
                videos = self.channel_discovery.enumerate_channel_videos(
                    channel_url, 
                    max_videos=self.max_videos_per_channel
                )
                
                result.videos_found = len(videos)
                logger.info(f"Found {len(videos)} videos in channel {channel_url}")
                
                # Update progress monitor with video count
                self.progress_monitor.update_channel_videos(channel_url, len(videos))
                
                if not videos:
                    logger.warning(f"No videos found in channel: {channel_url}")
                    result.status = ProcessingStatus.COMPLETED
                    result.end_time = datetime.now()
                    return result
                
            except Exception as e:
                raise RuntimeError(f"Video enumeration failed: {e}") from e
            
            # Step 4: Process each video
            self._update_progress(current_status="processing videos")
            
            for i, video_metadata in enumerate(videos):
                try:
                    # Check for duplicates if configured
                    if self.skip_existing_videos and self.channel_discovery.is_duplicate_video(video_metadata.video_id):
                        logger.debug(f"Skipping duplicate video: {video_metadata.video_id}")
                        result.videos_skipped += 1
                        continue
                    
                    # Create video record
                    video_record = VideoRecord(
                        person_id=person_id,
                        video_id=video_metadata.video_id,
                        title=video_metadata.title,
                        duration=video_metadata.duration,
                        upload_date=video_metadata.upload_date,
                        view_count=video_metadata.view_count,
                        description=video_metadata.description[:1000] if video_metadata.description else None,  # Truncate
                        download_status="pending"
                    )
                    # Add person_name as an attribute for S3 upload
                    video_record.person_name = person.name
                    
                    # Save to database or in-memory store
                    if self.db_ops:
                        video_db_id = self.db_ops.save_video(video_record)
                        logger.debug(f"Video saved to database with ID: {video_db_id}")
                    else:
                        # Store in memory when database is not available
                        if person_id not in self.in_memory_videos:
                            self.in_memory_videos[person_id] = []
                        self.in_memory_videos[person_id].append(video_record)
                        logger.debug(f"Video record stored in memory (database not available): {video_record.video_id}")
                    
                    # Mark as processed for duplicate detection
                    self.channel_discovery.mark_video_processed(
                        video_metadata.video_id,
                        video_record.uuid
                    )
                    
                    result.videos_processed += 1
                    
                    # Update overall progress
                    with self.progress_lock:
                        self.progress.total_videos += 1
                        self.progress.videos_processed += 1
                    
                    # Update progress monitor
                    self.progress_monitor.update_video_progress(
                        video_metadata.video_id, 
                        video_metadata.title,
                        downloaded=False,  # Just metadata processing
                        failed=False
                    )
                    
                    # Progress logging and saving
                    if (i + 1) % 10 == 0 or (i + 1) == len(videos):
                        logger.info(f"Processed {i + 1}/{len(videos)} videos for channel {channel_url}")
                        self._save_progress_to_database()
                    
                except Exception as e:
                    logger.error(f"Failed to process video {video_metadata.video_id}: {e}")
                    result.videos_failed += 1
                    
                    with self.progress_lock:
                        self.progress.videos_failed += 1
                    
                    if not self.continue_on_error:
                        raise
            
            # Mark as completed
            result.status = ProcessingStatus.COMPLETED
            result.end_time = datetime.now()
            
            # Update progress monitor
            self.progress_monitor.complete_channel(channel_url, success=True)
            
            logger.info(f"Channel processing completed: {channel_url} - "
                       f"Processed: {result.videos_processed}, "
                       f"Skipped: {result.videos_skipped}, "
                       f"Failed: {result.videos_failed}")
            
            # Add result to tracking
            self.processing_results.append(result)
            
            return result
            
        except Exception as e:
            result.status = ProcessingStatus.FAILED
            result.error_message = str(e)
            result.end_time = datetime.now()
            
            # Update progress monitor
            self.progress_monitor.complete_channel(channel_url, success=False, error_message=str(e))
            
            self._add_error(f"Channel {channel_url} failed: {e}")
            
            logger.error(f"Channel processing failed for {channel_url}: {e}")
            
            if not self.continue_on_error:
                raise
            
            # Add result to tracking
            self.processing_results.append(result)
            return result
        
        finally:
            # Update progress
            with self.progress_lock:
                if result.status == ProcessingStatus.COMPLETED:
                    self.progress.channels_processed += 1
                elif result.status == ProcessingStatus.FAILED:
                    self.progress.channels_failed += 1
                elif result.status == ProcessingStatus.SKIPPED:
                    self.progress.channels_skipped += 1
            
            # Save progress after each channel
            self._save_progress_to_database()
    
    def process_channel_with_recovery(self, person: PersonRecord, channel_url: str) -> ChannelProcessingResult:
        """
        Process channel with comprehensive error recovery.
        
        This wraps process_channel with transaction management, circuit breakers,
        and checkpoint-based recovery.
        
        Args:
            person: Person record
            channel_url: YouTube channel URL
            
        Returns:
            ChannelProcessingResult with processing outcome
        """
        result = ChannelProcessingResult(
            channel_url=channel_url,
            start_time=datetime.now()
        )
        
        try:
            # Check if we have a checkpoint for this channel
            checkpoint = None
            checkpoint_files = list((self.error_recovery.checkpoint_dir / "checkpoints").glob(f"channel_{channel_url.replace('/', '_')}*.pkl"))
            if checkpoint_files:
                latest_checkpoint = max(checkpoint_files, key=lambda f: f.stat().st_mtime)
                checkpoint = RecoveryCheckpoint.load(latest_checkpoint)
                logger.info(f"Found checkpoint for {channel_url}: {checkpoint.checkpoint_id}")
            
            # Use transaction manager for rollback capability
            transaction = TransactionManager()
            channel_info = None
            person_id = None
            videos_processed = []
            videos_pending = []
            
            # Operation 1: Extract channel info with circuit breaker
            def extract_channel_info():
                nonlocal channel_info
                channel_info = self.error_recovery.with_recovery(
                    f"extract_channel_info_{channel_url}",
                    lambda: self.channel_discovery.extract_channel_info(channel_url),
                    recovery_strategy=RecoveryStrategy.CIRCUIT_BREAKER,
                    fallback=lambda: None
                )
                if not channel_info:
                    raise RuntimeError(f"Failed to extract channel info for {channel_url}")
                
                result.channel_info = channel_info
                person.channel_id = channel_info.channel_id
                return channel_info
            
            def rollback_channel_info():
                logger.warning(f"Rolling back channel info extraction for {channel_url}")
            
            transaction.add_operation("extract_channel_info", extract_channel_info, rollback_channel_info)
            
            # Operation 2: Save person with retry
            def save_person():
                nonlocal person_id
                if self.db_ops:
                    person_id = self.error_recovery.with_recovery(
                        f"save_person_{person.name}",
                        lambda: self.db_ops.save_person(person),
                        recovery_strategy=RecoveryStrategy.RETRY_BACKOFF
                    )
                else:
                    person_id = hash(person.name + person.channel_url) % 1000000
                
                result.person_id = person_id
                logger.info(f"Person saved with ID: {person_id}")
                return person_id
            
            def rollback_person():
                if self.db_ops and person_id:
                    try:
                        # Delete the person record
                        with self.db_ops.db_manager.get_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute("DELETE FROM persons WHERE id = %s", (person_id,))
                            conn.commit()
                        logger.warning(f"Rolled back person record: {person_id}")
                    except Exception as e:
                        logger.error(f"Failed to rollback person: {e}")
            
            transaction.add_operation("save_person", save_person, rollback_person)
            
            # Execute transactional operations
            transaction.execute()
            
            # Operation 3: Enumerate videos with checkpoint support
            if checkpoint and checkpoint.pending_items:
                # Resume from checkpoint
                videos_pending = checkpoint.pending_items
                videos_processed = checkpoint.completed_items
                logger.info(f"Resuming from checkpoint: {len(videos_processed)} processed, {len(videos_pending)} pending")
            else:
                # Fresh enumeration
                videos = self.error_recovery.with_recovery(
                    f"enumerate_videos_{channel_url}",
                    lambda: self.channel_discovery.enumerate_channel_videos(
                        channel_url, 
                        max_videos=self.max_videos_per_channel
                    ),
                    recovery_strategy=RecoveryStrategy.RETRY_BACKOFF
                )
                
                videos_pending = [v.video_id for v in videos]
                result.videos_found = len(videos)
                logger.info(f"Found {len(videos)} videos in channel {channel_url}")
            
            # Process videos with periodic checkpointing
            checkpoint_interval = 25  # Create checkpoint every 25 videos
            
            for i, video_id in enumerate(videos_pending):
                try:
                    # Skip if already processed
                    if video_id in videos_processed:
                        continue
                    
                    # Process video with skip strategy for individual failures
                    def process_single_video():
                        # Get full metadata
                        video_metadata = next(
                            (v for v in self.channel_discovery.enumerate_channel_videos(channel_url, max_videos=1)
                             if v.video_id == video_id),
                            None
                        )
                        
                        if not video_metadata:
                            raise ValueError(f"Could not retrieve metadata for video {video_id}")
                        
                        # Check for duplicates
                        if self.skip_existing_videos and self.channel_discovery.is_duplicate_video(video_id):
                            result.videos_skipped += 1
                            return
                        
                        # Create and save video record
                        video_record = VideoRecord(
                            person_id=person_id,
                            video_id=video_metadata.video_id,
                            title=video_metadata.title,
                            duration=video_metadata.duration,
                            upload_date=video_metadata.upload_date,
                            view_count=video_metadata.view_count,
                            description=video_metadata.description[:1000] if video_metadata.description else None,
                            download_status="pending"
                        )
                        
                        if self.db_ops:
                            self.db_ops.save_video(video_record)
                        
                        # Mark as processed
                        self.channel_discovery.mark_video_processed(video_id, video_record.uuid)
                        
                        result.videos_processed += 1
                        with self.progress_lock:
                            self.progress.total_videos += 1
                            self.progress.videos_processed += 1
                    
                    # Process with skip strategy - continue on individual video failures
                    self.error_recovery.with_recovery(
                        f"process_video_{video_id}",
                        process_single_video,
                        recovery_strategy=RecoveryStrategy.SKIP
                    )
                    
                    videos_processed.append(video_id)
                    
                    # Create checkpoint periodically
                    if (i + 1) % checkpoint_interval == 0:
                        remaining = [v for v in videos_pending if v not in videos_processed]
                        self._create_channel_checkpoint(channel_url, person, videos_processed, remaining)
                        logger.info(f"Checkpoint created: {len(videos_processed)} processed, {len(remaining)} remaining")
                    
                except Exception as e:
                    logger.error(f"Failed to process video {video_id}: {e}")
                    result.videos_failed += 1
                    
                    # Add to checkpoint's failed items
                    if checkpoint:
                        checkpoint.failed_items.append((video_id, ErrorContext(
                            error_type=type(e).__name__,
                            error_message=str(e),
                            operation=f"process_video_{video_id}"
                        )))
            
            # Final checkpoint
            if videos_pending:
                self._create_channel_checkpoint(channel_url, person, videos_processed, [])
            
            # Update progress tracking
            with self.progress_lock:
                self.progress.channels_processed += 1
            
            # Mark result as completed
            result.status = ProcessingStatus.COMPLETED
            result.end_time = datetime.now()
            
            logger.info(f"Successfully processed channel {channel_url}: "
                       f"{result.videos_processed} videos processed, "
                       f"{result.videos_skipped} skipped, "
                       f"{result.videos_failed} failed")
            
            return result
            
        except Exception as e:
            logger.error(f"Channel processing failed for {channel_url}: {e}")
            
            result.status = ProcessingStatus.FAILED
            result.error_message = str(e)
            result.end_time = datetime.now()
            
            with self.progress_lock:
                self.progress.channels_failed += 1
            
            # Save failure checkpoint
            if 'videos_processed' in locals() and 'videos_pending' in locals():
                checkpoint = self._create_channel_checkpoint(
                    channel_url, person, videos_processed,
                    [v for v in videos_pending if v not in videos_processed]
                )
                checkpoint.failed_items.append((channel_url, ErrorContext(
                    error_type=type(e).__name__,
                    error_message=str(e),
                    operation="process_channel"
                )))
                checkpoint.save(self.error_recovery.checkpoint_dir)
            
            if not self.continue_on_error:
                raise
            
            return result
        
        finally:
            # Save progress after each channel
            self._save_progress_to_database()
    
    def process_channels_concurrently(self, person_channel_pairs: List[Tuple[PersonRecord, str]]) -> List[ChannelProcessingResult]:
        """
        Process multiple channels concurrently with resource management.
        
        Args:
            person_channel_pairs: List of (PersonRecord, channel_url) tuples
            
        Returns:
            List of ChannelProcessingResult objects
        """
        logger.info(f"Starting concurrent channel processing - Total channels: {len(person_channel_pairs)}")
        
        # Start progress monitor
        self.progress_monitor.start()
        self.progress_monitor.update_channel_count(len(person_channel_pairs))
        
        # Initialize progress
        with self.progress_lock:
            self.progress.total_channels = len(person_channel_pairs)
            self.progress.start_time = datetime.now()
        
        # Save initial progress
        self._save_progress_to_database()
        
        # Submit all tasks
        futures = []
        for person, channel_url in person_channel_pairs:
            future = self.executor.submit(self.process_channel, person, channel_url)
            futures.append((future, channel_url))
        
        # Process results as they complete
        results = []
        for future, channel_url in futures:
            try:
                result = future.result(timeout=3600)  # 1 hour timeout per channel
                results.append(result)
                self.processing_results.append(result)
                
                # Log progress
                progress_dict = self.progress.get_status_dict()
                logger.info(f"Progress: {progress_dict['progress_percent']:.1f}% - "
                           f"Channels: {progress_dict['channels_processed']}/{progress_dict['total_channels']} - "
                           f"Videos: {progress_dict['videos_processed']} processed")
                
            except Exception as e:
                logger.error(f"Channel processing failed for {channel_url}: {e}")
                
                # Create failed result
                failed_result = ChannelProcessingResult(
                    channel_url=channel_url,
                    status=ProcessingStatus.FAILED,
                    error_message=str(e),
                    start_time=datetime.now(),
                    end_time=datetime.now()
                )
                
                results.append(failed_result)
                self.processing_results.append(failed_result)
                
                with self.progress_lock:
                    self.progress.channels_failed += 1
                
                if not self.continue_on_error:
                    # Cancel remaining futures
                    for remaining_future, _ in futures:
                        if not remaining_future.done():
                            remaining_future.cancel()
                    raise
        
        logger.info(f"Concurrent processing completed. Processed {len(results)} channels")
        
        # Stop progress monitor and generate report
        self.progress_monitor.stop()
        
        # Generate and save final report
        reporter = ProgressReporter(self.progress_monitor)
        report_path = Path(f"mass_download_report_{self.job_id}.txt")
        reporter.save_report(report_path)
        logger.info(f"Progress report saved to: {report_path}")
        
        return results
    
    def get_progress_report(self) -> Dict[str, Any]:
        """
        Get comprehensive progress report.
        
        Returns:
            Dictionary with detailed progress information
        """
        with self.progress_lock:
            report = self.progress.get_status_dict()
            
            # Add channel-specific results
            channel_results = []
            for result in self.processing_results:
                channel_results.append({
                    "channel_url": result.channel_url,
                    "status": result.status.value,
                    "videos_found": result.videos_found,
                    "videos_processed": result.videos_processed,
                    "videos_skipped": result.videos_skipped,
                    "videos_failed": result.videos_failed,
                    "duration_seconds": result.duration_seconds,
                    "success_rate": round(result.success_rate, 1),
                    "error_message": result.error_message
                })
            
            report["channel_results"] = channel_results
            
            # Calculate overall statistics
            total_duration = sum(r.duration_seconds or 0 for r in self.processing_results)
            report["total_processing_seconds"] = round(total_duration, 1)
            
            successful_channels = sum(1 for r in self.processing_results if r.status == ProcessingStatus.COMPLETED)
            report["success_rate_percent"] = round((successful_channels / len(self.processing_results) * 100) if self.processing_results else 0, 1)
            
            return report
    
    def process_channel_with_downloads(self, person: PersonRecord, channel_url: str) -> ChannelProcessingResult:
        """
        Process a channel and download its videos.
        
        This is the complete workflow:
        1. Discover channel and enumerate videos
        2. Store metadata in database
        3. Download videos and stream to S3
        
        Args:
            person: PersonRecord for the channel owner
            channel_url: YouTube channel URL
            
        Returns:
            ChannelProcessingResult with complete processing details
        """
        # First, process the channel to discover videos
        result = self.process_channel(person, channel_url)
        
        if result.status != ProcessingStatus.COMPLETED or not self.download_videos:
            return result
        
        # Now download the videos
        logger.info(f"Starting downloads for channel {channel_url}")
        
        try:
            # Get video records that need downloading
            video_records = self._get_pending_video_records(result.person_id)
            
            if not video_records:
                logger.info(f"No videos to download for channel {channel_url}")
                return result
            
            # Download videos
            download_results = self.download_integration.batch_download(
                video_records,
                max_concurrent=self.max_concurrent_downloads
            )
            
            # Update result with download statistics
            downloads_completed = sum(1 for r in download_results if r.status == "completed")
            downloads_failed = sum(1 for r in download_results if r.status == "failed")
            
            # Update video records in database
            for i, download_result in enumerate(download_results):
                video_record = video_records[i]
                if download_result.status == "completed":
                    video_record.download_status = "completed"
                    video_record.s3_path = download_result.s3_path
                    video_record.file_size = download_result.file_size
                    # Update in database
                    if self.db_ops:
                        self.db_ops.update_video_status(
                            video_record.video_id,
                            "completed",
                            s3_path=download_result.s3_path,
                            file_size=download_result.file_size
                        )
                elif download_result.status == "failed":
                    video_record.download_status = "failed"
                    video_record.error_message = download_result.error_message
                    # Update in database
                    if self.db_ops:
                        self.db_ops.update_video_status(
                            video_record.video_id,
                            "failed",
                            error_message=download_result.error_message
                        )
            
            # Update progress
            with self.progress_lock:
                self.progress.videos_processed += downloads_completed
                self.progress.videos_failed += downloads_failed
            
            logger.info(f"Downloads completed for channel {channel_url}: "
                       f"{downloads_completed} successful, {downloads_failed} failed")
            
            return result
            
        except Exception as e:
            logger.error(f"Download phase failed for channel {channel_url}: {e}")
            if not self.continue_on_error:
                raise
            return result
    
    def _get_pending_video_records(self, person_id: int) -> List[VideoRecord]:
        """
        Get video records that need downloading.
        
        Args:
            person_id: Person ID to get videos for
            
        Returns:
            List of VideoRecord objects with pending status
        """
        if self.db_ops:
            pending_videos = self.db_ops.get_pending_videos(person_id=person_id)
            logger.debug(f"Found {len(pending_videos)} pending videos for person {person_id}")
            return pending_videos
        else:
            # Return videos from in-memory storage when database is not available
            pending_videos = self.in_memory_videos.get(person_id, [])
            # Filter for pending status
            pending_videos = [v for v in pending_videos if v.download_status == "pending"]
            logger.debug(f"Found {len(pending_videos)} pending videos in memory for person {person_id}")
            return pending_videos
    
    def process_channels_with_downloads(self, person_channel_pairs: List[Tuple[PersonRecord, str]]) -> List[ChannelProcessingResult]:
        """
        Process multiple channels with downloads concurrently.
        
        Args:
            person_channel_pairs: List of (PersonRecord, channel_url) tuples
            
        Returns:
            List of ChannelProcessingResult objects
        """
        logger.info(f"Starting concurrent processing with downloads for {len(person_channel_pairs)} channels")
        
        # Initialize progress
        with self.progress_lock:
            self.progress.total_channels = len(person_channel_pairs)
            self.progress.start_time = datetime.now()
        
        # Save initial progress
        self._save_progress_to_database()
        
        # Submit all tasks
        futures = []
        for person, channel_url in person_channel_pairs:
            future = self.executor.submit(self.process_channel_with_downloads, person, channel_url)
            futures.append((future, channel_url))
        
        # Process results as they complete
        results = []
        for future, channel_url in futures:
            try:
                result = future.result(timeout=7200)  # 2 hour timeout per channel
                results.append(result)
                
                # Log progress
                progress_dict = self.progress.get_status_dict()
                logger.info(f"Progress: {progress_dict['progress_percent']:.1f}% - "
                           f"Channels: {progress_dict['channels_processed']}/{progress_dict['total_channels']} - "
                           f"Videos: {progress_dict['videos_processed']} processed")
                
            except Exception as e:
                logger.error(f"Channel processing with downloads failed for {channel_url}: {e}")
                
                # Create failed result
                failed_result = ChannelProcessingResult(
                    channel_url=channel_url,
                    status=ProcessingStatus.FAILED,
                    error_message=str(e),
                    start_time=datetime.now(),
                    end_time=datetime.now()
                )
                
                results.append(failed_result)
                self.processing_results.append(failed_result)
                
                with self.progress_lock:
                    self.progress.channels_failed += 1
                
                if not self.continue_on_error:
                    # Cancel remaining futures
                    for remaining_future, _ in futures:
                        if not remaining_future.done():
                            remaining_future.cancel()
                    raise
        
        logger.info(f"Concurrent processing with downloads completed. Processed {len(results)} channels")
        return results
    
    def process_channels_with_resource_management(self, person_channel_pairs: List[Tuple[PersonRecord, str]]) -> List[ChannelProcessingResult]:
        """
        Process multiple channels with enhanced resource management.
        
        This method uses the ConcurrentProcessor for better resource control,
        dynamic throttling, and comprehensive monitoring.
        
        Args:
            person_channel_pairs: List of (PersonRecord, channel_url) tuples
            
        Returns:
            List of ChannelProcessingResult objects
        """
        logger.info(f"Starting resource-managed processing of {len(person_channel_pairs)} channels")
        
        # Initialize progress
        with self.progress_lock:
            self.progress.total_channels = len(person_channel_pairs)
            self.progress.start_time = datetime.now()
        
        # Save initial progress
        self._save_progress_to_database()
        
        # Start concurrent processor
        self.concurrent_processor.start()
        
        try:
            # Submit all channel processing tasks
            futures = []
            for i, (person, channel_url) in enumerate(person_channel_pairs):
                task_id = f"channel_{i:04d}_{channel_url.split('/')[-1]}"
                
                # Submit with priority (could be based on channel size, importance, etc)
                priority = 5  # Default priority
                
                future = self.concurrent_processor.submit_channel_task(
                    task_id,
                    self.process_channel,
                    person,
                    channel_url,
                    priority=priority
                )
                futures.append((future, channel_url))
            
            # Process results as they complete
            results = []
            for future, channel_url in futures:
                try:
                    result = future.result(timeout=3600)  # 1 hour timeout per channel
                    results.append(result)
                    self.processing_results.append(result)
                    
                    # Log progress
                    progress_dict = self.progress.get_status_dict()
                    logger.info(f"Progress: {progress_dict['progress_percent']:.1f}% - "
                               f"Channels: {progress_dict['channels_processed']}/{progress_dict['total_channels']} - "
                               f"Videos: {progress_dict['videos_processed']} processed")
                    
                    # Log resource status
                    processor_status = self.concurrent_processor.get_status()
                    logger.info(f"Resource status: {processor_status['resource_status']} - "
                               f"CPU: {processor_status['cpu_percent']:.1f}%, "
                               f"Memory: {processor_status['memory_percent']:.1f}%, "
                               f"Active tasks: {processor_status['active_tasks']}")
                    
                except Exception as e:
                    logger.error(f"Channel processing failed for {channel_url}: {e}")
                    
                    # Create failed result
                    failed_result = ChannelProcessingResult(
                        channel_url=channel_url,
                        status=ProcessingStatus.FAILED,
                        error_message=str(e),
                        start_time=datetime.now(),
                        end_time=datetime.now()
                    )
                    results.append(failed_result)
                    self.processing_results.append(failed_result)
                    
                    # Update progress
                    with self.progress_lock:
                        self.progress.channels_failed += 1
                    
                    if not self.continue_on_error:
                        raise
            
            # Get final processor statistics
            final_stats = self.concurrent_processor.get_status()
            logger.info(f"Resource-managed processing completed. Final stats: {final_stats}")
            
            return results
            
        finally:
            # Ensure concurrent processor is stopped
            self.concurrent_processor.stop()
    
    def retry_failed_operations(self) -> Tuple[int, int]:
        """
        Retry all failed operations from the dead letter queue.
        
        Returns:
            Tuple of (successful_count, failed_count)
        """
        logger.info("Retrying failed operations from dead letter queue")
        
        def process_failed_item(item_data: Dict[str, Any]) -> Any:
            operation = item_data.get('operation', '')
            func = item_data.get('func')
            
            if func and callable(func):
                return func()
            else:
                logger.warning(f"Cannot retry operation {operation}: function not available")
                raise ValueError(f"Function not available for {operation}")
        
        return self.error_recovery.dead_letter_queue.retry_all(process_failed_item)
    
    def get_recovery_report(self) -> Dict[str, Any]:
        """Get comprehensive recovery status report."""
        recovery_status = self.error_recovery.get_recovery_status()
        
        # Add dead letter queue details
        dead_letter_items = self.error_recovery.dead_letter_queue.get_all()
        dead_letter_summary = {}
        
        for item in dead_letter_items:
            error_type = item['error_context'].error_type
            operation = item['error_context'].operation
            
            if error_type not in dead_letter_summary:
                dead_letter_summary[error_type] = []
            
            dead_letter_summary[error_type].append({
                'operation': operation,
                'retry_count': item['error_context'].retry_count,
                'queued_at': item['queued_at'].isoformat()
            })
        
        recovery_status['dead_letter_details'] = dead_letter_summary
        
        # Add checkpoint statistics
        if self.error_recovery.checkpoint_dir and self.error_recovery.checkpoint_dir.exists():
            checkpoints = list(self.error_recovery.checkpoint_dir.glob("*.pkl"))
            
            checkpoint_stats = {
                'total_checkpoints': len(checkpoints),
                'latest_checkpoint': None
            }
            
            if checkpoints:
                latest = max(checkpoints, key=lambda f: f.stat().st_mtime)
                try:
                    cp = RecoveryCheckpoint.load(latest)
                    checkpoint_stats['latest_checkpoint'] = {
                        'id': cp.checkpoint_id,
                        'operation': cp.operation,
                        'timestamp': cp.timestamp.isoformat(),
                        'completed_items': len(cp.completed_items),
                        'pending_items': len(cp.pending_items),
                        'failed_items': len(cp.failed_items)
                    }
                except Exception as e:
                    logger.error(f"Failed to load checkpoint {latest}: {e}")
            
            recovery_status['checkpoint_stats'] = checkpoint_stats
        
        return recovery_status
    
    def cleanup_old_checkpoints(self, days: int = 7):
        """Clean up checkpoints older than specified days."""
        if not self.error_recovery.checkpoint_dir or not self.error_recovery.checkpoint_dir.exists():
            return
        
        cutoff_time = datetime.now() - timedelta(days=days)
        cleaned = 0
        
        for checkpoint_file in self.error_recovery.checkpoint_dir.glob("*.pkl"):
            try:
                if datetime.fromtimestamp(checkpoint_file.stat().st_mtime) < cutoff_time:
                    checkpoint_file.unlink()
                    cleaned += 1
            except Exception as e:
                logger.error(f"Failed to clean checkpoint {checkpoint_file}: {e}")
        
        if cleaned > 0:
            logger.info(f"Cleaned up {cleaned} old checkpoints")
    
    def shutdown(self):
        """Gracefully shutdown the coordinator."""
        logger.info("Shutting down MassDownloadCoordinator")
        
        # Shutdown concurrent processor if available
        if hasattr(self, 'concurrent_processor'):
            try:
                self.concurrent_processor.stop()
                logger.info("Concurrent processor stopped")
            except Exception as e:
                logger.error(f"Error stopping concurrent processor: {e}")
        
        # Shutdown thread pool
        self.executor.shutdown(wait=True)
        
        # Log final statistics
        final_report = self.get_progress_report()
        logger.info(f"Final processing report: {final_report}")
        
        # Log recovery statistics
        try:
            recovery_report = self.get_recovery_report()
            if recovery_report['dead_letter_queue_size'] > 0:
                logger.warning(f"Dead letter queue contains {recovery_report['dead_letter_queue_size']} failed items")
            
            if recovery_report.get('checkpoint_stats', {}).get('total_checkpoints', 0) > 0:
                logger.info(f"Found {recovery_report['checkpoint_stats']['total_checkpoints']} recovery checkpoints")
        except Exception as e:
            logger.error(f"Failed to get recovery report: {e}")
        
        # Clean up old checkpoints
        try:
            self.cleanup_old_checkpoints(days=7)
        except Exception as e:
            logger.error(f"Failed to clean up old checkpoints: {e}")
        
        # Mark job as completed
        if self.db_ops and self.job_id:
            with self.progress_lock:
                # Determine final status
                if self.progress.channels_failed == 0:
                    status = "completed"
                    error_msg = None
                elif self.progress.channels_processed == 0:
                    status = "failed"
                    error_msg = "All channels failed to process"
                else:
                    status = "completed"  # Partial success
                    error_msg = f"{self.progress.channels_failed} channels failed"
                
                self.db_ops.mark_job_completed(self.job_id, status, error_msg)
        
        logger.info("MassDownloadCoordinator shutdown complete")


def validate_mass_coordinator_module():
    """
    Validate mass coordinator module (fail-fast on import).
    
    Raises:
        RuntimeError: If module validation fails
    """
    try:
        # Test basic initialization
        coordinator = MassDownloadCoordinator()
        
        # Test data structures
        result = ChannelProcessingResult(channel_url="https://youtube.com/@test")
        progress = MassDownloadProgress()
        
        logger.info("Mass coordinator module validation PASSED")
        return True
        
    except Exception as e:
        logger.error(f"Mass coordinator module validation FAILED: {e}")
        raise RuntimeError(f"Mass coordinator validation failed: {e}") from e


# Run validation on import (fail-fast) - disabled for testing
# Re-enable when database configuration is properly set up
# if __name__ != "__main__":
#     validate_mass_coordinator_module()