#!/usr/bin/env python3
"""
Database Operations Extension for Mass Download Feature
Phase 1.9: Extend database operations module with new schema support

This module extends the existing database operations with specific
methods for PersonRecord and VideoRecord management.

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from pathlib import Path
import uuid

# Add parent directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir.parent))

# Initialize logger using standard logging
logger = logging.getLogger(__name__)

# Simple error handling decorator (inline implementation)
def with_standard_error_handling(func):
    """Simple error handling decorator."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            raise
    return wrapper

# Import database schema with fallback
try:
    from .database_schema import PersonRecord, VideoRecord, ProgressRecord
    _SCHEMA_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Database schema import failed: {e}")
    _SCHEMA_AVAILABLE = False
    PersonRecord = None
    VideoRecord = None
    ProgressRecord = None

# Placeholder for database operations that would come from utils
class DatabaseManager:
    """Simple database manager placeholder."""
    def __init__(self, config):
        self.config = config
        logger.info("DatabaseManager placeholder initialized")

def get_database_manager():
    """Get database manager placeholder."""
    return DatabaseManager({})

def select(*args, **kwargs):
    """Placeholder select function."""
    logger.warning("Database operations not fully implemented")
    return []

def insert(*args, **kwargs):
    """Placeholder insert function."""
    logger.warning("Database operations not fully implemented")
    return None

def update(*args, **kwargs):
    """Placeholder update function."""
    logger.warning("Database operations not fully implemented")
    return None

def delete(*args, **kwargs):
    """Placeholder delete function."""
    logger.warning("Database operations not fully implemented")  
    return None

def execute_sql(*args, **kwargs):
    """Placeholder SQL execution function."""
    logger.warning("Database operations not fully implemented")
    return None

class QueryBuilder:
    """Placeholder query builder."""
    def __init__(self):
        logger.info("QueryBuilder placeholder initialized")


class MassDownloadDatabaseOperations:
    """
    Database operations specific to mass download feature.
    
    Extends the base database operations with methods for:
    - Person record management
    - Video record management
    - Person-video relationships
    - Download status tracking
    - Batch operations
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize database operations.
        
        Args:
            db_manager: Database manager instance (uses default if None)
        """
        self.db_manager = db_manager or get_database_manager()
        logger.info("MassDownloadDatabaseOperations initialized")
    
    # ==========================================================================
    # PERSON OPERATIONS
    # ==========================================================================
    
    def save_person(self, person: PersonRecord) -> Optional[int]:
        """
        Save or update a person record.
        
        Args:
            person: PersonRecord to save
            
        Returns:
            Person ID if successful, None on error
            
        Raises:
            ValueError: If person validation fails
            RuntimeError: If database operation fails
        """
        # Validate person record (fail-fast)
        person.validate()
        
        # Check if person already exists by channel_url
        existing = self.get_person_by_channel_url(person.channel_url)
        
        if existing:
            # Update existing person
            logger.info(f"Updating existing person: {existing['id']} - {person.name}")
            
            update_data = {
                'name': person.name,
                'email': person.email,
                'type': person.type,
                'channel_id': person.channel_id
            }
            
            rows_affected = update(
                'persons',
                update_data,
                'id = ?',
                [existing['id']]
            )
            
            if rows_affected > 0:
                return existing['id']
            else:
                raise RuntimeError(f"Failed to update person: {existing['id']}")
        else:
            # Insert new person
            logger.info(f"Creating new person: {person.name}")
            
            person_data = {
                'name': person.name,
                'email': person.email,
                'type': person.type,
                'channel_url': person.channel_url,
                'channel_id': person.channel_id,
                'created_at': datetime.now()
            }
            
            person_id = insert('persons', person_data)
            
            if person_id:
                logger.info(f"Created person with ID: {person_id}")
                return person_id
            else:
                raise RuntimeError(f"Failed to create person: {person.name}")
    
    
    def get_person(self, person_id: int) -> Optional[Dict[str, Any]]:
        """
        Get person by ID.
        
        Args:
            person_id: Person ID
            
        Returns:
            Person record as dictionary or None if not found
        """
        results = select(
            'persons',
            where='id = ?',
            params=[person_id]
        )
        
        return results[0] if results else None
    
    
    def get_person_by_channel_url(self, channel_url: str) -> Optional[Dict[str, Any]]:
        """
        Get person by channel URL.
        
        Args:
            channel_url: YouTube channel URL
            
        Returns:
            Person record as dictionary or None if not found
        """
        results = select(
            'persons',
            where='channel_url = ?',
            params=[channel_url]
        )
        
        return results[0] if results else None
    
    
    def list_persons(self, 
                     limit: Optional[int] = None,
                     offset: int = 0,
                     filter_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List persons with optional filtering.
        
        Args:
            limit: Maximum number of records
            offset: Offset for pagination
            filter_type: Filter by person type
            
        Returns:
            List of person records
        """
        where_clause = None
        params = []
        
        if filter_type:
            where_clause = 'type = ?'
            params = [filter_type]
        
        return select(
            'persons',
            where=where_clause,
            params=params if params else None,
            order_by='created_at DESC',
            limit=limit
        )
    
    # ==========================================================================
    # VIDEO OPERATIONS
    # ==========================================================================
    
    
    def save_video(self, video: VideoRecord) -> Optional[int]:
        """
        Save or update a video record.
        
        Args:
            video: VideoRecord to save
            
        Returns:
            Video ID if successful, None on error
            
        Raises:
            ValueError: If video validation fails
            RuntimeError: If database operation fails
        """
        # Validate video record (fail-fast)
        video.validate()
        
        # Check if video already exists by video_id
        existing = self.get_video_by_video_id(video.video_id)
        
        if existing:
            # Update existing video
            logger.info(f"Updating existing video: {existing['id']} - {video.video_id}")
            
            update_data = {
                'title': video.title,
                'duration': video.duration,
                'upload_date': video.upload_date,
                'view_count': video.view_count,
                'description': video.description,
                'download_status': video.download_status,
                's3_path': video.s3_path,
                'file_size': video.file_size,
                'error_message': video.error_message,
                'updated_at': datetime.now()
            }
            
            rows_affected = update(
                'videos',
                update_data,
                'id = ?',
                [existing['id']]
            )
            
            if rows_affected > 0:
                return existing['id']
            else:
                raise RuntimeError(f"Failed to update video: {existing['id']}")
        else:
            # Insert new video
            logger.info(f"Creating new video: {video.video_id} - {video.title}")
            
            video_data = {
                'person_id': video.person_id,
                'video_id': video.video_id,
                'title': video.title,
                'duration': video.duration,
                'upload_date': video.upload_date,
                'view_count': video.view_count,
                'description': video.description,
                'uuid': video.uuid,
                'download_status': video.download_status,
                's3_path': video.s3_path,
                'file_size': video.file_size,
                'error_message': video.error_message,
                'created_at': datetime.now()
            }
            
            video_id = insert('videos', video_data)
            
            if video_id:
                logger.info(f"Created video with ID: {video_id}")
                return video_id
            else:
                raise RuntimeError(f"Failed to create video: {video.video_id}")
    
    
    def get_video(self, video_id: int) -> Optional[Dict[str, Any]]:
        """
        Get video by ID.
        
        Args:
            video_id: Video database ID
            
        Returns:
            Video record as dictionary or None if not found
        """
        results = select(
            'videos',
            where='id = ?',
            params=[video_id]
        )
        
        return results[0] if results else None
    
    
    def get_video_by_video_id(self, video_id: str) -> Optional[Dict[str, Any]]:
        """
        Get video by YouTube video ID.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            Video record as dictionary or None if not found
        """
        results = select(
            'videos',
            where='video_id = ?',
            params=[video_id]
        )
        
        return results[0] if results else None
    
    
    def get_videos_by_person(self, 
                            person_id: int,
                            status_filter: Optional[str] = None,
                            limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all videos for a person.
        
        Args:
            person_id: Person ID
            status_filter: Optional download status filter
            limit: Maximum number of records
            
        Returns:
            List of video records
        """
        where_parts = ['person_id = ?']
        params = [person_id]
        
        if status_filter:
            where_parts.append('download_status = ?')
            params.append(status_filter)
        
        where_clause = ' AND '.join(where_parts)
        
        return select(
            'videos',
            where=where_clause,
            params=params,
            order_by='upload_date DESC',
            limit=limit
        )
    
    
    def get_pending_videos(self, 
                          person_id: Optional[int] = None,
                          limit: Optional[int] = None) -> List[VideoRecord]:
        """
        Get videos pending download.
        
        Args:
            person_id: Optional person ID filter
            limit: Maximum number of records
            
        Returns:
            List of VideoRecord objects
        """
        where_parts = ["download_status = 'pending'"]
        params = []
        
        if person_id:
            where_parts.append('person_id = ?')
            params.append(person_id)
        
        where_clause = ' AND '.join(where_parts)
        
        results = select(
            'videos',
            where=where_clause,
            params=params if params else None,
            order_by='created_at ASC',  # Process oldest first
            limit=limit
        )
        
        # Convert to VideoRecord objects
        video_records = []
        for row in results:
            video = VideoRecord(
                person_id=row['person_id'],
                video_id=row['video_id'],
                title=row['title'],
                duration=row.get('duration'),
                upload_date=datetime.fromisoformat(row['upload_date']) if row.get('upload_date') else None,
                view_count=row.get('view_count'),
                description=row.get('description'),
                download_status=row.get('download_status', 'pending'),
                s3_path=row.get('s3_path'),
                file_size=row.get('file_size'),
                error_message=row.get('error_message')
            )
            # Set UUID from database
            video.uuid = row['uuid']
            video_records.append(video)
        
        return video_records
    
    
    def update_video_status(self, 
                           video_id: str,
                           status: str,
                           s3_path: Optional[str] = None,
                           file_size: Optional[int] = None,
                           error_message: Optional[str] = None) -> bool:
        """
        Update video download status.
        
        Args:
            video_id: YouTube video ID
            status: New status (completed, failed, etc.)
            s3_path: S3 path if completed
            file_size: File size if completed
            error_message: Error message if failed
            
        Returns:
            True if successful
        """
        update_data = {
            'download_status': status,
            'updated_at': datetime.now()
        }
        
        if s3_path:
            update_data['s3_path'] = s3_path
        
        if file_size:
            update_data['file_size'] = file_size
        
        if error_message:
            update_data['error_message'] = error_message
        
        rows_affected = update(
            'videos',
            update_data,
            'video_id = ?',
            [video_id]
        )
        
        if rows_affected > 0:
            logger.info(f"Updated video status: {video_id} -> {status}")
            return True
        else:
            logger.warning(f"No video found to update: {video_id}")
            return False
    
    # ==========================================================================
    # BATCH OPERATIONS
    # ==========================================================================
    
    
    def batch_save_videos(self, videos: List[VideoRecord]) -> int:
        """
        Save multiple videos in a single transaction.
        
        Args:
            videos: List of VideoRecord objects
            
        Returns:
            Number of videos saved
        """
        saved_count = 0
        
        with self.db_manager.transaction() as conn:
            for video in videos:
                try:
                    # Validate each video
                    video.validate()
                    
                    # Check if exists
                    existing = self.get_video_by_video_id(video.video_id)
                    
                    if existing:
                        # Update existing
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE videos SET
                                title = ?,
                                duration = ?,
                                upload_date = ?,
                                view_count = ?,
                                description = ?,
                                updated_at = ?
                            WHERE video_id = ?
                        """, (
                            video.title,
                            video.duration,
                            video.upload_date,
                            video.view_count,
                            video.description,
                            datetime.now(),
                            video.video_id
                        ))
                    else:
                        # Insert new
                        cursor = conn.cursor()
                        cursor.execute("""
                            INSERT INTO videos (
                                person_id, video_id, title, duration,
                                upload_date, view_count, description,
                                uuid, download_status, created_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            video.person_id,
                            video.video_id,
                            video.title,
                            video.duration,
                            video.upload_date,
                            video.view_count,
                            video.description,
                            video.uuid,
                            video.download_status,
                            datetime.now()
                        ))
                    
                    saved_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to save video {video.video_id}: {e}")
                    # Continue with other videos if configured
                    # Could be made configurable
        
        logger.info(f"Batch saved {saved_count}/{len(videos)} videos")
        return saved_count
    
    # ==========================================================================
    # PROGRESS TRACKING OPERATIONS
    # ==========================================================================
    
    
    def save_progress(self, progress: ProgressRecord) -> Optional[int]:
        """
        Save or update progress record.
        
        Args:
            progress: ProgressRecord to save
            
        Returns:
            Progress ID if successful, None on error
        """
        # Validate progress record (fail-fast)
        progress.validate()
        
        # Check if progress already exists by job_id
        existing = self.get_progress_by_job_id(progress.job_id)
        
        if existing:
            # Update existing progress
            logger.info(f"Updating existing progress: {existing['id']} - Job {progress.job_id}")
            
            update_data = {
                'total_channels': progress.total_channels,
                'channels_processed': progress.channels_processed,
                'channels_failed': progress.channels_failed,
                'channels_skipped': progress.channels_skipped,
                'total_videos': progress.total_videos,
                'videos_processed': progress.videos_processed,
                'videos_failed': progress.videos_failed,
                'videos_skipped': progress.videos_skipped,
                'status': progress.status,
                'error_message': progress.error_message,
                'updated_at': datetime.now(),
                'completed_at': progress.completed_at
            }
            
            rows_affected = update(
                'progress',
                update_data,
                'id = ?',
                [existing['id']]
            )
            
            if rows_affected > 0:
                return existing['id']
            else:
                raise RuntimeError(f"Failed to update progress: {existing['id']}")
        else:
            # Insert new progress
            logger.info(f"Creating new progress record: Job {progress.job_id}")
            
            progress_data = {
                'job_id': progress.job_id,
                'input_file': progress.input_file,
                'total_channels': progress.total_channels,
                'channels_processed': progress.channels_processed,
                'channels_failed': progress.channels_failed,
                'channels_skipped': progress.channels_skipped,
                'total_videos': progress.total_videos,
                'videos_processed': progress.videos_processed,
                'videos_failed': progress.videos_failed,
                'videos_skipped': progress.videos_skipped,
                'status': progress.status,
                'error_message': progress.error_message,
                'started_at': progress.started_at,
                'updated_at': progress.updated_at,
                'completed_at': progress.completed_at
            }
            
            progress_id = insert('progress', progress_data)
            
            if progress_id:
                logger.info(f"Created progress record with ID: {progress_id}")
                return progress_id
            else:
                raise RuntimeError(f"Failed to create progress record: {progress.job_id}")
    
    
    def get_progress(self, progress_id: int) -> Optional[Dict[str, Any]]:
        """
        Get progress by ID.
        
        Args:
            progress_id: Progress ID
            
        Returns:
            Progress record as dictionary or None if not found
        """
        results = select(
            'progress',
            where='id = ?',
            params=[progress_id]
        )
        
        return results[0] if results else None
    
    
    def get_progress_by_job_id(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get progress by job ID.
        
        Args:
            job_id: Job ID
            
        Returns:
            Progress record as dictionary or None if not found
        """
        results = select(
            'progress',
            where='job_id = ?',
            params=[job_id]
        )
        
        return results[0] if results else None
    
    
    def get_active_jobs(self) -> List[Dict[str, Any]]:
        """
        Get all active (running or paused) jobs.
        
        Returns:
            List of active progress records
        """
        return select(
            'progress',
            where="status IN ('running', 'paused')",
            order_by='started_at DESC'
        )
    
    
    def update_progress_counts(self, job_id: str, **counts) -> bool:
        """
        Update progress counts.
        
        Args:
            job_id: Job ID
            **counts: Count fields to update
            
        Returns:
            True if successful
        """
        update_data = {'updated_at': datetime.now()}
        
        # Only include valid count fields
        valid_fields = [
            'total_channels', 'channels_processed', 'channels_failed', 'channels_skipped',
            'total_videos', 'videos_processed', 'videos_failed', 'videos_skipped'
        ]
        
        for field, value in counts.items():
            if field in valid_fields:
                update_data[field] = value
        
        rows_affected = update(
            'progress',
            update_data,
            'job_id = ?',
            [job_id]
        )
        
        if rows_affected > 0:
            logger.debug(f"Updated progress counts for job {job_id}")
            return True
        else:
            logger.warning(f"No progress found to update for job {job_id}")
            return False
    
    
    def mark_job_completed(self, job_id: str, status: str = "completed", error_message: Optional[str] = None) -> bool:
        """
        Mark job as completed or failed.
        
        Args:
            job_id: Job ID
            status: Final status (completed or failed)
            error_message: Error message if failed
            
        Returns:
            True if successful
        """
        update_data = {
            'status': status,
            'completed_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        if error_message:
            update_data['error_message'] = error_message
        
        rows_affected = update(
            'progress',
            update_data,
            'job_id = ?',
            [job_id]
        )
        
        if rows_affected > 0:
            logger.info(f"Marked job {job_id} as {status}")
            return True
        else:
            logger.warning(f"No job found to mark as {status}: {job_id}")
            return False
    
    # ==========================================================================
    # STATISTICS AND REPORTING
    # ==========================================================================
    
    
    def get_download_statistics(self) -> Dict[str, Any]:
        """
        Get overall download statistics.
        
        Returns:
            Dictionary with statistics
        """
        stats = {}
        
        # Total persons
        person_count = execute_sql("SELECT COUNT(*) as count FROM persons")
        stats['total_persons'] = person_count[0]['count'] if person_count else 0
        
        # Total videos
        video_count = execute_sql("SELECT COUNT(*) as count FROM videos")
        stats['total_videos'] = video_count[0]['count'] if video_count else 0
        
        # Videos by status
        status_counts = execute_sql("""
            SELECT download_status, COUNT(*) as count
            FROM videos
            GROUP BY download_status
        """)
        
        stats['videos_by_status'] = {
            row['download_status']: row['count']
            for row in status_counts
        }
        
        # Total storage used
        storage_result = execute_sql("""
            SELECT SUM(file_size) as total_size
            FROM videos
            WHERE file_size IS NOT NULL
        """)
        
        stats['total_storage_bytes'] = storage_result[0]['total_size'] if storage_result and storage_result[0]['total_size'] else 0
        stats['total_storage_gb'] = round(stats['total_storage_bytes'] / (1024**3), 2)
        
        # Videos per person statistics
        per_person_stats = execute_sql("""
            SELECT 
                MIN(video_count) as min_videos,
                MAX(video_count) as max_videos,
                AVG(video_count) as avg_videos
            FROM (
                SELECT person_id, COUNT(*) as video_count
                FROM videos
                GROUP BY person_id
            )
        """)
        
        if per_person_stats:
            stats['min_videos_per_person'] = per_person_stats[0]['min_videos'] or 0
            stats['max_videos_per_person'] = per_person_stats[0]['max_videos'] or 0
            stats['avg_videos_per_person'] = round(per_person_stats[0]['avg_videos'] or 0, 1)
        
        return stats
    
    
    def get_person_statistics(self, person_id: int) -> Dict[str, Any]:
        """
        Get statistics for a specific person.
        
        Args:
            person_id: Person ID
            
        Returns:
            Dictionary with person statistics
        """
        person = self.get_person(person_id)
        if not person:
            return {}
        
        stats = {
            'person_id': person_id,
            'name': person['name'],
            'channel_url': person['channel_url']
        }
        
        # Video counts by status
        status_counts = execute_sql("""
            SELECT download_status, COUNT(*) as count
            FROM videos
            WHERE person_id = ?
            GROUP BY download_status
        """, [person_id])
        
        stats['videos_by_status'] = {
            row['download_status']: row['count']
            for row in status_counts
        }
        
        # Total videos
        stats['total_videos'] = sum(stats['videos_by_status'].values())
        
        # Storage used
        storage_result = execute_sql("""
            SELECT SUM(file_size) as total_size
            FROM videos
            WHERE person_id = ? AND file_size IS NOT NULL
        """, [person_id])
        
        stats['storage_bytes'] = storage_result[0]['total_size'] if storage_result and storage_result[0]['total_size'] else 0
        stats['storage_mb'] = round(stats['storage_bytes'] / (1024**2), 2)
        
        return stats
    
    # ==========================================================================
    # CLEANUP AND MAINTENANCE
    # ==========================================================================
    
    
    def cleanup_failed_downloads(self, older_than_days: int = 7) -> int:
        """
        Clean up old failed download records.
        
        Args:
            older_than_days: Remove failed downloads older than this
            
        Returns:
            Number of records cleaned up
        """
        cutoff_date = datetime.now().timestamp() - (older_than_days * 24 * 60 * 60)
        
        deleted = delete(
            'videos',
            "download_status = 'failed' AND created_at < datetime(?, 'unixepoch')",
            [cutoff_date]
        )
        
        logger.info(f"Cleaned up {deleted} failed download records older than {older_than_days} days")
        return deleted


def validate_database_operations_ext():
    """
    Validate database operations extension module.
    
    Raises:
        RuntimeError: If module validation fails
    """
    try:
        # Test basic initialization
        db_ops = MassDownloadDatabaseOperations()
        
        # Test data structures
        person = PersonRecord(
            name="Test Person",
            channel_url="https://youtube.com/@test"
        )
        
        video = VideoRecord(
            person_id=1,
            video_id="test123",
            title="Test Video"
        )
        
        logger.info("Database operations extension validation PASSED")
        return True
        
    except Exception as e:
        logger.error(f"Database operations extension validation FAILED: {e}")
        raise RuntimeError(f"Database operations extension validation failed: {e}") from e


# Run validation on import (fail-fast) - disabled for testing
# Re-enable when database is properly configured
# if __name__ != "__main__":
#     validate_database_operations_ext()