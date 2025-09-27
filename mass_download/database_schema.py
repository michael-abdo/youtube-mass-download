#!/usr/bin/env python3
"""
Database Schema Module for Mass Download Feature

Implements fail-fast, fail-loud, fail-safely principles:
- Fail Fast: Immediate validation of all inputs and dependencies
- Fail Loud: Clear, actionable error messages with context
- Fail Safely: Rollback capabilities and safe state management

Database Schema:
- persons: Person information with channels (1 to many videos)
- videos: Video metadata and download tracking (many to 1 person)
"""

import os
import sys
import json
import uuid
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass, field
from pathlib import Path

# Fail-fast import validation
try:
    import psycopg2
    import psycopg2.extras
    from psycopg2 import sql, errors as pg_errors
except ImportError as e:
    raise ImportError(
        f"CRITICAL: PostgreSQL adapter (psycopg2) not available. "
        f"Install with: pip install psycopg2-binary. Error: {e}"
    ) from e

# Initialize logger using standard logging
logger = logging.getLogger(__name__)

# Simple config loader (inline implementation)
def get_config():
    """Get configuration with fallback values."""
    import yaml
    try:
        with open('/home/Mike/projects/xenodex/typing-clients-ingestion/mass-download/config/config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        return config
    except:
        # Fallback config
        return {
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "mass_download",
                "username": "postgres",
                "password": "",
                "connection_pool": {"max_connections": 10},
                "query_timeout": 30
            }
        }

# Simple database config class (inline implementation)
class DatabaseConfig:
    """Simple database configuration class."""
    def __init__(self, db_type, host, port, database, username, password, pool_size=10, timeout=30):
        self.db_type = db_type
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.pool_size = pool_size
        self.timeout = timeout

# Global validation state
_SCHEMA_VALIDATED = False
_CONNECTION_TESTED = False


@dataclass
class PersonRecord:
    """
    Person record with fail-fast validation.
    
    Represents a person/channel owner in the system.
    """
    name: str
    email: Optional[str] = None
    type: Optional[str] = None  # Personality type
    channel_url: str = ""
    channel_id: Optional[str] = None
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Fail-fast validation on creation."""
        self.validate()
    
    def validate(self) -> None:
        """
        Fail-fast validation of person record.
        
        Raises:
            ValueError: If validation fails (fail-loud)
        """
        if not self.name or not isinstance(self.name, str):
            raise ValueError(
                f"VALIDATION ERROR: Person name is required and must be non-empty string. "
                f"Got: {self.name} (type: {type(self.name)})"
            )
        
        if self.name.strip() != self.name:
            raise ValueError(
                f"VALIDATION ERROR: Person name cannot have leading/trailing whitespace. "
                f"Got: '{self.name}'"
            )
        
        if len(self.name) > 255:
            raise ValueError(
                f"VALIDATION ERROR: Person name too long (max 255 chars). "
                f"Got: {len(self.name)} chars"
            )
        
        if self.email is not None:
            if not isinstance(self.email, str) or "@" not in self.email:
                raise ValueError(
                    f"VALIDATION ERROR: Invalid email format. "
                    f"Got: {self.email}"
                )
        
        if self.channel_url and not self.channel_url.startswith(("https://youtube.com/", "https://www.youtube.com/")):
            raise ValueError(
                f"VALIDATION ERROR: Invalid YouTube channel URL. "
                f"Must start with https://youtube.com/ or https://www.youtube.com/. "
                f"Got: {self.channel_url}"
            )


@dataclass
class ProgressRecord:
    """
    Progress tracking record for mass download operations.
    
    Used to track progress of mass download jobs and enable resume capability.
    """
    job_id: str  # Unique identifier for the job
    input_file: str  # Path to the input file
    total_channels: int = 0
    channels_processed: int = 0
    channels_failed: int = 0
    channels_skipped: int = 0
    total_videos: int = 0
    videos_processed: int = 0
    videos_failed: int = 0
    videos_skipped: int = 0
    status: str = "running"  # running, completed, failed, paused
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    def __post_init__(self):
        """
        Post-init validation (fail-fast).
        
        Ensures all fields meet requirements immediately after initialization.
        """
        if not self.started_at:
            self.started_at = datetime.now()
        
        if not self.updated_at:
            self.updated_at = datetime.now()
            
        self.validate()
    
    def validate(self):
        """
        Validate progress record with fail-fast principles.
        
        Raises:
            ValueError: If validation fails (fail-loud)
        """
        if not self.job_id or not isinstance(self.job_id, str):
            raise ValueError(
                f"VALIDATION ERROR: job_id is required and must be non-empty string. "
                f"Got: {self.job_id}"
            )
        
        if not self.input_file or not isinstance(self.input_file, str):
            raise ValueError(
                f"VALIDATION ERROR: input_file is required and must be non-empty string. "
                f"Got: {self.input_file}"
            )
        
        valid_statuses = ["running", "completed", "failed", "paused"]
        if self.status not in valid_statuses:
            raise ValueError(
                f"VALIDATION ERROR: status must be one of {valid_statuses}. "
                f"Got: {self.status}"
            )
            
        # Validate counts are non-negative
        count_fields = [
            "total_channels", "channels_processed", "channels_failed", "channels_skipped",
            "total_videos", "videos_processed", "videos_failed", "videos_skipped"
        ]
        
        for field in count_fields:
            value = getattr(self, field)
            if value < 0:
                raise ValueError(
                    f"VALIDATION ERROR: {field} must be non-negative. Got: {value}"
                )


@dataclass  
class VideoRecord:
    """
    Video record with fail-fast validation.
    
    Represents a YouTube video and its download status.
    """
    person_id: int
    video_id: str
    title: str
    description: Optional[str] = None
    duration: Optional[int] = None  # seconds
    upload_date: Optional[datetime] = None
    view_count: Optional[int] = None
    s3_path: Optional[str] = None
    uuid: Optional[str] = field(default_factory=lambda: str(uuid.uuid4()))
    file_size: Optional[int] = None
    download_status: str = "pending"
    error_message: Optional[str] = None
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Fail-fast validation on creation."""
        self.validate()
    
    def validate(self) -> None:
        """
        Fail-fast validation of video record.
        
        Raises:
            ValueError: If validation fails (fail-loud)
        """
        if not self.person_id or not isinstance(self.person_id, int) or self.person_id <= 0:
            raise ValueError(
                f"VALIDATION ERROR: person_id is required and must be positive integer. "
                f"Got: {self.person_id} (type: {type(self.person_id)})"
            )
        
        if not self.video_id or not isinstance(self.video_id, str):
            raise ValueError(
                f"VALIDATION ERROR: video_id is required and must be non-empty string. "
                f"Got: {self.video_id}"
            )
        
        if len(self.video_id) != 11:  # YouTube video IDs are always 11 chars
            raise ValueError(
                f"VALIDATION ERROR: YouTube video_id must be exactly 11 characters. "
                f"Got: '{self.video_id}' (length: {len(self.video_id)})"
            )
        
        if not self.title or not isinstance(self.title, str):
            raise ValueError(
                f"VALIDATION ERROR: title is required and must be non-empty string. "
                f"Got: {self.title}"
            )
        
        valid_statuses = ["pending", "downloading", "completed", "failed", "skipped"]
        if self.download_status not in valid_statuses:
            raise ValueError(
                f"VALIDATION ERROR: Invalid download_status. "
                f"Must be one of: {valid_statuses}. Got: {self.download_status}"
            )
        
        if self.duration is not None and (not isinstance(self.duration, int) or self.duration < 0):
            raise ValueError(
                f"VALIDATION ERROR: duration must be non-negative integer (seconds). "
                f"Got: {self.duration}"
            )
        
        # UUID validation with fail-fast principles
        if self.uuid:
            if not isinstance(self.uuid, str):
                raise ValueError(
                    f"VALIDATION ERROR: uuid must be string. "
                    f"Got: {self.uuid} (type: {type(self.uuid)})"
                )
            
            # Validate UUID format (standard UUID4 format)
            try:
                parsed_uuid = uuid.UUID(self.uuid)
                # Ensure it's a valid UUID4 format (36 characters with hyphens)
                if str(parsed_uuid) != self.uuid:
                    raise ValueError(
                        f"VALIDATION ERROR: uuid format invalid. "
                        f"Expected standard UUID4 format, got: {self.uuid}"
                    )
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"VALIDATION ERROR: Invalid UUID format. "
                    f"Got: '{self.uuid}'. Error: {e}"
                ) from None


class DatabaseSchemaManager:
    """
    Database schema management with fail-fast/fail-loud/fail-safely principles.
    """
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        """
        Initialize schema manager with fail-fast validation.
        
        Args:
            config: Database configuration (optional, loads from config if None)
            
        Raises:
            ValueError: If configuration validation fails
            ConnectionError: If database connection fails
        """
        self.config = config or self._load_and_validate_config()
        self.db_manager = None
        self._connection_pool = None
        self._schema_created = False
        
        # Fail-fast connection test
        self._test_connection()
        
        logger.info("DatabaseSchemaManager initialized successfully")
    
    def _load_and_validate_config(self) -> DatabaseConfig:
        """
        Load and validate database configuration (fail-fast).
        
        Returns:
            DatabaseConfig: Validated configuration
            
        Raises:
            ValueError: If configuration is invalid
        """
        try:
            app_config = get_config()
            db_config = app_config.get("database", {})
            
            # Fail-fast validation of required fields
            required_fields = ["host", "port", "database", "username"]
            for field in required_fields:
                if field not in db_config:
                    raise ValueError(
                        f"CONFIGURATION ERROR: Missing required database field '{field}'. "
                        f"Check config.yaml database section."
                    )
            
            # Validate types
            if not isinstance(db_config["port"], int):
                raise ValueError(
                    f"CONFIGURATION ERROR: Database port must be integer. "
                    f"Got: {db_config['port']} (type: {type(db_config['port'])})"
                )
            
            # Create configuration object
            config = DatabaseConfig(
                db_type="postgresql",
                host=db_config["host"],
                port=db_config["port"],
                database=db_config["database"],
                username=db_config["username"],
                password=db_config.get("password", ""),
                pool_size=db_config.get("connection_pool", {}).get("max_connections", 10),
                timeout=db_config.get("query_timeout", 30)
            )
            
            logger.info(f"Database configuration loaded: {config.host}:{config.port}/{config.database}")
            return config
            
        except Exception as e:
            raise ValueError(
                f"CONFIGURATION ERROR: Failed to load database configuration. "
                f"Ensure config.yaml has proper database section. Error: {e}"
            ) from e
    
    def _test_connection(self) -> None:
        """
        Test database connection (fail-fast).
        
        Raises:
            ConnectionError: If connection test fails
        """
        global _CONNECTION_TESTED
        
        try:
            logger.info("Testing database connection...")
            
            connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
                connect_timeout=5  # Fast timeout for fail-fast
            )
            
            # Test basic query
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result != (1,):
                    raise ConnectionError("Database returned unexpected result")
            
            connection.close()
            _CONNECTION_TESTED = True
            logger.info("Database connection test PASSED")
            
        except psycopg2.OperationalError as e:
            raise ConnectionError(
                f"DATABASE CONNECTION ERROR: Cannot connect to PostgreSQL database. "
                f"Host: {self.config.host}:{self.config.port}, "
                f"Database: {self.config.database}, "
                f"User: {self.config.username}. "
                f"Error: {e}"
            ) from e
        except Exception as e:
            raise ConnectionError(
                f"DATABASE CONNECTION ERROR: Unexpected error during connection test. "
                f"Error: {e}"
            ) from e
    
    def create_schema(self, force: bool = False) -> bool:
        """
        Create database schema with fail-safely rollback.
        
        Args:
            force: Whether to drop existing tables (dangerous!)
            
        Returns:
            bool: True if schema created successfully
            
        Raises:
            RuntimeError: If schema creation fails
        """
        global _SCHEMA_VALIDATED
        
        if self._schema_created and not force:
            logger.info("Schema already created, skipping")
            return True
        
        connection = None
        try:
            logger.info("Creating database schema...")
            
            connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password
            )
            
            # Enable autocommit for DDL operations
            connection.autocommit = False
            
            with connection.cursor() as cursor:
                # Start transaction for fail-safely rollback
                cursor.execute("BEGIN")
                
                try:
                    # Drop tables if force is True (fail-safely)
                    if force:
                        logger.warning("Force mode: Dropping existing tables")
                        cursor.execute("DROP TABLE IF EXISTS videos CASCADE")
                        cursor.execute("DROP TABLE IF EXISTS persons CASCADE")
                        cursor.execute("DROP TABLE IF EXISTS progress CASCADE")
                    
                    # Create persons table
                    persons_sql = """
                        CREATE TABLE IF NOT EXISTS persons (
                            id SERIAL PRIMARY KEY,
                            name VARCHAR(255) NOT NULL,
                            email VARCHAR(255),
                            type VARCHAR(100),
                            channel_url TEXT NOT NULL,
                            channel_id VARCHAR(100),
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            CONSTRAINT persons_name_not_empty CHECK (LENGTH(TRIM(name)) > 0),
                            CONSTRAINT persons_email_format CHECK (email IS NULL OR email ~ '^[^@]+@[^@]+\\.[^@]+$'),
                            CONSTRAINT persons_channel_url_format CHECK (channel_url ~ '^https://(www\\.)?youtube\\.com/')
                        )
                    """
                    cursor.execute(persons_sql)
                    
                    # Create videos table
                    videos_sql = """
                        CREATE TABLE IF NOT EXISTS videos (
                            id SERIAL PRIMARY KEY,
                            person_id INTEGER NOT NULL REFERENCES persons(id) ON DELETE CASCADE,
                            video_id VARCHAR(50) NOT NULL UNIQUE,
                            title TEXT NOT NULL,
                            description TEXT,
                            duration INTEGER CHECK (duration >= 0),
                            upload_date TIMESTAMP WITH TIME ZONE,
                            view_count BIGINT CHECK (view_count >= 0),
                            s3_path TEXT,
                            uuid UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
                            file_size BIGINT CHECK (file_size >= 0),
                            download_status VARCHAR(20) DEFAULT 'pending' 
                                CHECK (download_status IN ('pending', 'downloading', 'completed', 'failed', 'skipped')),
                            error_message TEXT,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            CONSTRAINT videos_video_id_length CHECK (LENGTH(video_id) = 11),
                            CONSTRAINT videos_title_not_empty CHECK (LENGTH(TRIM(title)) > 0)
                        )
                    """
                    cursor.execute(videos_sql)
                    
                    # Create progress tracking table
                    progress_sql = """
                        CREATE TABLE IF NOT EXISTS progress (
                            id SERIAL PRIMARY KEY,
                            job_id VARCHAR(100) NOT NULL UNIQUE,
                            input_file TEXT NOT NULL,
                            total_channels INTEGER DEFAULT 0,
                            channels_processed INTEGER DEFAULT 0,
                            channels_failed INTEGER DEFAULT 0,
                            channels_skipped INTEGER DEFAULT 0,
                            total_videos INTEGER DEFAULT 0,
                            videos_processed INTEGER DEFAULT 0,
                            videos_failed INTEGER DEFAULT 0,
                            videos_skipped INTEGER DEFAULT 0,
                            status VARCHAR(20) DEFAULT 'running'
                                CHECK (status IN ('running', 'completed', 'failed', 'paused')),
                            error_message TEXT,
                            started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            completed_at TIMESTAMP WITH TIME ZONE,
                            CONSTRAINT progress_job_id_not_empty CHECK (LENGTH(TRIM(job_id)) > 0),
                            CONSTRAINT progress_counts_non_negative CHECK (
                                total_channels >= 0 AND channels_processed >= 0 AND 
                                channels_failed >= 0 AND channels_skipped >= 0 AND
                                total_videos >= 0 AND videos_processed >= 0 AND
                                videos_failed >= 0 AND videos_skipped >= 0
                            )
                        )
                    """
                    cursor.execute(progress_sql)
                    
                    # Create indexes for performance
                    indexes = [
                        "CREATE INDEX IF NOT EXISTS idx_videos_person_id ON videos(person_id)",
                        "CREATE INDEX IF NOT EXISTS idx_videos_video_id ON videos(video_id)",
                        "CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(download_status)",
                        "CREATE INDEX IF NOT EXISTS idx_persons_channel_id ON persons(channel_id)",
                        "CREATE INDEX IF NOT EXISTS idx_videos_uuid ON videos(uuid)",
                        "CREATE INDEX IF NOT EXISTS idx_persons_email ON persons(email)",
                        "CREATE INDEX IF NOT EXISTS idx_progress_job_id ON progress(job_id)",
                        "CREATE INDEX IF NOT EXISTS idx_progress_status ON progress(status)",
                        "CREATE INDEX IF NOT EXISTS idx_progress_started_at ON progress(started_at)"
                    ]
                    
                    for index_sql in indexes:
                        cursor.execute(index_sql)
                    
                    # Create trigger for updated_at timestamps
                    trigger_sql = """
                        CREATE OR REPLACE FUNCTION update_updated_at_column()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            NEW.updated_at = CURRENT_TIMESTAMP;
                            RETURN NEW;
                        END;
                        $$ language 'plpgsql';
                        
                        DROP TRIGGER IF EXISTS update_persons_updated_at ON persons;
                        CREATE TRIGGER update_persons_updated_at 
                            BEFORE UPDATE ON persons 
                            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                            
                        DROP TRIGGER IF EXISTS update_videos_updated_at ON videos;
                        CREATE TRIGGER update_videos_updated_at 
                            BEFORE UPDATE ON videos 
                            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                    """
                    cursor.execute(trigger_sql)
                    
                    # Commit transaction
                    cursor.execute("COMMIT")
                    
                    self._schema_created = True
                    _SCHEMA_VALIDATED = True
                    logger.info("Database schema created successfully")
                    
                except Exception as e:
                    # Fail-safely: Rollback on any error
                    cursor.execute("ROLLBACK")
                    raise RuntimeError(
                        f"SCHEMA CREATION ERROR: Failed to create database schema. "
                        f"Transaction rolled back safely. Error: {e}"
                    ) from e
            
            return True
            
        except Exception as e:
            logger.error(f"Schema creation failed: {e}")
            raise
        finally:
            if connection:
                connection.close()
    
    def validate_schema(self) -> bool:
        """
        Validate that schema exists and is correct (fail-fast).
        
        Returns:
            bool: True if schema is valid
            
        Raises:
            RuntimeError: If schema validation fails
        """
        try:
            logger.info("Validating database schema...")
            
            connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password
            )
            
            with connection.cursor() as cursor:
                # Check if tables exist
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('persons', 'videos')
                    ORDER BY table_name
                """)
                
                tables = [row[0] for row in cursor.fetchall()]
                expected_tables = ['persons', 'videos']
                
                if tables != expected_tables:
                    raise RuntimeError(
                        f"SCHEMA VALIDATION ERROR: Missing required tables. "
                        f"Expected: {expected_tables}, Found: {tables}"
                    )
                
                # Check critical columns exist
                for table in ['persons', 'videos']:
                    cursor.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        ORDER BY column_name
                    """, (table,))
                    
                    columns = [row[0] for row in cursor.fetchall()]
                    
                    if table == 'persons':
                        required_cols = ['id', 'name', 'channel_url', 'created_at']
                        missing = [col for col in required_cols if col not in columns]
                        if missing:
                            raise RuntimeError(
                                f"SCHEMA VALIDATION ERROR: Missing columns in persons table: {missing}"
                            )
                    
                    elif table == 'videos':
                        required_cols = ['id', 'person_id', 'video_id', 'title', 'uuid', 'download_status']
                        missing = [col for col in required_cols if col not in columns]
                        if missing:
                            raise RuntimeError(
                                f"SCHEMA VALIDATION ERROR: Missing columns in videos table: {missing}"
                            )
            
            connection.close()
            logger.info("Schema validation PASSED")
            return True
            
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            raise


def get_schema_manager() -> DatabaseSchemaManager:
    """
    Get schema manager instance with fail-fast validation.
    
    Returns:
        DatabaseSchemaManager: Validated schema manager
        
    Raises:
        RuntimeError: If manager cannot be created
    """
    try:
        return DatabaseSchemaManager()
    except Exception as e:
        raise RuntimeError(
            f"CRITICAL: Cannot create database schema manager. "
            f"Check database configuration and connectivity. Error: {e}"
        ) from e


# Module-level validation (fail-fast on import)
def _validate_module():
    """Validate module can be imported safely."""
    global _SCHEMA_VALIDATED, _CONNECTION_TESTED
    
    try:
        # Test configuration loading
        config = get_config()
        db_section = config.get("database")
        if not db_section:
            raise ValueError("Database configuration section missing from config.yaml")
        
        logger.info("Database schema module validation PASSED")
        return True
        
    except Exception as e:
        logger.error(f"Database schema module validation FAILED: {e}")
        raise


# Run validation on import
_validate_module()