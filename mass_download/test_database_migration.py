#!/usr/bin/env python3
"""
Test Database Migration
Phase 1.8: Test migration with sample data and rollback capability

Tests:
1. Schema creation and validation
2. Sample data insertion
3. Foreign key constraints
4. Rollback on errors
5. Migration safety

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import os
import time
import tempfile
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
import uuid
import psycopg2
from psycopg2 import sql

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))

# Test database configuration
TEST_DB_CONFIG = {
    "host": os.environ.get("TEST_DB_HOST", "localhost"),
    "port": int(os.environ.get("TEST_DB_PORT", "5432")),
    "database": os.environ.get("TEST_DB_NAME", "mass_download_test"),
    "username": os.environ.get("TEST_DB_USER", "postgres"),
    "password": os.environ.get("TEST_DB_PASS", "")
}


def test_imports():
    """Test that all required modules import correctly."""
    print("🧪 Testing imports for database migration...")
    
    try:
        from database_schema import DatabaseSchemaManager, PersonRecord, VideoRecord
        from utils.database_operations import DatabaseConfig
        
        print("✅ SUCCESS: All required imports successful")
        return True
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import required modules")
        print(f"   Error: {e}")
        return False


def create_test_database():
    """Create a test database for migration testing."""
    print("\n🧪 Creating test database...")
    
    try:
        # Connect to postgres database to create test database
        conn = psycopg2.connect(
            host=TEST_DB_CONFIG["host"],
            port=TEST_DB_CONFIG["port"],
            database="postgres",
            user=TEST_DB_CONFIG["username"],
            password=TEST_DB_CONFIG["password"]
        )
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            # Drop test database if exists
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(
                sql.Identifier(TEST_DB_CONFIG["database"])
            ))
            
            # Create test database
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(TEST_DB_CONFIG["database"])
            ))
            
        conn.close()
        print(f"✅ SUCCESS: Test database '{TEST_DB_CONFIG['database']}' created")
        return True
        
    except Exception as e:
        print(f"❌ WARNING: Could not create test database: {e}")
        print("   This test requires a PostgreSQL server running locally")
        print("   Set TEST_DB_* environment variables to use a different server")
        return False


def test_schema_creation():
    """Test database schema creation."""
    print("\n🧪 Testing schema creation...")
    
    try:
        from database_schema import DatabaseSchemaManager
        from utils.database_operations import DatabaseConfig
        
        # Create test config
        config = DatabaseConfig(
            db_type="postgresql",
            host=TEST_DB_CONFIG["host"],
            port=TEST_DB_CONFIG["port"],
            database=TEST_DB_CONFIG["database"],
            username=TEST_DB_CONFIG["username"],
            password=TEST_DB_CONFIG["password"]
        )
        
        # Initialize schema manager
        schema_manager = DatabaseSchemaManager(config=config)
        
        # Create schema
        result = schema_manager.create_schema(force=True)
        
        if not result:
            print("❌ FAILURE: Schema creation returned False")
            return False
        
        # Validate schema
        validation_result = schema_manager.validate_schema()
        
        if not validation_result:
            print("❌ FAILURE: Schema validation failed")
            return False
        
        print("✅ SUCCESS: Schema created and validated successfully")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Schema creation failed: {e}")
        return False


def test_sample_data_insertion():
    """Test inserting sample data."""
    print("\n🧪 Testing sample data insertion...")
    
    try:
        from database_schema import PersonRecord, VideoRecord
        
        # Connect to test database
        conn = psycopg2.connect(
            host=TEST_DB_CONFIG["host"],
            port=TEST_DB_CONFIG["port"],
            database=TEST_DB_CONFIG["database"],
            user=TEST_DB_CONFIG["username"],
            password=TEST_DB_CONFIG["password"]
        )
        
        # Test data
        test_persons = [
            PersonRecord(
                name="Test Channel 1",
                email="test1@example.com",
                type="educational",
                channel_url="https://www.youtube.com/@testchannel1",
                channel_id="UCtest1234567890"
            ),
            PersonRecord(
                name="Test Channel 2",
                email="test2@example.com",
                type="entertainment",
                channel_url="https://www.youtube.com/@testchannel2",
                channel_id="UCtest0987654321"
            )
        ]
        
        with conn.cursor() as cursor:
            # Insert persons
            person_ids = []
            for person in test_persons:
                cursor.execute("""
                    INSERT INTO persons (name, email, type, channel_url, channel_id)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (person.name, person.email, person.type, person.channel_url, person.channel_id))
                
                person_id = cursor.fetchone()[0]
                person_ids.append(person_id)
            
            # Insert videos for each person
            video_count = 0
            for i, person_id in enumerate(person_ids):
                for j in range(3):  # 3 videos per person
                    video = VideoRecord(
                        person_id=person_id,
                        video_id=f"testVid{i}{j:02d}",
                        title=f"Test Video {j+1} from Channel {i+1}",
                        duration=120 + j * 60,
                        upload_date=datetime.now(),
                        view_count=1000 * (j + 1),
                        download_status="pending"
                    )
                    
                    cursor.execute("""
                        INSERT INTO videos (person_id, video_id, title, duration, upload_date, view_count, download_status, uuid)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        video.person_id,
                        video.video_id,
                        video.title,
                        video.duration,
                        video.upload_date,
                        video.view_count,
                        video.download_status,
                        video.uuid
                    ))
                    video_count += 1
            
            conn.commit()
            
        # Verify data was inserted
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM persons")
            person_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM videos")
            actual_video_count = cursor.fetchone()[0]
        
        conn.close()
        
        if person_count != 2:
            print(f"❌ FAILURE: Expected 2 persons, got {person_count}")
            return False
        
        if actual_video_count != 6:
            print(f"❌ FAILURE: Expected 6 videos, got {actual_video_count}")
            return False
        
        print("✅ SUCCESS: Sample data inserted successfully")
        print(f"   Persons: {person_count}")
        print(f"   Videos: {actual_video_count}")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Data insertion failed: {e}")
        return False


def test_foreign_key_constraints():
    """Test foreign key constraints work correctly."""
    print("\n🧪 Testing foreign key constraints...")
    
    try:
        conn = psycopg2.connect(
            host=TEST_DB_CONFIG["host"],
            port=TEST_DB_CONFIG["port"],
            database=TEST_DB_CONFIG["database"],
            user=TEST_DB_CONFIG["username"],
            password=TEST_DB_CONFIG["password"]
        )
        
        # Test Case 1: Try to insert video with non-existent person_id
        with conn.cursor() as cursor:
            try:
                cursor.execute("""
                    INSERT INTO videos (person_id, video_id, title, uuid)
                    VALUES (99999, 'invalidVid', 'Invalid Video', %s)
                """, (str(uuid.uuid4()),))
                
                conn.commit()
                print("❌ FAILURE: Foreign key constraint not enforced")
                return False
                
            except psycopg2.IntegrityError:
                conn.rollback()
                print("✅ SUCCESS: Foreign key constraint properly rejected invalid person_id")
        
        # Test Case 2: Test CASCADE DELETE
        with conn.cursor() as cursor:
            # Get a person with videos
            cursor.execute("""
                SELECT p.id, COUNT(v.id) as video_count
                FROM persons p
                JOIN videos v ON p.id = v.person_id
                GROUP BY p.id
                LIMIT 1
            """)
            
            person_id, video_count = cursor.fetchone()
            
            # Delete the person
            cursor.execute("DELETE FROM persons WHERE id = %s", (person_id,))
            
            # Check if videos were deleted
            cursor.execute("SELECT COUNT(*) FROM videos WHERE person_id = %s", (person_id,))
            remaining_videos = cursor.fetchone()[0]
            
            if remaining_videos != 0:
                print(f"❌ FAILURE: CASCADE DELETE failed, {remaining_videos} videos remain")
                return False
            
            conn.commit()
            print(f"✅ SUCCESS: CASCADE DELETE removed {video_count} videos when person was deleted")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Foreign key test failed: {e}")
        return False


def test_rollback_on_error():
    """Test rollback capability on errors."""
    print("\n🧪 Testing rollback on error...")
    
    try:
        conn = psycopg2.connect(
            host=TEST_DB_CONFIG["host"],
            port=TEST_DB_CONFIG["port"],
            database=TEST_DB_CONFIG["database"],
            user=TEST_DB_CONFIG["username"],
            password=TEST_DB_CONFIG["password"]
        )
        
        # Get initial counts
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM persons")
            initial_person_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM videos")
            initial_video_count = cursor.fetchone()[0]
        
        # Try a transaction that will fail
        try:
            with conn.cursor() as cursor:
                # Insert a valid person
                cursor.execute("""
                    INSERT INTO persons (name, channel_url)
                    VALUES ('Rollback Test', 'https://www.youtube.com/@rollback')
                    RETURNING id
                """)
                
                person_id = cursor.fetchone()[0]
                
                # Insert a valid video
                cursor.execute("""
                    INSERT INTO videos (person_id, video_id, title, uuid)
                    VALUES (%s, 'rollbackVid', 'Valid Video', %s)
                """, (person_id, str(uuid.uuid4())))
                
                # Try to insert an invalid video (duplicate video_id)
                cursor.execute("""
                    INSERT INTO videos (person_id, video_id, title, uuid)
                    VALUES (%s, 'testVid000', 'Duplicate Video ID', %s)
                """, (person_id, str(uuid.uuid4())))
                
                conn.commit()
                print("❌ FAILURE: Transaction should have failed due to duplicate video_id")
                return False
                
        except psycopg2.IntegrityError:
            conn.rollback()
            print("✅ SUCCESS: Transaction rolled back due to constraint violation")
        
        # Verify rollback - counts should be unchanged
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM persons")
            final_person_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM videos")
            final_video_count = cursor.fetchone()[0]
        
        conn.close()
        
        if final_person_count != initial_person_count:
            print(f"❌ FAILURE: Person count changed: {initial_person_count} → {final_person_count}")
            return False
        
        if final_video_count != initial_video_count:
            print(f"❌ FAILURE: Video count changed: {initial_video_count} → {final_video_count}")
            return False
        
        print("✅ SUCCESS: Database state unchanged after rollback")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Rollback test failed: {e}")
        return False


def cleanup_test_database():
    """Clean up test database."""
    print("\n🧹 Cleaning up test database...")
    
    try:
        conn = psycopg2.connect(
            host=TEST_DB_CONFIG["host"],
            port=TEST_DB_CONFIG["port"],
            database="postgres",
            user=TEST_DB_CONFIG["username"],
            password=TEST_DB_CONFIG["password"]
        )
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(
                sql.Identifier(TEST_DB_CONFIG["database"])
            ))
        
        conn.close()
        print("✅ Test database cleaned up")
        return True
        
    except Exception as e:
        print(f"⚠️  Could not clean up test database: {e}")
        return False


def main():
    """Run comprehensive database migration test suite."""
    print("🚀 Starting Database Migration Test Suite")
    print("   Testing Phase 1.8: Migration with sample data and rollback")
    print("   Validating schema creation, data integrity, and safety")
    print("=" * 80)
    
    # Check if we can connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=TEST_DB_CONFIG["host"],
            port=TEST_DB_CONFIG["port"],
            database="postgres",
            user=TEST_DB_CONFIG["username"],
            password=TEST_DB_CONFIG["password"]
        )
        conn.close()
    except Exception as e:
        print("⚠️  WARNING: Cannot connect to PostgreSQL server")
        print(f"   Error: {e}")
        print("   These tests require a running PostgreSQL instance")
        print("   Skipping database tests...")
        return 0
    
    all_tests_passed = True
    test_functions = [
        test_imports,
        create_test_database,
        test_schema_creation,
        test_sample_data_insertion,
        test_foreign_key_constraints,
        test_rollback_on_error
    ]
    
    for test_func in test_functions:
        result = test_func()
        if not result:
            all_tests_passed = False
            print(f"❌ {test_func.__name__} FAILED")
            
            # Stop if we can't create test database
            if test_func.__name__ == "create_test_database":
                print("   Cannot continue without test database")
                return 1
    
    # Always try to cleanup
    cleanup_test_database()
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL DATABASE MIGRATION TESTS PASSED!")
        print("✅ Schema creation working")
        print("✅ Sample data insertion successful")
        print("✅ Foreign key constraints enforced")
        print("✅ Rollback capability verified")
        print("\n🔥 Database migration is PRODUCTION-READY!")
        return 0
    else:
        print("💥 SOME DATABASE MIGRATION TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)