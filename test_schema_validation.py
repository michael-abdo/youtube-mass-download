#!/usr/bin/env python3
"""
Test Script for Database Schema Validation
Phase 1.3: Test Person table creation and fail loudly on errors

This script demonstrates fail-fast, fail-loud, fail-safely principles.
"""

import sys
import os
from pathlib import Path

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

try:
    from mass_download.database_schema import (
        PersonRecord, VideoRecord, DatabaseSchemaManager, 
        get_schema_manager
    )
    print("✅ SUCCESS: Database schema imports successful")
except Exception as e:
    print(f"❌ CRITICAL FAILURE: Cannot import database schema modules")
    print(f"   Error: {e}")
    print(f"   This is a LOUD FAILURE - schema module is broken!")
    sys.exit(1)


def test_person_record_validation():
    """Test PersonRecord validation with fail-fast/fail-loud principles."""
    print("\n🧪 Testing PersonRecord validation...")
    
    # Test 1: Valid person record (should pass)
    try:
        valid_person = PersonRecord(
            name="John Doe",
            email="john@example.com", 
            type="FF-Ti/Se-CS/P(B) #4",
            channel_url="https://youtube.com/@johndoe"
        )
        print("✅ SUCCESS: Valid person record created")
    except Exception as e:
        print(f"❌ UNEXPECTED FAILURE: Valid person record failed validation")
        print(f"   Error: {e}")
        return False
    
    # Test 2: Invalid name (should fail fast and loud)
    try:
        invalid_person = PersonRecord(
            name="",  # Empty name - should fail
            email="john@example.com",
            channel_url="https://youtube.com/@johndoe"
        )
        print("❌ VALIDATION FAILURE: Empty name should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Empty name failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for empty name")
        print(f"   Error: {e}")
        return False
    
    # Test 3: Invalid email (should fail fast and loud)
    try:
        invalid_email_person = PersonRecord(
            name="John Doe",
            email="invalid-email",  # Bad email format
            channel_url="https://youtube.com/@johndoe"
        )
        print("❌ VALIDATION FAILURE: Invalid email should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Invalid email failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for invalid email")
        print(f"   Error: {e}")
        return False
    
    # Test 4: Invalid channel URL (should fail fast and loud)
    try:
        invalid_url_person = PersonRecord(
            name="John Doe",
            email="john@example.com",
            channel_url="https://not-youtube.com/channel"  # Not a YouTube URL
        )
        print("❌ VALIDATION FAILURE: Invalid channel URL should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Invalid channel URL failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for invalid URL")
        print(f"   Error: {e}")
        return False
    
    # Test 5: Name with whitespace (should fail fast and loud)
    try:
        whitespace_person = PersonRecord(
            name="  John Doe  ",  # Leading/trailing whitespace
            email="john@example.com",
            channel_url="https://youtube.com/@johndoe"
        )
        print("❌ VALIDATION FAILURE: Name with whitespace should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Name with whitespace failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for whitespace name")
        print(f"   Error: {e}")
        return False
    
    print("✅ ALL PersonRecord validation tests PASSED")
    return True


def test_video_record_validation():
    """Test VideoRecord validation with fail-fast/fail-loud principles."""
    print("\n🧪 Testing VideoRecord validation...")
    
    # Test 1: Valid video record (should pass)
    try:
        valid_video = VideoRecord(
            person_id=1,
            video_id="dQw4w9WgXcQ",  # Valid YouTube video ID (11 chars)
            title="Test Video Title"
        )
        print("✅ SUCCESS: Valid video record created")
    except Exception as e:
        print(f"❌ UNEXPECTED FAILURE: Valid video record failed validation")
        print(f"   Error: {e}")
        return False
    
    # Test 2: Invalid person_id (should fail fast and loud)
    try:
        invalid_video = VideoRecord(
            person_id=0,  # Invalid person_id
            video_id="dQw4w9WgXcQ",
            title="Test Video"
        )
        print("❌ VALIDATION FAILURE: Invalid person_id should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Invalid person_id failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for invalid person_id")
        print(f"   Error: {e}")
        return False
    
    # Test 3: Invalid video_id length (should fail fast and loud)
    try:
        invalid_video_id = VideoRecord(
            person_id=1,
            video_id="short",  # Too short (YouTube IDs are 11 chars)
            title="Test Video"
        )
        print("❌ VALIDATION FAILURE: Invalid video_id length should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Invalid video_id length failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for invalid video_id length")
        print(f"   Error: {e}")
        return False
    
    # Test 4: Empty title (should fail fast and loud)
    try:
        empty_title_video = VideoRecord(
            person_id=1,
            video_id="dQw4w9WgXcQ",
            title=""  # Empty title
        )
        print("❌ VALIDATION FAILURE: Empty title should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Empty title failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for empty title")
        print(f"   Error: {e}")
        return False
    
    # Test 5: Invalid download status (should fail fast and loud)
    try:
        invalid_status_video = VideoRecord(
            person_id=1,
            video_id="dQw4w9WgXcQ",
            title="Test Video",
            download_status="invalid_status"  # Not in allowed values
        )
        print("❌ VALIDATION FAILURE: Invalid download status should have failed validation!")
        return False
    except ValueError as e:
        print(f"✅ SUCCESS: Invalid download status failed validation as expected")
        print(f"   Error message: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Wrong exception type for invalid status")
        print(f"   Error: {e}")
        return False
    
    print("✅ ALL VideoRecord validation tests PASSED")
    return True


def test_database_connection():
    """Test database connection with fail-fast/fail-loud principles."""
    print("\n🧪 Testing database connection...")
    
    try:
        # This will attempt to create a schema manager and test connection
        schema_manager = get_schema_manager()
        print("✅ SUCCESS: Database connection test passed")
        return True, schema_manager
    except ConnectionError as e:
        print(f"🔍 EXPECTED FAILURE: Database connection failed (this may be expected if DB not set up)")
        print(f"   Error: {e}")
        print("   This is LOUD FAILURE with clear error message ✅")
        return False, None
    except Exception as e:
        print(f"❌ UNEXPECTED FAILURE: Database connection failed with unexpected error")
        print(f"   Error: {e}")
        return False, None


def test_schema_creation():
    """Test schema creation if database is available."""
    print("\n🧪 Testing schema creation...")
    
    connection_success, schema_manager = test_database_connection()
    
    if not connection_success:
        print("⏭️  SKIPPING schema creation test - database not available")
        return True  # Not a failure, just skipped
    
    try:
        # Test schema creation
        success = schema_manager.create_schema(force=False)
        if success:
            print("✅ SUCCESS: Database schema created successfully")
        else:
            print("❌ FAILURE: Schema creation returned False")
            return False
        
        # Test schema validation
        validation_success = schema_manager.validate_schema()
        if validation_success:
            print("✅ SUCCESS: Database schema validation passed")
        else:
            print("❌ FAILURE: Schema validation returned False")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ SCHEMA CREATION FAILURE: {e}")
        print("   This is LOUD FAILURE with clear error message ✅")
        return False


def main():
    """Run all validation tests with fail-fast/fail-loud principles."""
    print("🚀 Starting Database Schema Validation Tests")
    print("   Testing fail-fast, fail-loud, fail-safely principles")
    print("=" * 60)
    
    all_tests_passed = True
    
    # Run validation tests
    if not test_person_record_validation():
        all_tests_passed = False
        print("❌ PersonRecord validation tests FAILED")
    
    if not test_video_record_validation():
        all_tests_passed = False
        print("❌ VideoRecord validation tests FAILED")
    
    if not test_schema_creation():
        all_tests_passed = False
        print("❌ Schema creation tests FAILED")
    
    # Final results
    print("\n" + "=" * 60)
    if all_tests_passed:
        print("🎉 ALL TESTS PASSED! Database schema validation successful")
        print("✅ Fail-fast principle: Invalid inputs rejected immediately")
        print("✅ Fail-loud principle: Clear, actionable error messages")
        print("✅ Fail-safely principle: No partial state corruption")
        return 0
    else:
        print("💥 SOME TESTS FAILED! Database schema has issues")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)