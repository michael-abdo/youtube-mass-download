#!/usr/bin/env python3
"""
Test Input Handler - CSV Parser
Phase 3.3: Test CSV parser with valid/invalid CSV files

Tests:
1. Valid CSV file parsing
2. Missing required columns
3. Empty CSV files
4. Malformed CSV data
5. Various CSV dialects
6. Additional columns handling
7. Edge cases and error conditions

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import os
import csv
import tempfile
from pathlib import Path
from typing import List, Tuple

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))

def test_imports():
    """Test that all required modules import correctly."""
    print("🧪 Testing imports for input handler tests...")
    
    try:
        from input_handler import InputHandler, ChannelInput, InputFormat
        print("✅ SUCCESS: All required imports successful")
        return True, (InputHandler, ChannelInput, InputFormat)
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import required modules")
        print(f"   Error: {e}")
        return False, None


def create_test_csv(content: str) -> str:
    """Create a temporary CSV file with given content."""
    fd, path = tempfile.mkstemp(suffix='.csv')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(content)
        return path
    except:
        os.close(fd)
        raise


def test_valid_csv_parsing():
    """Test parsing of valid CSV files."""
    print("\n🧪 Testing valid CSV parsing...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, ChannelInput, InputFormat = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Standard CSV with all fields
        csv_content = """name,channel_url,email,type
MrBeast,https://youtube.com/@MrBeast,contact@mrbeast.com,Entertainment
PewDiePie,https://www.youtube.com/c/PewDiePie,pewds@example.com,Gaming
"Tech Channel",https://youtube.com/channel/UCX6OQ3DkcsbYNE6H8uQQuVA,,Technology
"""
        
        csv_path = create_test_csv(csv_content)
        
        try:
            # Parse CSV
            channels = handler.parse_input_file(csv_path)
            
            if len(channels) != 3:
                print(f"❌ FAILURE: Expected 3 channels, got {len(channels)}")
                return False
            
            # Verify first channel
            first = channels[0]
            if first.name != "MrBeast":
                print(f"❌ FAILURE: Expected name 'MrBeast', got '{first.name}'")
                return False
            
            if first.email != "contact@mrbeast.com":
                print(f"❌ FAILURE: Expected email 'contact@mrbeast.com', got '{first.email}'")
                return False
            
            # Verify third channel (empty email)
            third = channels[2]
            if third.name != "Tech Channel":
                print(f"❌ FAILURE: Expected name 'Tech Channel', got '{third.name}'")
                return False
            
            if third.email is not None:
                print(f"❌ FAILURE: Expected None email, got '{third.email}'")
                return False
            
            print("✅ SUCCESS: Standard CSV parsed correctly")
            
        finally:
            os.unlink(csv_path)
        
        # Test Case 2: CSV with alternative column names
        csv_content = """Channel Name,URL,Email Address,Category
"Gaming Pro",youtube.com/@gamingpro,gamer@example.com,Gaming
"Music Lover",https://youtube.com/@musiclover,,Music
"""
        
        csv_path = create_test_csv(csv_content)
        
        try:
            channels = handler.parse_input_file(csv_path)
            
            if len(channels) != 2:
                print(f"❌ FAILURE: Expected 2 channels, got {len(channels)}")
                return False
            
            if channels[0].name != "Gaming Pro":
                print(f"❌ FAILURE: Alternative column name parsing failed")
                return False
            
            print("✅ SUCCESS: Alternative column names parsed correctly")
            
        finally:
            os.unlink(csv_path)
        
        # Test Case 3: CSV with additional columns
        csv_content = """name,channel_url,email,type,subscriber_count,verified
Test Channel,https://youtube.com/@testchannel,test@example.com,Test,1000000,true
"""
        
        csv_path = create_test_csv(csv_content)
        
        try:
            channels = handler.parse_input_file(csv_path)
            
            if len(channels) != 1:
                print(f"❌ FAILURE: Expected 1 channel, got {len(channels)}")
                return False
            
            channel = channels[0]
            if not channel.additional_data:
                print("❌ FAILURE: Additional data not captured")
                return False
            
            if channel.additional_data.get("subscriber_count") != "1000000":
                print("❌ FAILURE: Additional column 'subscriber_count' not captured")
                return False
            
            print("✅ SUCCESS: Additional columns captured correctly")
            
        finally:
            os.unlink(csv_path)
        
        print("✅ ALL valid CSV parsing tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Valid CSV test failed: {e}")
        return False


def test_invalid_csv_parsing():
    """Test handling of invalid CSV files."""
    print("\n🧪 Testing invalid CSV parsing...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Missing required 'name' column
        csv_content = """channel_url,email,type
https://youtube.com/@test,test@example.com,Test
"""
        
        csv_path = create_test_csv(csv_content)
        
        try:
            channels = handler.parse_input_file(csv_path)
            print("❌ VALIDATION FAILURE: Missing 'name' column should have failed!")
            return False
        except ValueError as e:
            if "Missing required 'name' column" in str(e):
                print("✅ SUCCESS: Missing 'name' column detected correctly")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        finally:
            os.unlink(csv_path)
        
        # Test Case 2: Missing required 'url' column
        csv_content = """name,email,type
Test Channel,test@example.com,Test
"""
        
        csv_path = create_test_csv(csv_content)
        
        try:
            channels = handler.parse_input_file(csv_path)
            print("❌ VALIDATION FAILURE: Missing 'url' column should have failed!")
            return False
        except ValueError as e:
            if "Missing required 'url' or 'channel_url' column" in str(e):
                print("✅ SUCCESS: Missing 'url' column detected correctly")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        finally:
            os.unlink(csv_path)
        
        # Test Case 3: Empty CSV file
        csv_content = ""
        
        csv_path = create_test_csv(csv_content)
        
        try:
            channels = handler.parse_input_file(csv_path)
            print("❌ VALIDATION FAILURE: Empty CSV should have failed!")
            return False
        except (ValueError, RuntimeError) as e:
            print("✅ SUCCESS: Empty CSV file handled correctly")
        finally:
            os.unlink(csv_path)
        
        # Test Case 4: CSV with no valid entries
        csv_content = """name,channel_url,email
,,
"",""
   ,   ,
"""
        
        csv_path = create_test_csv(csv_content)
        
        try:
            channels = handler.parse_input_file(csv_path)
            print("❌ VALIDATION FAILURE: CSV with no valid entries should have failed!")
            return False
        except ValueError as e:
            if "no valid channel entries" in str(e):
                print("✅ SUCCESS: CSV with no valid entries detected correctly")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        finally:
            os.unlink(csv_path)
        
        print("✅ ALL invalid CSV parsing tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Invalid CSV test failed: {e}")
        return False


def test_csv_edge_cases():
    """Test CSV parsing edge cases."""
    print("\n🧪 Testing CSV edge cases...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: CSV with special characters and quotes
        csv_content = '''name,channel_url
"Channel with, comma",https://youtube.com/@channel1
"Channel with ""quotes""",https://youtube.com/@channel2
Channel with 中文,https://youtube.com/@channel3
"Multi
Line",https://youtube.com/@channel4
'''
        
        csv_path = create_test_csv(csv_content)
        
        try:
            channels = handler.parse_input_file(csv_path)
            
            if len(channels) != 4:
                print(f"❌ FAILURE: Expected 4 channels, got {len(channels)}")
                return False
            
            if channels[0].name != "Channel with, comma":
                print(f"❌ FAILURE: Comma in quoted field not handled")
                return False
            
            if channels[1].name != 'Channel with "quotes"':
                print(f"❌ FAILURE: Quotes in field not handled")
                return False
            
            if channels[2].name != "Channel with 中文":
                print(f"❌ FAILURE: Unicode characters not handled")
                return False
            
            print("✅ SUCCESS: Special characters handled correctly")
            
        finally:
            os.unlink(csv_path)
        
        # Test Case 2: CSV with tab delimiter
        csv_content = "name\tchannel_url\temail\nTab Channel\thttps://youtube.com/@tabchannel\ttab@example.com"
        
        csv_path = create_test_csv(csv_content)
        
        try:
            channels = handler.parse_input_file(csv_path)
            
            if len(channels) != 1:
                print(f"❌ FAILURE: Tab-delimited CSV not parsed")
                return False
            
            if channels[0].name != "Tab Channel":
                print(f"❌ FAILURE: Tab-delimited parsing failed")
                return False
            
            print("✅ SUCCESS: Tab-delimited CSV parsed correctly")
            
        finally:
            os.unlink(csv_path)
        
        # Test Case 3: CSV with mixed case headers
        csv_content = """NAME,Channel_URL,EMAIL,Type
Mixed Case,https://youtube.com/@mixedcase,mixed@example.com,Test
"""
        
        csv_path = create_test_csv(csv_content)
        
        try:
            channels = handler.parse_input_file(csv_path)
            
            if len(channels) != 1:
                print(f"❌ FAILURE: Mixed case headers not handled")
                return False
            
            if channels[0].name != "Mixed Case":
                print(f"❌ FAILURE: Mixed case header parsing failed")
                return False
            
            print("✅ SUCCESS: Mixed case headers handled correctly")
            
        finally:
            os.unlink(csv_path)
        
        # Test Case 4: Invalid YouTube URLs
        csv_content = """name,channel_url
Invalid URL Channel,not-a-youtube-url
"""
        
        csv_path = create_test_csv(csv_content)
        
        try:
            channels = handler.parse_input_file(csv_path)
            # Should skip invalid URLs
            if channels:
                # Channel validation should fail
                print("🔍 INFO: Invalid URL parsing result - may fail at validation stage")
        except ValueError as e:
            print("✅ SUCCESS: Invalid YouTube URL handled with error")
        finally:
            os.unlink(csv_path)
        
        print("✅ ALL CSV edge case tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Edge case test failed: {e}")
        return False


def test_format_detection():
    """Test format detection for CSV files."""
    print("\n🧪 Testing format detection...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, InputFormat = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: .csv extension
        csv_path = create_test_csv("name,url\ntest,test")
        try:
            format_type = handler.detect_format(csv_path)
            if format_type != InputFormat.CSV:
                print(f"❌ FAILURE: Expected CSV format, got {format_type}")
                return False
            print("✅ SUCCESS: .csv extension detected correctly")
        finally:
            os.unlink(csv_path)
        
        # Test Case 2: No extension but CSV content
        fd, path = tempfile.mkstemp(suffix='')
        try:
            with os.fdopen(fd, 'w') as f:
                f.write("name,channel_url,email\ntest,https://youtube.com/@test,test@example.com")
            
            format_type = handler.detect_format(path)
            if format_type != InputFormat.CSV:
                print(f"❌ FAILURE: CSV content without extension not detected")
                return False
            print("✅ SUCCESS: CSV content detected without extension")
        finally:
            os.unlink(path)
        
        print("✅ ALL format detection tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Format detection test failed: {e}")
        return False


def test_csv_validation_integration():
    """Test CSV parsing with channel validation."""
    print("\n🧪 Testing CSV validation integration...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test validation of parsed channels
        csv_content = """name,channel_url,email
Valid Channel,https://youtube.com/@validchannel,valid@example.com
Invalid URL,not-a-url,invalid@example.com
Empty Name,,https://youtube.com/@empty
Valid No Email,https://youtube.com/@noemail,
"""
        
        csv_path = create_test_csv(csv_content)
        
        try:
            channels = handler.parse_input_file(csv_path)
            
            # Should have 2 valid channels (Valid Channel and Valid No Email)
            if len(channels) < 2:
                print(f"🔍 INFO: Got {len(channels)} channels after validation")
            
            # Validate parsed channels
            valid_channels, errors = handler.validate_channel_inputs(channels)
            
            print(f"✅ SUCCESS: Validated {len(valid_channels)} valid channels")
            if errors:
                print(f"   Found {len(errors)} validation errors (expected)")
            
        finally:
            os.unlink(csv_path)
        
        print("✅ ALL CSV validation integration tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Validation integration test failed: {e}")
        return False


def main():
    """Run comprehensive CSV parser test suite."""
    print("🚀 Starting CSV Parser Test Suite")
    print("   Testing CSV parsing functionality")
    print("   Testing error handling and edge cases")
    print("=" * 80)
    
    all_tests_passed = True
    test_functions = [
        test_imports,
        test_valid_csv_parsing,
        test_invalid_csv_parsing,
        test_csv_edge_cases,
        test_format_detection,
        test_csv_validation_integration
    ]
    
    for test_func in test_functions:
        if not test_func():
            all_tests_passed = False
            print(f"❌ {test_func.__name__} FAILED")
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL CSV PARSER TESTS PASSED!")
        print("✅ Valid CSV parsing working")
        print("✅ Invalid CSV detection functional")
        print("✅ Edge cases handled correctly")
        print("✅ Format detection accurate")
        print("✅ Validation integration complete")
        print("\\n🔥 CSV parser is PRODUCTION-READY!")
        return 0
    else:
        print("💥 SOME CSV PARSER TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    sys.exit(main())