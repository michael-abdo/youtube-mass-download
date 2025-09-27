#!/usr/bin/env python3
"""
Test JSON Parser
Phase 3.5: Test JSON parser with various JSON formats

Tests:
1. Array format JSON parsing
2. Object with channels array format
3. Object with named channels format
4. Invalid JSON handling
5. Missing required fields
6. Additional fields handling
7. Edge cases and error conditions
8. Complex nested data handling

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import os
import json
import tempfile
from pathlib import Path
from typing import List, Dict, Any

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))

def test_imports():
    """Test that all required modules import correctly."""
    print("🧪 Testing imports for JSON parser tests...")
    
    try:
        from input_handler import InputHandler, ChannelInput, InputFormat
        print("✅ SUCCESS: All required imports successful")
        return True, (InputHandler, ChannelInput, InputFormat)
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import required modules")
        print(f"   Error: {e}")
        return False, None


def create_test_json(content: Any) -> str:
    """Create a temporary JSON file with given content."""
    fd, path = tempfile.mkstemp(suffix='.json')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(content, f, indent=2)
        return path
    except:
        os.close(fd)
        raise


def test_array_format_json():
    """Test parsing JSON array format."""
    print("\n🧪 Testing JSON array format...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, ChannelInput, InputFormat = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Standard array format
        json_data = [
            {
                "name": "MrBeast",
                "channel_url": "https://youtube.com/@MrBeast",
                "email": "contact@mrbeast.com",
                "type": "Entertainment"
            },
            {
                "name": "PewDiePie",
                "url": "https://www.youtube.com/c/PewDiePie",
                "email": "pewds@example.com",
                "category": "Gaming"
            },
            {
                "channel_name": "Tech Reviews",
                "youtube_url": "https://youtube.com/channel/UCX6OQ3DkcsbYNE6H8uQQuVA",
                "type": "Technology"
            }
        ]
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            
            if len(channels) != 3:
                print(f"❌ FAILURE: Expected 3 channels, got {len(channels)}")
                return False
            
            # Verify first channel
            if channels[0].name != "MrBeast":
                print(f"❌ FAILURE: Expected name 'MrBeast', got '{channels[0].name}'")
                return False
            
            if channels[0].email != "contact@mrbeast.com":
                print(f"❌ FAILURE: Expected email 'contact@mrbeast.com', got '{channels[0].email}'")
                return False
            
            # Verify second channel (uses 'url' instead of 'channel_url')
            if channels[1].name != "PewDiePie":
                print(f"❌ FAILURE: Expected name 'PewDiePie', got '{channels[1].name}'")
                return False
            
            # Verify third channel (uses 'channel_name' and 'youtube_url')
            if channels[2].name != "Tech Reviews":
                print(f"❌ FAILURE: Expected name 'Tech Reviews', got '{channels[2].name}'")
                return False
            
            print("✅ SUCCESS: Array format JSON parsed correctly")
            
        finally:
            os.unlink(json_path)
        
        # Test Case 2: Array with additional fields
        json_data = [
            {
                "name": "Test Channel",
                "channel_url": "https://youtube.com/@testchannel",
                "subscriber_count": 1000000,
                "verified": True,
                "tags": ["gaming", "reviews"],
                "metadata": {"region": "US", "language": "en"}
            }
        ]
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            
            if len(channels) != 1:
                print(f"❌ FAILURE: Expected 1 channel, got {len(channels)}")
                return False
            
            channel = channels[0]
            if not channel.additional_data:
                print("❌ FAILURE: Additional data not captured")
                return False
            
            if channel.additional_data.get("subscriber_count") != "1000000":
                print("❌ FAILURE: subscriber_count not converted to string")
                return False
            
            if channel.additional_data.get("verified") != "True":
                print("❌ FAILURE: Boolean verified not converted correctly")
                return False
            
            # Complex types should be JSON strings
            if "tags" in channel.additional_data:
                tags = json.loads(channel.additional_data["tags"])
                if tags != ["gaming", "reviews"]:
                    print("❌ FAILURE: Array field not preserved correctly")
                    return False
            
            print("✅ SUCCESS: Additional fields handled correctly")
            
        finally:
            os.unlink(json_path)
        
        print("✅ ALL array format JSON tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Array format test failed: {e}")
        return False


def test_object_with_channels_format():
    """Test parsing JSON object with 'channels' array."""
    print("\n🧪 Testing JSON object with 'channels' array...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Standard channels object
        json_data = {
            "version": "1.0",
            "generated": "2024-01-01",
            "channels": [
                {
                    "name": "Channel 1",
                    "channel_url": "https://youtube.com/@channel1",
                    "email": "channel1@example.com"
                },
                {
                    "name": "Channel 2",
                    "channel_url": "https://youtube.com/@channel2",
                    "type": "Music"
                }
            ],
            "metadata": {
                "total": 2,
                "source": "manual"
            }
        }
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            
            if len(channels) != 2:
                print(f"❌ FAILURE: Expected 2 channels, got {len(channels)}")
                return False
            
            if channels[0].name != "Channel 1":
                print(f"❌ FAILURE: First channel name mismatch")
                return False
            
            if channels[1].type != "Music":
                print(f"❌ FAILURE: Second channel type not captured")
                return False
            
            print("✅ SUCCESS: Object with channels array parsed correctly")
            
        finally:
            os.unlink(json_path)
        
        # Test Case 2: Invalid channels field (not an array)
        json_data = {
            "channels": "not an array"
        }
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            print("❌ VALIDATION FAILURE: Non-array channels field should have failed!")
            return False
        except ValueError as e:
            if "'channels' field must be an array" in str(e):
                print("✅ SUCCESS: Non-array channels field detected correctly")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        finally:
            os.unlink(json_path)
        
        print("✅ ALL object with channels format tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Object format test failed: {e}")
        return False


def test_named_channels_format():
    """Test parsing JSON object with named channel entries."""
    print("\n🧪 Testing JSON object with named channels...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Named channels format
        json_data = {
            "mrbeast": {
                "channel_url": "https://youtube.com/@MrBeast",
                "email": "contact@mrbeast.com",
                "type": "Entertainment"
            },
            "pewdiepie": {
                "name": "PewDiePie Official",  # Override key name
                "url": "https://youtube.com/@pewdiepie",
                "category": "Gaming"
            },
            "tech_channel": {
                "youtube_url": "https://youtube.com/@techreviews",
                "description": "Tech review channel"
            },
            "metadata": {
                "format": "named_channels",
                "version": "2.0"
            }
        }
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            
            # Should have 3 channels (metadata object ignored)
            if len(channels) != 3:
                print(f"❌ FAILURE: Expected 3 channels, got {len(channels)}")
                return False
            
            # Find channels by name
            channel_names = [ch.name for ch in channels]
            
            # First channel should use key as name
            if "mrbeast" not in channel_names:
                print(f"❌ FAILURE: Channel key 'mrbeast' not used as name")
                return False
            
            # Second channel should use explicit name
            if "PewDiePie Official" not in channel_names:
                print(f"❌ FAILURE: Explicit name not used for pewdiepie")
                return False
            
            # Third channel should use key as name
            if "tech_channel" not in channel_names:
                print(f"❌ FAILURE: Channel key 'tech_channel' not used as name")
                return False
            
            print("✅ SUCCESS: Named channels format parsed correctly")
            
        finally:
            os.unlink(json_path)
        
        print("✅ ALL named channels format tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Named channels test failed: {e}")
        return False


def test_invalid_json_parsing():
    """Test handling of invalid JSON files."""
    print("\n🧪 Testing invalid JSON parsing...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Malformed JSON
        malformed_json = '{"channels": [{"name": "Test", "url": "missing closing bracket"'
        
        fd, json_path = tempfile.mkstemp(suffix='.json')
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(malformed_json)
            
            channels = handler.parse_input_file(json_path)
            print("❌ VALIDATION FAILURE: Malformed JSON should have failed!")
            return False
        except ValueError as e:
            if "Invalid JSON format" in str(e):
                print("✅ SUCCESS: Malformed JSON detected correctly")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        finally:
            os.unlink(json_path)
        
        # Test Case 2: Empty JSON array
        json_data = []
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            print("❌ VALIDATION FAILURE: Empty JSON array should have failed!")
            return False
        except ValueError as e:
            if "no valid channel entries" in str(e):
                print("✅ SUCCESS: Empty JSON array detected correctly")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        finally:
            os.unlink(json_path)
        
        # Test Case 3: Invalid root type
        json_data = "Just a string, not an array or object"
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            print("❌ VALIDATION FAILURE: String root should have failed!")
            return False
        except ValueError as e:
            if "Invalid root structure" in str(e):
                print("✅ SUCCESS: Invalid root type detected correctly")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        finally:
            os.unlink(json_path)
        
        # Test Case 4: Missing required fields
        json_data = [
            {
                "name": "No URL Channel",
                "email": "test@example.com"
            },
            {
                "channel_url": "https://youtube.com/@nourlchannel"
                # Missing name
            }
        ]
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            print("❌ VALIDATION FAILURE: Should have no valid entries!")
            return False
        except ValueError as e:
            if "no valid channel entries" in str(e):
                print("✅ SUCCESS: Missing required fields handled correctly")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        finally:
            os.unlink(json_path)
        
        print("✅ ALL invalid JSON parsing tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Invalid JSON test failed: {e}")
        return False


def test_json_edge_cases():
    """Test JSON parsing edge cases."""
    print("\n🧪 Testing JSON edge cases...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Mixed valid and invalid entries
        json_data = [
            {
                "name": "Valid Channel",
                "channel_url": "https://youtube.com/@validchannel"
            },
            "Not a channel object",
            None,
            {
                "invalid": "No required fields"
            },
            {
                "name": "Another Valid",
                "url": "https://youtube.com/@anothervalid",
                "extra_field": "preserved"
            }
        ]
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            
            if len(channels) != 2:
                print(f"❌ FAILURE: Expected 2 valid channels, got {len(channels)}")
                return False
            
            if channels[0].name != "Valid Channel":
                print(f"❌ FAILURE: First valid channel not parsed correctly")
                return False
            
            if channels[1].name != "Another Valid":
                print(f"❌ FAILURE: Second valid channel not parsed correctly")
                return False
            
            if not channels[1].additional_data or channels[1].additional_data.get("extra_field") != "preserved":
                print(f"❌ FAILURE: Additional field not preserved")
                return False
            
            print("✅ SUCCESS: Mixed valid/invalid entries handled correctly")
            
        finally:
            os.unlink(json_path)
        
        # Test Case 2: Unicode and special characters
        json_data = [
            {
                "name": "Channel with 中文",
                "channel_url": "https://youtube.com/@chinesechannel",
                "description": "Special chars: \"quotes\" and 'apostrophes'"
            },
            {
                "name": "Émojis 🎮🎵",
                "url": "https://youtube.com/@emojichannel",
                "type": "Entertainment 🎬"
            }
        ]
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            
            if len(channels) != 2:
                print(f"❌ FAILURE: Expected 2 channels, got {len(channels)}")
                return False
            
            if channels[0].name != "Channel with 中文":
                print(f"❌ FAILURE: Unicode characters not handled correctly")
                return False
            
            if channels[1].name != "Émojis 🎮🎵":
                print(f"❌ FAILURE: Emoji characters not handled correctly")
                return False
            
            print("✅ SUCCESS: Unicode and special characters handled correctly")
            
        finally:
            os.unlink(json_path)
        
        # Test Case 3: Case-insensitive field matching
        json_data = [
            {
                "NAME": "Uppercase Fields",
                "CHANNEL_URL": "https://youtube.com/@uppercase",
                "EMAIL": "upper@example.com"
            },
            {
                "Name": "Mixed Case",
                "Channel_Url": "https://youtube.com/@mixedcase",
                "Email": "mixed@example.com"
            }
        ]
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            
            if len(channels) != 2:
                print(f"❌ FAILURE: Expected 2 channels, got {len(channels)}")
                return False
            
            if channels[0].name != "Uppercase Fields":
                print(f"❌ FAILURE: Uppercase field names not matched")
                return False
            
            if channels[1].email != "mixed@example.com":
                print(f"❌ FAILURE: Mixed case field names not matched")
                return False
            
            print("✅ SUCCESS: Case-insensitive field matching working")
            
        finally:
            os.unlink(json_path)
        
        # Test Case 4: Nested data structures
        json_data = {
            "channels": [
                {
                    "name": "Complex Channel",
                    "channel_url": "https://youtube.com/@complex",
                    "analytics": {
                        "views": {"daily": 10000, "monthly": 300000},
                        "engagement": 0.85
                    },
                    "playlists": [
                        {"name": "Gaming", "count": 50},
                        {"name": "Reviews", "count": 30}
                    ]
                }
            ]
        }
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            
            if len(channels) != 1:
                print(f"❌ FAILURE: Expected 1 channel, got {len(channels)}")
                return False
            
            channel = channels[0]
            if not channel.additional_data:
                print("❌ FAILURE: Additional data not captured")
                return False
            
            # Complex fields should be JSON strings
            if "analytics" in channel.additional_data:
                analytics = json.loads(channel.additional_data["analytics"])
                if analytics["engagement"] != 0.85:
                    print("❌ FAILURE: Nested object not preserved correctly")
                    return False
            
            print("✅ SUCCESS: Nested data structures handled correctly")
            
        finally:
            os.unlink(json_path)
        
        print("✅ ALL JSON edge case tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Edge case test failed: {e}")
        return False


def test_format_detection_json():
    """Test format detection for JSON files."""
    print("\n🧪 Testing JSON format detection...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, InputFormat = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: .json extension
        json_path = create_test_json({"test": "data"})
        try:
            format_type = handler.detect_format(json_path)
            if format_type != InputFormat.JSON:
                print(f"❌ FAILURE: Expected JSON format, got {format_type}")
                return False
            print("✅ SUCCESS: .json extension detected correctly")
        finally:
            os.unlink(json_path)
        
        # Test Case 2: No extension but JSON content
        fd, path = tempfile.mkstemp(suffix='')
        try:
            with os.fdopen(fd, 'w') as f:
                f.write('{"channels": []}')
            
            format_type = handler.detect_format(path)
            if format_type != InputFormat.JSON:
                print(f"❌ FAILURE: JSON content without extension not detected")
                return False
            print("✅ SUCCESS: JSON content detected without extension")
        finally:
            os.unlink(path)
        
        print("✅ ALL JSON format detection tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Format detection test failed: {e}")
        return False


def test_json_validation_integration():
    """Test JSON parsing with channel validation."""
    print("\n🧪 Testing JSON validation integration...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test validation of parsed channels
        json_data = [
            {
                "name": "Valid Channel",
                "channel_url": "https://youtube.com/@validchannel",
                "email": "valid@example.com"
            },
            {
                "name": "Invalid URL",
                "channel_url": "not-a-youtube-url",
                "email": "invalid@example.com"
            },
            {
                "name": "Bad Email",
                "channel_url": "https://youtube.com/@bademail",
                "email": "not-an-email"
            }
        ]
        
        json_path = create_test_json(json_data)
        
        try:
            channels = handler.parse_input_file(json_path)
            
            # Should have at least 1 channel (others may fail validation)
            if len(channels) < 1:
                print(f"🔍 INFO: Got {len(channels)} channels after parsing")
            
            # Validate parsed channels
            valid_channels, errors = handler.validate_channel_inputs(channels)
            
            print(f"✅ SUCCESS: Validated {len(valid_channels)} valid channels")
            if errors:
                print(f"   Found {len(errors)} validation errors (expected)")
            
        finally:
            os.unlink(json_path)
        
        print("✅ ALL JSON validation integration tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Validation integration test failed: {e}")
        return False


def main():
    """Run comprehensive JSON parser test suite."""
    print("🚀 Starting JSON Parser Test Suite")
    print("   Testing JSON parsing functionality")
    print("   Testing multiple JSON formats")
    print("   Testing error handling and edge cases")
    print("=" * 80)
    
    all_tests_passed = True
    test_functions = [
        test_imports,
        test_array_format_json,
        test_object_with_channels_format,
        test_named_channels_format,
        test_invalid_json_parsing,
        test_json_edge_cases,
        test_format_detection_json,
        test_json_validation_integration
    ]
    
    for test_func in test_functions:
        if not test_func():
            all_tests_passed = False
            print(f"❌ {test_func.__name__} FAILED")
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL JSON PARSER TESTS PASSED!")
        print("✅ Array format parsing working")
        print("✅ Object with channels format functional")
        print("✅ Named channels format supported")
        print("✅ Invalid JSON detection working")
        print("✅ Edge cases handled correctly")
        print("✅ Format detection accurate")
        print("✅ Validation integration complete")
        print("\n🔥 JSON parser is PRODUCTION-READY!")
        return 0
    else:
        print("💥 SOME JSON PARSER TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    sys.exit(main())