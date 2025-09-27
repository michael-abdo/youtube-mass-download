#!/usr/bin/env python3
"""
Test Text Parser
Phase 3.7: Test text parser with different text formats

Tests:
1. URL per line format
2. Named entries format (Name: URL)
3. Markdown link format
4. Comma-separated URLs
5. Mixed text with URLs
6. Empty and invalid files
7. Comment handling
8. URL extraction from complex text

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""
import sys
import os
import tempfile
from pathlib import Path
from typing import List

# Add the current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))

def test_imports():
    """Test that all required modules import correctly."""
    print("🧪 Testing imports for text parser tests...")
    
    try:
        from input_handler import InputHandler, ChannelInput, InputFormat
        print("✅ SUCCESS: All required imports successful")
        return True, (InputHandler, ChannelInput, InputFormat)
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Cannot import required modules")
        print(f"   Error: {e}")
        return False, None


def create_test_text(content: str) -> str:
    """Create a temporary text file with given content."""
    fd, path = tempfile.mkstemp(suffix='.txt')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(content)
        return path
    except:
        os.close(fd)
        raise


def test_url_per_line_format():
    """Test parsing text file with one URL per line."""
    print("\n🧪 Testing URL per line format...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, ChannelInput, InputFormat = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Simple URLs per line
        text_content = """https://youtube.com/@MrBeast
https://www.youtube.com/c/PewDiePie
https://youtube.com/channel/UCX6OQ3DkcsbYNE6H8uQQuVA
https://youtube.com/user/SomeUser

# This is a comment
https://youtube.com/@AnotherChannel
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            if len(channels) != 5:
                print(f"❌ FAILURE: Expected 5 channels, got {len(channels)}")
                return False
            
            # Check extracted names
            names = [ch.name for ch in channels]
            expected_names = ["MrBeast", "PewDiePie", "Channel UCX6OQ3D", "SomeUser", "AnotherChannel"]
            
            for expected in expected_names:
                if expected not in names:
                    print(f"❌ FAILURE: Expected name '{expected}' not found in {names}")
                    return False
            
            print("✅ SUCCESS: URL per line format parsed correctly")
            
        finally:
            os.unlink(text_path)
        
        # Test Case 2: URLs with trailing/leading whitespace
        text_content = """  https://youtube.com/@Channel1  
    https://youtube.com/@Channel2    


https://youtube.com/@Channel3
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            if len(channels) != 3:
                print(f"❌ FAILURE: Expected 3 channels with whitespace, got {len(channels)}")
                return False
            
            print("✅ SUCCESS: Whitespace handling working correctly")
            
        finally:
            os.unlink(text_path)
        
        print("✅ ALL URL per line format tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: URL per line test failed: {e}")
        return False


def test_named_entries_format():
    """Test parsing text with named entries (Name: URL format)."""
    print("\n🧪 Testing named entries format...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Colon-separated format
        text_content = """Gaming Channel: https://youtube.com/@gamingpro
Tech Reviews: https://www.youtube.com/c/techreviews
Music Channel: https://youtube.com/channel/UCmusicID123
Educational Content: https://youtube.com/user/educator
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            if len(channels) != 4:
                print(f"❌ FAILURE: Expected 4 channels, got {len(channels)}")
                return False
            
            # Check names are preserved
            channel_by_name = {ch.name: ch for ch in channels}
            
            if "Gaming Channel" not in channel_by_name:
                print(f"❌ FAILURE: Named entry 'Gaming Channel' not preserved")
                return False
            
            if "Tech Reviews" not in channel_by_name:
                print(f"❌ FAILURE: Named entry 'Tech Reviews' not preserved")
                return False
            
            print("✅ SUCCESS: Colon-separated named entries parsed correctly")
            
        finally:
            os.unlink(text_path)
        
        # Test Case 2: Dash-separated format
        text_content = """My Favorite Channel - https://youtube.com/@favorite
Another Great Channel - https://youtube.com/@another
Best Content - https://youtube.com/@bestcontent
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            if len(channels) != 3:
                print(f"❌ FAILURE: Expected 3 dash-separated channels, got {len(channels)}")
                return False
            
            names = [ch.name for ch in channels]
            if "My Favorite Channel" not in names:
                print(f"❌ FAILURE: Dash-separated name not preserved")
                return False
            
            print("✅ SUCCESS: Dash-separated named entries parsed correctly")
            
        finally:
            os.unlink(text_path)
        
        # Test Case 3: Mixed separators
        text_content = """Channel One: https://youtube.com/@one
Channel Two - https://youtube.com/@two
Channel Three | https://youtube.com/@three
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            if len(channels) != 3:
                print(f"❌ FAILURE: Expected 3 mixed separator channels, got {len(channels)}")
                return False
            
            print("✅ SUCCESS: Mixed separator formats handled correctly")
            
        finally:
            os.unlink(text_path)
        
        print("✅ ALL named entries format tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Named entries test failed: {e}")
        return False


def test_markdown_format():
    """Test parsing Markdown-style links."""
    print("\n🧪 Testing Markdown link format...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Standard Markdown links
        text_content = """Here are my favorite channels:

[MrBeast](https://youtube.com/@MrBeast)
[PewDiePie](https://youtube.com/@PewDiePie)
[Tech With Tim](https://youtube.com/@TechWithTim)

Check them out!
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            if len(channels) != 3:
                print(f"❌ FAILURE: Expected 3 Markdown links, got {len(channels)}")
                return False
            
            names = [ch.name for ch in channels]
            if "Tech With Tim" not in names:
                print(f"❌ FAILURE: Markdown link name 'Tech With Tim' not preserved")
                return False
            
            print("✅ SUCCESS: Markdown links parsed correctly")
            
        finally:
            os.unlink(text_path)
        
        # Test Case 2: Markdown links with special characters
        text_content = """[Channel with "Quotes"](https://youtube.com/@quotes)
[Channel (with parentheses)](https://youtube.com/@parens)
[Channel with [brackets]](https://youtube.com/@brackets)
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            if len(channels) != 3:
                print(f"❌ FAILURE: Expected 3 special character links, got {len(channels)}")
                return False
            
            print("✅ SUCCESS: Markdown links with special characters handled")
            
        finally:
            os.unlink(text_path)
        
        print("✅ ALL Markdown format tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Markdown format test failed: {e}")
        return False


def test_comma_separated_format():
    """Test parsing comma-separated URLs."""
    print("\n🧪 Testing comma-separated format...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Single line with comma-separated URLs
        text_content = """https://youtube.com/@channel1, https://youtube.com/@channel2, https://youtube.com/@channel3"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            if len(channels) != 3:
                print(f"❌ FAILURE: Expected 3 comma-separated channels, got {len(channels)}")
                return False
            
            print("✅ SUCCESS: Comma-separated URLs parsed correctly")
            
        finally:
            os.unlink(text_path)
        
        # Test Case 2: Multiple lines with comma-separated URLs
        text_content = """https://youtube.com/@ch1, https://youtube.com/@ch2
https://youtube.com/@ch3, https://youtube.com/@ch4, https://youtube.com/@ch5
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            if len(channels) != 5:
                print(f"❌ FAILURE: Expected 5 multi-line comma-separated channels, got {len(channels)}")
                return False
            
            print("✅ SUCCESS: Multi-line comma-separated URLs parsed correctly")
            
        finally:
            os.unlink(text_path)
        
        print("✅ ALL comma-separated format tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Comma-separated test failed: {e}")
        return False


def test_mixed_text_extraction():
    """Test extracting URLs from mixed text content."""
    print("\n🧪 Testing mixed text extraction...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: URLs embedded in paragraph text
        text_content = """Welcome to my channel recommendations!

I really enjoy watching https://youtube.com/@creator1 for gaming content.
For tech reviews, check out https://www.youtube.com/c/techguru - they have great videos!
Don't miss https://youtube.com/channel/UCeducational123 for learning.

Also, youtube.com/@shorturl is pretty good (even without https).

Thanks for reading!
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            if len(channels) < 3:
                print(f"❌ FAILURE: Expected at least 3 embedded URLs, got {len(channels)}")
                return False
            
            # Check that URLs without https:// are also found
            urls = [ch.channel_url for ch in channels]
            if not any("shorturl" in url for url in urls):
                print(f"❌ FAILURE: URL without https:// not extracted")
                return False
            
            print("✅ SUCCESS: URLs extracted from mixed text correctly")
            
        finally:
            os.unlink(text_path)
        
        # Test Case 2: Complex mixed format
        text_content = """## My YouTube Channel List

1. Gaming Channels:
   - FPS Games: https://youtube.com/@fpsgamer
   - RPG Content: https://youtube.com/@rpgmaster
   
2. Educational:
   [Science Explained](https://youtube.com/@science)
   Math Tutorials - https://youtube.com/@mathtutorials
   
3. Entertainment:
   https://youtube.com/@comedy, https://youtube.com/@music
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            # Should get 6 channels: fpsgamer, rpgmaster, science, mathtutorials, comedy, music
            if len(channels) != 6:
                print(f"❌ FAILURE: Expected 6 channels from complex text, got {len(channels)}")
                return False
            
            # Check that both named and unnamed entries are found
            names = [ch.name for ch in channels]
            if "Science Explained" not in names:
                print(f"❌ FAILURE: Markdown link in mixed text not extracted")
                return False
            
            if "Math Tutorials" not in names:
                print(f"❌ FAILURE: Named entry in mixed text not extracted")
                return False
            
            print("✅ SUCCESS: Complex mixed format parsed correctly")
            
        finally:
            os.unlink(text_path)
        
        print("✅ ALL mixed text extraction tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Mixed text test failed: {e}")
        return False


def test_invalid_text_parsing():
    """Test handling of invalid text files."""
    print("\n🧪 Testing invalid text parsing...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Empty file
        text_content = ""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            print("❌ VALIDATION FAILURE: Empty file should have failed!")
            return False
        except ValueError as e:
            if "File is empty" in str(e):
                print("✅ SUCCESS: Empty file detected correctly")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        finally:
            os.unlink(text_path)
        
        # Test Case 2: File with no YouTube URLs
        text_content = """This is just regular text.
No YouTube links here.
Just some random content.
Maybe a non-YouTube URL: https://example.com
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            print("❌ VALIDATION FAILURE: File with no YouTube URLs should have failed!")
            return False
        except ValueError as e:
            if "No valid YouTube channel URLs found" in str(e):
                print("✅ SUCCESS: No YouTube URLs detected correctly")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        finally:
            os.unlink(text_path)
        
        # Test Case 3: Only comments
        text_content = """# This is a comment
# Another comment
# No actual URLs here
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            print("❌ VALIDATION FAILURE: File with only comments should have failed!")
            return False
        except ValueError as e:
            if "No valid YouTube channel URLs found" in str(e):
                print("✅ SUCCESS: Comment-only file handled correctly")
            else:
                print(f"❌ VALIDATION FAILURE: Wrong error message: {e}")
                return False
        finally:
            os.unlink(text_path)
        
        print("✅ ALL invalid text parsing tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Invalid text test failed: {e}")
        return False


def test_url_extraction_edge_cases():
    """Test URL extraction edge cases."""
    print("\n🧪 Testing URL extraction edge cases...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, _ = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: Various YouTube URL formats
        text_content = """Different URL formats:
https://youtube.com/@username
https://www.youtube.com/@username
youtube.com/@username
https://youtube.com/c/channelname
https://youtube.com/channel/UCX6OQ3DkcsbYNE6H8uQQuVA
https://youtube.com/user/username
HTTPS://YOUTUBE.COM/@UPPERCASE
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            if len(channels) < 5:
                print(f"❌ FAILURE: Expected at least 5 different URL formats, got {len(channels)}")
                return False
            
            # Check case-insensitive matching
            urls = [ch.channel_url for ch in channels]
            names = [ch.name for ch in channels]
            # The uppercase URL should be parsed (even if the URL itself gets normalized)
            if not any("UPPERCASE" in name for name in names):
                print(f"❌ FAILURE: Case-insensitive URL not parsed (should have UPPERCASE in name)")
                return False
            
            print("✅ SUCCESS: Various URL formats extracted correctly")
            
        finally:
            os.unlink(text_path)
        
        # Test Case 2: URLs with special characters and Unicode
        text_content = """International channels:
中文频道: https://youtube.com/@chinesechannel
Émojis 🎮: https://youtube.com/@emojichannel
Channel (with parens): https://youtube.com/@parenchannel
"Quoted Channel": https://youtube.com/@quotedchannel
"""
        
        text_path = create_test_text(text_content)
        
        try:
            channels = handler.parse_input_file(text_path)
            
            if len(channels) != 4:
                print(f"❌ FAILURE: Expected 4 special character URLs, got {len(channels)}")
                return False
            
            names = [ch.name for ch in channels]
            if "中文频道" not in names:
                print(f"❌ FAILURE: Unicode channel name not preserved")
                return False
            
            print("✅ SUCCESS: Special characters and Unicode handled correctly")
            
        finally:
            os.unlink(text_path)
        
        print("✅ ALL URL extraction edge case tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Edge case test failed: {e}")
        return False


def test_format_detection_text():
    """Test format detection for text files."""
    print("\n🧪 Testing text format detection...")
    
    success, classes = test_imports()
    if not success:
        return False
    
    InputHandler, _, InputFormat = classes
    
    try:
        handler = InputHandler()
        
        # Test Case 1: .txt extension
        text_path = create_test_text("https://youtube.com/@test")
        try:
            format_type = handler.detect_format(text_path)
            if format_type != InputFormat.TEXT:
                print(f"❌ FAILURE: Expected TEXT format, got {format_type}")
                return False
            print("✅ SUCCESS: .txt extension detected correctly")
        finally:
            os.unlink(text_path)
        
        # Test Case 2: .text extension
        fd, path = tempfile.mkstemp(suffix='.text')
        try:
            with os.fdopen(fd, 'w') as f:
                f.write("https://youtube.com/@test")
            
            format_type = handler.detect_format(path)
            if format_type != InputFormat.TEXT:
                print(f"❌ FAILURE: .text extension not detected as TEXT format")
                return False
            print("✅ SUCCESS: .text extension detected correctly")
        finally:
            os.unlink(path)
        
        # Test Case 3: No extension but YouTube URL content
        fd, path = tempfile.mkstemp(suffix='')
        try:
            with os.fdopen(fd, 'w') as f:
                f.write("Check out https://youtube.com/@mychannel")
            
            format_type = handler.detect_format(path)
            if format_type != InputFormat.TEXT:
                print(f"❌ FAILURE: YouTube URL content not detected as TEXT")
                return False
            print("✅ SUCCESS: YouTube URL content detected without extension")
        finally:
            os.unlink(path)
        
        print("✅ ALL text format detection tests PASSED")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: Format detection test failed: {e}")
        return False


def main():
    """Run comprehensive text parser test suite."""
    print("🚀 Starting Text Parser Test Suite")
    print("   Testing text parsing functionality")
    print("   Testing multiple text formats")
    print("   Testing URL extraction capabilities")
    print("=" * 80)
    
    all_tests_passed = True
    test_functions = [
        test_imports,
        test_url_per_line_format,
        test_named_entries_format,
        test_markdown_format,
        test_comma_separated_format,
        test_mixed_text_extraction,
        test_invalid_text_parsing,
        test_url_extraction_edge_cases,
        test_format_detection_text
    ]
    
    for test_func in test_functions:
        if not test_func():
            all_tests_passed = False
            print(f"❌ {test_func.__name__} FAILED")
    
    # Final results
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 ALL TEXT PARSER TESTS PASSED!")
        print("✅ URL per line format working")
        print("✅ Named entries format functional")
        print("✅ Markdown links supported")
        print("✅ Comma-separated URLs parsed")
        print("✅ Mixed text extraction working")
        print("✅ Invalid file detection correct")
        print("✅ Edge cases handled properly")
        print("✅ Format detection accurate")
        print("\n🔥 Text parser is PRODUCTION-READY!")
        return 0
    else:
        print("💥 SOME TEXT PARSER TESTS FAILED!")
        print("   This is LOUD FAILURE - fix the issues before proceeding!")
        return 1


if __name__ == "__main__":
    sys.exit(main())