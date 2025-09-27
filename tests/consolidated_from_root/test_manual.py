#!/usr/bin/env python3

import sys
import os

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
print("Testing path setup...")
print(f"Current directory: {current_dir}")
print(f"Python path: {sys.path[:3]}")

try:
    from utils.path_setup import setup_project_path
    print("✓ path_setup imported successfully")
    
    setup_project_path()
    print("✓ setup_project_path() executed successfully")
    
    from utils.patterns import get_selenium_driver, cleanup_selenium_driver
    print("✓ patterns imported successfully")
    
    print("\n=== Testing Selenium driver (minimal) ===")
    driver = get_selenium_driver()
    if driver:
        print("✓ Selenium driver initialized successfully!")
        print(f"✓ Driver type: {type(driver)}")
        
        # Test basic functionality
        try:
            driver.get("https://www.google.com")
            print("✓ Successfully loaded Google homepage")
            print(f"✓ Page title: {driver.title}")
        except Exception as e:
            print(f"⚠ Page load error: {e}")
        
        cleanup_selenium_driver()
        print("✓ Driver cleaned up successfully")
    else:
        print("✗ Failed to initialize Selenium driver")
        
    print("\n=== All tests completed ===")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()