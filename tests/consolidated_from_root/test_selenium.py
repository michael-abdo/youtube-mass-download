#!/usr/bin/env python3
"""Test if Selenium works after our fixes"""

from utils.path_setup import setup_project_path
setup_project_path()

from utils.patterns import get_selenium_driver, cleanup_selenium_driver

print("Testing Selenium driver initialization...")
try:
    driver = get_selenium_driver()
    if driver:
        print("✓ Selenium driver initialized successfully!")
        driver.get("https://www.google.com")
        print("✓ Successfully loaded Google homepage")
        print(f"✓ Page title: {driver.title}")
        cleanup_selenium_driver()
        print("✓ Driver cleaned up successfully")
    else:
        print("✗ Failed to initialize Selenium driver")
except Exception as e:
    print(f"✗ Error: {e}")