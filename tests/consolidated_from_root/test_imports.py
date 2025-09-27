#!/usr/bin/env python3
"""Test imports to verify path_setup works"""

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

import sys
import os
print("Testing path_setup import...")
try:
    from utils.path_setup import setup_project_path
    print("✓ path_setup imported successfully")
    
    setup_project_path()
    print("✓ setup_project_path() executed successfully")
    
    from utils.patterns import get_selenium_driver, cleanup_selenium_driver
    print("✓ patterns imported successfully")
    
    print("\nAll imports working correctly!")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()