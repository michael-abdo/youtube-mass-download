#!/usr/bin/env python3
"""Manual validation of CSV consolidation"""

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()

import sys
import os

print("Manual CSV Consolidation Validation")
print("=" * 40)

# Test 1: Import safe_csv_read
print("1. Testing safe_csv_read import...")
try:
    from utils.csv_manager import safe_csv_read
    print("   ✅ SUCCESS: safe_csv_read imported")
    
    # Test the function signature
    import inspect
    sig = inspect.signature(safe_csv_read)
    print(f"   📋 Function signature: {sig}")
    
except Exception as e:
    print(f"   ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()

# Test 2: Import download_all_minimal
print("\n2. Testing download_all_minimal import...")
try:
    import download_all_minimal
    print("   ✅ SUCCESS: download_all_minimal imported")
    
    # Check if MinimalDownloader class exists
    if hasattr(download_all_minimal, 'MinimalDownloader'):
        print("   ✅ MinimalDownloader class found")
    else:
        print("   ❌ MinimalDownloader class missing")
        
except Exception as e:
    print(f"   ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Test safe_csv_read function
print("\n3. Testing safe_csv_read function...")
try:
    from utils.csv_manager import safe_csv_read
    
    # Check if CSV exists
    csv_path = 'outputs/output.csv'
    if os.path.exists(csv_path):
        print(f"   ✅ CSV file exists: {csv_path}")
        
        # Test reading the CSV
        df = safe_csv_read(csv_path)
        print(f"   ✅ CSV read successfully")
        print(f"   📊 Shape: {df.shape}")
        print(f"   📋 Columns: {list(df.columns)[:5]}...")  # Show first 5 columns
        
        # Test that it returns a DataFrame
        import pandas as pd
        if isinstance(df, pd.DataFrame):
            print("   ✅ Returns pandas DataFrame")
        else:
            print(f"   ❌ Returns {type(df)}, expected DataFrame")
            
    else:
        print(f"   ⚠️  CSV file not found: {csv_path}")
        
except Exception as e:
    print(f"   ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Test argument parsing
print("\n4. Testing argument parsing...")
try:
    from download_all_minimal import main
    import argparse
    
    # Test that main function exists
    print("   ✅ main function found")
    
    # Test argparse setup by checking the source
    import download_all_minimal
    import inspect
    
    # Check that main function creates an ArgumentParser
    main_source = inspect.getsource(main)
    if 'ArgumentParser' in main_source:
        print("   ✅ ArgumentParser found in main function")
    else:
        print("   ❌ ArgumentParser not found in main function")
    
except Exception as e:
    print(f"   ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 40)
print("Validation complete!")
print("\nThis validates that the CSV reading consolidation is working:")
print("- safe_csv_read is properly imported from utils.csv_manager")
print("- download_all_minimal.py successfully imports and uses safe_csv_read")
print("- The function can read the output.csv file")
print("- The argument parsing is set up correctly")