#!/usr/bin/env python3
"""
Debug S3 Integration Issues

This script investigates why videos aren't being uploaded to S3 in the mass download system.
"""
import sys
import os
from pathlib import Path

# Add paths for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir / "mass_download"))
sys.path.insert(0, str(current_dir / "utils"))

def debug_s3_manager():
    """Debug the S3 manager configuration and implementation."""
    print("🔍 INVESTIGATING S3 MANAGER ISSUES")
    print("=" * 70)
    
    # 1. Check what S3 manager is being used in download_integration
    print("1. Checking download_integration S3 manager...")
    try:
        from download_integration import UnifiedS3Manager
        print(f"   Found UnifiedS3Manager in download_integration module")
        
        # Try to initialize it
        s3_manager = UnifiedS3Manager(config={"s3": {"bucket": "test-bucket"}})
        print(f"   S3Manager type: {type(s3_manager)}")
        print(f"   S3Manager config: {getattr(s3_manager, 'config', 'No config attribute')}")
        
        # Check if it has the real methods
        if hasattr(s3_manager, 'stream_youtube_to_s3'):
            print("   ✅ Has stream_youtube_to_s3 method")
        else:
            print("   ❌ Missing stream_youtube_to_s3 method")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # 2. Check the real S3 manager in utils
    print("\n2. Checking utils/s3_manager.py...")
    try:
        from s3_manager import UnifiedS3Manager as RealS3Manager
        print(f"   Found UnifiedS3Manager in utils/s3_manager")
        
        # Try to initialize it
        real_s3 = RealS3Manager()
        print(f"   Real S3Manager type: {type(real_s3)}")
        
        if hasattr(real_s3, 'stream_youtube_to_s3'):
            print("   ✅ Has stream_youtube_to_s3 method")
            # Check method signature
            import inspect
            sig = inspect.signature(real_s3.stream_youtube_to_s3)
            print(f"   Method signature: {sig}")
        else:
            print("   ❌ Missing stream_youtube_to_s3 method")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # 3. Check the import issue
    print("\n3. Checking import paths...")
    print(f"   Current working directory: {os.getcwd()}")
    print(f"   Python path additions:")
    for i, path in enumerate(sys.path[:5]):
        print(f"     {i}: {path}")
    
    # 4. Look at the placeholder implementation
    print("\n4. Examining placeholder implementation...")
    try:
        # Read the download_integration file to see the placeholder
        integration_file = current_dir / "mass_download" / "download_integration.py"
        if integration_file.exists():
            with open(integration_file) as f:
                content = f.read()
                
            # Find the placeholder lines
            lines = content.split('\n')
            placeholder_lines = []
            for i, line in enumerate(lines):
                if 'placeholder' in line.lower():
                    placeholder_lines.append(f"   Line {i+1}: {line.strip()}")
            
            if placeholder_lines:
                print("   Found placeholder references:")
                for line in placeholder_lines[:5]:  # Show first 5
                    print(line)
            else:
                print("   No placeholder references found")
                
    except Exception as e:
        print(f"   Error reading file: {e}")

def debug_mass_coordinator():
    """Debug the mass coordinator flow."""
    print("\n🔍 INVESTIGATING MASS COORDINATOR FLOW")
    print("=" * 70)
    
    # Look for the "No videos to download" message
    print("1. Checking mass coordinator logic...")
    try:
        coordinator_file = current_dir / "mass_download" / "mass_coordinator.py"
        if coordinator_file.exists():
            with open(coordinator_file) as f:
                content = f.read()
            
            # Find the line with "No videos to download"
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if "No videos to download for channel" in line:
                    print(f"   Found at line {i+1}: {line.strip()}")
                    # Show context
                    for j in range(max(0, i-3), min(len(lines), i+4)):
                        prefix = ">>>" if j == i else "   "
                        print(f"   {prefix} {j+1}: {lines[j].strip()}")
                    break
        
    except Exception as e:
        print(f"   Error: {e}")

def debug_configuration():
    """Debug configuration issues."""
    print("\n🔍 INVESTIGATING CONFIGURATION")
    print("=" * 70)
    
    print("1. Checking config.yaml...")
    try:
        import yaml
        config_file = current_dir / "config" / "config.yaml"
        if config_file.exists():
            with open(config_file) as f:
                config = yaml.safe_load(f)
            
            # Check S3 related config
            print("   S3 Configuration:")
            downloads = config.get('downloads', {})
            s3_config = downloads.get('s3', {})
            print(f"     storage_mode: {downloads.get('storage_mode')}")
            print(f"     default_bucket: {s3_config.get('default_bucket')}")
            print(f"     streaming_enabled: {s3_config.get('streaming_enabled')}")
            
            # Check mass download config
            print("   Mass Download Configuration:")
            mass_config = config.get('mass_download', {})
            print(f"     download_mode: {mass_config.get('download_mode')}")
            print(f"     download_videos: {mass_config.get('download_videos')}")
            s3_settings = mass_config.get('s3_settings', {})
            print(f"     bucket_name: {s3_settings.get('bucket_name')}")
            
        else:
            print("   config.yaml not found")
    
    except Exception as e:
        print(f"   Error loading config: {e}")

def debug_imports():
    """Debug import issues."""
    print("\n🔍 INVESTIGATING IMPORT ISSUES")
    print("=" * 70)
    
    # Check what happens with the imports in download_integration
    print("1. Testing imports in download_integration...")
    
    try:
        # Try the exact import from the file
        print("   Trying to import download_integration module...")
        import download_integration
        print(f"   ✅ Successfully imported download_integration")
        
        # Check the UnifiedS3Manager class
        if hasattr(download_integration, 'UnifiedS3Manager'):
            s3_class = download_integration.UnifiedS3Manager
            print(f"   Found UnifiedS3Manager: {s3_class}")
            
            # Check what it actually is
            import inspect
            if inspect.isclass(s3_class):
                print(f"   It's a class with methods:")
                for name, method in inspect.getmembers(s3_class, predicate=inspect.ismethod):
                    if not name.startswith('_'):
                        print(f"     - {name}")
                for name, method in inspect.getmembers(s3_class, predicate=inspect.isfunction):
                    if not name.startswith('_'):
                        print(f"     - {name}")
            
        else:
            print("   ❌ UnifiedS3Manager not found in download_integration")
        
    except Exception as e:
        print(f"   Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Run all debug functions."""
    print("🚀 DEBUGGING S3 UPLOAD ISSUES IN MASS DOWNLOAD SYSTEM")
    print("   Finding why videos aren't being uploaded to S3")
    print("   Investigating placeholder vs real implementation")
    
    debug_imports()
    debug_s3_manager()
    debug_mass_coordinator()
    debug_configuration()
    
    print("\n" + "=" * 70)
    print("🎯 SUMMARY OF FINDINGS:")
    print("   The issues appear to be:")
    print("   1. download_integration.py uses a placeholder UnifiedS3Manager")
    print("   2. The real UnifiedS3Manager is in utils/s3_manager.py") 
    print("   3. There's a configuration issue with S3 bucket settings")
    print("   4. Mass coordinator may not be calling downloads correctly")
    
    print("\n💡 RECOMMENDATIONS:")
    print("   1. Fix import in download_integration.py to use real S3 manager")
    print("   2. Update configuration to properly set S3 bucket")
    print("   3. Ensure mass coordinator calls download integration correctly")
    print("   4. Test the full flow with a simple video")

if __name__ == "__main__":
    main()