#!/usr/bin/env python3
"""
Health Check Script for Typing Clients Ingestion Pipeline
Validates system readiness and configuration before deployment.
"""

import sys
import os
import subprocess
import importlib
from pathlib import Path
from typing import List, Tuple

def check_python_version() -> Tuple[bool, str]:
    """Check Python version compatibility."""
    if sys.version_info >= (3, 8):
        return True, f"✅ Python {sys.version.split()[0]} (compatible)"
    else:
        return False, f"❌ Python {sys.version.split()[0]} (requires 3.8+)"

def check_dependencies() -> Tuple[bool, str]:
    """Check if all required dependencies are installed."""
    required_packages = [
        'pandas', 'numpy', 'requests', 'bs4', 'selenium', 
        'boto3', 'psycopg2', 'yaml'
    ]
    
    missing = []
    for package in required_packages:
        try:
            importlib.import_module(package)
        except ImportError:
            missing.append(package)
    
    if not missing:
        return True, f"✅ All {len(required_packages)} dependencies installed"
    else:
        return False, f"❌ Missing dependencies: {', '.join(missing)}"

def check_configuration() -> Tuple[bool, str]:
    """Check if configuration files exist and are valid."""
    config_files = [
        ('.env', 'Environment variables'),
        ('config/config.yaml', 'Main configuration'),
        ('.gitignore', 'Git ignore rules')
    ]
    
    issues = []
    for file_path, description in config_files:
        if not Path(file_path).exists():
            issues.append(f"{description} ({file_path})")
    
    if not issues:
        return True, "✅ All configuration files present"
    else:
        return False, f"❌ Missing config files: {', '.join(issues)}"

def check_core_imports() -> Tuple[bool, str]:
    """Test critical application imports."""
    try:
        sys.path.insert(0, '.')
        from utils.config import get_config
        from utils.csv_manager import CSVManager
        from utils.validation import validate_url
        from simple_workflow import main
        return True, "✅ All core imports successful"
    except Exception as e:
        return False, f"❌ Import error: {str(e)}"

def check_directories() -> Tuple[bool, str]:
    """Check if required directories exist."""
    required_dirs = ['outputs', 'cache', 'logs', 'config', 'utils']
    missing = [d for d in required_dirs if not Path(d).exists()]
    
    if not missing:
        return True, f"✅ All {len(required_dirs)} required directories exist"
    else:
        return False, f"❌ Missing directories: {', '.join(missing)}"

def check_external_tools() -> Tuple[bool, str]:
    """Check availability of external tools."""
    tools_status = []
    
    # Check ChromeDriver
    try:
        result = subprocess.run(['chromedriver', '--version'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            tools_status.append("✅ ChromeDriver available")
        else:
            tools_status.append("⚠️ ChromeDriver not responding")
    except (FileNotFoundError, subprocess.TimeoutExpired):
        tools_status.append("⚠️ ChromeDriver not found")
    
    # Check Chrome/Chromium
    chrome_found = False
    for chrome_cmd in ['google-chrome', 'chromium', 'chromium-browser']:
        try:
            subprocess.run([chrome_cmd, '--version'], 
                         capture_output=True, timeout=5)
            tools_status.append(f"✅ {chrome_cmd} available")
            chrome_found = True
            break
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
    
    if not chrome_found:
        tools_status.append("⚠️ Chrome/Chromium not found")
    
    return True, " | ".join(tools_status)

def check_environment_variables() -> Tuple[bool, str]:
    """Check critical environment variables."""
    # Optional but recommended variables
    env_vars = {
        'DB_PASSWORD': 'Database password',
        'AWS_ACCESS_KEY_ID': 'AWS access key (optional)',
        'S3_BUCKET': 'S3 bucket name (optional)'
    }
    
    set_vars = []
    missing_vars = []
    
    for var, description in env_vars.items():
        if os.environ.get(var):
            set_vars.append(var)
        else:
            missing_vars.append(f"{var} ({description})")
    
    if set_vars:
        status = f"✅ {len(set_vars)} env vars set"
        if missing_vars:
            status += f" | ⚠️ Optional vars not set: {len(missing_vars)}"
        return True, status
    else:
        return False, f"⚠️ No environment variables set (check .env file)"

def run_health_check() -> int:
    """Run complete health check and return exit code."""
    print("🏥 HEALTH CHECK - Typing Clients Ingestion Pipeline")
    print("=" * 60)
    
    checks = [
        ("Python Version", check_python_version),
        ("Dependencies", check_dependencies),
        ("Configuration", check_configuration),
        ("Core Imports", check_core_imports),
        ("Directories", check_directories),
        ("External Tools", check_external_tools),
        ("Environment Variables", check_environment_variables)
    ]
    
    all_passed = True
    
    for check_name, check_func in checks:
        try:
            passed, message = check_func()
            print(f"{check_name:20} {message}")
            if not passed:
                all_passed = False
        except Exception as e:
            print(f"{check_name:20} ❌ Check failed: {str(e)}")
            all_passed = False
    
    print("=" * 60)
    
    if all_passed:
        print("🎉 HEALTH CHECK PASSED - System ready for deployment!")
        return 0
    else:
        print("⚠️ HEALTH CHECK ISSUES - Review errors above before deployment")
        return 1

if __name__ == "__main__":
    exit_code = run_health_check()
    sys.exit(exit_code)