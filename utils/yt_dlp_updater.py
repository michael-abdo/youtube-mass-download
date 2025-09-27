#!/usr/bin/env python3
"""
Utility to ensure yt-dlp is always up to date before running downloads
"""

import subprocess
import sys
import logging
from packaging import version
import importlib.util

logger = logging.getLogger(__name__)

def get_current_yt_dlp_version():
    """Get currently installed yt-dlp version"""
    try:
        import yt_dlp
        return yt_dlp.version.__version__
    except ImportError:
        return None
    except Exception:
        return None

def get_latest_yt_dlp_version():
    """Get latest yt-dlp version from PyPI"""
    try:
        import requests
        response = requests.get("https://pypi.org/pypi/yt-dlp/json", timeout=10)
        response.raise_for_status()
        data = response.json()
        return data["info"]["version"]
    except Exception as e:
        logger.warning(f"Could not fetch latest yt-dlp version: {e}")
        return None

def update_yt_dlp(force=False):
    """
    Update yt-dlp to the latest version
    
    Args:
        force (bool): Force update even if already latest version
        
    Returns:
        bool: True if update was successful or not needed, False if failed
    """
    try:
        current_version = get_current_yt_dlp_version()
        
        if not force and current_version:
            latest_version = get_latest_yt_dlp_version()
            
            if latest_version and current_version == latest_version:
                logger.info(f"yt-dlp is already up to date (v{current_version})")
                return True
            
            if latest_version:
                logger.info(f"Updating yt-dlp from v{current_version} to v{latest_version}")
            else:
                logger.info(f"Updating yt-dlp from v{current_version} (couldn't check latest)")
        else:
            logger.info("Installing/updating yt-dlp...")
        
        # Use pip to install/upgrade yt-dlp
        cmd = [sys.executable, "-m", "pip", "install", "--upgrade", "yt-dlp", "--user"]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            new_version = get_current_yt_dlp_version()
            if new_version:
                logger.info(f"✅ yt-dlp successfully updated to v{new_version}")
            else:
                logger.info("✅ yt-dlp update completed")
            return True
        else:
            logger.error(f"❌ Failed to update yt-dlp: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("❌ yt-dlp update timed out")
        return False
    except Exception as e:
        logger.error(f"❌ Error updating yt-dlp: {e}")
        return False

def ensure_yt_dlp_updated(force=False, fail_on_error=False):
    """
    Ensure yt-dlp is updated before proceeding
    
    Args:
        force (bool): Force update even if already latest
        fail_on_error (bool): Raise exception if update fails
        
    Returns:
        bool: True if yt-dlp is ready to use
    """
    logger.info("🔄 Checking yt-dlp version...")
    
    success = update_yt_dlp(force=force)
    
    if not success:
        error_msg = "Failed to update yt-dlp"
        if fail_on_error:
            raise RuntimeError(error_msg)
        else:
            logger.warning(f"⚠️ {error_msg}, continuing with existing version")
    
    # Verify yt-dlp is importable
    try:
        import yt_dlp
        current_version = get_current_yt_dlp_version()
        if current_version:
            logger.info(f"✅ yt-dlp ready (v{current_version})")
        else:
            logger.info("✅ yt-dlp ready")
        return True
    except ImportError:
        error_msg = "yt-dlp is not available after update attempt"
        if fail_on_error:
            raise RuntimeError(error_msg)
        else:
            logger.error(f"❌ {error_msg}")
            return False

def check_yt_dlp_binary():
    """Check if yt-dlp binary is available and working"""
    try:
        # Try to find yt-dlp binary
        possible_paths = [
            "/home/Mike/.local/bin/yt-dlp",
            "yt-dlp"
        ]
        
        yt_dlp_path = None
        for path in possible_paths:
            try:
                result = subprocess.run([path, "--version"], 
                                      capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    yt_dlp_path = path
                    logger.info(f"✅ yt-dlp binary found at: {path} (v{result.stdout.strip()})")
                    break
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue
        
        if not yt_dlp_path:
            logger.warning("⚠️ yt-dlp binary not found in expected locations")
            return None
            
        return yt_dlp_path
        
    except Exception as e:
        logger.error(f"❌ Error checking yt-dlp binary: {e}")
        return None

def get_yt_dlp_command(extra_args=None):
    """
    Get the yt-dlp command to use, ensuring it's updated first
    
    Args:
        extra_args (list): Additional arguments to include
        
    Returns:
        list: Command array ready for subprocess
    """
    # Ensure yt-dlp is updated
    ensure_yt_dlp_updated()
    
    # Try to find the binary
    yt_dlp_path = check_yt_dlp_binary()
    
    if not yt_dlp_path:
        # Fallback to python module
        cmd = [sys.executable, "-m", "yt_dlp"]
    else:
        cmd = [yt_dlp_path]
    
    if extra_args:
        cmd.extend(extra_args)
    
    return cmd

if __name__ == "__main__":
    # Test the updater
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("Testing yt-dlp updater...")
    ensure_yt_dlp_updated(force=True)
    
    binary_path = check_yt_dlp_binary()
    if binary_path:
        print(f"Binary available at: {binary_path}")
    
    cmd = get_yt_dlp_command(["--version"])
    print(f"Command to use: {' '.join(cmd)}")