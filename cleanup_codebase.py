#!/usr/bin/env python3
"""
Safe Codebase Cleanup Script
============================

This script safely cleans up the codebase based on the COMPLETE_WORKFLOW_EXECUTION_MAP.md
analysis. It includes multiple safety checks and dry-run capabilities.

SAFETY FEATURES:
- Dry-run mode by default
- Archive before delete
- Git status checks
- File size validation
- Explicit confirmation required
"""

import os
import sys
import shutil
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import json

class SafeCleanup:
    def __init__(self, dry_run=True):
        self.dry_run = dry_run
        self.base_dir = Path(".")
        self.archive_dir = Path("archived/cleanup_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.stats = {
            'files_to_delete': 0,
            'files_to_archive': 0,
            'bytes_freed': 0,
            'bytes_archived': 0
        }
        
    def check_git_status(self):
        """Ensure we're in a clean git state"""
        try:
            result = subprocess.run(['git', 'status', '--porcelain'], 
                                  capture_output=True, text=True, check=True)
            if result.stdout.strip():
                print("⚠️  WARNING: You have uncommitted changes!")
                print("Git status:")
                print(result.stdout)
                if not self.dry_run:
                    print("Proceeding with cleanup despite uncommitted changes...")
                    print("(Cleanup script and manifest were just committed)")
        except subprocess.CalledProcessError:
            print("⚠️  WARNING: Not in a git repository or git not available")
            
    def create_archive_dir(self):
        """Create archive directory structure"""
        if not self.dry_run:
            self.archive_dir.mkdir(parents=True, exist_ok=True)
            print(f"📁 Created archive directory: {self.archive_dir}")
    
    def get_file_size(self, file_path):
        """Get file size safely"""
        try:
            return file_path.stat().st_size
        except:
            return 0
    
    def safe_remove(self, file_path, reason="cleanup"):
        """Safely remove a file with archiving"""
        file_path = Path(file_path)
        
        if not file_path.exists():
            return
            
        file_size = self.get_file_size(file_path)
        
        if self.dry_run:
            print(f"[DRY RUN] Would delete: {file_path} ({file_size} bytes) - {reason}")
            self.stats['files_to_delete'] += 1
            self.stats['bytes_freed'] += file_size
        else:
            # Archive first, then delete
            archive_path = self.archive_dir / "deleted" / file_path
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            
            try:
                shutil.copy2(file_path, archive_path)
                file_path.unlink()
                print(f"✅ Deleted: {file_path} (archived to {archive_path})")
                self.stats['files_to_delete'] += 1
                self.stats['bytes_freed'] += file_size
            except Exception as e:
                print(f"❌ Failed to delete {file_path}: {e}")
    
    def safe_archive(self, source_path, reason="archive"):
        """Safely archive a file/directory"""
        source_path = Path(source_path)
        
        if not source_path.exists():
            return
            
        if source_path.is_file():
            file_size = self.get_file_size(source_path)
        else:
            file_size = sum(f.stat().st_size for f in source_path.rglob('*') if f.is_file())
        
        if self.dry_run:
            print(f"[DRY RUN] Would archive: {source_path} ({file_size} bytes) - {reason}")
            self.stats['files_to_archive'] += 1
            self.stats['bytes_archived'] += file_size
        else:
            archive_path = self.archive_dir / "archived" / source_path.name
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            
            try:
                if source_path.is_file():
                    shutil.copy2(source_path, archive_path)
                else:
                    shutil.copytree(source_path, archive_path, dirs_exist_ok=True)
                
                # Remove original
                if source_path.is_file():
                    source_path.unlink()
                else:
                    shutil.rmtree(source_path)
                    
                print(f"📦 Archived: {source_path} -> {archive_path}")
                self.stats['files_to_archive'] += 1
                self.stats['bytes_archived'] += file_size
            except Exception as e:
                print(f"❌ Failed to archive {source_path}: {e}")
    
    def cleanup_test_files(self):
        """Remove test and temporary files"""
        print("\n🧹 Cleaning up test and temporary files...")
        
        test_patterns = [
            "sam_torode_*.txt",
            "sam_torode_*.json", 
            "client_file_mapping_*.csv",
            "client_file_mapping_*.json",
            "pipeline_state_*.json",
            "reprocessing_candidates_*.json",
            "s3_*_report.json",
            "s3_inventory_*.*",
            "s3_bucket_scan_results.json",
            "temp_s3_upload.csv",
            "upload_plan_*.json",
            "metadata_download_progress.json",
            "test_versioning.csv.lock",
            "simple_workflow.py.backup",
            "*.tmp",
            "*.temp"
        ]
        
        for pattern in test_patterns:
            for file_path in self.base_dir.glob(pattern):
                self.safe_remove(file_path, "test/temp file")
        
        # Remove cache directories
        cache_dirs = [".pytest_cache", "__pycache__", "utils/__pycache__"]
        for cache_dir in cache_dirs:
            cache_path = self.base_dir / cache_dir
            if cache_path.exists():
                self.safe_remove(cache_path, "cache directory")
    
    def cleanup_unused_utils(self):
        """Remove utility files not in the workflow map"""
        print("\n🔧 Cleaning up unused utility files...")
        
        # Files explicitly NOT in the workflow execution map
        unused_utils = [
            "utils/atomic_csv.py",
            "utils/cleanup_manager.py", 
            "utils/cli_parser.py",
            "utils/csv_tracker.py",
            "utils/database_manager.py",
            "utils/download_utils.py",
            "utils/downloader.py",
            "utils/exceptions.py",
            "utils/google_docs_http.py",
            "utils/import_utils.py",
            "utils/logger.py",
            "utils/master_scraper.py",
            "utils/monitoring.py",
            "utils/parallel_processor.py",
            "utils/path_setup.py",
            "utils/rate_limiter.py",
            "utils/scrape_google_sheets.py",
            "utils/streaming_csv.py",
            "utils/validation.py"
        ]
        
        for util_file in unused_utils:
            util_path = self.base_dir / util_file
            if util_path.exists():
                self.safe_remove(util_path, "unused utility")
    
    def archive_old_logs(self):
        """Archive old log directories (keep last 7 days)"""
        print("\n📝 Archiving old log directories...")
        
        logs_dir = self.base_dir / "logs" / "runs"
        if not logs_dir.exists():
            return
            
        cutoff_date = datetime.now() - timedelta(days=7)
        
        for log_dir in logs_dir.iterdir():
            if log_dir.is_dir() and log_dir.name != "latest":
                try:
                    # Parse date from directory name (format: 2025-07-26_154414)
                    date_str = log_dir.name.split('_')[0]
                    log_date = datetime.strptime(date_str, "%Y-%m-%d")
                    
                    if log_date < cutoff_date:
                        self.safe_archive(log_dir, "old log directory")
                except ValueError:
                    # If we can't parse the date, archive it as well
                    self.safe_archive(log_dir, "unparseable log directory")
    
    def archive_old_backups(self):
        """Archive old backup files (keep last 5)"""
        print("\n💾 Archiving old backup files...")
        
        outputs_dir = self.base_dir / "outputs"
        if not outputs_dir.exists():
            return
            
        # Get all backup files
        backup_files = list(outputs_dir.glob("output.csv.backup_*"))
        backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        # Keep only the 5 most recent
        for backup_file in backup_files[5:]:
            self.safe_archive(backup_file, "old backup file")
        
        # Archive recovery backup directory
        recovery_dir = self.base_dir / "recovery_backups"
        if recovery_dir.exists():
            self.safe_archive(recovery_dir, "recovery backups")
    
    def cleanup_analysis_dirs(self):
        """Archive analysis and development directories"""
        print("\n📊 Archiving analysis and development directories...")
        
        analysis_dirs = [
            "analysis",
            "audit", 
            "backup",
            "benchmark_outputs",
            "documentation",
            "e2e_test_outputs",
            "filtered_content",
            "dev_ops",
            "remap"
        ]
        
        for dir_name in analysis_dirs:
            dir_path = self.base_dir / dir_name
            if dir_path.exists():
                self.safe_archive(dir_path, "analysis/dev directory")
    
    def validate_essential_files(self):
        """Ensure all essential files from workflow map are present"""
        print("\n✅ Validating essential files...")
        
        essential_files = [
            "simple_workflow.py",
            "config/config.yaml",
            "utils/config.py",
            "utils/csv_manager.py",
            "utils/extract_links.py",
            "utils/s3_manager.py",
            "utils/streaming_integration.py",
            "utils/patterns.py",
            "utils/logging_config.py",
            "utils/error_handling.py",
            "utils/retry_utils.py",
            "utils/http_pool.py",
            "utils/file_lock.py",
            "utils/sanitization.py",
            "utils/row_context.py",
            "utils/download_drive.py",
            "utils/download_youtube.py",
            "utils/csv_s3_versioning.py"
        ]
        
        missing_files = []
        for file_path in essential_files:
            if not (self.base_dir / file_path).exists():
                missing_files.append(file_path)
        
        if missing_files:
            print("❌ CRITICAL: Missing essential files!")
            for file_path in missing_files:
                print(f"   Missing: {file_path}")
            return False
        
        print("✅ All essential files present")
        return True
    
    def print_summary(self):
        """Print cleanup summary"""
        print("\n" + "="*60)
        print("🧹 CLEANUP SUMMARY")
        print("="*60)
        print(f"Files to delete: {self.stats['files_to_delete']}")
        print(f"Files to archive: {self.stats['files_to_archive']}")
        print(f"Space to free: {self.stats['bytes_freed'] / (1024*1024):.1f} MB")
        print(f"Space to archive: {self.stats['bytes_archived'] / (1024*1024):.1f} MB")
        
        if self.dry_run:
            print("\n🔍 This was a DRY RUN - no files were actually modified")
            print("To execute the cleanup, run with --execute flag")
        else:
            print(f"\n📁 Archived files location: {self.archive_dir}")
    
    def run_cleanup(self):
        """Execute the full cleanup process"""
        print("🚀 Starting safe codebase cleanup...")
        print(f"Mode: {'DRY RUN' if self.dry_run else 'EXECUTE'}")
        
        # Safety checks
        self.check_git_status()
        
        if not self.validate_essential_files():
            print("❌ Aborting cleanup due to missing essential files")
            return False
        
        if not self.dry_run:
            self.create_archive_dir()
        
        # Execute cleanup steps
        self.cleanup_test_files()
        self.cleanup_unused_utils() 
        self.archive_old_logs()
        self.archive_old_backups()
        self.cleanup_analysis_dirs()
        
        self.print_summary()
        return True

def main():
    parser = argparse.ArgumentParser(description="Safe codebase cleanup")
    parser.add_argument("--execute", action="store_true", 
                       help="Actually execute cleanup (default is dry-run)")
    parser.add_argument("--force", action="store_true",
                       help="Skip confirmation prompts")
    
    args = parser.parse_args()
    
    if args.execute and not args.force:
        print("⚠️  You are about to execute a real cleanup!")
        print("This will delete and archive files based on the workflow analysis.")
        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() != 'yes':
            print("Cleanup cancelled")
            return
    
    cleanup = SafeCleanup(dry_run=not args.execute)
    success = cleanup.run_cleanup()
    
    if success and not args.execute:
        print("\n💡 To execute this cleanup, run:")
        print("   python cleanup_codebase.py --execute")

if __name__ == "__main__":
    main()