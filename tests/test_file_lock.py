#!/usr/bin/env python3
"""
Unit tests for file locking module - critical for preventing race conditions.
"""

# Standardized project imports
from utils.config import setup_project_imports
setup_project_imports()
import unittest
import sys
import os
import tempfile
import threading
import time
from pathlib import Path

from utils.file_lock import FileLock, file_lock, atomic_write_with_lock

class TestFileLock(unittest.TestCase):
    """Test file locking functionality"""
    
    def setUp(self):
        """Create a temporary directory for tests"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file = Path(self.temp_dir) / "test.txt"
        self.lock_file = Path(self.temp_dir) / "test.lock"
    
    def tearDown(self):
        """Clean up temporary files"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_exclusive_lock(self):
        """Test exclusive lock prevents concurrent access"""
        results = []
        
        def writer(writer_id, delay=0.1):
            """Write to file with lock"""
            with file_lock(self.test_file, exclusive=True, timeout=5.0):
                results.append(f"start_{writer_id}")
                time.sleep(delay)
                with open(self.test_file, 'a') as f:
                    f.write(f"Writer {writer_id}\n")
                results.append(f"end_{writer_id}")
        
        # Start two threads that try to write
        t1 = threading.Thread(target=writer, args=(1, 0.2))
        t2 = threading.Thread(target=writer, args=(2, 0.1))
        
        t1.start()
        time.sleep(0.05)  # Ensure t1 gets lock first
        t2.start()
        
        t1.join()
        t2.join()
        
        # Check that operations were serialized
        self.assertEqual(results[0], "start_1")
        self.assertEqual(results[1], "end_1")
        self.assertEqual(results[2], "start_2")
        self.assertEqual(results[3], "end_2")
    
    def test_shared_lock(self):
        """Test shared locks allow concurrent reads"""
        results = []
        
        def reader(reader_id):
            """Read file with shared lock"""
            with file_lock(self.test_file, exclusive=False, timeout=2.0):
                results.append(f"start_{reader_id}")
                time.sleep(0.1)
                results.append(f"end_{reader_id}")
        
        # Create test file
        self.test_file.write_text("test content")
        
        # Start multiple readers
        threads = []
        for i in range(3):
            t = threading.Thread(target=reader, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # All readers should have started before any finished
        # (they can run concurrently)
        start_count = sum(1 for r in results[:3] if r.startswith("start_"))
        self.assertEqual(start_count, 3)
    
    def test_lock_timeout(self):
        """Test lock timeout behavior"""
        lock1 = FileLock(self.lock_file, timeout=0.5)
        lock2 = FileLock(self.lock_file, timeout=0.5)
        
        # Acquire first lock
        lock1.acquire()
        
        # Second lock should timeout
        start_time = time.time()
        try:
            lock2.acquire()
            self.fail("Should have timed out")
        except Exception:
            elapsed = time.time() - start_time
            self.assertGreater(elapsed, 0.4)
            self.assertLess(elapsed, 0.7)
        
        lock1.release()
    
    def test_atomic_write_with_lock(self):
        """Test atomic write operation with locking"""
        # Create the file first
        self.test_file.write_text("")
        
        def writer(content):
            with atomic_write_with_lock(
                self.test_file,
                mode='a',
                encoding='utf-8',
                timeout=2.0
            ) as f:
                f.write(content)
        
        # Multiple threads writing
        threads = []
        for i in range(5):
            t = threading.Thread(target=writer, args=(f"Line {i}\n",))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # All writes should be present
        content = self.test_file.read_text()
        lines = content.strip().split('\n')
        self.assertEqual(len(lines), 5)
        
        # Each line should be complete (no interleaving)
        for i in range(5):
            self.assertIn(f"Line {i}", lines)
    
    def test_lock_cleanup_on_exception(self):
        """Test locks are cleaned up on exception"""
        try:
            with file_lock(self.test_file, exclusive=True):
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # Should be able to acquire lock again immediately
        with file_lock(self.test_file, exclusive=True, timeout=0.1) as lock:
            self.assertIsNotNone(lock)

class TestFileLockIntegration(unittest.TestCase):
    """Integration tests for file locking with real file operations"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.csv_file = Path(self.temp_dir) / "test.csv"
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_concurrent_csv_writes(self):
        """Test concurrent CSV writes don't corrupt data"""
        import csv
        
        # Initialize CSV
        with open(self.csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'value'])
        
        def append_row(row_id):
            """Append a row to CSV with locking"""
            with file_lock(self.csv_file, exclusive=True, timeout=5.0):
                # Read current content
                rows = []
                with open(self.csv_file, 'r', newline='') as f:
                    reader = csv.reader(f)
                    rows = list(reader)
                
                # Append new row
                rows.append([str(row_id), f"value_{row_id}"])
                
                # Write back atomically
                temp_file = self.csv_file.with_suffix('.tmp')
                with open(temp_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)
                
                temp_file.replace(self.csv_file)
        
        # Multiple threads appending
        threads = []
        for i in range(10):
            t = threading.Thread(target=append_row, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Verify all rows present and CSV is valid
        with open(self.csv_file, 'r', newline='') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        self.assertEqual(len(rows), 11)  # Header + 10 data rows
        self.assertEqual(rows[0], ['id', 'value'])
        
        # Check all IDs present
        ids = [int(row[0]) for row in rows[1:]]
        self.assertEqual(sorted(ids), list(range(10)))

if __name__ == "__main__":
    unittest.main()