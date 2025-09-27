"""File locking utilities to prevent race conditions in concurrent operations."""
import os
import time
import fcntl
import contextlib
import tempfile
from pathlib import Path
from typing import Optional, Union, IO
import logging

try:
    from config import get_config, get_timeout
except ImportError:
    from .config import get_config, get_timeout

# Get configuration
config = get_config()


class FileLockError(Exception):
    """Exception raised when file locking fails."""
    pass


class FileLock:
    """
    A file-based lock using fcntl for Unix-like systems.
    
    This provides both exclusive and shared locks for preventing race conditions
    when multiple processes access the same file.
    """
    
    def __init__(self, 
                 lock_file: Union[str, Path],
                 timeout: float = None,
                 check_interval: float = None,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize a file lock.
        
        Args:
            lock_file: Path to the lock file (will be created if doesn't exist)
            timeout: Maximum time to wait for lock acquisition (seconds)
            check_interval: Time between lock acquisition attempts (seconds)
            logger: Optional logger for debugging
        """
        self.lock_file = Path(lock_file)
        self.timeout = timeout if timeout is not None else get_timeout('file_lock')
        self.check_interval = check_interval if check_interval is not None else config.get('file_locking.check_interval', 0.1)
        self.logger = logger or logging.getLogger(__name__)
        self._lock_fd: Optional[IO] = None
        
        # Ensure lock directory exists
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
    
    def acquire(self, exclusive: bool = True) -> None:
        """
        Acquire the file lock.
        
        Args:
            exclusive: If True, acquire exclusive lock. If False, acquire shared lock.
        
        Raises:
            FileLockError: If lock cannot be acquired within timeout
        """
        start_time = time.time()
        
        # Open or create the lock file
        self._lock_fd = open(self.lock_file, 'a+')
        
        # Determine lock type
        lock_type = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        
        while True:
            try:
                # Try to acquire lock (non-blocking)
                fcntl.flock(self._lock_fd.fileno(), lock_type | fcntl.LOCK_NB)
                self.logger.debug(f"Acquired {'exclusive' if exclusive else 'shared'} lock on {self.lock_file}")
                return
            except IOError:
                # Lock is held by another process
                if time.time() - start_time > self.timeout:
                    self._cleanup()
                    raise FileLockError(
                        f"Could not acquire lock on {self.lock_file} within {self.timeout} seconds"
                    )
                
                time.sleep(self.check_interval)
    
    def release(self) -> None:
        """Release the file lock."""
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_UN)
                self.logger.debug(f"Released lock on {self.lock_file}")
            finally:
                self._cleanup()
    
    def _cleanup(self) -> None:
        """Clean up resources."""
        if self._lock_fd is not None:
            try:
                self._lock_fd.close()
            except Exception:
                pass
            self._lock_fd = None
    
    def __enter__(self):
        """Context manager entry."""
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release()


@contextlib.contextmanager
def file_lock(file_path: Union[str, Path], 
              exclusive: bool = True,
              timeout: float = None,
              logger: Optional[logging.Logger] = None):
    """
    Context manager for file locking.
    
    Args:
        file_path: Path to the file to lock (lock file will be file_path + '.lock')
        exclusive: If True, acquire exclusive lock. If False, acquire shared lock.
        timeout: Maximum time to wait for lock acquisition
        logger: Optional logger
    
    Yields:
        The file path (for convenience)
    
    Example:
        with file_lock('/path/to/data.csv') as locked_file:
            # File is locked, safe to read/write
            with open(locked_file, 'r') as f:
                data = f.read()
    """
    lock_file = Path(f"{file_path}.lock")
    lock = FileLock(lock_file, timeout=timeout, logger=logger)
    
    try:
        lock.acquire(exclusive=exclusive)
        yield Path(file_path)
    finally:
        lock.release()


@contextlib.contextmanager
def atomic_write_with_lock(file_path: Union[str, Path],
                          mode: str = 'w',
                          encoding: str = 'utf-8',
                          timeout: float = 30.0,
                          logger: Optional[logging.Logger] = None):
    """
    Atomic file write with file locking.
    
    Combines atomic writes (using temp file + rename) with file locking
    to ensure both atomicity and prevention of race conditions.
    
    Args:
        file_path: Path to the file to write
        mode: File open mode (must be a write mode)
        encoding: File encoding
        timeout: Maximum time to wait for lock acquisition
        logger: Optional logger
    
    Yields:
        File handle to write to
    
    Example:
        with atomic_write_with_lock('/path/to/data.csv') as f:
            writer = csv.writer(f)
            writer.writerows(data)
    """
    if 'r' in mode or '+' not in mode and 'w' not in mode and 'a' not in mode:
        raise ValueError(f"Mode must be a write mode, got: {mode}")
    
    file_path = Path(file_path)
    
    # Create a temporary file in the same directory
    temp_fd, temp_path = tempfile.mkstemp(
        dir=file_path.parent,
        prefix=f".{file_path.name}.",
        suffix='.tmp'
    )
    
    try:
        # Acquire lock on the target file
        with file_lock(file_path, exclusive=True, timeout=timeout, logger=logger):
            # Write to temporary file
            with os.fdopen(temp_fd, mode, encoding=encoding) as temp_file:
                yield temp_file
            
            # Atomic rename
            os.replace(temp_path, file_path)
            
            if logger:
                logger.debug(f"Atomically wrote to {file_path}")
    except Exception:
        # Clean up temp file on error
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise


def ensure_parent_dir(file_path: Union[str, Path]) -> Path:
    """
    Ensure parent directory exists (thread-safe).
    
    Args:
        file_path: Path to file
    
    Returns:
        The file path as a Path object
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    return file_path


def safe_file_operation(file_path: Union[str, Path],
                       operation: str,
                       exclusive: bool = True,
                       timeout: float = 30.0,
                       logger: Optional[logging.Logger] = None,
                       **kwargs):
    """
    Perform a file operation with locking.
    
    Args:
        file_path: Path to the file
        operation: Operation to perform ('read', 'write', 'append', 'exists', 'rename', 'delete')
        exclusive: Whether to use exclusive lock
        timeout: Lock timeout
        logger: Optional logger
        **kwargs: Additional arguments for the operation
    
    Returns:
        Result of the operation
    
    Example:
        # Read with shared lock
        content = safe_file_operation('/path/to/file.txt', 'read', exclusive=False)
        
        # Write with exclusive lock
        safe_file_operation('/path/to/file.txt', 'write', content="Hello")
        
        # Check existence with shared lock
        exists = safe_file_operation('/path/to/file.txt', 'exists', exclusive=False)
    """
    file_path = Path(file_path)
    
    with file_lock(file_path, exclusive=exclusive, timeout=timeout, logger=logger):
        if operation == 'read':
            if file_path.exists():
                return file_path.read_text(encoding=kwargs.get('encoding', 'utf-8'))
            return None
            
        elif operation == 'write':
            ensure_parent_dir(file_path)
            file_path.write_text(
                kwargs.get('content', ''),
                encoding=kwargs.get('encoding', 'utf-8')
            )
            
        elif operation == 'append':
            ensure_parent_dir(file_path)
            with open(file_path, 'a', encoding=kwargs.get('encoding', 'utf-8')) as f:
                f.write(kwargs.get('content', ''))
                
        elif operation == 'exists':
            return file_path.exists()
            
        elif operation == 'rename':
            new_path = Path(kwargs.get('new_path'))
            if not new_path:
                raise ValueError("new_path is required for rename operation")
            os.rename(file_path, new_path)
            return new_path
            
        elif operation == 'delete':
            if file_path.exists():
                file_path.unlink()
                return True
            return False
            
        else:
            raise ValueError(f"Unknown operation: {operation}")