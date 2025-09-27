#!/usr/bin/env python3
"""
Comprehensive Error Recovery Mechanisms
Phase 4.12: Add comprehensive error recovery mechanisms

This module provides robust error recovery capabilities including:
1. Transaction-like rollback for database operations
2. Circuit breaker pattern for external services
3. Exponential backoff with jitter
4. Checkpoint-based recovery
5. Partial result preservation
6. Dead letter queue for failed items
7. Graceful degradation strategies

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""

import os
import sys
import time
import json
import pickle
import logging
from typing import Dict, List, Any, Optional, Callable, TypeVar, Generic, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum
import threading
from collections import deque
import random

# Add parent directory to path for imports
from pathlib import Path as PathImport
current_dir = PathImport(__file__).parent
sys.path.insert(0, str(current_dir.parent))

# Import mass download logger
from .logging_setup import get_mass_download_logger

# Configure logging
logger = get_mass_download_logger(__name__)

T = TypeVar('T')


class RecoveryStrategy(Enum):
    """Recovery strategies for different failure types."""
    RETRY_IMMEDIATE = "retry_immediate"
    RETRY_BACKOFF = "retry_backoff"
    CIRCUIT_BREAKER = "circuit_breaker"
    FALLBACK = "fallback"
    SKIP = "skip"
    ROLLBACK = "rollback"
    CHECKPOINT = "checkpoint"


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class ErrorContext:
    """Context for an error occurrence."""
    error_type: str
    error_message: str
    timestamp: datetime = field(default_factory=datetime.now)
    operation: str = ""
    retry_count: int = 0
    recovery_strategy: RecoveryStrategy = RecoveryStrategy.RETRY_BACKOFF
    additional_info: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryCheckpoint:
    """Checkpoint for recovery operations."""
    checkpoint_id: str
    operation: str
    timestamp: datetime
    state: Dict[str, Any]
    completed_items: List[str]
    pending_items: List[str]
    failed_items: List[Tuple[str, ErrorContext]]
    
    def save(self, checkpoint_dir: Path):
        """Save checkpoint to disk."""
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_file = checkpoint_dir / f"{self.checkpoint_id}.pkl"
        
        with open(checkpoint_file, 'wb') as f:
            pickle.dump(self, f)
        
        logger.info(f"Saved checkpoint: {self.checkpoint_id}")
    
    @classmethod
    def load(cls, checkpoint_file: Path) -> 'RecoveryCheckpoint':
        """Load checkpoint from disk."""
        with open(checkpoint_file, 'rb') as f:
            checkpoint = pickle.load(f)
        
        logger.info(f"Loaded checkpoint: {checkpoint.checkpoint_id}")
        return checkpoint


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.
    
    Prevents cascading failures by stopping calls to failing services.
    """
    
    def __init__(self,
                 failure_threshold: int = 5,
                 recovery_timeout: timedelta = timedelta(minutes=1),
                 success_threshold: int = 2):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time to wait before trying half-open
            success_threshold: Successes needed to close circuit
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._lock = threading.RLock()
    
    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                # Check if we should transition to half-open
                if (self._last_failure_time and 
                    datetime.now() - self._last_failure_time > self.recovery_timeout):
                    self._state = CircuitState.HALF_OPEN
                    self._failure_count = 0
                    self._success_count = 0
                    logger.info("Circuit breaker transitioned to HALF_OPEN")
            
            return self._state
    
    def call(self, func: Callable[[], T], fallback: Optional[Callable[[], T]] = None) -> T:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            fallback: Optional fallback function
            
        Returns:
            Function result or fallback result
            
        Raises:
            Exception: If circuit is open and no fallback provided
        """
        if self.state == CircuitState.OPEN:
            if fallback:
                logger.warning("Circuit is OPEN, using fallback")
                return fallback()
            else:
                raise RuntimeError("Circuit breaker is OPEN - service unavailable")
        
        try:
            result = func()
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _on_success(self):
        """Handle successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    logger.info("Circuit breaker CLOSED - service recovered")
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0
    
    def _on_failure(self):
        """Handle failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = datetime.now()
            
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                logger.warning("Circuit breaker reopened due to failure in HALF_OPEN state")
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                logger.error(f"Circuit breaker OPEN after {self._failure_count} failures")


class RetryManager:
    """
    Advanced retry management with exponential backoff and jitter.
    """
    
    def __init__(self,
                 max_retries: int = 3,
                 base_delay: float = 1.0,
                 max_delay: float = 60.0,
                 exponential_base: float = 2.0,
                 jitter: bool = True):
        """
        Initialize retry manager.
        
        Args:
            max_retries: Maximum retry attempts
            base_delay: Initial delay in seconds
            max_delay: Maximum delay in seconds
            exponential_base: Base for exponential backoff
            jitter: Add randomization to prevent thundering herd
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
    
    def get_delay(self, retry_count: int) -> float:
        """Calculate delay for given retry count."""
        delay = min(
            self.base_delay * (self.exponential_base ** retry_count),
            self.max_delay
        )
        
        if self.jitter:
            # Add jitter: 0.5 to 1.5 times the delay
            delay *= (0.5 + random.random())
        
        return delay
    
    def retry(self, 
             func: Callable[[], T],
             should_retry: Optional[Callable[[Exception], bool]] = None,
             on_retry: Optional[Callable[[Exception, int], None]] = None) -> T:
        """
        Execute function with retry logic.
        
        Args:
            func: Function to execute
            should_retry: Function to determine if retry is appropriate
            on_retry: Callback for each retry attempt
            
        Returns:
            Function result
            
        Raises:
            Last exception if all retries exhausted
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return func()
            except Exception as e:
                last_exception = e
                
                if attempt >= self.max_retries:
                    logger.error(f"All {self.max_retries} retries exhausted")
                    raise
                
                if should_retry and not should_retry(e):
                    logger.warning(f"Error not retryable: {e}")
                    raise
                
                delay = self.get_delay(attempt)
                logger.warning(f"Retry {attempt + 1}/{self.max_retries} after {delay:.1f}s delay: {e}")
                
                if on_retry:
                    on_retry(e, attempt + 1)
                
                time.sleep(delay)
        
        raise last_exception


class TransactionManager:
    """
    Manage transaction-like operations with rollback capability.
    """
    
    def __init__(self):
        self._operations: List[Tuple[str, Callable, Callable]] = []
        self._completed: List[str] = []
        self._lock = threading.RLock()
    
    def add_operation(self, 
                     name: str,
                     operation: Callable[[], Any],
                     rollback: Callable[[], None]):
        """
        Add an operation with its rollback function.
        
        Args:
            name: Operation name
            operation: Function to execute
            rollback: Function to rollback the operation
        """
        with self._lock:
            self._operations.append((name, operation, rollback))
    
    def execute(self) -> List[Any]:
        """
        Execute all operations with automatic rollback on failure.
        
        Returns:
            List of operation results
            
        Raises:
            Exception: If any operation fails (after rollback)
        """
        results = []
        
        try:
            for name, operation, _ in self._operations:
                logger.info(f"Executing operation: {name}")
                result = operation()
                results.append(result)
                
                with self._lock:
                    self._completed.append(name)
                
            logger.info(f"All {len(self._operations)} operations completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Operation failed, initiating rollback: {e}")
            self._rollback()
            raise
    
    def _rollback(self):
        """Rollback completed operations in reverse order."""
        with self._lock:
            # Get completed operations in reverse order
            for name in reversed(self._completed):
                # Find the rollback function
                for op_name, _, rollback in self._operations:
                    if op_name == name:
                        try:
                            logger.info(f"Rolling back operation: {name}")
                            rollback()
                        except Exception as e:
                            logger.error(f"Rollback failed for {name}: {e}")
                        break
            
            self._completed.clear()


class DeadLetterQueue(Generic[T]):
    """
    Queue for items that failed processing after all retries.
    """
    
    def __init__(self, max_size: int = 1000, persist_path: Optional[Path] = None):
        """
        Initialize dead letter queue.
        
        Args:
            max_size: Maximum queue size
            persist_path: Optional path to persist queue
        """
        self.max_size = max_size
        self.persist_path = persist_path
        self._queue: deque = deque(maxlen=max_size)
        self._lock = threading.RLock()
        
        # Load persisted items if path provided
        if persist_path and persist_path.exists():
            self._load()
    
    def add(self, item: T, error_context: ErrorContext):
        """Add failed item to queue."""
        with self._lock:
            self._queue.append({
                'item': item,
                'error_context': error_context,
                'queued_at': datetime.now()
            })
            
            if self.persist_path:
                self._save()
            
            logger.warning(f"Added item to dead letter queue: {error_context.operation}")
    
    def get_all(self) -> List[Dict[str, Any]]:
        """Get all items in queue."""
        with self._lock:
            return list(self._queue)
    
    def retry_all(self, processor: Callable[[T], Any]) -> Tuple[int, int]:
        """
        Retry all items in queue.
        
        Args:
            processor: Function to process items
            
        Returns:
            Tuple of (successful_count, failed_count)
        """
        successful = 0
        failed = 0
        
        with self._lock:
            items = list(self._queue)
            self._queue.clear()
        
        for item_data in items:
            try:
                processor(item_data['item'])
                successful += 1
            except Exception as e:
                failed += 1
                # Re-add to queue with updated error
                error_context = item_data['error_context']
                error_context.retry_count += 1
                error_context.error_message = str(e)
                self.add(item_data['item'], error_context)
        
        logger.info(f"Dead letter retry: {successful} successful, {failed} failed")
        return successful, failed
    
    def _save(self):
        """Persist queue to disk."""
        if not self.persist_path:
            return
        
        self.persist_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.persist_path, 'w') as f:
            json.dump(
                [{'item': item['item'], 
                  'error': item['error_context'].__dict__,
                  'queued_at': item['queued_at'].isoformat()}
                 for item in self._queue],
                f,
                default=str
            )
    
    def _load(self):
        """Load queue from disk."""
        if not self.persist_path or not self.persist_path.exists():
            return
        
        try:
            with open(self.persist_path, 'r') as f:
                data = json.load(f)
                
            for item_data in data:
                error_dict = item_data['error']
                error_context = ErrorContext(
                    error_type=error_dict['error_type'],
                    error_message=error_dict['error_message'],
                    operation=error_dict.get('operation', ''),
                    retry_count=error_dict.get('retry_count', 0)
                )
                
                self._queue.append({
                    'item': item_data['item'],
                    'error_context': error_context,
                    'queued_at': datetime.fromisoformat(item_data['queued_at'])
                })
                
            logger.info(f"Loaded {len(self._queue)} items from dead letter queue")
            
        except Exception as e:
            logger.error(f"Failed to load dead letter queue: {e}")


class ErrorRecoveryManager:
    """
    Central manager for error recovery strategies.
    """
    
    def __init__(self,
                 checkpoint_dir: Optional[Path] = None,
                 dead_letter_path: Optional[Path] = None):
        """
        Initialize error recovery manager.
        
        Args:
            checkpoint_dir: Directory for checkpoints
            dead_letter_path: Path for dead letter queue persistence
        """
        self.checkpoint_dir = checkpoint_dir
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.retry_manager = RetryManager()
        self.transaction_manager = TransactionManager()
        self.dead_letter_queue = DeadLetterQueue(persist_path=dead_letter_path)
        self._lock = threading.RLock()
    
    def get_circuit_breaker(self, service_name: str) -> CircuitBreaker:
        """Get or create circuit breaker for service."""
        with self._lock:
            if service_name not in self.circuit_breakers:
                self.circuit_breakers[service_name] = CircuitBreaker()
            return self.circuit_breakers[service_name]
    
    def with_recovery(self,
                     operation_name: str,
                     func: Callable[[], T],
                     recovery_strategy: RecoveryStrategy = RecoveryStrategy.RETRY_BACKOFF,
                     fallback: Optional[Callable[[], T]] = None) -> Optional[T]:
        """
        Execute function with comprehensive error recovery.
        
        Args:
            operation_name: Name of the operation
            func: Function to execute
            recovery_strategy: Recovery strategy to use
            fallback: Optional fallback function
            
        Returns:
            Function result or fallback result
        """
        try:
            if recovery_strategy == RecoveryStrategy.CIRCUIT_BREAKER:
                breaker = self.get_circuit_breaker(operation_name)
                return breaker.call(func, fallback)
            
            elif recovery_strategy == RecoveryStrategy.RETRY_BACKOFF:
                return self.retry_manager.retry(func)
            
            elif recovery_strategy == RecoveryStrategy.RETRY_IMMEDIATE:
                # Simple retry without backoff
                for attempt in range(3):
                    try:
                        return func()
                    except Exception as e:
                        if attempt == 2:
                            raise
                        logger.warning(f"Immediate retry {attempt + 1}/3: {e}")
            
            elif recovery_strategy == RecoveryStrategy.FALLBACK:
                try:
                    return func()
                except Exception as e:
                    logger.warning(f"Using fallback for {operation_name}: {e}")
                    return fallback() if fallback else None
            
            elif recovery_strategy == RecoveryStrategy.SKIP:
                try:
                    return func()
                except Exception as e:
                    logger.warning(f"Skipping failed operation {operation_name}: {e}")
                    return None
            
            else:
                return func()
                
        except Exception as e:
            # Add to dead letter queue if all recovery attempts failed
            error_context = ErrorContext(
                error_type=type(e).__name__,
                error_message=str(e),
                operation=operation_name,
                recovery_strategy=recovery_strategy
            )
            
            self.dead_letter_queue.add(
                {'operation': operation_name, 'func': func},
                error_context
            )
            
            raise
    
    def create_checkpoint(self,
                         checkpoint_id: str,
                         operation: str,
                         state: Dict[str, Any],
                         completed_items: List[str],
                         pending_items: List[str]) -> RecoveryCheckpoint:
        """Create and save a recovery checkpoint."""
        checkpoint = RecoveryCheckpoint(
            checkpoint_id=checkpoint_id,
            operation=operation,
            timestamp=datetime.now(),
            state=state,
            completed_items=completed_items,
            pending_items=pending_items,
            failed_items=[]
        )
        
        if self.checkpoint_dir:
            checkpoint.save(self.checkpoint_dir)
        
        return checkpoint
    
    def load_checkpoint(self, checkpoint_id: str) -> Optional[RecoveryCheckpoint]:
        """Load a checkpoint by ID."""
        if not self.checkpoint_dir:
            return None
        
        checkpoint_file = self.checkpoint_dir / f"{checkpoint_id}.pkl"
        
        if checkpoint_file.exists():
            return RecoveryCheckpoint.load(checkpoint_file)
        
        return None
    
    def get_recovery_status(self) -> Dict[str, Any]:
        """Get status of all recovery mechanisms."""
        return {
            'circuit_breakers': {
                name: breaker.state.value
                for name, breaker in self.circuit_breakers.items()
            },
            'dead_letter_queue_size': len(self.dead_letter_queue.get_all()),
            'checkpoints': [
                f.stem for f in (self.checkpoint_dir.glob("*.pkl") 
                                if self.checkpoint_dir and self.checkpoint_dir.exists() 
                                else [])
            ]
        }


# Example usage
if __name__ == "__main__":
    import tempfile
    
    def run_examples():
        """Run example usage of error recovery mechanisms."""
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'
        )
        
        # Create recovery manager
        with tempfile.TemporaryDirectory() as temp_dir:
            recovery_manager = ErrorRecoveryManager(
                checkpoint_dir=Path(temp_dir) / "checkpoints",
                dead_letter_path=Path(temp_dir) / "dead_letter.json"
            )
            
            # Example 1: Circuit breaker
            print("\n=== Circuit Breaker Example ===")
            
            call_count = 0
            def flaky_service():
                nonlocal call_count
                call_count += 1
                if call_count < 6:
                    raise RuntimeError("Service temporarily unavailable")
                return "Success!"
            
            breaker = recovery_manager.get_circuit_breaker("flaky_service")
            
            for i in range(10):
                try:
                    result = breaker.call(flaky_service, lambda: "Fallback result")
                    print(f"Call {i+1}: {result}")
                except Exception as e:
                    print(f"Call {i+1}: Failed - {e}")
                
                if i == 6:
                    print("Waiting for recovery timeout...")
                    time.sleep(61)  # Wait for circuit to potentially close
            
            # Example 2: Transaction with rollback
            print("\n=== Transaction Example ===")
            
            created_files = []
            
            def create_file(name):
                path = Path(temp_dir) / name
                path.write_text("test")
                created_files.append(path)
                print(f"Created: {name}")
                return path
            
            def delete_file(path):
                if path and path.exists():
                    path.unlink()
                    print(f"Rolled back: {path.name}")
            
            transaction = TransactionManager()
            transaction.add_operation("file1", 
                                    lambda: create_file("file1.txt"),
                                    lambda: delete_file(created_files[0] if created_files else None))
            transaction.add_operation("file2",
                                    lambda: create_file("file2.txt"),
                                    lambda: delete_file(created_files[1] if len(created_files) > 1 else None))
            transaction.add_operation("failing_op",
                                    lambda: 1/0,  # This will fail
                                    lambda: print("Rolling back failing operation"))
            
            try:
                transaction.execute()
            except Exception as e:
                print(f"Transaction failed: {e}")
            
            # Check if files were rolled back
            print(f"Files exist after rollback: {[f.name for f in Path(temp_dir).glob('*.txt')]}")
            
            # Example 3: Dead letter queue
            print("\n=== Dead Letter Queue Example ===")
            
            def process_item(item):
                if item < 5:
                    raise ValueError(f"Item {item} is invalid")
                return f"Processed {item}"
            
            # Process items with recovery
            items = [1, 2, 3, 6, 7, 8]
            for item in items:
                try:
                    result = recovery_manager.with_recovery(
                        f"process_item_{item}",
                        lambda i=item: process_item(i),
                        recovery_strategy=RecoveryStrategy.RETRY_IMMEDIATE
                    )
                    print(f"Success: {result}")
                except Exception as e:
                    print(f"Failed: Item {item} - {e}")
            
            # Show dead letter queue
            dead_items = recovery_manager.dead_letter_queue.get_all()
            print(f"\nDead letter queue contains {len(dead_items)} items")
            
            # Get recovery status
            print(f"\nRecovery Status: {recovery_manager.get_recovery_status()}")
    
    # Run the examples
    run_examples()