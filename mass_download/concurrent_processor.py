#!/usr/bin/env python3
"""
Concurrent Processing with Resource Management
Phase 4.10: Implement concurrent processing with limits

This module provides enhanced concurrent processing capabilities with:
1. Resource monitoring (CPU, memory)
2. Dynamic throttling based on system resources
3. Semaphore-based concurrency control
4. Download queue management
5. Comprehensive error handling

Implements fail-fast, fail-loud, fail-safely principles throughout.
"""

import os
import sys
import time
import psutil
import queue
import threading
import logging
from typing import List, Dict, Any, Optional, Callable, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from enum import Enum

# Add parent directory to path for imports
from pathlib import Path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir.parent))

# Import mass download logger
from .logging_setup import get_mass_download_logger

# Configure logging
logger = get_mass_download_logger(__name__)


class ResourceStatus(Enum):
    """System resource status levels."""
    NORMAL = "normal"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class ResourceLimits:
    """Resource limits configuration."""
    max_cpu_percent: float = 80.0
    max_memory_percent: float = 80.0
    max_concurrent_channels: int = 3
    max_concurrent_downloads: int = 3
    max_queue_size: int = 100
    check_interval_seconds: float = 5.0
    throttle_factor: float = 0.5  # Reduce concurrency by this factor when resources are high
    min_concurrent: int = 1  # Never go below this


@dataclass
class ResourceMetrics:
    """Current resource metrics."""
    cpu_percent: float
    memory_percent: float
    active_threads: int
    queue_size: int
    timestamp: datetime = field(default_factory=datetime.now)
    status: ResourceStatus = ResourceStatus.NORMAL
    
    def __post_init__(self):
        """Determine resource status based on metrics."""
        if self.cpu_percent > 90 or self.memory_percent > 90:
            self.status = ResourceStatus.CRITICAL
        elif self.cpu_percent > 75 or self.memory_percent > 75:
            self.status = ResourceStatus.WARNING
        else:
            self.status = ResourceStatus.NORMAL


class ResourceMonitor:
    """Monitor system resources and provide throttling recommendations."""
    
    def __init__(self, limits: ResourceLimits):
        """
        Initialize resource monitor.
        
        Args:
            limits: Resource limits configuration
        """
        self.limits = limits
        self.metrics_history: List[ResourceMetrics] = []
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        
        logger.info(f"ResourceMonitor initialized with limits: CPU={limits.max_cpu_percent}%, "
                   f"Memory={limits.max_memory_percent}%")
    
    def start_monitoring(self):
        """Start background resource monitoring."""
        if self.monitoring:
            logger.warning("Resource monitoring already started")
            return
        
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("Resource monitoring started")
    
    def stop_monitoring(self):
        """Stop background resource monitoring."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=10)
        logger.info("Resource monitoring stopped")
    
    def _monitor_loop(self):
        """Background monitoring loop."""
        while self.monitoring:
            try:
                metrics = self.get_current_metrics()
                
                with self._lock:
                    self.metrics_history.append(metrics)
                    # Keep only last 100 metrics
                    if len(self.metrics_history) > 100:
                        self.metrics_history = self.metrics_history[-100:]
                
                if metrics.status != ResourceStatus.NORMAL:
                    logger.warning(f"Resource status: {metrics.status.value} - "
                                 f"CPU: {metrics.cpu_percent:.1f}%, "
                                 f"Memory: {metrics.memory_percent:.1f}%")
                
                time.sleep(self.limits.check_interval_seconds)
                
            except Exception as e:
                logger.error(f"Error in resource monitoring: {e}")
                time.sleep(self.limits.check_interval_seconds)
    
    def get_current_metrics(self, queue_size: int = 0) -> ResourceMetrics:
        """Get current resource metrics."""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            active_threads = threading.active_count()
            
            return ResourceMetrics(
                cpu_percent=cpu_percent,
                memory_percent=memory.percent,
                active_threads=active_threads,
                queue_size=queue_size
            )
        except Exception as e:
            logger.error(f"Failed to get resource metrics: {e}")
            # Return conservative estimates
            return ResourceMetrics(
                cpu_percent=50.0,
                memory_percent=50.0,
                active_threads=threading.active_count(),
                queue_size=queue_size
            )
    
    def get_recommended_concurrency(self, base_concurrency: int) -> int:
        """
        Get recommended concurrency based on current resources.
        
        Args:
            base_concurrency: Base/configured concurrency level
            
        Returns:
            Recommended concurrency level
        """
        with self._lock:
            if not self.metrics_history:
                return base_concurrency
            
            # Use average of last 3 metrics
            recent_metrics = self.metrics_history[-3:]
            avg_cpu = sum(m.cpu_percent for m in recent_metrics) / len(recent_metrics)
            avg_memory = sum(m.memory_percent for m in recent_metrics) / len(recent_metrics)
            
            # Determine throttling
            if avg_cpu > self.limits.max_cpu_percent or avg_memory > self.limits.max_memory_percent:
                # Throttle down
                recommended = int(base_concurrency * self.limits.throttle_factor)
                recommended = max(recommended, self.limits.min_concurrent)
                
                if recommended < base_concurrency:
                    logger.info(f"Throttling concurrency: {base_concurrency} -> {recommended} "
                              f"(CPU: {avg_cpu:.1f}%, Memory: {avg_memory:.1f}%)")
                
                return recommended
            
            return base_concurrency


class ConcurrentProcessor:
    """
    Enhanced concurrent processor with resource management.
    
    Features:
    - Dynamic thread pool sizing based on resources
    - Semaphore-based concurrency control
    - Work queue with priority support
    - Comprehensive error handling
    - Progress tracking integration
    """
    
    def __init__(self, 
                 resource_limits: Optional[ResourceLimits] = None,
                 progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None):
        """
        Initialize concurrent processor.
        
        Args:
            resource_limits: Resource limits configuration
            progress_callback: Callback for progress updates
        """
        self.limits = resource_limits or ResourceLimits()
        self.progress_callback = progress_callback
        
        # Resource monitoring
        self.resource_monitor = ResourceMonitor(self.limits)
        
        # Thread pool (will be dynamically sized)
        self.executor: Optional[ThreadPoolExecutor] = None
        self.current_workers = self.limits.max_concurrent_channels
        
        # Concurrency control
        self.channel_semaphore = threading.Semaphore(self.limits.max_concurrent_channels)
        self.download_semaphore = threading.Semaphore(self.limits.max_concurrent_downloads)
        
        # Work queue
        self.work_queue: queue.PriorityQueue = queue.PriorityQueue(maxsize=self.limits.max_queue_size)
        
        # State tracking
        self.active_tasks: Dict[str, Future] = {}
        self.completed_tasks: List[str] = []
        self.failed_tasks: List[Tuple[str, Exception]] = []
        self._lock = threading.RLock()
        
        logger.info("ConcurrentProcessor initialized with dynamic resource management")
    
    def start(self):
        """Start the concurrent processor."""
        # Start resource monitoring
        self.resource_monitor.start_monitoring()
        
        # Create initial thread pool
        self._resize_thread_pool(self.current_workers)
        
        logger.info(f"ConcurrentProcessor started with {self.current_workers} workers")
    
    def stop(self):
        """Stop the concurrent processor."""
        logger.info("Stopping ConcurrentProcessor...")
        
        # Stop resource monitoring
        self.resource_monitor.stop_monitoring()
        
        # Cancel active tasks
        with self._lock:
            for task_id, future in self.active_tasks.items():
                if not future.done():
                    future.cancel()
                    logger.info(f"Cancelled task: {task_id}")
        
        # Shutdown thread pool
        if self.executor:
            self.executor.shutdown(wait=True)
            logger.info("Thread pool shutdown complete")
        
        logger.info("ConcurrentProcessor stopped")
    
    def _resize_thread_pool(self, new_size: int):
        """Resize the thread pool based on resource availability."""
        if self.executor and new_size == self.current_workers:
            return
        
        old_executor = self.executor
        
        # Create new executor with new size
        self.executor = ThreadPoolExecutor(max_workers=new_size)
        self.current_workers = new_size
        
        # Shutdown old executor
        if old_executor:
            old_executor.shutdown(wait=False)
        
        logger.info(f"Thread pool resized to {new_size} workers")
    
    def submit_channel_task(self, 
                          task_id: str,
                          task_func: Callable,
                          *args,
                          priority: int = 5,
                          **kwargs) -> Future:
        """
        Submit a channel processing task.
        
        Args:
            task_id: Unique task identifier
            task_func: Function to execute
            *args: Function arguments
            priority: Task priority (lower = higher priority)
            **kwargs: Function keyword arguments
            
        Returns:
            Future object for the task
        """
        # Check resources and adjust pool size
        metrics = self.resource_monitor.get_current_metrics(self.work_queue.qsize())
        recommended_size = self.resource_monitor.get_recommended_concurrency(
            self.limits.max_concurrent_channels
        )
        
        if recommended_size != self.current_workers:
            self._resize_thread_pool(recommended_size)
        
        # Submit task with semaphore control
        def wrapped_task():
            with self.channel_semaphore:
                try:
                    logger.info(f"Starting task: {task_id}")
                    result = task_func(*args, **kwargs)
                    
                    with self._lock:
                        self.completed_tasks.append(task_id)
                        if task_id in self.active_tasks:
                            del self.active_tasks[task_id]
                    
                    if self.progress_callback:
                        self.progress_callback("task_completed", {
                            "task_id": task_id,
                            "status": "success"
                        })
                    
                    return result
                    
                except Exception as e:
                    logger.error(f"Task failed: {task_id} - {e}")
                    
                    with self._lock:
                        self.failed_tasks.append((task_id, e))
                        if task_id in self.active_tasks:
                            del self.active_tasks[task_id]
                    
                    if self.progress_callback:
                        self.progress_callback("task_failed", {
                            "task_id": task_id,
                            "error": str(e)
                        })
                    
                    raise
        
        # Submit to executor
        future = self.executor.submit(wrapped_task)
        
        with self._lock:
            self.active_tasks[task_id] = future
        
        logger.info(f"Submitted task: {task_id} (priority={priority})")
        return future
    
    def submit_download_task(self,
                           task_id: str,
                           download_func: Callable,
                           *args,
                           **kwargs) -> Future:
        """
        Submit a download task with separate concurrency control.
        
        Args:
            task_id: Unique task identifier
            download_func: Download function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Future object for the download
        """
        def wrapped_download():
            with self.download_semaphore:
                try:
                    logger.info(f"Starting download: {task_id}")
                    result = download_func(*args, **kwargs)
                    
                    if self.progress_callback:
                        self.progress_callback("download_completed", {
                            "task_id": task_id,
                            "status": "success"
                        })
                    
                    return result
                    
                except Exception as e:
                    logger.error(f"Download failed: {task_id} - {e}")
                    
                    if self.progress_callback:
                        self.progress_callback("download_failed", {
                            "task_id": task_id,
                            "error": str(e)
                        })
                    
                    raise
        
        # Submit to executor
        future = self.executor.submit(wrapped_download)
        logger.info(f"Submitted download: {task_id}")
        return future
    
    def wait_for_completion(self, 
                          futures: List[Future],
                          timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Wait for a list of futures to complete.
        
        Args:
            futures: List of Future objects to wait for
            timeout: Optional timeout in seconds
            
        Returns:
            Dictionary with completion statistics
        """
        completed = 0
        failed = 0
        results = []
        
        try:
            for future in as_completed(futures, timeout=timeout):
                try:
                    result = future.result()
                    results.append(result)
                    completed += 1
                except Exception as e:
                    logger.error(f"Task failed during wait: {e}")
                    failed += 1
        
        except TimeoutError:
            logger.error(f"Timeout waiting for {len(futures)} tasks")
            # Cancel remaining tasks
            for future in futures:
                if not future.done():
                    future.cancel()
        
        return {
            "total": len(futures),
            "completed": completed,
            "failed": failed,
            "results": results,
            "success_rate": completed / len(futures) if futures else 0
        }
    
    def get_status(self) -> Dict[str, Any]:
        """Get current processor status."""
        with self._lock:
            metrics = self.resource_monitor.get_current_metrics(self.work_queue.qsize())
            
            return {
                "current_workers": self.current_workers,
                "active_tasks": len(self.active_tasks),
                "completed_tasks": len(self.completed_tasks),
                "failed_tasks": len(self.failed_tasks),
                "queue_size": self.work_queue.qsize(),
                "resource_status": metrics.status.value,
                "cpu_percent": metrics.cpu_percent,
                "memory_percent": metrics.memory_percent,
                "channel_semaphore_available": self.channel_semaphore._value,
                "download_semaphore_available": self.download_semaphore._value
            }


def process_batch_with_resource_management(
        items: List[Tuple[str, Callable, tuple, dict]],
        resource_limits: Optional[ResourceLimits] = None,
        progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
    """
    Process a batch of items with resource management.
    
    Args:
        items: List of (task_id, function, args, kwargs) tuples
        resource_limits: Resource limits configuration
        progress_callback: Progress callback function
        
    Returns:
        Processing results dictionary
    """
    processor = ConcurrentProcessor(resource_limits, progress_callback)
    
    try:
        processor.start()
        
        # Submit all tasks
        futures = []
        for task_id, func, args, kwargs in items:
            future = processor.submit_channel_task(task_id, func, *args, **kwargs)
            futures.append(future)
        
        # Wait for completion
        results = processor.wait_for_completion(futures)
        
        # Get final status
        final_status = processor.get_status()
        results["final_status"] = final_status
        
        return results
        
    finally:
        processor.stop()


# Example usage
if __name__ == "__main__":
    import time
    import random
    
    def example_task(task_id: str, duration: float):
        """Example task that simulates work."""
        logger.info(f"Task {task_id} starting (duration: {duration}s)")
        time.sleep(duration)
        
        # Simulate occasional failures
        if random.random() < 0.1:
            raise RuntimeError(f"Task {task_id} simulated failure")
        
        logger.info(f"Task {task_id} completed")
        return f"Result from {task_id}"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'
    )
    
    # Create tasks
    tasks = [
        (f"task_{i}", example_task, (f"task_{i}", random.uniform(1, 5)), {})
        for i in range(10)
    ]
    
    # Process with resource management
    results = process_batch_with_resource_management(
        tasks,
        resource_limits=ResourceLimits(
            max_concurrent_channels=3,
            max_cpu_percent=70,
            max_memory_percent=70
        )
    )
    
    print(f"\nProcessing Results:")
    print(f"  Total: {results['total']}")
    print(f"  Completed: {results['completed']}")
    print(f"  Failed: {results['failed']}")
    print(f"  Success Rate: {results['success_rate']:.1%}")
    print(f"  Final Status: {results['final_status']}")