#!/usr/bin/env python3
"""
Progress Monitoring and Reporting Module
Phase 5.7: Add progress monitoring and reporting

This module provides real-time progress monitoring and reporting capabilities
for the mass download feature, including:
- Real-time progress updates
- ETA calculations
- Performance metrics
- Progress visualization
- Detailed statistics reporting
"""
import time
import threading
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import json
from pathlib import Path

# Import logging
import logging

logger = logging.getLogger(__name__)

# Simple operation logger creator (inline implementation)
def create_operation_logger(operation_name):
    """Create a logger for a specific operation."""
    return logging.getLogger(f"mass_download.{operation_name}")


class ProgressState(Enum):
    """Progress states for monitoring."""
    NOT_STARTED = "not_started"
    INITIALIZING = "initializing"
    PROCESSING = "processing"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ProgressMetrics:
    """Detailed progress metrics."""
    # Overall metrics
    total_channels: int = 0
    channels_processed: int = 0
    channels_failed: int = 0
    channels_skipped: int = 0
    
    # Video metrics
    total_videos: int = 0
    videos_downloaded: int = 0
    videos_failed: int = 0
    videos_skipped: int = 0
    
    # Performance metrics
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    bytes_downloaded: int = 0
    average_speed_mbps: float = 0.0
    
    # Current operation
    current_channel: Optional[str] = None
    current_video: Optional[str] = None
    current_operation: str = "idle"
    
    def get_elapsed_time(self) -> timedelta:
        """Get elapsed time since start."""
        if not self.start_time:
            return timedelta(0)
        end = self.end_time or datetime.now()
        return end - self.start_time
    
    def get_eta(self) -> Optional[timedelta]:
        """Calculate estimated time to completion."""
        if self.channels_processed == 0 or self.total_channels == 0:
            return None
        
        elapsed = self.get_elapsed_time().total_seconds()
        rate = self.channels_processed / elapsed
        remaining = self.total_channels - self.channels_processed
        
        if rate > 0:
            eta_seconds = remaining / rate
            return timedelta(seconds=int(eta_seconds))
        return None
    
    def get_progress_percent(self) -> float:
        """Get overall progress percentage."""
        if self.total_channels == 0:
            return 0.0
        return (self.channels_processed / self.total_channels) * 100


@dataclass
class ChannelProgress:
    """Progress tracking for individual channel."""
    channel_url: str
    channel_name: Optional[str] = None
    total_videos: int = 0
    videos_processed: int = 0
    videos_failed: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: str = "pending"
    error_message: Optional[str] = None


class ProgressMonitor:
    """
    Real-time progress monitoring for mass downloads.
    
    Features:
    - Real-time progress updates
    - Performance metrics calculation
    - Progress persistence
    - Event callbacks
    - Terminal-friendly display
    """
    
    def __init__(self, 
                 update_interval: float = 1.0,
                 persist_interval: float = 10.0,
                 progress_file: Optional[Path] = None):
        """
        Initialize progress monitor.
        
        Args:
            update_interval: Seconds between display updates
            persist_interval: Seconds between progress saves
            progress_file: Path to save progress (optional)
        """
        self.update_interval = update_interval
        self.persist_interval = persist_interval
        self.progress_file = progress_file or Path("mass_download_progress.json")
        
        # Progress tracking
        self.metrics = ProgressMetrics()
        self.channel_progress: Dict[str, ChannelProgress] = {}
        self.state = ProgressState.NOT_STARTED
        
        # Threading
        self._lock = threading.Lock()
        self._update_thread: Optional[threading.Thread] = None
        self._persist_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # Callbacks
        self._callbacks: List[Callable[[ProgressMetrics], None]] = []
        
        # Performance tracking
        self._last_update_time = time.time()
        self._last_bytes = 0
        
        logger.info("ProgressMonitor initialized")
    
    def start(self):
        """Start progress monitoring."""
        with self._lock:
            if self.state != ProgressState.NOT_STARTED:
                logger.warning(f"Cannot start monitor in state: {self.state}")
                return
            
            self.state = ProgressState.INITIALIZING
            self.metrics.start_time = datetime.now()
            
            # Start update thread
            self._stop_event.clear()
            self._update_thread = threading.Thread(
                target=self._update_loop,
                name="ProgressUpdateThread"
            )
            self._update_thread.daemon = True
            self._update_thread.start()
            
            # Start persistence thread
            self._persist_thread = threading.Thread(
                target=self._persist_loop,
                name="ProgressPersistThread"
            )
            self._persist_thread.daemon = True
            self._persist_thread.start()
            
            self.state = ProgressState.PROCESSING
            logger.info("Progress monitoring started")
    
    def stop(self):
        """Stop progress monitoring."""
        with self._lock:
            if self.state not in [ProgressState.PROCESSING, ProgressState.PAUSED]:
                return
            
            self.state = ProgressState.COMPLETED
            self.metrics.end_time = datetime.now()
        
        # Stop threads
        self._stop_event.set()
        if self._update_thread:
            self._update_thread.join(timeout=2)
        if self._persist_thread:
            self._persist_thread.join(timeout=2)
        
        # Final save
        self._save_progress()
        
        logger.info("Progress monitoring stopped")
    
    def pause(self):
        """Pause progress monitoring."""
        with self._lock:
            if self.state == ProgressState.PROCESSING:
                self.state = ProgressState.PAUSED
                logger.info("Progress monitoring paused")
    
    def resume(self):
        """Resume progress monitoring."""
        with self._lock:
            if self.state == ProgressState.PAUSED:
                self.state = ProgressState.PROCESSING
                logger.info("Progress monitoring resumed")
    
    def update_channel_count(self, total: int):
        """Update total channel count."""
        with self._lock:
            self.metrics.total_channels = total
    
    def start_channel(self, channel_url: str, channel_name: Optional[str] = None):
        """Mark channel processing start."""
        with self._lock:
            self.metrics.current_channel = channel_url
            self.metrics.current_operation = f"Processing {channel_name or channel_url}"
            
            # Create channel progress entry
            progress = ChannelProgress(
                channel_url=channel_url,
                channel_name=channel_name,
                start_time=datetime.now(),
                status="processing"
            )
            self.channel_progress[channel_url] = progress
            
            logger.info(f"Started processing channel: {channel_url}")
    
    def update_channel_videos(self, channel_url: str, total_videos: int):
        """Update total video count for channel."""
        with self._lock:
            if channel_url in self.channel_progress:
                self.channel_progress[channel_url].total_videos = total_videos
                self.metrics.total_videos += total_videos
    
    def complete_channel(self, channel_url: str, success: bool = True, 
                        error_message: Optional[str] = None):
        """Mark channel processing complete."""
        with self._lock:
            if channel_url in self.channel_progress:
                progress = self.channel_progress[channel_url]
                progress.end_time = datetime.now()
                progress.status = "completed" if success else "failed"
                progress.error_message = error_message
                
                self.metrics.channels_processed += 1
                if not success:
                    self.metrics.channels_failed += 1
                
                if self.metrics.current_channel == channel_url:
                    self.metrics.current_channel = None
                    self.metrics.current_operation = "idle"
            
            logger.info(f"Completed channel: {channel_url} (success={success})")
    
    def update_video_progress(self, video_id: str, video_title: str, 
                            downloaded: bool = True, failed: bool = False):
        """Update video download progress."""
        with self._lock:
            self.metrics.current_video = video_title
            
            if downloaded:
                self.metrics.videos_downloaded += 1
            elif failed:
                self.metrics.videos_failed += 1
            else:
                self.metrics.videos_skipped += 1
            
            # Update channel progress
            if self.metrics.current_channel:
                channel_url = self.metrics.current_channel
                if channel_url in self.channel_progress:
                    self.channel_progress[channel_url].videos_processed += 1
                    if failed:
                        self.channel_progress[channel_url].videos_failed += 1
    
    def update_download_stats(self, bytes_downloaded: int):
        """Update download statistics."""
        with self._lock:
            self.metrics.bytes_downloaded += bytes_downloaded
            
            # Calculate speed
            current_time = time.time()
            time_diff = current_time - self._last_update_time
            if time_diff > 0:
                bytes_diff = self.metrics.bytes_downloaded - self._last_bytes
                speed_bps = bytes_diff / time_diff
                self.metrics.average_speed_mbps = (speed_bps * 8) / 1_000_000  # Convert to Mbps
                
                self._last_update_time = current_time
                self._last_bytes = self.metrics.bytes_downloaded
    
    def add_callback(self, callback: Callable[[ProgressMetrics], None]):
        """Add a progress update callback."""
        self._callbacks.append(callback)
    
    def get_current_metrics(self) -> ProgressMetrics:
        """Get current progress metrics."""
        with self._lock:
            # Create a copy to avoid threading issues
            import copy
            return copy.deepcopy(self.metrics)
    
    def get_summary_report(self) -> Dict[str, Any]:
        """Generate a summary report."""
        with self._lock:
            elapsed = self.metrics.get_elapsed_time()
            eta = self.metrics.get_eta()
            
            report = {
                "state": self.state.value,
                "overall_progress": {
                    "percent": self.metrics.get_progress_percent(),
                    "channels": {
                        "total": self.metrics.total_channels,
                        "processed": self.metrics.channels_processed,
                        "failed": self.metrics.channels_failed,
                        "skipped": self.metrics.channels_skipped
                    },
                    "videos": {
                        "total": self.metrics.total_videos,
                        "downloaded": self.metrics.videos_downloaded,
                        "failed": self.metrics.videos_failed,
                        "skipped": self.metrics.videos_skipped
                    }
                },
                "performance": {
                    "elapsed_time": str(elapsed),
                    "eta": str(eta) if eta else None,
                    "average_speed_mbps": round(self.metrics.average_speed_mbps, 2),
                    "total_downloaded_gb": round(self.metrics.bytes_downloaded / 1_073_741_824, 2)
                },
                "current_operation": {
                    "channel": self.metrics.current_channel,
                    "video": self.metrics.current_video,
                    "operation": self.metrics.current_operation
                }
            }
            
            return report
    
    def print_progress(self):
        """Print formatted progress to console."""
        report = self.get_summary_report()
        
        # Clear line and print progress
        print("\r" + " " * 100 + "\r", end="")  # Clear line
        
        # Format progress bar
        progress = report["overall_progress"]["percent"]
        bar_width = 30
        filled = int(bar_width * progress / 100)
        bar = "█" * filled + "░" * (bar_width - filled)
        
        # Build status line
        channels = report["overall_progress"]["channels"]
        status = (
            f"[{bar}] {progress:.1f}% | "
            f"Channels: {channels['processed']}/{channels['total']} | "
            f"Speed: {report['performance']['average_speed_mbps']:.1f} Mbps"
        )
        
        if report["performance"]["eta"]:
            status += f" | ETA: {report['performance']['eta']}"
        
        print(status, end="", flush=True)
    
    def _update_loop(self):
        """Background thread for progress updates."""
        while not self._stop_event.is_set():
            if self.state == ProgressState.PROCESSING:
                # Call callbacks
                metrics = self.get_current_metrics()
                for callback in self._callbacks:
                    try:
                        callback(metrics)
                    except Exception as e:
                        logger.error(f"Error in progress callback: {e}")
                
                # Print progress
                self.print_progress()
            
            self._stop_event.wait(self.update_interval)
    
    def _persist_loop(self):
        """Background thread for progress persistence."""
        while not self._stop_event.is_set():
            if self.state in [ProgressState.PROCESSING, ProgressState.PAUSED]:
                self._save_progress()
            
            self._stop_event.wait(self.persist_interval)
    
    def _save_progress(self):
        """Save current progress to file."""
        try:
            with self._lock:
                progress_data = {
                    "timestamp": datetime.now().isoformat(),
                    "state": self.state.value,
                    "metrics": {
                        "total_channels": self.metrics.total_channels,
                        "channels_processed": self.metrics.channels_processed,
                        "channels_failed": self.metrics.channels_failed,
                        "total_videos": self.metrics.total_videos,
                        "videos_downloaded": self.metrics.videos_downloaded,
                        "videos_failed": self.metrics.videos_failed,
                        "bytes_downloaded": self.metrics.bytes_downloaded,
                        "start_time": self.metrics.start_time.isoformat() if self.metrics.start_time else None
                    },
                    "channel_progress": {
                        url: {
                            "name": cp.channel_name,
                            "total_videos": cp.total_videos,
                            "videos_processed": cp.videos_processed,
                            "status": cp.status
                        }
                        for url, cp in self.channel_progress.items()
                    }
                }
            
            # Write to temp file first
            temp_file = self.progress_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
            
            # Atomic rename
            temp_file.replace(self.progress_file)
            
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")
    
    def load_progress(self) -> bool:
        """Load progress from file."""
        if not self.progress_file.exists():
            return False
        
        try:
            with open(self.progress_file, 'r') as f:
                data = json.load(f)
            
            with self._lock:
                # Restore metrics
                metrics = data.get("metrics", {})
                self.metrics.total_channels = metrics.get("total_channels", 0)
                self.metrics.channels_processed = metrics.get("channels_processed", 0)
                self.metrics.channels_failed = metrics.get("channels_failed", 0)
                self.metrics.total_videos = metrics.get("total_videos", 0)
                self.metrics.videos_downloaded = metrics.get("videos_downloaded", 0)
                self.metrics.videos_failed = metrics.get("videos_failed", 0)
                self.metrics.bytes_downloaded = metrics.get("bytes_downloaded", 0)
                
                if metrics.get("start_time"):
                    self.metrics.start_time = datetime.fromisoformat(metrics["start_time"])
                
                # Restore channel progress
                for url, cp_data in data.get("channel_progress", {}).items():
                    self.channel_progress[url] = ChannelProgress(
                        channel_url=url,
                        channel_name=cp_data.get("name"),
                        total_videos=cp_data.get("total_videos", 0),
                        videos_processed=cp_data.get("videos_processed", 0),
                        status=cp_data.get("status", "pending")
                    )
            
            logger.info("Progress loaded from file")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load progress: {e}")
            return False


class ProgressReporter:
    """
    Generate detailed progress reports.
    """
    
    def __init__(self, monitor: ProgressMonitor):
        self.monitor = monitor
    
    def generate_text_report(self) -> str:
        """Generate a detailed text report."""
        report = self.monitor.get_summary_report()
        metrics = self.monitor.get_current_metrics()
        
        lines = [
            "Mass Download Progress Report",
            "=" * 80,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Status: {report['state']}",
            "",
            "Overall Progress",
            "-" * 40,
            f"Progress: {report['overall_progress']['percent']:.1f}%",
            f"Channels: {report['overall_progress']['channels']['processed']}/{report['overall_progress']['channels']['total']}",
            f"  - Failed: {report['overall_progress']['channels']['failed']}",
            f"  - Skipped: {report['overall_progress']['channels']['skipped']}",
            f"Videos: {report['overall_progress']['videos']['downloaded']}/{report['overall_progress']['videos']['total']}",
            f"  - Failed: {report['overall_progress']['videos']['failed']}",
            f"  - Skipped: {report['overall_progress']['videos']['skipped']}",
            "",
            "Performance Metrics",
            "-" * 40,
            f"Elapsed Time: {report['performance']['elapsed_time']}",
            f"ETA: {report['performance']['eta'] or 'N/A'}",
            f"Average Speed: {report['performance']['average_speed_mbps']:.2f} Mbps",
            f"Total Downloaded: {report['performance']['total_downloaded_gb']:.2f} GB",
            "",
            "Current Operation",
            "-" * 40,
            f"Channel: {report['current_operation']['channel'] or 'None'}",
            f"Video: {report['current_operation']['video'] or 'None'}",
            f"Operation: {report['current_operation']['operation']}",
            "",
            "Channel Details",
            "-" * 40
        ]
        
        # Add channel details
        for url, progress in self.monitor.channel_progress.items():
            lines.append(f"\n{progress.channel_name or url}")
            lines.append(f"  Status: {progress.status}")
            lines.append(f"  Videos: {progress.videos_processed}/{progress.total_videos}")
            if progress.videos_failed > 0:
                lines.append(f"  Failed: {progress.videos_failed}")
            if progress.error_message:
                lines.append(f"  Error: {progress.error_message}")
        
        return "\n".join(lines)
    
    def save_report(self, filepath: Path):
        """Save report to file."""
        report = self.generate_text_report()
        with open(filepath, 'w') as f:
            f.write(report)
        logger.info(f"Progress report saved to: {filepath}")


# Example usage
if __name__ == "__main__":
    # Create progress monitor
    monitor = ProgressMonitor()
    
    # Add a callback
    def on_progress(metrics: ProgressMetrics):
        print(f"\nCallback: {metrics.get_progress_percent():.1f}% complete")
    
    monitor.add_callback(on_progress)
    
    # Start monitoring
    monitor.start()
    
    # Simulate progress
    monitor.update_channel_count(5)
    
    for i in range(5):
        channel = f"https://youtube.com/@channel{i}"
        monitor.start_channel(channel, f"Channel {i}")
        monitor.update_channel_videos(channel, 10)
        
        for j in range(10):
            time.sleep(0.1)
            monitor.update_video_progress(f"video_{i}_{j}", f"Video {j}", downloaded=True)
            monitor.update_download_stats(1024 * 1024 * 10)  # 10MB
        
        monitor.complete_channel(channel)
    
    # Stop monitoring
    monitor.stop()
    
    # Generate report
    reporter = ProgressReporter(monitor)
    print("\n\nFinal Report:")
    print(reporter.generate_text_report())