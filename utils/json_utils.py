#!/usr/bin/env python3
"""
DRY JSON State Management Utilities
Consolidates JSON file operations and state management patterns.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union
from datetime import datetime

def read_json_safe(file_path: Union[str, Path], default: Any = None) -> Any:
    """
    Safely read JSON file with error handling.
    
    Args:
        file_path: Path to JSON file
        default: Default value if file doesn't exist or is invalid
        
    Returns:
        Parsed JSON data or default value
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default

def write_json_safe(file_path: Union[str, Path], data: Any, indent: int = 2, ensure_dir: bool = True) -> bool:
    """
    Safely write JSON file with error handling.
    
    Args:
        file_path: Path to JSON file
        data: Data to write
        indent: JSON indentation
        ensure_dir: Create parent directory if needed
        
    Returns:
        True if successful, False otherwise
    """
    try:
        file_path = Path(file_path)
        
        if ensure_dir:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
        return True
    except (OSError, TypeError):
        return False

def update_json_state(file_path: Union[str, Path], updates: Dict[str, Any], create_if_missing: bool = True) -> bool:
    """
    Update JSON state file with new data.
    
    Args:
        file_path: Path to JSON state file
        updates: Dictionary of updates to apply
        create_if_missing: Create file if it doesn't exist
        
    Returns:
        True if successful, False otherwise
    """
    current_data = read_json_safe(file_path, {} if create_if_missing else None)
    
    if current_data is None:
        return False
    
    current_data.update(updates)
    return write_json_safe(file_path, current_data)

def create_timestamped_state(base_data: Dict[str, Any], timestamp_key: str = 'timestamp') -> Dict[str, Any]:
    """
    Create state dictionary with timestamp.
    
    Args:
        base_data: Base state data
        timestamp_key: Key for timestamp field
        
    Returns:
        State dictionary with timestamp
    """
    state = base_data.copy()
    state[timestamp_key] = datetime.now().isoformat()
    return state

def load_progress_state(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load progress state with default structure.
    
    Args:
        file_path: Path to progress file
        
    Returns:
        Progress state dictionary
    """
    default_state = {
        'started_at': datetime.now().isoformat(),
        'completed': {},
        'failed': {},
        'skipped': {},
        'total_processed': 0,
        'last_updated': datetime.now().isoformat()
    }
    
    return read_json_safe(file_path, default_state)

def save_progress_state(file_path: Union[str, Path], state: Dict[str, Any]) -> bool:
    """
    Save progress state with updated timestamp.
    
    Args:
        file_path: Path to progress file
        state: Progress state to save
        
    Returns:
        True if successful, False otherwise
    """
    state['last_updated'] = datetime.now().isoformat()
    return write_json_safe(file_path, state)

def create_report_structure(title: str, summary: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Create standardized report structure.
    
    Args:
        title: Report title
        summary: Optional summary data
        
    Returns:
        Report structure dictionary
    """
    return {
        'title': title,
        'generated_at': datetime.now().isoformat(),
        'summary': summary or {},
        'details': [],
        'errors': [],
        'warnings': []
    }

def append_to_json_array(file_path: Union[str, Path], item: Any, max_items: Optional[int] = None) -> bool:
    """
    Append item to JSON array file.
    
    Args:
        file_path: Path to JSON array file
        item: Item to append
        max_items: Maximum items to keep (removes oldest)
        
    Returns:
        True if successful, False otherwise
    """
    current_array = read_json_safe(file_path, [])
    
    if not isinstance(current_array, list):
        current_array = []
    
    current_array.append(item)
    
    if max_items and len(current_array) > max_items:
        current_array = current_array[-max_items:]
    
    return write_json_safe(file_path, current_array)

# ============================================================================
# UNIFIED PROGRESS & STATE TRACKING (DRY ITERATION 2 - Step 3)  
# ============================================================================

from contextlib import contextmanager
from typing import Set, Callable
import threading


class ProgressTracker:
    """
    Unified progress tracking for all download and processing operations (DRY CONSOLIDATION - Step 3).
    
    ELIMINATES DUPLICATION:
    - core/process_pending_metadata_downloads.py:60-75 (progress file management)
    - Multiple ad-hoc progress tracking patterns
    - Inconsistent progress state structures across modules
    
    BUSINESS IMPACT: Prevents progress loss and inconsistent tracking across workflows
    """
    
    def __init__(self, progress_file: Union[str, Path], operation_name: str = "operation"):
        self.progress_file = Path(progress_file)
        self.operation_name = operation_name
        self.lock = threading.Lock()
        self._state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load progress state with default structure."""
        default_state = {
            'operation': self.operation_name,
            'started_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'processed': [],
            'failed': {},
            'skipped': [],
            'stats': {
                'total_items': 0,
                'completed': 0,
                'failed': 0,
                'skipped': 0
            }
        }
        
        existing_state = read_json_safe(self.progress_file, default_state)
        
        # Ensure all required keys exist
        for key, value in default_state.items():
            if key not in existing_state:
                existing_state[key] = value
        
        return existing_state
    
    def _save_state(self) -> None:
        """Save progress state to file."""
        self._state['last_updated'] = datetime.now().isoformat()
        write_json_safe(self.progress_file, self._state)
    
    def mark_processed(self, item_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Mark an item as successfully processed."""
        with self.lock:
            if item_id not in [entry.get('id') for entry in self._state['processed']]:
                entry = {'id': item_id, 'timestamp': datetime.now().isoformat()}
                if metadata:
                    entry.update(metadata)
                
                self._state['processed'].append(entry)
                self._state['stats']['completed'] += 1
                self._save_state()
    
    def mark_failed(self, item_id: str, error_message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Mark an item as failed."""
        with self.lock:
            failure_entry = {
                'error': error_message,
                'timestamp': datetime.now().isoformat()
            }
            if metadata:
                failure_entry.update(metadata)
            
            self._state['failed'][item_id] = failure_entry
            self._state['stats']['failed'] += 1
            self._save_state()
    
    def mark_skipped(self, item_id: str, reason: str) -> None:
        """Mark an item as skipped."""
        with self.lock:
            if item_id not in [entry.get('id') for entry in self._state['skipped']]:
                self._state['skipped'].append({
                    'id': item_id,
                    'reason': reason,
                    'timestamp': datetime.now().isoformat()
                })
                self._state['stats']['skipped'] += 1
                self._save_state()
    
    def is_processed(self, item_id: str) -> bool:
        """Check if an item has been processed."""
        return any(entry.get('id') == item_id for entry in self._state['processed'])
    
    def is_failed(self, item_id: str) -> bool:
        """Check if an item has failed."""
        return item_id in self._state['failed']
    
    def is_skipped(self, item_id: str) -> bool:
        """Check if an item was skipped."""
        return any(entry.get('id') == item_id for entry in self._state['skipped'])
    
    def get_summary(self) -> Dict[str, Any]:
        """Get progress summary."""
        return {
            'operation': self._state['operation'],
            'started_at': self._state['started_at'],
            'last_updated': self._state['last_updated'],
            'stats': self._state['stats'].copy(),
            'total_processed': len(self._state['processed']),
            'total_failed': len(self._state['failed']),
            'total_skipped': len(self._state['skipped'])
        }


class StateManager:
    """
    Unified state management for complex workflows (DRY CONSOLIDATION - Step 3).
    
    ELIMINATES PATTERNS FROM:
    - Multiple JSON state files with inconsistent structure
    - Manual state loading/saving throughout codebase  
    - Different error handling approaches for state files
    
    BUSINESS IMPACT: Prevents state corruption and provides consistent workflow recovery
    """
    
    def __init__(self, state_file: Union[str, Path], default_state: Optional[Dict] = None):
        self.state_file = Path(state_file)
        self.default_state = default_state or {}
        self.lock = threading.Lock()
        self._state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load state with validation and recovery."""
        base_state = {
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'version': '1.0',
            'workflow_stage': 'initialized'
        }
        base_state.update(self.default_state)
        
        if not self.state_file.exists():
            return base_state
        
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                loaded_state = json.load(f)
            
            # Validate loaded state
            if not isinstance(loaded_state, dict):
                raise ValueError("State file does not contain a valid dictionary")
            
            # Merge with base state to ensure required keys exist
            merged_state = base_state.copy()
            merged_state.update(loaded_state)
            
            return merged_state
            
        except Exception as e:
            # Create backup of corrupted state
            if self.state_file.exists():
                backup_path = f"{self.state_file}.corrupt.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                try:
                    os.rename(self.state_file, backup_path)
                except OSError:
                    pass
            
            return base_state
    
    def save_state(self, updates: Optional[Dict[str, Any]] = None) -> bool:
        """Save current state with optional updates."""
        with self.lock:
            if updates:
                self._state.update(updates)
            
            self._state['last_updated'] = datetime.now().isoformat()
            
            return write_json_safe(self.state_file, self._state)
    
    def get_state(self, key: Optional[str] = None) -> Any:
        """Get state value or entire state."""
        if key is None:
            return self._state.copy()
        return self._state.get(key)
    
    def set_state(self, key: str, value: Any, save: bool = True) -> bool:
        """Set state value."""
        with self.lock:
            self._state[key] = value
            if save:
                return self.save_state()
        return True
    
    def update_workflow_stage(self, stage: str, metadata: Optional[Dict] = None) -> bool:
        """Update workflow stage with optional metadata."""
        updates = {'workflow_stage': stage}
        if metadata:
            updates[f'{stage}_metadata'] = metadata
        return self.save_state(updates)
    
    @contextmanager
    def atomic_update(self):
        """Context manager for atomic state updates."""
        with self.lock:
            original_state = self._state.copy()
            try:
                yield self._state
                self.save_state()
            except Exception:
                self._state = original_state
                raise


class BatchProgressTracker:
    """
    Progress tracking for batch operations (DRY CONSOLIDATION - Step 3).
    
    CONSOLIDATES BATCH TRACKING PATTERNS:
    - S3 upload batch tracking
    - CSV processing progress  
    - Download batch operations
    
    BUSINESS IMPACT: Provides consistent batch operation monitoring and recovery
    """
    
    def __init__(self, operation_name: str, total_items: int, 
                 progress_file: Optional[Union[str, Path]] = None,
                 callback: Optional[Callable] = None):
        self.operation_name = operation_name
        self.total_items = total_items
        self.callback = callback
        self.start_time = datetime.now()
        
        # Use progress tracker if file specified
        if progress_file:
            self.progress_tracker = ProgressTracker(progress_file, operation_name)
        else:
            self.progress_tracker = None
        
        self.stats = {
            'completed': 0,
            'failed': 0,
            'skipped': 0,
            'current_item': None
        }
    
    def start_item(self, item_id: str) -> None:
        """Start processing an item."""
        self.stats['current_item'] = item_id
        if self.callback:
            self.callback('start', item_id, self.get_progress_summary())
    
    def complete_item(self, item_id: str, metadata: Optional[Dict] = None) -> None:
        """Mark item as completed."""
        self.stats['completed'] += 1
        if self.progress_tracker:
            self.progress_tracker.mark_processed(item_id, metadata)
        
        if self.callback:
            self.callback('complete', item_id, self.get_progress_summary())
    
    def fail_item(self, item_id: str, error: str, metadata: Optional[Dict] = None) -> None:
        """Mark item as failed."""
        self.stats['failed'] += 1
        if self.progress_tracker:
            self.progress_tracker.mark_failed(item_id, error, metadata)
        
        if self.callback:
            self.callback('failed', item_id, self.get_progress_summary())
    
    def skip_item(self, item_id: str, reason: str) -> None:
        """Mark item as skipped."""
        self.stats['skipped'] += 1
        if self.progress_tracker:
            self.progress_tracker.mark_skipped(item_id, reason)
        
        if self.callback:
            self.callback('skipped', item_id, self.get_progress_summary())
    
    def get_progress_summary(self) -> Dict[str, Any]:
        """Get progress summary."""
        processed = self.stats['completed'] + self.stats['failed'] + self.stats['skipped']
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        return {
            'operation': self.operation_name,
            'total_items': self.total_items,
            'completed': self.stats['completed'],
            'failed': self.stats['failed'],
            'skipped': self.stats['skipped'],
            'processed': processed,
            'remaining': self.total_items - processed,
            'progress_percent': (processed / self.total_items * 100) if self.total_items > 0 else 0,
            'elapsed_seconds': elapsed,
            'estimated_remaining_seconds': (elapsed / processed * (self.total_items - processed)) if processed > 0 else None,
            'current_item': self.stats['current_item']
        }
EOF < /dev/null
