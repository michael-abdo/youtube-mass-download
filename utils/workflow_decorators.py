#!/usr/bin/env python3
"""
DRY Workflow Step Decorators
Provides decorators for workflow steps with consistent logging, timing, and error handling.
"""

import functools
import time
from typing import Any, Callable, Optional, Dict
from datetime import datetime

# Import formatting utilities
from .error_formatting import (
    format_error_message,
    format_success_message,
    format_processing_message,
    format_completion_message
)

def workflow_step(step_name: str, log_args: bool = False, measure_time: bool = True):
    """
    Decorator for workflow steps with consistent logging and timing.
    
    Args:
        step_name: Name of the workflow step
        log_args: Whether to log function arguments
        measure_time: Whether to measure execution time
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Get logger from first arg if it's a class method
            logger = None
            if args and hasattr(args[0], 'logger'):
                logger = args[0].logger
            
            # Log step start
            start_msg = f"Starting {step_name}"
            if log_args and (args[1:] or kwargs):
                arg_str = ', '.join([str(a) for a in args[1:]])
                kwarg_str = ', '.join([f"{k}={v}" for k, v in kwargs.items()])
                all_args = ', '.join(filter(None, [arg_str, kwarg_str]))
                if all_args:
                    start_msg += f" with args: {all_args}"
            
            if logger:
                logger.info(format_processing_message(step_name, "Starting"))
            else:
                print(format_processing_message(step_name, "Starting"))
            
            # Measure execution time
            start_time = time.time() if measure_time else None
            
            try:
                # Execute the function
                result = func(*args, **kwargs)
                
                # Log success
                duration = time.time() - start_time if start_time else None
                success_msg = format_success_message(f"{step_name} completed")
                if duration:
                    success_msg += f" in {duration:.2f}s"
                
                if logger:
                    logger.info(success_msg)
                else:
                    print(success_msg)
                
                return result
                
            except Exception as e:
                # Log failure
                error_msg = format_error_message(e, f"{step_name} failed")
                
                if logger:
                    logger.error(error_msg)
                else:
                    print(error_msg)
                
                # Re-raise the exception
                raise
        
        return wrapper
    return decorator

def pipeline_step(step_number: int, total_steps: int, description: str):
    """
    Decorator for pipeline steps with progress tracking.
    
    Args:
        step_number: Current step number
        total_steps: Total number of steps
        description: Step description
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Get logger from first arg if it's a class method
            logger = None
            if args and hasattr(args[0], 'logger'):
                logger = args[0].logger
            
            # Log step progress
            progress_msg = f"[Step {step_number}/{total_steps}] {description}"
            
            if logger:
                logger.info(f"\n{'='*60}")
                logger.info(progress_msg)
                logger.info('='*60)
            else:
                print(f"\n{'='*60}")
                print(progress_msg)
                print('='*60)
            
            # Execute with timing
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                
                # Log completion
                duration = time.time() - start_time
                completion_msg = f"✅ Step {step_number} completed in {duration:.2f}s"
                
                if logger:
                    logger.info(completion_msg)
                else:
                    print(completion_msg)
                
                return result
                
            except Exception as e:
                # Log failure with step context
                duration = time.time() - start_time
                error_msg = f"❌ Step {step_number} failed after {duration:.2f}s: {str(e)}"
                
                if logger:
                    logger.error(error_msg)
                else:
                    print(error_msg)
                
                raise
        
        return wrapper
    return decorator

def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator to retry workflow steps on failure.
    
    This decorator now delegates to the centralized retry_utils module
    for consistent retry behavior across the codebase.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier for delay (kept for compatibility)
        
    Returns:
        Decorated function
    """
    # Import centralized retry utilities
    from .retry_utils import retry_with_backoff
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Extract logger from args for compatibility
            logger = None
            if args and hasattr(args[0], 'logger'):
                logger = args[0].logger
            
            # Delegate to centralized retry implementation
            # Note: retry_with_backoff expects max_attempts = max_retries + 1
            retry_decorator = retry_with_backoff(
                max_attempts=max_retries + 1,
                base_delay=delay,
                exceptions=(Exception,),
                logger=logger
            )
            
            # Apply the centralized retry decorator
            retried_func = retry_decorator(func)
            return retried_func(*args, **kwargs)
        
        return wrapper
    return decorator

def validate_inputs(**validators: Dict[str, Callable]):
    """
    Decorator to validate function inputs before execution.
    
    Args:
        validators: Dict mapping parameter names to validation functions
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Get function signature
            import inspect
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            # Validate each parameter
            for param_name, validator in validators.items():
                if param_name in bound_args.arguments:
                    value = bound_args.arguments[param_name]
                    try:
                        if not validator(value):
                            raise ValueError(f"Validation failed for parameter '{param_name}' with value '{value}'")
                    except Exception as e:
                        raise ValueError(f"Validation error for parameter '{param_name}': {str(e)}")
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator

def track_progress(total_items_param: str = 'items'):
    """
    Decorator to track progress for batch processing functions.
    
    Args:
        total_items_param: Parameter name containing items to process
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Get logger
            logger = None
            if args and hasattr(args[0], 'logger'):
                logger = args[0].logger
            
            # Get items to process
            import inspect
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            items = bound_args.arguments.get(total_items_param, [])
            total = len(items) if hasattr(items, '__len__') else 0
            
            if total > 0:
                msg = f"Processing {total} items in {func.__name__}"
                if logger:
                    logger.info(msg)
                else:
                    print(msg)
            
            # Execute function
            start_time = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            
            # Log completion
            if total > 0:
                rate = total / duration if duration > 0 else 0
                msg = format_completion_message(func.__name__, total, duration)
                msg += f" ({rate:.1f} items/s)"
                
                if logger:
                    logger.info(msg)
                else:
                    print(msg)
            
            return result
        
        return wrapper
    return decorator