#!/usr/bin/env python3
"""
Logging configuration for SyncStock system.
Import this module to get consistent logging setup across all scripts.
"""

import logging
import sys
from typing import Optional

def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    date_format: str = "%H:%M:%S",
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    Set up logging configuration for SyncStock.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Custom format string (uses default if None)
        date_format: Date format for timestamps
        log_file: Optional file path for logging to file
    
    Returns:
        Configured logger instance
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Default format if none provided
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure root logging
    logging.basicConfig(
        level=numeric_level,
        format=format_string,
        datefmt=date_format,
        handlers=[]
    )
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_formatter = logging.Formatter(format_string, date_format)
    console_handler.setFormatter(console_formatter)
    
    # Add file handler if specified
    handlers = [console_handler]
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_formatter = logging.Formatter(format_string, date_format)
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()  # Remove any existing handlers
    for handler in handlers:
        root_logger.addHandler(handler)
    
    # Set specific logger levels for verbose modules
    logging.getLogger('db').setLevel(numeric_level)
    logging.getLogger('query').setLevel(numeric_level)
    logging.getLogger('syncstock').setLevel(numeric_level)
    logging.getLogger('webhook_server').setLevel(numeric_level)
    
    # Create and return logger for the calling module
    caller_name = __name__ if __name__ != '__main__' else 'main'
    logger = logging.getLogger(caller_name)
    
    logger.info(f"Logging configured - Level: {level}, Format: {format_string}")
    if log_file:
        logger.info(f"Logging to file: {log_file}")
    
    return logger

def set_log_level(level: str):
    """Quick function to change log level after setup."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.getLogger().setLevel(numeric_level)
    logging.info(f"Log level changed to: {level}")

# Example usage:
if __name__ == "__main__":
    # Set up logging with DEBUG level
    logger = setup_logging(level="DEBUG")
    
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    
    # Change log level
    set_log_level("INFO")
    logger.debug("This debug message won't show")
    logger.info("This info message will show")
