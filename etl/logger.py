import logging
import os
from datetime import datetime


def setup_logger():
    """
    Set up a single logger that writes to etl/log.txt
    This should only be called once when the application starts.
    """
    # define where to store the log file
    project_root = os.path.dirname(os.path.dirname(__file__))  # go up one level
    log_dir = os.path.join(project_root, "etl_log")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "log.txt")
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    root_logger.handlers.clear()
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # File handler - writes to etl/log.txt
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    return log_file


def get_logger(name):
    """
    Get a logger for a specific module.
    Args:
        name: Module name (use __name__
    Returns:
        logging.Logger: Configured logger
        
    """
    return logging.getLogger(name)

LOG_FILE = setup_logger()