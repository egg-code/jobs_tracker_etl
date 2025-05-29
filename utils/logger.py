import logging
from datetime import datetime
import os
import sys

def get_module_logger(module_name: str, group: str = None, log_dir: str = 'logs'):
    """Create a logger for the specified module and group."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_dir = os.path.join(log_dir, group)
    os.makedirs(log_dir, exist_ok=True)

    logfile_name = os.path.join(log_dir, f"{module_name}_{group}_{timestamp}.log" if group else f"{module_name}_{timestamp}.log")
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        return logger

    ## File Handler
    file_handler = logging.FileHandler(logfile_name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    ## Stream Handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(stream_formatter)
    logger.addHandler(stream_handler)

    return logger