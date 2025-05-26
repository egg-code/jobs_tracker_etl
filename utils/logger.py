import logging
from datetime import datetime
import os

def get_module_logger(module_name: str, group: str = None):
    """Create a logger for the specified module and group."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_dir = os.path.join('logs', group)
    os.makedirs(log_dir, exist_ok=True)

    logfile_name = os.path.join(log_dir, f"{module_name}_{group}_{timestamp}.log" if group else f"{module_name}_{timestamp}.log")
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.FileHandler(logfile_name)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger