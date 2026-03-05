import logging
import os
from datetime import datetime

def get_logger(name="equipment_rental"):
    logger = logging.getLogger(name)

    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)

    # Create year/month/day folder structure
    now = datetime.now()
    log_dir = os.path.join(
        "logs",
        str(now.year),
        f"{now.month:02d}",
        f"{now.day:02d}"
    )
    os.makedirs(log_dir, exist_ok=True)

    # Log file name with timestamp
    log_file = os.path.join(log_dir, f"{now.strftime('%Y%m%d_%H%M%S')}.log")

    # Formatter including filename and line number
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger