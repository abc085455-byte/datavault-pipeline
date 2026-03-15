"""
DataVault Pipeline - Logger
Poore project mein ek jaisi logging ke liye.
"""

import logging
import os
import sys
from datetime import datetime


def get_logger(name: str) -> logging.Logger:
    """
    Logger banao aur return karo.
    
    Usage:
        from scripts.utils.logger import get_logger
        log = get_logger(__name__)
        log.info("Kaam shuru hua")
    """
    logger = logging.getLogger(name)

    # Agar already configured hai to dobara mat karo
    if logger.handlers:
        return logger

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))

    # Console handler - terminal mein dikhaye
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level, logging.INFO))

    # Format - time | level | file | message
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler - logs/ folder mein save kare
    log_dir = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
    os.makedirs(log_dir, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"datavault_{today}.log")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger