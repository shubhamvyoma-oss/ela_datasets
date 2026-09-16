"""
edmingle_logger.py

One job: give every run a timestamped logger that writes to both a log
file and stdout (so it shows up live in tmux).
"""

import logging
import sys
from pathlib import Path


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("edmingle_export")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger
